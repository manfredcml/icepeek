use anyhow::{bail, Context, Result};
use iceberg::io::FileIO;

use super::file_io::{build_file_io, StorageConfig};
use super::TableHandle;

/// Load an Iceberg table by resolving its metadata directly from storage.
///
/// Works uniformly across all storage backends (local FS, S3)
/// by delegating byte I/O to `FileIO`.
///
/// Auto-discovery logic:
/// 1. If path ends in `.json` → use directly as metadata file
/// 2. Try `{path}/metadata/version-hint.text` → read version → `v{N}.metadata.json`
/// 3. (Local FS only) Scan `metadata/` for highest-numbered `v*.metadata.json`
pub async fn load_direct(path: &str, config: &StorageConfig) -> Result<TableHandle> {
    let path = &normalize_local_path(path);
    let file_io = build_file_io(path, config)?;
    let metadata_location = resolve_metadata_path(path, &file_io)
        .await
        .context("failed to locate metadata file")?;

    let input = file_io
        .new_input(&metadata_location)
        .context("failed to create input for metadata")?;
    let bytes = input
        .read()
        .await
        .with_context(|| format!("failed to read metadata from: {}", metadata_location))?;

    let table_metadata: iceberg::spec::TableMetadata = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse metadata JSON: {}", metadata_location))?;

    let table = iceberg::table::Table::builder()
        .metadata(table_metadata)
        .identifier(iceberg::TableIdent::from_strs(["default", "table"])?)
        .file_io(file_io)
        .metadata_location(metadata_location)
        .build()?;

    Ok(TableHandle::new(table))
}

async fn resolve_metadata_path(path: &str, file_io: &FileIO) -> Result<String> {
    if path.ends_with(".json") {
        return Ok(path.to_string());
    }

    let base = path.trim_end_matches('/');

    // Try version-hint.text first (works on all backends)
    let hint_path = format!("{}/metadata/version-hint.text", base);
    if let Ok(input) = file_io.new_input(&hint_path) {
        if let Ok(bytes) = input.read().await {
            let hint = String::from_utf8(bytes.to_vec())
                .context("version-hint.text is not valid UTF-8")?;
            let version = hint.trim();
            return Ok(format!("{}/metadata/v{}.metadata.json", base, version));
        }
    }

    // Fallback: scan directory (local filesystem only)
    if !is_remote_path(base) {
        if let Some(p) = scan_local_metadata_dir(base).await {
            return Ok(p);
        }
    }

    bail!(
        "no Iceberg metadata found at: {}\n\
         Tried: {}/metadata/version-hint.text\n\
         \n\
         Hint: ensure the table has a version-hint.text file, or pass the \
         full path to the metadata JSON file directly",
        path,
        base
    )
}

/// Iceberg's FileIO requires absolute paths for local files.
/// Canonicalize relative paths; leave remote URLs untouched.
fn normalize_local_path(path: &str) -> String {
    if is_remote_path(path) {
        return path.to_string();
    }
    std::path::Path::new(path)
        .canonicalize()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| path.to_string())
}

fn is_remote_path(path: &str) -> bool {
    path.starts_with("s3://") || path.starts_with("gs://")
}

async fn scan_local_metadata_dir(base: &str) -> Option<String> {
    let metadata_dir = std::path::PathBuf::from(base).join("metadata");
    let mut entries = tokio::fs::read_dir(&metadata_dir).await.ok()?;

    let mut max_version: Option<i64> = None;
    let mut best_path: Option<String> = None;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with('v') || !name.ends_with(".metadata.json") {
            continue;
        }
        let version_str = &name[1..name.len() - ".metadata.json".len()];
        let Ok(v) = version_str.parse::<i64>() else {
            continue;
        };
        if max_version.is_none_or(|mv| v > mv) {
            max_version = Some(v);
            best_path = Some(entry.path().to_string_lossy().to_string());
        }
    }

    best_path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_path_detection() {
        assert!(is_remote_path("s3://bucket/table"));
        assert!(is_remote_path("gs://bucket/path"));
        assert!(!is_remote_path("/local/path"));
        assert!(!is_remote_path("./relative/path"));
    }

    #[tokio::test]
    async fn load_from_nonexistent_path_errors() {
        let config = StorageConfig::default();
        let result = load_direct("/nonexistent/path", &config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn direct_json_path_nonexistent() {
        let config = StorageConfig::default();
        let result = load_direct("/nonexistent/v1.metadata.json", &config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn s3_nonexistent_path_errors() {
        let config = StorageConfig {
            s3_endpoint: Some("http://localhost:1".to_string()),
            ..Default::default()
        };
        let result = load_direct("s3://nonexistent-bucket/nonexistent-table", &config).await;
        assert!(result.is_err(), "expected error for nonexistent S3 path");
    }

    #[test]
    fn normalize_leaves_remote_paths_unchanged() {
        assert_eq!(
            normalize_local_path("s3://bucket/table"),
            "s3://bucket/table"
        );
    }

    #[test]
    fn normalize_converts_relative_to_absolute() {
        let result = normalize_local_path(".");
        assert!(
            std::path::Path::new(&result).is_absolute(),
            "expected absolute path, got: {}",
            result
        );
    }

    #[test]
    fn normalize_preserves_absolute_paths() {
        let result = normalize_local_path("/tmp");
        assert_eq!(result, "/private/tmp"); // macOS canonicalizes /tmp → /private/tmp
    }

    #[test]
    fn normalize_nonexistent_falls_back_to_original() {
        let result = normalize_local_path("/nonexistent/path/that/does/not/exist");
        assert_eq!(result, "/nonexistent/path/that/does/not/exist");
    }
}
