use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use clap::Args;
use iceberg::io::{FileIO, FileIOBuilder};

#[derive(Args, Clone, Debug)]
pub struct StorageConfig {
    #[arg(long, env = "S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    pub s3_region: String,

    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    pub s3_access_key_id: Option<String>,

    #[arg(long, env = "AWS_SECRET_ACCESS_KEY", hide = true)]
    pub s3_secret_access_key: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            s3_endpoint: None,
            s3_region: "us-east-1".to_string(),
            s3_access_key_id: None,
            s3_secret_access_key: None,
        }
    }
}

pub fn build_file_io(path: &str, config: &StorageConfig) -> Result<FileIO> {
    if path.starts_with("s3://") {
        return build_s3_file_io(config);
    }
    if path.starts_with("gs://") {
        return build_gcs_file_io(config);
    }

    FileIOBuilder::new_fs_io()
        .build()
        .context("failed to build local filesystem FileIO")
}

/// Build catalog properties for storage credential forwarding.
///
/// REST catalogs need storage credentials so their FileIO can reach data files.
/// This centralizes the property mapping for all backends.
pub fn storage_props(config: &StorageConfig) -> HashMap<String, String> {
    let mut props = HashMap::new();

    props.insert("s3.region".to_string(), config.s3_region.clone());
    if let Some(ref ep) = config.s3_endpoint {
        props.insert("s3.endpoint".to_string(), ep.clone());
        props.insert("s3.path-style-access".to_string(), "true".to_string());
    }
    if let Some(ref key) = config.s3_access_key_id {
        props.insert("s3.access-key-id".to_string(), key.clone());
    }
    if let Some(ref key) = config.s3_secret_access_key {
        props.insert("s3.secret-access-key".to_string(), key.clone());
    }

    // TODO: GCS — gcs.project-id, gcs.credential, gcs.endpoint

    props
}

fn build_s3_file_io(config: &StorageConfig) -> Result<FileIO> {
    let mut builder = FileIOBuilder::new("s3");
    builder = builder.with_prop("s3.region", &config.s3_region);

    if let Some(ref ep) = config.s3_endpoint {
        builder = builder.with_prop("s3.endpoint", ep);
        builder = builder.with_prop("s3.path-style-access", "true");
    }

    if let Some(ref key) = config.s3_access_key_id {
        builder = builder.with_prop("s3.access-key-id", key);
    }
    if let Some(ref key) = config.s3_secret_access_key {
        builder = builder.with_prop("s3.secret-access-key", key);
    }

    builder
        .build()
        .context("failed to build S3 FileIO — check credentials and endpoint config")
}

fn build_gcs_file_io(_config: &StorageConfig) -> Result<FileIO> {
    // TODO: Implement Google Cloud Storage support
    // Will need StorageConfig fields like:
    //   gcs_project_id, gcs_credential, gcs_endpoint
    bail!("Google Cloud Storage support is not yet implemented")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_fs_file_io() {
        let config = StorageConfig::default();
        let io = build_file_io("/some/local/path", &config);
        assert!(io.is_ok());
    }

    #[test]
    fn s3_file_io_minimal() {
        let config = StorageConfig::default();
        let io = build_file_io("s3://bucket/table", &config);
        assert!(io.is_ok());
    }

    #[test]
    fn s3_file_io_with_endpoint() {
        let config = StorageConfig {
            s3_endpoint: Some("http://localhost:9000".to_string()),
            ..Default::default()
        };
        let io = build_file_io("s3://bucket/table", &config);
        assert!(io.is_ok());
    }

    #[test]
    fn storage_props_includes_region() {
        let config = StorageConfig {
            s3_region: "eu-west-1".to_string(),
            ..Default::default()
        };
        let props = storage_props(&config);
        assert_eq!(props.get("s3.region").unwrap(), "eu-west-1");
    }

    #[test]
    fn storage_props_with_endpoint() {
        let config = StorageConfig {
            s3_endpoint: Some("http://minio:9000".to_string()),
            ..Default::default()
        };
        let props = storage_props(&config);
        assert_eq!(props.get("s3.endpoint").unwrap(), "http://minio:9000");
        assert_eq!(props.get("s3.path-style-access").unwrap(), "true");
    }

    #[test]
    fn storage_props_with_credentials() {
        let config = StorageConfig {
            s3_access_key_id: Some("AKID".to_string()),
            s3_secret_access_key: Some("SECRET".to_string()),
            ..Default::default()
        };
        let props = storage_props(&config);
        assert_eq!(props.get("s3.access-key-id").unwrap(), "AKID");
        assert_eq!(props.get("s3.secret-access-key").unwrap(), "SECRET");
    }

    #[test]
    fn storage_props_omits_none_fields() {
        let config = StorageConfig::default();
        let props = storage_props(&config);
        assert!(!props.contains_key("s3.endpoint"));
        assert!(!props.contains_key("s3.access-key-id"));
        assert!(!props.contains_key("s3.secret-access-key"));
    }

    #[test]
    fn gcs_not_yet_implemented() {
        let config = StorageConfig::default();
        let err = build_file_io("gs://bucket/path", &config).unwrap_err();
        assert!(err.to_string().contains("not yet implemented"));
    }
}
