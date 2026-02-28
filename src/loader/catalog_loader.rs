use anyhow::{bail, Context, Result};
use iceberg::Catalog;
use iceberg_catalog_rest::RestCatalogBuilder;

use super::file_io::{storage_props, StorageConfig};
use super::TableHandle;

/// Load an Iceberg table from a REST catalog.
pub async fn load_from_catalog(
    uri: &str,
    table_name: &str,
    config: &StorageConfig,
) -> Result<TableHandle> {
    let parts: Vec<&str> = table_name.split('.').collect();
    if parts.len() < 2 {
        bail!(
            "table name must be fully qualified (e.g., 'database.table'), got: {}",
            table_name
        );
    }

    let namespace = &parts[..parts.len() - 1];
    let table = parts[parts.len() - 1];

    let mut props = storage_props(config);
    props.insert("uri".to_string(), uri.to_string());

    let catalog =
        iceberg::CatalogBuilder::load(RestCatalogBuilder::default(), "rest_catalog", props)
            .await
            .with_context(|| format!("failed to connect to REST catalog at {}", uri))?;

    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::from_strs(namespace)?,
        table.to_string(),
    );

    let loaded_table = catalog.load_table(&table_ident).await.with_context(|| {
        format!(
            "failed to load table '{}' from catalog at {}",
            table_name, uri
        )
    })?;

    Ok(TableHandle::new(loaded_table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalid_table_name_errors() {
        let config = StorageConfig::default();
        let result = load_from_catalog("http://localhost:8181", "no_namespace", &config).await;
        assert!(result.is_err());
        let err = format!("{}", result.err().unwrap());
        assert!(err.contains("fully qualified"));
    }
}
