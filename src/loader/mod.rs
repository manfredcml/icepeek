pub mod arrow_convert;
pub mod catalog_loader;
pub mod direct_loader;
pub mod file_io;
pub mod scan;

use anyhow::{Context, Result};
use iceberg::table::Table;

use crate::model::table_info::TableMetadata;

/// Abstraction over a loaded Iceberg table.
/// The Table is Clone (wraps Arc), so it can be shared with background tasks.
#[derive(Clone)]
pub struct TableHandle {
    pub table: Table,
}

impl TableHandle {
    pub fn new(table: Table) -> Self {
        Self { table }
    }

    /// Extract metadata from the table into our display-friendly structs.
    pub fn extract_metadata(&self) -> Result<TableMetadata> {
        extract_metadata_from_table(&self.table)
    }

    /// Count total rows by summing `record_count` from live data files in manifests.
    pub async fn count_total_rows(&self, snapshot_id: Option<i64>) -> Result<usize> {
        let metadata = self.table.metadata();
        let snapshot = match snapshot_id {
            Some(id) => metadata.snapshot_by_id(id),
            None => metadata.current_snapshot(),
        }
        .context("no snapshot found")?;

        let file_io = self.table.file_io().clone();
        let manifest_list = snapshot
            .load_manifest_list(&file_io, metadata)
            .await
            .context("failed to load manifest list")?;

        let mut total = 0usize;
        for mf in manifest_list.entries() {
            let manifest = mf
                .load_manifest(&file_io)
                .await
                .context("failed to load manifest")?;
            for entry in manifest.entries() {
                if entry.is_alive() {
                    total += entry.data_file().record_count() as usize;
                }
            }
        }
        Ok(total)
    }
}

fn extract_metadata_from_table(table: &Table) -> Result<TableMetadata> {
    use crate::model::table_info::*;

    let metadata = table.metadata();

    let current_schema = schema_to_info(metadata.current_schema());

    let schemas: Vec<SchemaInfo> = metadata.schemas_iter().map(|s| schema_to_info(s)).collect();

    let snapshots: Vec<SnapshotInfo> = metadata
        .snapshots()
        .map(|snap| {
            let summary: std::collections::HashMap<String, String> = snap
                .summary()
                .additional_properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            SnapshotInfo {
                snapshot_id: snap.snapshot_id(),
                parent_snapshot_id: snap.parent_snapshot_id(),
                sequence_number: snap.sequence_number(),
                timestamp_ms: snap.timestamp_ms(),
                operation: snap.summary().operation.as_str().to_string(),
                summary,
                manifest_list: snap.manifest_list().to_string(),
                schema_id: snap.schema_id(),
            }
        })
        .collect();

    let partition_specs: Vec<PartitionSpecInfo> = metadata
        .partition_specs_iter()
        .map(|spec| {
            let fields: Vec<PartitionFieldInfo> = spec
                .fields()
                .iter()
                .map(|f| PartitionFieldInfo {
                    name: f.name.clone(),
                    transform: f.transform.to_string(),
                    source_id: f.source_id,
                })
                .collect();
            PartitionSpecInfo {
                spec_id: spec.spec_id(),
                fields,
            }
        })
        .collect();

    let sort_orders: Vec<SortOrderInfo> = metadata
        .sort_orders_iter()
        .map(|order| {
            let fields: Vec<SortFieldInfo> = order
                .fields
                .iter()
                .map(|f| SortFieldInfo {
                    source_id: f.source_id,
                    transform: f.transform.to_string(),
                    direction: format!("{:?}", f.direction),
                    null_order: format!("{:?}", f.null_order),
                })
                .collect();
            SortOrderInfo {
                order_id: order.order_id,
                fields,
            }
        })
        .collect();

    let properties: std::collections::HashMap<String, String> = metadata
        .properties()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    Ok(TableMetadata {
        location: metadata.location().to_string(),
        current_schema,
        schemas,
        snapshots,
        partition_specs,
        sort_orders,
        properties,
        current_snapshot_id: metadata.current_snapshot().map(|s| s.snapshot_id()),
        format_version: match metadata.format_version() {
            iceberg::spec::FormatVersion::V1 => 1,
            iceberg::spec::FormatVersion::V2 => 2,
            iceberg::spec::FormatVersion::V3 => 3,
        },
        table_uuid: metadata.uuid().to_string(),
        last_updated_ms: metadata.last_updated_ms(),
    })
}

fn schema_to_info(schema: &iceberg::spec::Schema) -> crate::model::table_info::SchemaInfo {
    use crate::model::table_info::SchemaInfo;

    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(nested_field_to_info)
        .collect();

    SchemaInfo {
        schema_id: schema.schema_id(),
        fields,
    }
}

fn nested_field_to_info(
    field: &iceberg::spec::NestedFieldRef,
) -> crate::model::table_info::FieldInfo {
    use crate::model::table_info::FieldInfo;
    use iceberg::spec::Type;

    let children = match field.field_type.as_ref() {
        Type::Struct(s) => s.fields().iter().map(nested_field_to_info).collect(),
        Type::List(l) => {
            vec![FieldInfo {
                id: l.element_field.id,
                name: "element".to_string(),
                field_type: l.element_field.field_type.to_string(),
                required: l.element_field.required,
                doc: l.element_field.doc.clone(),
                children: vec![],
            }]
        }
        Type::Map(m) => {
            vec![
                FieldInfo {
                    id: m.key_field.id,
                    name: "key".to_string(),
                    field_type: m.key_field.field_type.to_string(),
                    required: m.key_field.required,
                    doc: m.key_field.doc.clone(),
                    children: vec![],
                },
                FieldInfo {
                    id: m.value_field.id,
                    name: "value".to_string(),
                    field_type: m.value_field.field_type.to_string(),
                    required: m.value_field.required,
                    doc: m.value_field.doc.clone(),
                    children: vec![],
                },
            ]
        }
        _ => vec![],
    };

    FieldInfo {
        id: field.id,
        name: field.name.clone(),
        field_type: field.field_type.to_string(),
        required: field.required,
        doc: field.doc.clone(),
        children,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_handle_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<TableHandle>();
    }

    /// Integration test: loads the sample table and runs a full scan.
    #[tokio::test]
    async fn load_and_scan_sample_table() {
        // Only run if sample table exists
        let table_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("examples")
            .join("sample_table");
        if !table_path.exists() {
            eprintln!("Skipping integration test: sample table not found");
            return;
        }

        let path_str = table_path.to_string_lossy().to_string();
        eprintln!("Loading table from: {}", path_str);

        let config = file_io::StorageConfig::default();
        let handle = direct_loader::load_direct(&path_str, &config)
            .await
            .unwrap();
        eprintln!("Table loaded successfully");

        let metadata = handle.extract_metadata().unwrap();
        eprintln!(
            "Metadata extracted: {} snapshots, {} schemas",
            metadata.snapshots.len(),
            metadata.schemas.len()
        );

        let request = scan::ScanRequest::default();
        eprintln!("Starting scan...");
        let result = scan::execute_scan(&handle, &request).await.unwrap();
        eprintln!("Scan complete: {} batches", result.batches.len());

        let total_rows = arrow_convert::total_row_count(&result.batches);
        eprintln!("Total rows: {}", total_rows);
        assert_eq!(total_rows, 200, "Expected 200 rows in sample table");
    }
}
