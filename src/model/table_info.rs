use std::collections::HashMap;

/// Top-level metadata container for an Iceberg table.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub location: String,
    pub current_schema: SchemaInfo,
    pub schemas: Vec<SchemaInfo>,
    pub snapshots: Vec<SnapshotInfo>,
    pub partition_specs: Vec<PartitionSpecInfo>,
    pub sort_orders: Vec<SortOrderInfo>,
    pub properties: HashMap<String, String>,
    pub current_snapshot_id: Option<i64>,
    pub format_version: i32,
    pub table_uuid: String,
    pub last_updated_ms: i64,
}

/// Schema information.
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    pub schema_id: i32,
    pub fields: Vec<FieldInfo>,
}

/// Information about a single field in a schema.
#[derive(Debug, Clone)]
pub struct FieldInfo {
    pub id: i32,
    pub name: String,
    pub field_type: String,
    pub required: bool,
    pub doc: Option<String>,
    pub children: Vec<FieldInfo>,
}

/// Snapshot information.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub operation: String,
    pub summary: HashMap<String, String>,
    pub manifest_list: String,
    pub schema_id: Option<i32>,
}

/// Manifest file information.
#[derive(Debug, Clone)]
pub struct ManifestInfo {
    pub path: String,
    pub content_type: String,
    pub added_data_files_count: Option<i32>,
    pub added_rows_count: Option<i64>,
    pub existing_data_files_count: Option<i32>,
    pub existing_rows_count: Option<i64>,
    pub deleted_data_files_count: Option<i32>,
    pub deleted_rows_count: Option<i64>,
    pub sequence_number: i64,
    pub partition_spec_id: i32,
}

/// Data file information with column-level statistics.
#[derive(Debug, Clone)]
pub struct DataFileInfo {
    pub file_path: String,
    pub file_format: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub null_value_counts: HashMap<i32, i64>,
    pub lower_bounds: HashMap<i32, String>,
    pub upper_bounds: HashMap<i32, String>,
    pub partition_data: HashMap<String, String>,
}

/// Partition spec information.
#[derive(Debug, Clone)]
pub struct PartitionSpecInfo {
    pub spec_id: i32,
    pub fields: Vec<PartitionFieldInfo>,
}

/// A single field within a partition spec (source column + transform).
#[derive(Debug, Clone)]
pub struct PartitionFieldInfo {
    pub name: String,
    pub transform: String,
    pub source_id: i32,
}

/// Sort order information.
#[derive(Debug, Clone)]
pub struct SortOrderInfo {
    pub order_id: i64,
    pub fields: Vec<SortFieldInfo>,
}

/// A single field within a sort order (source column + transform + direction).
#[derive(Debug, Clone)]
pub struct SortFieldInfo {
    pub source_id: i32,
    pub transform: String,
    pub direction: String,
    pub null_order: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_info_can_have_children() {
        let field = FieldInfo {
            id: 1,
            name: "address".to_string(),
            field_type: "struct".to_string(),
            required: false,
            doc: None,
            children: vec![FieldInfo {
                id: 2,
                name: "street".to_string(),
                field_type: "string".to_string(),
                required: true,
                doc: None,
                children: vec![],
            }],
        };
        assert_eq!(field.children.len(), 1);
        assert_eq!(field.children[0].name, "street");
    }

    #[test]
    fn manifest_info_data_manifest() {
        let m = ManifestInfo {
            path: "/path/to/manifest.avro".into(),
            content_type: "data".into(),
            added_data_files_count: Some(5),
            added_rows_count: Some(1000),
            existing_data_files_count: Some(3),
            existing_rows_count: Some(500),
            deleted_data_files_count: Some(1),
            deleted_rows_count: Some(100),
            sequence_number: 42,
            partition_spec_id: 0,
        };
        assert_eq!(m.content_type, "data");
        assert_eq!(m.added_data_files_count, Some(5));
        assert_eq!(m.existing_data_files_count, Some(3));
        assert_eq!(m.deleted_data_files_count, Some(1));
        assert_eq!(m.sequence_number, 42);
        assert_eq!(m.partition_spec_id, 0);
    }

    #[test]
    fn manifest_info_deletes_manifest() {
        let m = ManifestInfo {
            path: "/path/to/delete-manifest.avro".into(),
            content_type: "deletes".into(),
            added_data_files_count: None,
            added_rows_count: None,
            existing_data_files_count: None,
            existing_rows_count: None,
            deleted_data_files_count: None,
            deleted_rows_count: None,
            sequence_number: 0,
            partition_spec_id: 1,
        };
        assert_eq!(m.content_type, "deletes");
        assert!(m.added_data_files_count.is_none());
        assert_eq!(m.partition_spec_id, 1);
    }

    #[test]
    fn snapshot_info_defaults() {
        let snap = SnapshotInfo {
            snapshot_id: 1234,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 1700000000000,
            operation: "append".to_string(),
            summary: HashMap::new(),
            manifest_list: "/path/to/manifest-list.avro".to_string(),
            schema_id: Some(0),
        };
        assert_eq!(snap.operation, "append");
        assert!(snap.parent_snapshot_id.is_none());
    }
}
