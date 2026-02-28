use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use iceberg::expr::Predicate;

use super::TableHandle;

/// Configuration for a scan request.
#[derive(Debug, Clone, Default)]
pub struct ScanRequest {
    pub columns: Option<Vec<String>>,
    pub filter: Option<Predicate>,
    pub snapshot_id: Option<i64>,
    pub limit: Option<usize>,
}

pub struct ScanResult {
    pub batches: Vec<RecordBatch>,
    pub has_more: bool,
}

/// Execute a scan against an Iceberg table with early termination when limit is reached.
pub async fn execute_scan(handle: &TableHandle, request: &ScanRequest) -> Result<ScanResult> {
    let mut builder = handle.table.scan();

    if let Some(ref cols) = request.columns {
        builder = builder.select(cols.iter().map(|s| s.as_str()));
    }

    if let Some(ref filter) = request.filter {
        builder = builder.with_filter(filter.clone());
    }

    if let Some(snapshot_id) = request.snapshot_id {
        builder = builder.snapshot_id(snapshot_id);
    }

    let scan = builder.build().context("failed to build table scan")?;

    let stream = scan.to_arrow().await.context("failed to execute scan")?;

    let mut batches = Vec::new();
    let mut collected = 0;

    futures::pin_mut!(stream);
    while let Some(batch) = stream
        .try_next()
        .await
        .context("failed to collect scan results")?
    {
        collected += batch.num_rows();
        batches.push(batch);
        if request.limit.is_some_and(|lim| collected >= lim) {
            break;
        }
    }

    let has_more = request.limit.is_some_and(|lim| collected >= lim);

    if let Some(limit) = request.limit {
        batches = limit_batches(batches, limit);
    }

    Ok(ScanResult { batches, has_more })
}

/// Limit the total number of rows across batches.
fn limit_batches(batches: Vec<RecordBatch>, limit: usize) -> Vec<RecordBatch> {
    let mut result = Vec::new();
    let mut remaining = limit;

    for batch in batches {
        if remaining == 0 {
            break;
        }
        let take = remaining.min(batch.num_rows());
        result.push(batch.slice(0, take));
        remaining -= take;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_request_default() {
        let req = ScanRequest::default();
        assert!(req.columns.is_none());
        assert!(req.filter.is_none());
        assert!(req.snapshot_id.is_none());
        assert!(req.limit.is_none());
    }

    #[test]
    fn limit_batches_empty() {
        let result = limit_batches(vec![], 100);
        assert!(result.is_empty());
    }

    #[test]
    fn limit_batches_with_arrow_data() {
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        // Limit to 3 rows
        let limited = limit_batches(vec![batch], 3);
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].num_rows(), 3);
    }

    #[test]
    fn limit_batches_across_multiple() {
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
        )
        .unwrap();

        // Limit to 4 rows across 2 batches
        let limited = limit_batches(vec![batch1, batch2], 4);
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].num_rows(), 3);
        assert_eq!(limited[1].num_rows(), 1);
    }
}
