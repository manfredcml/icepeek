use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_cast::display::ArrayFormatter;

/// Convert a list of RecordBatches to displayable string rows.
///
/// Returns (column_names, rows) where each row is a Vec<String> of cell values.
/// Applies offset/limit for pagination.
pub fn batches_to_string_rows(
    batches: &[RecordBatch],
    offset: usize,
    limit: usize,
) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    if batches.is_empty() {
        return Ok((vec![], vec![]));
    }

    let schema = batches[0].schema();
    let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let num_columns = column_names.len();
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut current_offset = 0;

    for batch in batches {
        let batch_rows = batch.num_rows();

        if current_offset + batch_rows <= offset {
            current_offset += batch_rows;
            continue;
        }

        let start_in_batch = offset.saturating_sub(current_offset);

        let formatters: Vec<ArrayFormatter> = (0..num_columns)
            .map(|col_idx| {
                let array = batch.column(col_idx);
                ArrayFormatter::try_new(array.as_ref(), &Default::default())
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        for row_idx in start_in_batch..batch_rows {
            if rows.len() >= limit {
                return Ok((column_names, rows));
            }

            let row: Vec<String> = formatters
                .iter()
                .map(|fmt| fmt.value(row_idx).to_string())
                .collect();
            rows.push(row);
        }

        current_offset += batch_rows;
    }

    Ok((column_names, rows))
}

/// Count total rows across all batches.
pub fn total_row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Extract column names from record batches.
pub fn column_names(batches: &[RecordBatch]) -> Vec<String> {
    if batches.is_empty() {
        return vec![];
    }
    batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn empty_batches() {
        let (cols, rows) = batches_to_string_rows(&[], 0, 100).unwrap();
        assert!(cols.is_empty());
        assert!(rows.is_empty());
    }

    #[test]
    fn basic_conversion() {
        let batch = make_test_batch();
        let (cols, rows) = batches_to_string_rows(&[batch], 0, 100).unwrap();
        assert_eq!(cols, vec!["id", "name"]);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec!["1", "Alice"]);
        assert_eq!(rows[1], vec!["2", "Bob"]);
        assert_eq!(rows[2], vec!["3", "Charlie"]);
    }

    #[test]
    fn pagination_offset() {
        let batch = make_test_batch();
        let (_, rows) = batches_to_string_rows(&[batch], 1, 100).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec!["2", "Bob"]);
    }

    #[test]
    fn pagination_limit() {
        let batch = make_test_batch();
        let (_, rows) = batches_to_string_rows(&[batch], 0, 2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1], vec!["2", "Bob"]);
    }

    #[test]
    fn total_row_count_works() {
        let batch = make_test_batch();
        assert_eq!(total_row_count(&[batch.clone(), batch]), 6);
        assert_eq!(total_row_count(&[]), 0);
    }

    #[test]
    fn column_names_works() {
        let batch = make_test_batch();
        assert_eq!(column_names(&[batch]), vec!["id", "name"]);
        assert!(column_names(&[]).is_empty());
    }
}
