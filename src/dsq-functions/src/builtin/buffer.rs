use crate::FunctionRegistration;
use dsq_shared::error::operation_error;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_buffer(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(operation_error("buffer() expects at least 1 argument"));
    }

    let input = &args[0];
    let batch_size = if args.len() > 1 {
        match &args[1] {
            Value::Int(size) if *size > 0 => Some(*size as usize),
            Value::Int(_) => return Err(operation_error("buffer() batch size must be positive")),
            _ => return Err(operation_error("buffer() batch size must be an integer")),
        }
    } else {
        None
    };

    match input {
        Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(Value::Array(vec![]));
            }

            if let Some(size) = batch_size {
                // Split into batches of the specified size
                let mut batches = Vec::new();
                for chunk in arr.chunks(size) {
                    batches.push(Value::Array(chunk.to_vec()));
                }
                Ok(Value::Array(batches))
            } else {
                // No batch size specified, return all items as one batch
                Ok(Value::Array(vec![Value::Array(arr.clone())]))
            }
        }
        Value::DataFrame(df) => {
            if df.height() == 0 {
                return Ok(Value::Array(vec![]));
            }

            if let Some(size) = batch_size {
                // Split DataFrame into batches
                let mut batches = Vec::new();
                let total_rows = df.height();

                for start in (0..total_rows).step_by(size) {
                    let end = (start + size).min(total_rows);
                    let batch_df = df.slice(start as i64, (end - start) as usize);
                    batches.push(Value::DataFrame(batch_df));
                }
                Ok(Value::Array(batches))
            } else {
                // No batch size specified, return entire DataFrame as one batch
                Ok(Value::Array(vec![Value::DataFrame(df.clone())]))
            }
        }
        Value::Series(series) => {
            if series.len() == 0 {
                return Ok(Value::Array(vec![]));
            }

            if let Some(size) = batch_size {
                // Split Series into batches
                let mut batches = Vec::new();
                let total_len = series.len();

                for start in (0..total_len).step_by(size) {
                    let end = (start + size).min(total_len);
                    let batch_series = series.slice(start as i64, (end - start) as usize);
                    batches.push(Value::Series(batch_series));
                }
                Ok(Value::Array(batches))
            } else {
                // No batch size specified, return entire Series as one batch
                Ok(Value::Array(vec![Value::Series(series.clone())]))
            }
        }
        _ => {
            // For other types, wrap in an array as a single batch
            if let Some(_) = batch_size {
                // If batch size is specified but input is not a collection, return as single-item batches
                Ok(Value::Array(vec![input.clone()]))
            } else {
                // No batch size, return as one batch
                Ok(Value::Array(vec![input.clone()]))
            }
        }
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "buffer",
        func: builtin_buffer,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;

    #[test]
    fn test_buffer_array_with_batch_size() {
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
            Value::Int(6),
        ];
        let result = builtin_buffer(&[Value::Array(arr), Value::Int(2)]).unwrap();

        match result {
            Value::Array(batches) => {
                assert_eq!(batches.len(), 3);
                // First batch: [1, 2]
                if let Value::Array(batch1) = &batches[0] {
                    assert_eq!(batch1.len(), 2);
                    assert_eq!(batch1[0], Value::Int(1));
                    assert_eq!(batch1[1], Value::Int(2));
                } else {
                    panic!("Expected array batch");
                }
                // Second batch: [3, 4]
                if let Value::Array(batch2) = &batches[1] {
                    assert_eq!(batch2.len(), 2);
                    assert_eq!(batch2[0], Value::Int(3));
                    assert_eq!(batch2[1], Value::Int(4));
                } else {
                    panic!("Expected array batch");
                }
                // Third batch: [5, 6]
                if let Value::Array(batch3) = &batches[2] {
                    assert_eq!(batch3.len(), 2);
                    assert_eq!(batch3[0], Value::Int(5));
                    assert_eq!(batch3[1], Value::Int(6));
                } else {
                    panic!("Expected array batch");
                }
            }
            _ => panic!("Expected array of batches"),
        }
    }

    #[test]
    fn test_buffer_array_without_batch_size() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_buffer(&[Value::Array(arr)]).unwrap();

        match result {
            Value::Array(batches) => {
                assert_eq!(batches.len(), 1);
                // Single batch containing all items
                if let Value::Array(batch) = &batches[0] {
                    assert_eq!(batch.len(), 3);
                    assert_eq!(batch[0], Value::Int(1));
                    assert_eq!(batch[1], Value::Int(2));
                    assert_eq!(batch[2], Value::Int(3));
                } else {
                    panic!("Expected array batch");
                }
            }
            _ => panic!("Expected array of batches"),
        }
    }

    #[test]
    fn test_buffer_array_uneven_batches() {
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        let result = builtin_buffer(&[Value::Array(arr), Value::Int(2)]).unwrap();

        match result {
            Value::Array(batches) => {
                assert_eq!(batches.len(), 3);
                // First batch: [1, 2]
                if let Value::Array(batch1) = &batches[0] {
                    assert_eq!(batch1.len(), 2);
                }
                // Second batch: [3, 4]
                if let Value::Array(batch2) = &batches[1] {
                    assert_eq!(batch2.len(), 2);
                }
                // Third batch: [5]
                if let Value::Array(batch3) = &batches[2] {
                    assert_eq!(batch3.len(), 1);
                    assert_eq!(batch3[0], Value::Int(5));
                }
            }
            _ => panic!("Expected array of batches"),
        }
    }

    #[test]
    fn test_buffer_empty_array() {
        let arr = vec![];
        let result = builtin_buffer(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(batches) => {
                assert_eq!(batches.len(), 0);
            }
            _ => panic!("Expected array of batches"),
        }
    }

    #[test]
    fn test_buffer_dataframe_with_batch_size() {
        let names = Series::new(
            PlSmallStr::from("name"),
            &["Alice", "Bob", "Charlie", "David"],
        )
        .into();
        let ages = Series::new(PlSmallStr::from("age"), &[25i64, 30, 35, 28]).into();
        let df = DataFrame::new(vec![names, ages]).unwrap();

        let result = builtin_buffer(&[Value::DataFrame(df), Value::Int(2)]).unwrap();

        match result {
            Value::Array(batches) => {
                assert_eq!(batches.len(), 2);
                // First batch
                if let Value::DataFrame(batch1) = &batches[0] {
                    assert_eq!(batch1.height(), 2);
                } else {
                    panic!("Expected DataFrame batch");
                }
                // Second batch
                if let Value::DataFrame(batch2) = &batches[1] {
                    assert_eq!(batch2.height(), 2);
                } else {
                    panic!("Expected DataFrame batch");
                }
            }
            _ => panic!("Expected array of DataFrame batches"),
        }
    }

    #[test]
    fn test_buffer_dataframe_without_batch_size() {
        let names = Series::new(PlSmallStr::from("name"), &["Alice", "Bob"]).into();
        let df = DataFrame::new(vec![names]).unwrap();

        let result = builtin_buffer(&[Value::DataFrame(df)]).unwrap();

        match result {
            Value::Array(batches) => {
                assert_eq!(batches.len(), 1);
                if let Value::DataFrame(batch) = &batches[0] {
                    assert_eq!(batch.height(), 2);
                } else {
                    panic!("Expected DataFrame batch");
                }
            }
            _ => panic!("Expected array of DataFrame batches"),
        }
    }

    #[test]
    fn test_buffer_series_with_batch_size() {
        let series = Series::new(PlSmallStr::from("values"), &[1, 2, 3, 4, 5]);
        let result = builtin_buffer(&[Value::Series(series), Value::Int(2)]).unwrap();

        match result {
            Value::Array(batches) => {
                assert_eq!(batches.len(), 3);
                // First batch
                if let Value::Series(batch1) = &batches[0] {
                    assert_eq!(batch1.len(), 2);
                }
                // Second batch
                if let Value::Series(batch2) = &batches[1] {
                    assert_eq!(batch2.len(), 2);
                }
                // Third batch
                if let Value::Series(batch3) = &batches[2] {
                    assert_eq!(batch3.len(), 1);
                }
            }
            _ => panic!("Expected array of Series batches"),
        }
    }

    #[test]
    fn test_buffer_invalid_batch_size() {
        let arr = vec![Value::Int(1), Value::Int(2)];
        let result = builtin_buffer(&[Value::Array(arr), Value::Int(-1)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("batch size must be positive"));
    }

    #[test]
    fn test_buffer_non_integer_batch_size() {
        let arr = vec![Value::Int(1), Value::Int(2)];
        let result = builtin_buffer(&[Value::Array(arr), Value::String("2".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("batch size must be an integer"));
    }

    #[test]
    fn test_buffer_no_args() {
        let result = builtin_buffer(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects at least 1 argument"));
    }

    #[test]
    fn test_buffer_single_value() {
        let result = builtin_buffer(&[Value::Int(42)]).unwrap();
        match result {
            Value::Array(batches) => {
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0], Value::Int(42));
            }
            _ => panic!("Expected array of batches"),
        }
    }

    #[test]
    fn test_buffer_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("buffer"));
    }
}
