use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use uuid;

pub fn builtin_generate_uuidv7(args: &[Value]) -> Result<Value> {
    match args.len() {
        0 => {
            // Generate a new UUID v7 with current timestamp
            let uuid = uuid::Uuid::now_v7();
            Ok(Value::String(uuid.to_string()))
        }
        1 => {
            match &args[0] {
                Value::Array(arr) => {
                    let uuids: Vec<Value> = arr
                        .iter()
                        .map(|_| {
                            let uuid = uuid::Uuid::now_v7();
                            Value::String(uuid.to_string())
                        })
                        .collect();
                    Ok(Value::Array(uuids))
                }
                Value::DataFrame(df) => {
                    // Add a new column with UUIDs for each row
                    let mut new_df = df.clone();
                    let uuid_series = Series::new(
                        "uuid_v7",
                        (0..df.height())
                            .map(|_| {
                                let uuid = uuid::Uuid::now_v7();
                                uuid.to_string()
                            })
                            .collect::<Vec<String>>(),
                    );
                    match new_df.with_column(uuid_series) {
                        Ok(_) => Ok(Value::DataFrame(new_df)),
                        Err(e) => Err(dsq_shared::error::operation_error(format!(
                            "generate_uuidv7() failed on DataFrame: {}",
                            e
                        ))),
                    }
                }
                Value::Series(series) => {
                    let uuid_series = Series::new(
                        series.name(),
                        (0..series.len())
                            .map(|_| {
                                let uuid = uuid::Uuid::now_v7();
                                uuid.to_string()
                            })
                            .collect::<Vec<String>>(),
                    );
                    Ok(Value::Series(uuid_series))
                }
                _ => {
                    // For single values, generate one UUID
                    let uuid = uuid::Uuid::now_v7();
                    Ok(Value::String(uuid.to_string()))
                }
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "generate_uuidv7() expects 0 or 1 arguments",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "generate_uuidv7",
        func: builtin_generate_uuidv7,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_generate_uuidv7_no_args() {
        let result = builtin_generate_uuidv7(&[]).unwrap();
        match result {
            Value::String(uuid_str) => {
                // UUID v7 should be a valid UUID string (36 characters with dashes)
                assert_eq!(uuid_str.len(), 36);
                // Should contain dashes at expected positions
                assert_eq!(uuid_str.chars().nth(8), Some('-'));
                assert_eq!(uuid_str.chars().nth(13), Some('-'));
                assert_eq!(uuid_str.chars().nth(18), Some('-'));
                assert_eq!(uuid_str.chars().nth(23), Some('-'));
                // Should be able to parse as UUID
                assert!(uuid::Uuid::parse_str(&uuid_str).is_ok());
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_builtin_generate_uuidv7_with_array() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_generate_uuidv7(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(uuids) => {
                assert_eq!(uuids.len(), 3);
                for uuid_val in uuids {
                    match uuid_val {
                        Value::String(uuid_str) => {
                            assert_eq!(uuid_str.len(), 36);
                            assert!(uuid::Uuid::parse_str(&uuid_str).is_ok());
                        }
                        _ => panic!("Expected String in array"),
                    }
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_generate_uuidv7_with_dataframe() {
        let df = DataFrame::new(vec![
            Series::new("col1", vec![1, 2, 3]),
            Series::new("col2", vec!["a", "b", "c"]),
        ])
        .unwrap();
        let result = builtin_generate_uuidv7(&[Value::DataFrame(df.clone())]).unwrap();
        match result {
            Value::DataFrame(result_df) => {
                assert_eq!(result_df.height(), 3);
                assert!(result_df.get_column_names().contains(&"uuid_v7"));
                let uuid_col = result_df.column("uuid_v7").unwrap();
                assert_eq!(uuid_col.dtype(), &DataType::Utf8);
                for i in 0..uuid_col.len() {
                    if let Ok(AnyValue::Utf8(uuid_str)) = uuid_col.get(i) {
                        assert_eq!(uuid_str.len(), 36);
                        assert!(uuid::Uuid::parse_str(uuid_str).is_ok());
                    }
                }
                // Original columns should remain
                assert!(result_df.get_column_names().contains(&"col1"));
                assert!(result_df.get_column_names().contains(&"col2"));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_builtin_generate_uuidv7_with_series() {
        let series = Series::new("test", vec![1, 2, 3]);
        let result = builtin_generate_uuidv7(&[Value::Series(series)]).unwrap();
        match result {
            Value::Series(result_series) => {
                assert_eq!(result_series.len(), 3);
                assert_eq!(result_series.dtype(), &DataType::Utf8);
                for i in 0..result_series.len() {
                    if let Ok(AnyValue::Utf8(uuid_str)) = result_series.get(i) {
                        assert_eq!(uuid_str.len(), 36);
                        assert!(uuid::Uuid::parse_str(uuid_str).is_ok());
                    }
                }
            }
            _ => panic!("Expected Series"),
        }
    }

    #[test]
    fn test_builtin_generate_uuidv7_with_other_value() {
        let result = builtin_generate_uuidv7(&[Value::String("test".to_string())]).unwrap();
        match result {
            Value::String(uuid_str) => {
                assert_eq!(uuid_str.len(), 36);
                assert!(uuid::Uuid::parse_str(&uuid_str).is_ok());
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_builtin_generate_uuidv7_invalid_args() {
        // Too many args
        let result = builtin_generate_uuidv7(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
    }
}
