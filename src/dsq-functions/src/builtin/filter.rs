use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_filter(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "filter() expects 2 arguments",
        ));
    }

    let input = args[0].clone();
    let filter_value = args[1].clone();

    match input {
        Value::Array(arr) => {
            let filtered: Vec<Value> = arr.iter().filter(|v| &filter_value == v).cloned().collect();
            Ok(Value::Array(filtered))
        }
        Value::DataFrame(df) => {
            // Filter rows where the first column matches filter_value
            if let Some(first_col) = df.get_column_names().first() {
                if let Ok(series) = df.column(first_col) {
                    let mut mask = Vec::new();
                    for i in 0..series.len() {
                        if let Ok(val) = series.get(i) {
                            let value = value_from_any_value(val).unwrap_or(Value::Null);
                            mask.push(value == filter_value);
                        } else {
                            mask.push(false);
                        }
                    }
                    let mask_chunked = BooleanChunked::from_slice("mask".into(), &mask);
                    match df.filter(&mask_chunked) {
                        Ok(filtered_df) => Ok(Value::DataFrame(filtered_df)),
                        Err(e) => Err(dsq_shared::error::operation_error(format!(
                            "filter() failed on DataFrame: {}",
                            e
                        ))),
                    }
                } else {
                    Ok(Value::DataFrame(df.clone()))
                }
            } else {
                Ok(Value::DataFrame(df.clone()))
            }
        }
        Value::Series(series) => {
            let mut mask = Vec::new();
            for i in 0..series.len() {
                if let Ok(val) = series.get(i) {
                    let value = value_from_any_value(val).unwrap_or(Value::Null);
                    mask.push(value == filter_value);
                } else {
                    mask.push(false);
                }
            }
            let mask_chunked = BooleanChunked::from_slice("mask".into(), &mask);
            match series.filter(&mask_chunked) {
                Ok(filtered_series) => Ok(Value::Series(filtered_series)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "filter() failed on Series: {}",
                    e
                ))),
            }
        }
        _ => Ok(input.clone()),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "filter",
        func: builtin_filter,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    use std::collections::HashMap;

    #[test]
    fn test_filter_array() {
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(1),
            Value::String("test".to_string()),
            Value::Int(1),
        ];
        let result = builtin_filter(&[Value::Array(arr), Value::Int(1)]).unwrap();
        match result {
            Value::Array(filtered) => {
                assert_eq!(filtered.len(), 3);
                assert!(filtered.iter().all(|v| *v == Value::Int(1)));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_filter_array_strings() {
        let arr = vec![
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
            Value::String("hello".to_string()),
        ];
        let result =
            builtin_filter(&[Value::Array(arr), Value::String("hello".to_string())]).unwrap();
        match result {
            Value::Array(filtered) => {
                assert_eq!(filtered.len(), 2);
                assert!(filtered
                    .iter()
                    .all(|v| *v == Value::String("hello".to_string())));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_filter_array_no_matches() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_filter(&[Value::Array(arr), Value::Int(4)]).unwrap();
        match result {
            Value::Array(filtered) => {
                assert_eq!(filtered.len(), 0);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_filter_dataframe() {
        let df = DataFrame::new(vec![
            Series::new("col1".into(), vec![1, 2, 1, 3]),
            Series::new("col2".into(), vec!["a", "b", "c", "d"]),
        ])
        .unwrap();
        let result = builtin_filter(&[Value::DataFrame(df), Value::Int(1)]).unwrap();
        match result {
            Value::DataFrame(filtered_df) => {
                assert_eq!(filtered_df.height(), 2);
                let col1 = filtered_df.column("col1").unwrap();
                for i in 0..col1.len() {
                    let val = col1.get(i).unwrap();
                    assert_eq!(val, AnyValue::Int32(1));
                }
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_filter_series() {
        let series = Series::new("test".into(), vec![1, 2, 1, 3]);
        let result = builtin_filter(&[Value::Series(series), Value::Int(1)]).unwrap();
        match result {
            Value::Series(filtered_series) => {
                assert_eq!(filtered_series.len(), 2);
                for i in 0..filtered_series.len() {
                    let val = filtered_series.get(i).unwrap();
                    assert_eq!(val, AnyValue::Int32(1));
                }
            }
            _ => panic!("Expected Series"),
        }
    }

    #[test]
    fn test_filter_other_types() {
        let result = builtin_filter(&[Value::String("test".to_string()), Value::Int(1)]).unwrap();
        assert_eq!(result, Value::String("test".to_string()));

        let result = builtin_filter(&[Value::Int(42), Value::Int(1)]).unwrap();
        assert_eq!(result, Value::Int(42));

        let obj = HashMap::new();
        let result = builtin_filter(&[Value::Object(obj), Value::Int(1)]).unwrap();
        match result {
            Value::Object(_) => {}
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_filter_wrong_number_of_args() {
        let result = builtin_filter(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));

        let result = builtin_filter(&[Value::Int(1)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));

        let result = builtin_filter(&[Value::Int(1), Value::Int(2), Value::Int(3)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_filter_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("filter"));
    }
}
