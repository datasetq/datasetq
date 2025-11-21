use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use polars::prelude::*;
use std::collections::HashMap;

use crate::inventory;
use crate::FunctionRegistration;

pub fn builtin_array_pop(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "array_pop() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            if arr.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(arr[arr.len() - 1].clone())
            }
        }
        Value::DataFrame(df) => {
            if df.height() == 0 {
                Ok(Value::Null)
            } else {
                // Return last row as object
                let last_idx = df.height() - 1;
                let mut row_obj = HashMap::new();
                for col_name in df.get_column_names() {
                    if let Ok(series) = df.column(col_name) {
                        if let Some(val) = series.get(last_idx).ok() {
                            let value = value_from_any_value(val).unwrap_or(Value::Null);
                            row_obj.insert(col_name.to_string(), value);
                        }
                    }
                }
                Ok(Value::Object(row_obj))
            }
        }
        Value::Series(series) => {
            if matches!(series.dtype(), DataType::List(_)) {
                if series.len() == 1 {
                    if let Some(list_series) = series.list().unwrap().get_as_series(0) {
                        if list_series.len() > 0 {
                            let last_idx = list_series.len() - 1;
                            if let Ok(val) = list_series.get(last_idx) {
                                Ok(value_from_any_value(val).unwrap_or(Value::Null))
                            } else {
                                Ok(Value::Null)
                            }
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Err(dsq_shared::error::operation_error(format!(
                        "array_pop() on series with {} elements not supported",
                        series.len()
                    )))
                }
            } else {
                Err(dsq_shared::error::operation_error(
                    "array_pop() requires an array, DataFrame, or list series",
                ))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "array_pop() requires an array, DataFrame, or list series",
        )),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "array_pop",
        func: builtin_array_pop,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;

    #[test]
    fn test_builtin_array_pop_array() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_array_pop(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn test_builtin_array_pop_empty_array() {
        let arr = vec![];
        let result = builtin_array_pop(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_array_pop_dataframe() {
        let s1 = Series::new("col1", &[1i64, 2i64]);
        let s2 = Series::new("col2", &["a", "b"]);
        let df = DataFrame::new(vec![s1, s2]).unwrap();
        let result = builtin_array_pop(&[Value::DataFrame(df)]).unwrap();
        match result {
            Value::Object(obj) => {
                assert_eq!(obj.get("col1"), Some(&Value::Int(2)));
                assert_eq!(obj.get("col2"), Some(&Value::String("b".to_string())));
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_builtin_array_pop_empty_dataframe() {
        let df = DataFrame::new(vec![Series::new("empty", Vec::<String>::new())]).unwrap();
        let result = builtin_array_pop(&[Value::DataFrame(df)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_array_pop_series() {
        let s1 = Series::new("", &[1i64, 2i64, 3i64]);
        let list_series = Series::new("list_col", &[s1]);
        let result = builtin_array_pop(&[Value::Series(list_series)]).unwrap();
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn test_builtin_array_pop_empty_series() {
        let s1 = Series::new("", &[] as &[i64]);
        let list_series = Series::new("list_col", &[s1]);
        let result = builtin_array_pop(&[Value::Series(list_series)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_array_pop_invalid_args() {
        let result = builtin_array_pop(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        let result = builtin_array_pop(&[Value::Array(vec![Value::Int(1)]), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        let result = builtin_array_pop(&[Value::String("not array".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires an array"));
    }

    #[test]
    fn test_array_pop_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("array_pop"));
    }
}
