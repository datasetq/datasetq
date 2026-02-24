use dsq_shared::value::{df_row_to_value, value_from_any_value, Value};
use dsq_shared::Result;
use polars::prelude::*;

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
        Value::LazyFrame(lf) => {
            // Collect to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_array_pop(&[Value::DataFrame(df)])
        }
        Value::DataFrame(df) => {
            if df.height() == 0 {
                Ok(Value::Null)
            } else {
                // Return last row as object using the robust conversion
                let last_idx = df.height() - 1;
                df_row_to_value(df, last_idx)
            }
        }
        Value::Series(series) => {
            if matches!(series.dtype(), DataType::List(_)) {
                if series.len() == 1 {
                    match series.list().unwrap().get_as_series(0) {
                        Some(list_series) => {
                            if !list_series.is_empty() {
                                let last_idx = list_series.len() - 1;
                                match list_series.get(last_idx) {
                                    Ok(val) => Ok(value_from_any_value(val).unwrap_or(Value::Null)),
                                    _ => Ok(Value::Null),
                                }
                            } else {
                                Ok(Value::Null)
                            }
                        }
                        _ => Ok(Value::Null),
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
            "array_pop() requires an array, DataFrame, LazyFrame, or list series",
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
        let s1 = Series::new(PlSmallStr::from("col1"), &[1i64, 2i64]);
        let s2 = Series::new(PlSmallStr::from("col2"), &["a", "b"]);
        let df = DataFrame::new(vec![s1.into(), s2.into()]).unwrap();
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
        let df = DataFrame::new(vec![Series::new(
            PlSmallStr::from("empty"),
            Vec::<String>::new(),
        )
        .into()])
        .unwrap();
        let result = builtin_array_pop(&[Value::DataFrame(df)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_array_pop_series() {
        let s1 = Series::new(PlSmallStr::from(""), &[1i64, 2i64, 3i64]);
        let list_series = Series::new(PlSmallStr::from("list_col"), &[s1]);
        let result = builtin_array_pop(&[Value::Series(list_series)]).unwrap();
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn test_builtin_array_pop_empty_series() {
        let s1 = Series::new(PlSmallStr::from(""), &[] as &[i64]);
        let list_series = Series::new(PlSmallStr::from("list_col"), &[s1]);
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
