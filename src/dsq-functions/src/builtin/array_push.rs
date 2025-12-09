use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use polars::prelude::*;

use crate::inventory;
use crate::FunctionRegistration;

pub fn builtin_array_push(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(dsq_shared::error::operation_error(
            "array_push() expects at least 2 arguments",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let mut new_arr = arr.clone();
            new_arr.extend_from_slice(&args[1..]);
            Ok(Value::Array(new_arr))
        }
        Value::Series(series) => {
            if matches!(series.dtype(), DataType::List(_)) {
                let list_chunked = series.list().unwrap();
                if series.len() == 1 {
                    match list_chunked.get_as_series(0) {
                        Some(list_series) => {
                            let mut arr = Vec::new();
                            for i in 0..list_series.len() {
                                if let Ok(val) = list_series.get(i) {
                                    let value = value_from_any_value(val).unwrap_or(Value::Null);
                                    arr.push(value);
                                }
                            }
                            arr.extend_from_slice(&args[1..]);
                            Ok(Value::Array(arr))
                        }
                        _ => Ok(Value::Array(args[1..].to_vec())),
                    }
                } else {
                    Err(dsq_shared::error::operation_error(format!(
                        "array_push() on series with {} elements not supported",
                        series.len()
                    )))
                }
            } else {
                Err(dsq_shared::error::operation_error(
                    "array_push() requires an array or list series",
                ))
            }
        }
        Value::DataFrame(df) => {
            let value_to_push = &args[1];
            let any_value = match value_to_push {
                Value::Int(i) => AnyValue::Int64(*i),
                Value::Float(f) => AnyValue::Float64(*f),
                Value::String(s) => AnyValue::String(s),
                Value::Bool(b) => AnyValue::Boolean(*b),
                Value::Null => AnyValue::Null,
                _ => AnyValue::Null, // For complex types
            };
            let mut new_series_vec = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if matches!(series.dtype(), DataType::List(_)) {
                        let list_chunked = series.list().unwrap();
                        let mut new_lists = Vec::new();
                        for i in 0..df.height() {
                            match list_chunked.get_as_series(i) {
                                Some(list_series) => {
                                    let mut values = vec![];
                                    for j in 0..list_series.len() {
                                        values.push(list_series.get(j).unwrap());
                                    }
                                    values.push(any_value.clone());
                                    new_lists.push(Series::new("".into(), values));
                                }
                                _ => {
                                    new_lists.push(Series::new("".into(), vec![any_value.clone()]));
                                }
                            }
                        }
                        let new_list_series = Series::new(col_name.clone(), new_lists);
                        new_series_vec.push(new_list_series.into());
                    } else {
                        let mut s = series.clone();
                        s.rename(col_name.clone());
                        new_series_vec.push(s.into());
                    }
                }
            }
            match DataFrame::new(new_series_vec) {
                Ok(new_df) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "array_push() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        _ => Err(dsq_shared::error::operation_error(format!(
            "array_push() first argument must be an array, list series, or DataFrame, got {}",
            args[0].type_name()
        ))),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "array_push",
        func: builtin_array_push,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_array_push_array() {
        let arr = vec![Value::Int(1), Value::Int(2)];
        let result = builtin_array_push(&[Value::Array(arr), Value::Int(3)]).unwrap();
        match result {
            Value::Array(pushed) => {
                assert_eq!(pushed.len(), 3);
                assert_eq!(pushed[0], Value::Int(1));
                assert_eq!(pushed[1], Value::Int(2));
                assert_eq!(pushed[2], Value::Int(3));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_array_push_dataframe() {
        let s1 = Series::new(PlSmallStr::from(""), &[1i64, 2i64]);
        let s2 = Series::new(PlSmallStr::from(""), &[3i64]);
        let list_series = Series::new(PlSmallStr::from("list_col"), &[s1, s2]).into();
        let df = DataFrame::new(vec![list_series]).unwrap();
        let result = builtin_array_push(&[Value::DataFrame(df), Value::Int(4)]).unwrap();
        match result {
            Value::DataFrame(new_df) => {
                let list_col = new_df.column("list_col").unwrap().list().unwrap();
                let first_list = list_col.get_as_series(0).unwrap();
                assert_eq!(first_list.get(0).unwrap(), AnyValue::Int64(1));
                assert_eq!(first_list.get(1).unwrap(), AnyValue::Int64(2));
                assert_eq!(first_list.get(2).unwrap(), AnyValue::Int64(4));
                let second_list = list_col.get_as_series(1).unwrap();
                assert_eq!(second_list.get(0).unwrap(), AnyValue::Int64(3));
                assert_eq!(second_list.get(1).unwrap(), AnyValue::Int64(4));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_builtin_array_push_multiple_values() {
        let arr = vec![Value::Int(1)];
        let result = builtin_array_push(&[
            Value::Array(arr),
            Value::Int(2),
            Value::String("three".to_string()),
        ])
        .unwrap();
        match result {
            Value::Array(pushed) => {
                assert_eq!(pushed.len(), 3);
                assert_eq!(pushed[0], Value::Int(1));
                assert_eq!(pushed[1], Value::Int(2));
                assert_eq!(pushed[2], Value::String("three".to_string()));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_array_push_empty_array() {
        let arr = vec![];
        let result = builtin_array_push(&[Value::Array(arr), Value::Int(1)]).unwrap();
        match result {
            Value::Array(pushed) => {
                assert_eq!(pushed.len(), 1);
                assert_eq!(pushed[0], Value::Int(1));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_array_push_error_too_few_args() {
        let result = builtin_array_push(&[Value::Array(vec![Value::Int(1)])]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects at least 2 arguments"));
    }

    #[test]
    fn test_builtin_array_push_error_invalid_type() {
        let result = builtin_array_push(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("first argument must be an array"));
    }

    #[test]
    fn test_array_push_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("array_push"));
    }
}
