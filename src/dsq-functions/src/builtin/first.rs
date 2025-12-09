use crate::FunctionRegistration;
use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use inventory;
use std::collections::HashMap;

inventory::submit! {
    FunctionRegistration {
        name: "first",
        func: builtin_first,
    }
}

pub fn builtin_first(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "first() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            if arr.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(arr[0].clone())
            }
        }
        Value::DataFrame(df) => {
            if df.height() == 0 {
                Ok(Value::Null)
            } else {
                // Return first row as object
                let mut row_obj = HashMap::new();
                for col_name in df.get_column_names() {
                    if let Ok(series) = df.column(col_name) {
                        if let Some(val) = series.get(0).ok() {
                            let value = value_from_any_value(val).unwrap_or(Value::Null);
                            row_obj.insert(col_name.to_string(), value);
                        }
                    }
                }
                Ok(Value::Object(row_obj))
            }
        }
        _ => Ok(args[0].clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;

    #[test]
    fn test_first_empty_array() {
        let result = builtin_first(&[Value::Array(vec![])]);
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_first_array_with_elements() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_first(&[Value::Array(arr)]);
        assert_eq!(result.unwrap(), Value::Int(1));
    }

    #[test]
    fn test_first_dataframe_empty() {
        let df = DataFrame::empty();
        let result = builtin_first(&[Value::DataFrame(df)]);
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_first_dataframe_with_rows() {
        let series1 = Series::new(PlSmallStr::from("col1"), vec![1i64, 2]);
        let series2 = Series::new(
            PlSmallStr::from("col2"),
            vec!["a".to_string(), "b".to_string()],
        );
        let df = DataFrame::new(vec![series1.into(), series2.into()]).unwrap();
        let result = builtin_first(&[Value::DataFrame(df)]).unwrap();

        if let Value::Object(obj) = result {
            assert_eq!(obj.get("col1"), Some(&Value::Int(1)));
            assert_eq!(obj.get("col2"), Some(&Value::String("a".to_string())));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_first_other_types() {
        let result = builtin_first(&[Value::Int(42)]);
        assert_eq!(result.unwrap(), Value::Int(42));

        let result = builtin_first(&[Value::String("hello".to_string())]);
        assert_eq!(result.unwrap(), Value::String("hello".to_string()));
    }

    #[test]
    fn test_first_wrong_number_of_args() {
        let result = builtin_first(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        let result = builtin_first(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_first_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("first"));
    }
}
