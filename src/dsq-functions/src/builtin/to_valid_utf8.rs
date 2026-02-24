use dsq_shared::value::Value;
use dsq_shared::Result;
use polars::prelude::*;

use crate::inventory;
use crate::FunctionRegistration;

pub fn builtin_to_valid_utf8(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "to_valid_utf8() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            // Since Rust strings are always valid UTF-8, just return the string
            Ok(Value::String(s.clone()))
        }
        Value::Array(arr) => {
            let mut result = Vec::new();
            for val in arr {
                match val {
                    Value::String(s) => {
                        result.push(Value::String(s.clone()));
                    }
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "to_valid_utf8() requires string elements in array",
                        ));
                    }
                }
            }
            Ok(Value::Array(result))
        }
        Value::DataFrame(df) => {
            // For DataFrames, just return as-is since string columns are already valid UTF-8
            Ok(Value::DataFrame(df.clone()))
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                // String series are already valid UTF-8
                Ok(Value::Series(series.clone()))
            } else {
                Err(dsq_shared::error::operation_error(
                    "to_valid_utf8() requires string Series",
                ))
            }
        }
        Value::LazyFrame(lf) => {
            // LazyFrames are already valid UTF-8
            Ok(Value::LazyFrame(lf.clone()))
        }
        _ => Err(dsq_shared::error::operation_error(
            "to_valid_utf8() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "to_valid_utf8",
        func: builtin_to_valid_utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_to_valid_utf8_string() {
        let result = builtin_to_valid_utf8(&[Value::String("hello world".to_string())]).unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));

        let result = builtin_to_valid_utf8(&[Value::String("café".to_string())]).unwrap();
        assert_eq!(result, Value::String("café".to_string()));
    }

    #[test]
    fn test_builtin_to_valid_utf8_array() {
        let arr = vec![
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
        ];
        let result = builtin_to_valid_utf8(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(res_arr) => {
                assert_eq!(res_arr.len(), 2);
                assert_eq!(res_arr[0], Value::String("hello".to_string()));
                assert_eq!(res_arr[1], Value::String("world".to_string()));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_builtin_to_valid_utf8_invalid_args() {
        let result = builtin_to_valid_utf8(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        let result = builtin_to_valid_utf8(&[
            Value::String("test".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        let invalid_array = vec![Value::Int(1)];
        let result = builtin_to_valid_utf8(&[Value::Array(invalid_array)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string elements in array"));
    }

    #[test]
    fn test_to_valid_utf8_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("to_valid_utf8"));
    }
}
