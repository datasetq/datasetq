use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

inventory::submit! {
    FunctionRegistration {
        name: "unix2dos",
        func: builtin_unix2dos,
    }
}

pub fn builtin_unix2dos(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "unix2dos() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            let converted = s.replace("\n", "\r\n");
            Ok(Value::String(converted))
        }
        Value::Array(arr) => {
            let converted: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => Ok(Value::String(s.replace("\n", "\r\n"))),
                    _ => Err(dsq_shared::error::operation_error(
                        "unix2dos() requires string elements in array",
                    )),
                })
                .collect();
            Ok(Value::Array(converted?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::Utf8 {
                        let converted_series = series
                            .utf8()
                            .unwrap()
                            .apply(|s| s.map(|s| Cow::Owned(s.replace("\n", "\r\n"))))
                            .into_series();
                        let mut s = converted_series;
                        s.rename(col_name);
                        new_series.push(s);
                    } else {
                        let mut s = series.clone();
                        s.rename(col_name);
                        new_series.push(s);
                    }
                }
            }
            match DataFrame::new(new_series) {
                Ok(new_df) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "unix2dos() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::Utf8 {
                let converted_series = series
                    .utf8()
                    .unwrap()
                    .apply(|s| s.map(|s| Cow::Owned(s.replace("\n", "\r\n"))))
                    .into_series();
                Ok(Value::Series(converted_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "unix2dos() requires string, array, DataFrame, or Series",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_unix2dos_string() {
        let result = builtin_unix2dos(&[Value::String("hello\nworld".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("hello\r\nworld".to_string()));
    }

    #[test]
    fn test_unix2dos_no_newlines() {
        let result = builtin_unix2dos(&[Value::String("hello world".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("hello world".to_string()));
    }

    #[test]
    fn test_unix2dos_multiple_newlines() {
        let result = builtin_unix2dos(&[Value::String("line1\nline2\nline3".to_string())]);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Value::String("line1\r\nline2\r\nline3".to_string())
        );
    }

    #[test]
    fn test_unix2dos_empty_string() {
        let result = builtin_unix2dos(&[Value::String("".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("".to_string()));
    }

    #[test]
    fn test_unix2dos_array() {
        let arr = vec![
            Value::String("hello\nworld".to_string()),
            Value::String("foo\nbar".to_string()),
        ];
        let result = builtin_unix2dos(&[Value::Array(arr)]);
        assert!(result.is_ok());
        if let Value::Array(result_arr) = result.unwrap() {
            assert_eq!(result_arr.len(), 2);
            assert_eq!(result_arr[0], Value::String("hello\r\nworld".to_string()));
            assert_eq!(result_arr[1], Value::String("foo\r\nbar".to_string()));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_unix2dos_array_non_string() {
        let arr = vec![Value::String("test".to_string()), Value::Int(42)];
        let result = builtin_unix2dos(&[Value::Array(arr)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string elements in array"));
    }

    #[test]
    fn test_unix2dos_no_args() {
        let result = builtin_unix2dos(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unix2dos() expects 1 argument"));
    }

    #[test]
    fn test_unix2dos_too_many_args() {
        let result = builtin_unix2dos(&[
            Value::String("test".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unix2dos() expects 1 argument"));
    }

    #[test]
    fn test_unix2dos_non_supported_type() {
        let result = builtin_unix2dos(&[Value::Int(42)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unix2dos() requires string, array, DataFrame, or Series"));
    }

    #[test]
    fn test_unix2dos_registered_via_inventory() {
        // Test that the function is registered via inventory
        let mut found = false;
        for func in inventory::iter::<FunctionRegistration> {
            if func.name == "unix2dos" {
                found = true;
                // Test that calling the function works
                let result = (func.func)(&[Value::String("test\nline".to_string())]);
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), Value::String("test\r\nline".to_string()));
                break;
            }
        }
        assert!(found, "unix2dos function not found in inventory");
    }
}
