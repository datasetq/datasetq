use dsq_shared::value::Value;
use dsq_shared::Result;
use polars::prelude::*;

use crate::inventory;
use crate::FunctionRegistration;

pub fn builtin_is_valid_utf8(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "is_valid_utf8() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            // If string looks like hex, decode it and check
            if s.len() % 2 == 0 && s.chars().all(|c| c.is_ascii_hexdigit()) && !s.is_empty() {
                let bytes = (0..s.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap_or(0))
                    .collect::<Vec<u8>>();
                Ok(Value::Bool(std::str::from_utf8(&bytes).is_ok()))
            } else {
                Ok(Value::Bool(true)) // Regular string
            }
        }
        Value::Array(arr) => {
            // Check if array contains bytes (integers 0-255)
            let mut bytes = Vec::new();
            for v in arr {
                match v {
                    Value::Int(i) if *i >= 0 && *i <= 255 => bytes.push(*i as u8),
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "is_valid_utf8() array must contain integers 0-255",
                        ));
                    }
                }
            }
            Ok(Value::Bool(std::str::from_utf8(&bytes).is_ok()))
        }
        Value::DataFrame(_df) => {
            // return true for DataFrames as they're always constructed with valid data
            Ok(Value::Bool(true))
        }
        Value::Series(series) => {
            // For Series, check based on type
            match series.dtype() {
                DataType::String => Ok(Value::Bool(true)), // String series are valid
                DataType::Binary => {
                    // Check if binary data is valid UTF-8
                    let is_valid = series.binary().unwrap().into_iter().all(|opt_bytes| {
                        opt_bytes.is_none_or(|bytes| std::str::from_utf8(bytes).is_ok())
                    });
                    Ok(Value::Bool(is_valid))
                }
                _ => Ok(Value::Bool(true)), // Other types are not byte data
            }
        }
        Value::LazyFrame(_lf) => {
            // LazyFrames are always constructed with valid data
            Ok(Value::Bool(true))
        }
        _ => Err(dsq_shared::error::operation_error(
            "is_valid_utf8() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "is_valid_utf8",
        func: builtin_is_valid_utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_is_valid_utf8_string() {
        let result = builtin_is_valid_utf8(&[Value::String("hello world".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(true));

        let result = builtin_is_valid_utf8(&[Value::String("café".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_builtin_is_valid_utf8_hex_string() {
        // Valid UTF-8 bytes as hex
        let result = builtin_is_valid_utf8(&[Value::String("68656c6c6f".to_string())]).unwrap(); // "hello"
        assert_eq!(result, Value::Bool(true));

        // Invalid UTF-8 bytes as hex
        let result = builtin_is_valid_utf8(&[Value::String("fffe".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_builtin_is_valid_utf8_array() {
        let valid_bytes = vec![
            Value::Int(104),
            Value::Int(101),
            Value::Int(108),
            Value::Int(108),
            Value::Int(111),
        ]; // "hello"
        let result = builtin_is_valid_utf8(&[Value::Array(valid_bytes)]).unwrap();
        assert_eq!(result, Value::Bool(true));

        let invalid_bytes = vec![Value::Int(255), Value::Int(254)];
        let result = builtin_is_valid_utf8(&[Value::Array(invalid_bytes)]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_builtin_is_valid_utf8_invalid_args() {
        let result = builtin_is_valid_utf8(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        let result = builtin_is_valid_utf8(&[
            Value::String("test".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        let invalid_array = vec![Value::Int(300)]; // > 255
        let result = builtin_is_valid_utf8(&[Value::Array(invalid_array)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must contain integers 0-255"));
    }

    #[test]
    fn test_is_valid_utf8_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("is_valid_utf8"));
    }
}
