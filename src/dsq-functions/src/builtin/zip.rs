use dsq_shared::value::Value;
use dsq_shared::Result;

use crate::inventory;
use crate::FunctionRegistration;

pub fn builtin_zip(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Array(Vec::new()));
    }

    // Check if all arguments are arrays
    let arrays: Vec<&Vec<Value>> = args
        .iter()
        .filter_map(|arg| match arg {
            Value::Array(arr) => Some(arr),
            _ => None,
        })
        .collect();

    if arrays.len() != args.len() {
        return Err(dsq_shared::error::operation_error(
            "zip() all arguments must be arrays",
        ));
    }

    if arrays.is_empty() {
        return Ok(Value::Array(Vec::new()));
    }

    let min_len = arrays.iter().map(|arr| arr.len()).min().unwrap_or(0);
    let mut result = Vec::with_capacity(min_len);

    for i in 0..min_len {
        let tuple: Vec<Value> = arrays.iter().map(|arr| arr[i].clone()).collect();
        result.push(Value::Array(tuple));
    }

    Ok(Value::Array(result))
}

inventory::submit! {
    FunctionRegistration {
        name: "zip",
        func: builtin_zip,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_zip_empty_args() {
        let result = builtin_zip(&[]).unwrap();
        match result {
            Value::Array(arr) => assert_eq!(arr.len(), 0),
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_zip_single_array() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_zip(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(zipped) => {
                assert_eq!(zipped.len(), 3);
                for (i, item) in zipped.iter().enumerate() {
                    match item {
                        Value::Array(tuple) => {
                            assert_eq!(tuple.len(), 1);
                            assert_eq!(tuple[0], Value::Int((i + 1) as i64));
                        }
                        _ => panic!("Expected Array"),
                    }
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_zip_two_arrays() {
        let arr1 = vec![Value::Int(1), Value::Int(2)];
        let arr2 = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let result = builtin_zip(&[Value::Array(arr1), Value::Array(arr2)]).unwrap();
        match result {
            Value::Array(zipped) => {
                assert_eq!(zipped.len(), 2);
                match &zipped[0] {
                    Value::Array(tuple) => {
                        assert_eq!(tuple.len(), 2);
                        assert_eq!(tuple[0], Value::Int(1));
                        assert_eq!(tuple[1], Value::String("a".to_string()));
                    }
                    _ => panic!("Expected Array"),
                }
                match &zipped[1] {
                    Value::Array(tuple) => {
                        assert_eq!(tuple.len(), 2);
                        assert_eq!(tuple[0], Value::Int(2));
                        assert_eq!(tuple[1], Value::String("b".to_string()));
                    }
                    _ => panic!("Expected Array"),
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_zip_three_arrays() {
        let arr1 = vec![Value::Int(1), Value::Int(2)];
        let arr2 = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let arr3 = vec![Value::Bool(true), Value::Bool(false)];
        let result =
            builtin_zip(&[Value::Array(arr1), Value::Array(arr2), Value::Array(arr3)]).unwrap();
        match result {
            Value::Array(zipped) => {
                assert_eq!(zipped.len(), 2);
                match &zipped[0] {
                    Value::Array(tuple) => {
                        assert_eq!(tuple.len(), 3);
                        assert_eq!(tuple[0], Value::Int(1));
                        assert_eq!(tuple[1], Value::String("a".to_string()));
                        assert_eq!(tuple[2], Value::Bool(true));
                    }
                    _ => panic!("Expected Array"),
                }
                match &zipped[1] {
                    Value::Array(tuple) => {
                        assert_eq!(tuple.len(), 3);
                        assert_eq!(tuple[0], Value::Int(2));
                        assert_eq!(tuple[1], Value::String("b".to_string()));
                        assert_eq!(tuple[2], Value::Bool(false));
                    }
                    _ => panic!("Expected Array"),
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_zip_unequal_lengths() {
        let arr1 = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let arr2 = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let result = builtin_zip(&[Value::Array(arr1), Value::Array(arr2)]).unwrap();
        match result {
            Value::Array(zipped) => {
                assert_eq!(zipped.len(), 2); // Should use minimum length
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_zip_non_array_arg() {
        let result = builtin_zip(&[Value::Int(1), Value::Array(vec![Value::Int(2)])]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("all arguments must be arrays"));
    }

    #[test]
    fn test_zip_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("zip"));
    }
}
