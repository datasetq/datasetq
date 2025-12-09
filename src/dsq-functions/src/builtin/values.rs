use dsq_shared::{value::Value, Result};
use inventory;

inventory::submit! {
    super::super::FunctionRegistration {
        name: "values",
        func: builtin_values,
    }
}

pub fn builtin_values(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "values() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Object(obj) => {
            let values: Vec<Value> = obj.values().cloned().collect();
            Ok(Value::Array(values))
        }
        Value::Array(arr) => Ok(Value::Array(arr.clone())),
        _ => Ok(Value::Array(vec![args[0].clone()])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_builtin_values_object() {
        let mut obj = HashMap::new();
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("b".to_string(), Value::String("hello".to_string()));
        obj.insert("c".to_string(), Value::Bool(true));
        let obj_val = Value::Object(obj);
        let result = builtin_values(&[obj_val]).unwrap();
        if let Value::Array(values) = result {
            assert_eq!(values.len(), 3);
            assert!(values.contains(&Value::Int(1)));
            assert!(values.contains(&Value::String("hello".to_string())));
            assert!(values.contains(&Value::Bool(true)));
        } else {
            panic!("Expected array of values");
        }
    }

    #[test]
    fn test_builtin_values_empty_object() {
        let obj = HashMap::new();
        let obj_val = Value::Object(obj);
        let result = builtin_values(&[obj_val]).unwrap();
        if let Value::Array(values) = result {
            assert_eq!(values.len(), 0);
        } else {
            panic!("Expected empty array");
        }
    }

    #[test]
    fn test_builtin_values_array() {
        let arr = vec![
            Value::Int(1),
            Value::String("test".to_string()),
            Value::Bool(false),
        ];
        let arr_val = Value::Array(arr.clone());
        let result = builtin_values(&[arr_val]).unwrap();
        if let Value::Array(result_arr) = result {
            assert_eq!(result_arr, arr);
        } else {
            panic!("Expected same array");
        }
    }

    #[test]
    fn test_builtin_values_empty_array() {
        let arr = Vec::new();
        let arr_val = Value::Array(arr.clone());
        let result = builtin_values(&[arr_val]).unwrap();
        if let Value::Array(result_arr) = result {
            assert_eq!(result_arr, arr);
        } else {
            panic!("Expected same empty array");
        }
    }

    #[test]
    fn test_builtin_values_single_value() {
        let test_cases = vec![
            Value::Int(42),
            Value::Float(3.14),
            Value::String("hello".to_string()),
            Value::Bool(true),
            Value::Null,
        ];

        for value in test_cases {
            let result = builtin_values(std::slice::from_ref(&value)).unwrap();
            if let Value::Array(arr) = result {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], value);
            } else {
                panic!("Expected array with single value");
            }
        }
    }

    #[test]
    fn test_builtin_values_invalid_args() {
        // No arguments
        let result = builtin_values(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Too many arguments
        let result = builtin_values(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_builtin_values_nested_structures() {
        // Object with nested array
        let mut obj = HashMap::new();
        let nested_arr = vec![Value::Int(1), Value::Int(2)];
        obj.insert("arr".to_string(), Value::Array(nested_arr));
        obj.insert("num".to_string(), Value::Int(3));
        let obj_val = Value::Object(obj);
        let result = builtin_values(&[obj_val]).unwrap();
        if let Value::Array(values) = result {
            assert_eq!(values.len(), 2);
            assert!(values.contains(&Value::Array(vec![Value::Int(1), Value::Int(2)])));
            assert!(values.contains(&Value::Int(3)));
        } else {
            panic!("Expected array of values");
        }
    }
}
