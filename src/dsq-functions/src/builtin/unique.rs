use dsq_shared::value::Value;
use dsq_shared::Result;
use polars::prelude::*;

use crate::inventory;
use crate::FunctionRegistration;

pub fn builtin_unique(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "unique() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let mut unique = Vec::new();
            for item in arr {
                if !unique.contains(item) {
                    unique.push(item.clone());
                }
            }
            Ok(Value::Array(unique))
        }
        Value::DataFrame(df) => {
            // Remove duplicate rows
            match df.unique::<String, &str>(None, UniqueKeepStrategy::First, None) {
                Ok(unique_df) => Ok(Value::DataFrame(unique_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "unique() failed: {}",
                    e
                ))),
            }
        }
        _ => Ok(args[0].clone()),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "unique",
        func: builtin_unique,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_builtin_unique_array() {
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(2),
            Value::Int(3),
            Value::Int(1),
        ];
        let result = builtin_unique(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(unique_arr) => {
                assert_eq!(unique_arr.len(), 3);
                assert!(unique_arr.contains(&Value::Int(1)));
                assert!(unique_arr.contains(&Value::Int(2)));
                assert!(unique_arr.contains(&Value::Int(3)));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_unique_array_strings() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("a".to_string()),
            Value::String("c".to_string()),
        ];
        let result = builtin_unique(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(unique_arr) => {
                assert_eq!(unique_arr.len(), 3);
                assert!(unique_arr.contains(&Value::String("a".to_string())));
                assert!(unique_arr.contains(&Value::String("b".to_string())));
                assert!(unique_arr.contains(&Value::String("c".to_string())));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_unique_array_empty() {
        let arr = vec![];
        let result = builtin_unique(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(unique_arr) => {
                assert_eq!(unique_arr.len(), 0);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_unique_array_single_element() {
        let arr = vec![Value::Int(42)];
        let result = builtin_unique(&[Value::Array(arr.clone())]).unwrap();
        match result {
            Value::Array(unique_arr) => {
                assert_eq!(unique_arr.len(), 1);
                assert_eq!(unique_arr[0], Value::Int(42));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_unique_array_objects() {
        let mut obj1 = HashMap::new();
        obj1.insert("x".to_string(), Value::Int(1));
        let mut obj2 = HashMap::new();
        obj2.insert("x".to_string(), Value::Int(2));
        let mut obj3 = HashMap::new();
        obj3.insert("x".to_string(), Value::Int(1)); // duplicate

        let arr = vec![
            Value::Object(obj1.clone()),
            Value::Object(obj2.clone()),
            Value::Object(obj3.clone()),
        ];
        let result = builtin_unique(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(unique_arr) => {
                assert_eq!(unique_arr.len(), 2);
                // Note: Since we're using contains, and objects are compared by value,
                // this should work as long as the objects are properly compared
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_unique_other_types() {
        // Test with string
        let result = builtin_unique(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(result, Value::String("hello".to_string()));

        // Test with int
        let result = builtin_unique(&[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(42));

        // Test with null
        let result = builtin_unique(&[Value::Null]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_unique_invalid_args() {
        // No arguments
        let result = builtin_unique(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Too many arguments
        let result = builtin_unique(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_unique_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("unique"));
    }
}
