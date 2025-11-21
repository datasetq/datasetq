use dsq_shared::value::Value;
use dsq_shared::Result;

use crate::inventory;
use crate::FunctionRegistration;

pub fn builtin_del(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(dsq_shared::error::operation_error(
            "del() expects at least 2 arguments",
        ));
    }

    match &args[0] {
        Value::Object(obj) => {
            let mut new_obj = obj.clone();
            for key_arg in &args[1..] {
                if let Value::String(key) = key_arg {
                    new_obj.remove(key);
                }
            }
            Ok(Value::Object(new_obj))
        }
        Value::Array(arr) => {
            let indices: Result<Vec<usize>> = args[1..]
                .iter()
                .map(|arg| match arg {
                    Value::Int(i) if *i >= 0 => Ok(*i as usize),
                    _ => Err(dsq_shared::error::operation_error(
                        "del() indices must be non-negative integers",
                    )),
                })
                .collect();

            let mut indices = indices?;
            indices.sort_by(|a, b| b.cmp(a)); // Sort in reverse to delete from end
            indices.dedup(); // Remove duplicates

            let mut new_arr = arr.clone();
            for &i in &indices {
                if i < new_arr.len() {
                    new_arr.remove(i);
                }
            }
            Ok(Value::Array(new_arr))
        }
        _ => Err(dsq_shared::error::operation_error(
            "del() first argument must be object or array",
        )),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "del",
        func: builtin_del,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_builtin_del_object() {
        let mut obj = HashMap::new();
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("b".to_string(), Value::Int(2));
        obj.insert("c".to_string(), Value::Int(3));
        let result = builtin_del(&[Value::Object(obj), Value::String("b".to_string())]).unwrap();
        match result {
            Value::Object(new_obj) => {
                assert_eq!(new_obj.len(), 2);
                assert_eq!(new_obj.get("a"), Some(&Value::Int(1)));
                assert_eq!(new_obj.get("c"), Some(&Value::Int(3)));
                assert!(!new_obj.contains_key("b"));
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_builtin_del_object_multiple_keys() {
        let mut obj = HashMap::new();
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("b".to_string(), Value::Int(2));
        obj.insert("c".to_string(), Value::Int(3));
        obj.insert("d".to_string(), Value::Int(4));
        let result = builtin_del(&[
            Value::Object(obj),
            Value::String("b".to_string()),
            Value::String("d".to_string()),
        ])
        .unwrap();
        match result {
            Value::Object(new_obj) => {
                assert_eq!(new_obj.len(), 2);
                assert_eq!(new_obj.get("a"), Some(&Value::Int(1)));
                assert_eq!(new_obj.get("c"), Some(&Value::Int(3)));
                assert!(!new_obj.contains_key("b"));
                assert!(!new_obj.contains_key("d"));
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_builtin_del_array() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)];
        let result = builtin_del(&[Value::Array(arr), Value::Int(1)]).unwrap();
        match result {
            Value::Array(new_arr) => {
                assert_eq!(new_arr.len(), 3);
                assert_eq!(new_arr[0], Value::Int(1));
                assert_eq!(new_arr[1], Value::Int(3));
                assert_eq!(new_arr[2], Value::Int(4));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_del_array_multiple_indices() {
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        let result = builtin_del(&[Value::Array(arr), Value::Int(1), Value::Int(3)]).unwrap();
        match result {
            Value::Array(new_arr) => {
                assert_eq!(new_arr.len(), 3);
                assert_eq!(new_arr[0], Value::Int(1));
                assert_eq!(new_arr[1], Value::Int(3));
                assert_eq!(new_arr[2], Value::Int(5));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_del_array_out_of_bounds() {
        let arr = vec![Value::Int(1), Value::Int(2)];
        let result = builtin_del(&[Value::Array(arr), Value::Int(5)]).unwrap();
        match result {
            Value::Array(new_arr) => {
                assert_eq!(new_arr.len(), 2); // Should remain unchanged
                assert_eq!(new_arr[0], Value::Int(1));
                assert_eq!(new_arr[1], Value::Int(2));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_del_invalid_args() {
        let result = builtin_del(&[Value::Int(1)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects at least 2 arguments"));

        let result = builtin_del(&[
            Value::String("not object or array".to_string()),
            Value::String("key".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("first argument must be object or array"));

        let result = builtin_del(&[
            Value::Array(vec![Value::Int(1)]),
            Value::String("not int".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("indices must be non-negative integers"));
    }

    #[test]
    fn test_del_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("del"));
    }
}
