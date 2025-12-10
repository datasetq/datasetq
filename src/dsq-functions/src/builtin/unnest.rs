use dsq_shared::value::Value;
use dsq_shared::Result;
use std::collections::HashMap;

use crate::inventory;
use crate::FunctionRegistration;

fn flatten_object(obj: &HashMap<String, Value>, separator: &str) -> HashMap<String, Value> {
    let mut result = HashMap::new();

    for (key, value) in obj {
        match value {
            Value::Object(nested_obj) => {
                let flattened = flatten_object(nested_obj, separator);
                for (nested_key, nested_value) in flattened {
                    let new_key = format!("{}{}{}", key, separator, nested_key);
                    result.insert(new_key, nested_value);
                }
            }
            _ => {
                result.insert(key.clone(), value.clone());
            }
        }
    }

    result
}

pub fn builtin_unnest(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "unnest() expects 1 or 2 arguments",
        ));
    }

    let separator = if args.len() == 2 {
        match &args[1] {
            Value::String(s) => s.as_str(),
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "unnest() separator must be a string",
                ));
            }
        }
    } else {
        "."
    };

    match &args[0] {
        Value::Object(obj) => {
            let flattened = flatten_object(obj, separator);
            Ok(Value::Object(flattened))
        }
        _ => Ok(args[0].clone()),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "unnest",
        func: builtin_unnest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_builtin_unnest_nested_object() {
        let mut inner = HashMap::new();
        inner.insert("c".to_string(), Value::String("value".to_string()));

        let mut middle = HashMap::new();
        middle.insert("b".to_string(), Value::Object(inner));

        let mut outer = HashMap::new();
        outer.insert("a".to_string(), Value::Object(middle));

        let result = builtin_unnest(&[Value::Object(outer)]).unwrap();
        match result {
            Value::Object(flattened) => {
                assert_eq!(flattened.len(), 1);
                assert_eq!(
                    flattened.get("a.b.c"),
                    Some(&Value::String("value".to_string()))
                );
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_builtin_unnest_custom_separator() {
        let mut inner = HashMap::new();
        inner.insert("y".to_string(), Value::Int(42));

        let mut outer = HashMap::new();
        outer.insert("x".to_string(), Value::Object(inner));
        outer.insert("z".to_string(), Value::String("test".to_string()));

        let result =
            builtin_unnest(&[Value::Object(outer), Value::String("_".to_string())]).unwrap();
        match result {
            Value::Object(flattened) => {
                assert_eq!(flattened.get("x_y"), Some(&Value::Int(42)));
                assert_eq!(flattened.get("z"), Some(&Value::String("test".to_string())));
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_builtin_unnest_non_object() {
        let value = Value::String("not an object".to_string());
        let result = builtin_unnest(std::slice::from_ref(&value)).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_builtin_unnest_error_too_few_args() {
        let result = builtin_unnest(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 or 2 arguments"));
    }

    #[test]
    fn test_builtin_unnest_error_too_many_args() {
        let result = builtin_unnest(&[
            Value::Object(HashMap::new()),
            Value::String(".".to_string()),
            Value::Int(1),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 or 2 arguments"));
    }

    #[test]
    fn test_unnest_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("unnest"));
    }
}
