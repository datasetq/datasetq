use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use std::collections::HashMap;

pub fn builtin_transform_keys(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "transform_keys() expects 2 arguments",
        ));
    }

    match (&args[0], &args[1]) {
        (Value::Object(obj), Value::Object(mapping)) => {
            let mut new_obj = HashMap::new();
            for (key, value) in obj {
                if let Some(new_key) = mapping.get(key) {
                    if let Value::String(s) = new_key {
                        new_obj.insert(s.clone(), value.clone());
                    } else {
                        return Err(dsq_shared::error::operation_error(
                            "transform_keys() mapping values must be strings",
                        ));
                    }
                } else {
                    new_obj.insert(key.clone(), value.clone());
                }
            }
            Ok(Value::Object(new_obj))
        }
        _ => Err(dsq_shared::error::operation_error(
            "transform_keys() expects (object, object)",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "transform_keys",
        func: builtin_transform_keys,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_transform_keys_basic() {
        let mut obj = HashMap::new();
        obj.insert("old_key".to_string(), Value::String("value".to_string()));
        obj.insert("another_key".to_string(), Value::Int(42));

        let mut mapping = HashMap::new();
        mapping.insert("old_key".to_string(), Value::String("new_key".to_string()));

        let args = vec![Value::Object(obj), Value::Object(mapping)];
        let result = builtin_transform_keys(&args).unwrap();

        if let Value::Object(result_obj) = result {
            assert!(result_obj.contains_key("new_key"));
            assert!(!result_obj.contains_key("old_key"));
            assert!(result_obj.contains_key("another_key"));
            assert_eq!(
                result_obj.get("new_key"),
                Some(&Value::String("value".to_string()))
            );
            assert_eq!(result_obj.get("another_key"), Some(&Value::Int(42)));
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_transform_keys_no_mapping() {
        let mut obj = HashMap::new();
        obj.insert("key1".to_string(), Value::String("value1".to_string()));
        obj.insert("key2".to_string(), Value::Int(42));

        let mapping = HashMap::new();

        let args = vec![Value::Object(obj.clone()), Value::Object(mapping)];
        let result = builtin_transform_keys(&args).unwrap();

        if let Value::Object(result_obj) = result {
            assert_eq!(result_obj, obj);
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_transform_keys_invalid_mapping_value() {
        let mut obj = HashMap::new();
        obj.insert("key".to_string(), Value::String("value".to_string()));

        let mut mapping = HashMap::new();
        mapping.insert("key".to_string(), Value::Int(123)); // Should be string

        let args = vec![Value::Object(obj), Value::Object(mapping)];
        let result = builtin_transform_keys(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_keys_wrong_args() {
        let args = vec![Value::String("test".to_string())];
        let result = builtin_transform_keys(&args);
        assert!(result.is_err());

        let args = vec![
            Value::String("test".to_string()),
            Value::String("mapping".to_string()),
            Value::String("extra".to_string()),
        ];
        let result = builtin_transform_keys(&args);
        assert!(result.is_err());
    }
}
