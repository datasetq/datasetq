use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use std::collections::HashMap;

pub fn builtin_transform_values(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "transform_values() expects 2 arguments",
        ));
    }

    let value = &args[0];
    let mapping = &args[1];

    if let Value::Object(mapping_obj) = mapping {
        transform_value_recursive(value, mapping_obj)
    } else {
        Err(dsq_shared::error::operation_error(
            "transform_values() second argument must be an object mapping old values to new values",
        ))
    }
}

fn transform_value_recursive(value: &Value, mapping: &HashMap<String, Value>) -> Result<Value> {
    match value {
        Value::Object(obj) => {
            let mut new_obj = HashMap::new();
            for (key, val) in obj {
                let transformed_val = transform_value_recursive(val, mapping)?;
                new_obj.insert(key.clone(), transformed_val);
            }
            Ok(Value::Object(new_obj))
        }
        Value::Array(arr) => {
            let mut new_arr = Vec::new();
            for item in arr {
                let transformed_item = transform_value_recursive(item, mapping)?;
                new_arr.push(transformed_item);
            }
            Ok(Value::Array(new_arr))
        }
        _ => {
            // For primitive values, check if there's a mapping
            let key = serde_json::to_string(value).map_err(|_| {
                dsq_shared::error::operation_error("Failed to serialize value for mapping")
            })?;
            if let Some(mapped_value) = mapping.get(&key) {
                Ok(mapped_value.clone())
            } else {
                Ok(value.clone())
            }
        }
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "transform_values",
        func: builtin_transform_values,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_transform_values_basic() {
        let mut obj = HashMap::new();
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("b".to_string(), Value::Int(2));

        let mut mapping = HashMap::new();
        mapping.insert("1".to_string(), Value::String("one".to_string()));
        mapping.insert("2".to_string(), Value::String("two".to_string()));

        let args = vec![Value::Object(obj), Value::Object(mapping)];
        let result = builtin_transform_values(&args).unwrap();

        if let Value::Object(result_obj) = result {
            assert_eq!(result_obj.get("a"), Some(&Value::String("one".to_string())));
            assert_eq!(result_obj.get("b"), Some(&Value::String("two".to_string())));
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_transform_values_array() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];

        let mut mapping = HashMap::new();
        mapping.insert("2".to_string(), Value::String("two".to_string()));

        let args = vec![Value::Array(arr), Value::Object(mapping)];
        let result = builtin_transform_values(&args).unwrap();

        if let Value::Array(result_arr) = result {
            assert_eq!(result_arr[0], Value::Int(1));
            assert_eq!(result_arr[1], Value::String("two".to_string()));
            assert_eq!(result_arr[2], Value::Int(3));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_transform_values_nested() {
        let mut inner_obj = HashMap::new();
        inner_obj.insert("x".to_string(), Value::Int(1));

        let mut obj = HashMap::new();
        obj.insert("nested".to_string(), Value::Object(inner_obj));
        obj.insert("value".to_string(), Value::Int(2));

        let mut mapping = HashMap::new();
        mapping.insert("1".to_string(), Value::String("one".to_string()));

        let args = vec![Value::Object(obj), Value::Object(mapping)];
        let result = builtin_transform_values(&args).unwrap();

        if let Value::Object(result_obj) = result {
            assert_eq!(result_obj.get("value"), Some(&Value::Int(2)));
            if let Some(Value::Object(nested)) = result_obj.get("nested") {
                assert_eq!(nested.get("x"), Some(&Value::String("one".to_string())));
            } else {
                panic!("Expected nested object");
            }
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_transform_values_no_mapping() {
        let mut obj = HashMap::new();
        obj.insert("a".to_string(), Value::Int(1));

        let mapping = HashMap::new();

        let args = vec![Value::Object(obj.clone()), Value::Object(mapping)];
        let result = builtin_transform_values(&args).unwrap();

        if let Value::Object(result_obj) = result {
            assert_eq!(result_obj, obj);
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_transform_values_wrong_args() {
        let args = vec![Value::String("test".to_string())];
        let result = builtin_transform_values(&args);
        assert!(result.is_err());

        let args = vec![
            Value::String("test".to_string()),
            Value::String("mapping".to_string()),
            Value::String("extra".to_string()),
        ];
        let result = builtin_transform_values(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_values_invalid_mapping() {
        let obj = HashMap::new();
        let args = vec![
            Value::Object(obj),
            Value::String("not an object".to_string()),
        ];
        let result = builtin_transform_values(&args);
        assert!(result.is_err());
    }
}
