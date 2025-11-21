use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use serde_json;
use std::collections::HashMap;

inventory::submit! {
    FunctionRegistration {
        name: "fromjson",
        func: builtin_fromjson,
    }
}

pub fn builtin_fromjson(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "fromjson() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            match serde_json::from_str::<serde_json::Value>(s) {
                Ok(json_val) => {
                    // Convert serde_json::Value to dsq Value
                    match json_val {
                        serde_json::Value::Null => Ok(Value::Null),
                        serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                Ok(Value::Int(i))
                            } else if let Some(f) = n.as_f64() {
                                Ok(Value::Float(f))
                            } else {
                                Ok(Value::String(n.to_string()))
                            }
                        }
                        serde_json::Value::String(s) => Ok(Value::String(s)),
                        serde_json::Value::Array(arr) => {
                            let values: Result<Vec<Value>> = arr
                                .into_iter()
                                .map(|v| {
                                    match v {
                                        serde_json::Value::Null => Ok(Value::Null),
                                        serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
                                        serde_json::Value::Number(n) => {
                                            if let Some(i) = n.as_i64() {
                                                Ok(Value::Int(i))
                                            } else if let Some(f) = n.as_f64() {
                                                Ok(Value::Float(f))
                                            } else {
                                                Ok(Value::String(n.to_string()))
                                            }
                                        }
                                        serde_json::Value::String(s) => Ok(Value::String(s)),
                                        _ => Ok(Value::String(v.to_string())), // Complex types as string
                                    }
                                })
                                .collect();
                            Ok(Value::Array(values?))
                        }
                        serde_json::Value::Object(obj) => {
                            let mut map = HashMap::new();
                            for (k, v) in obj {
                                let val = match v {
                                    serde_json::Value::Null => Value::Null,
                                    serde_json::Value::Bool(b) => Value::Bool(b),
                                    serde_json::Value::Number(n) => {
                                        if let Some(i) = n.as_i64() {
                                            Value::Int(i)
                                        } else if let Some(f) = n.as_f64() {
                                            Value::Float(f)
                                        } else {
                                            Value::String(n.to_string())
                                        }
                                    }
                                    serde_json::Value::String(s) => Value::String(s),
                                    _ => Value::String(v.to_string()),
                                };
                                map.insert(k, val);
                            }
                            Ok(Value::Object(map))
                        }
                    }
                }
                Err(_) => Err(dsq_shared::error::operation_error(
                    "fromjson() invalid JSON",
                )),
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "fromjson() requires string argument",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_fromjson_null() {
        let result = builtin_fromjson(&[Value::String("null".to_string())]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_fromjson_bool_true() {
        let result = builtin_fromjson(&[Value::String("true".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_builtin_fromjson_bool_false() {
        let result = builtin_fromjson(&[Value::String("false".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_builtin_fromjson_int() {
        let result = builtin_fromjson(&[Value::String("42".to_string())]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_builtin_fromjson_float() {
        let result = builtin_fromjson(&[Value::String("3.14".to_string())]).unwrap();
        assert_eq!(result, Value::Float(3.14));
    }

    #[test]
    fn test_builtin_fromjson_string() {
        let result = builtin_fromjson(&[Value::String("\"hello\"".to_string())]).unwrap();
        assert_eq!(result, Value::String("hello".to_string()));
    }

    #[test]
    fn test_builtin_fromjson_array() {
        let result = builtin_fromjson(&[Value::String("[1, 2, 3]".to_string())]).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
    }

    #[test]
    fn test_builtin_fromjson_object() {
        let result =
            builtin_fromjson(&[Value::String("{\"key\": \"value\"}".to_string())]).unwrap();
        let mut expected = HashMap::new();
        expected.insert("key".to_string(), Value::String("value".to_string()));
        assert_eq!(result, Value::Object(expected));
    }

    #[test]
    fn test_builtin_fromjson_complex_object() {
        let result = builtin_fromjson(&[Value::String(
            "{\"name\": \"John\", \"age\": 30, \"active\": true}".to_string(),
        )])
        .unwrap();
        let mut expected = HashMap::new();
        expected.insert("name".to_string(), Value::String("John".to_string()));
        expected.insert("age".to_string(), Value::Int(30));
        expected.insert("active".to_string(), Value::Bool(true));
        assert_eq!(result, Value::Object(expected));
    }

    #[test]
    fn test_builtin_fromjson_invalid_json() {
        let result = builtin_fromjson(&[Value::String("invalid".to_string())]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid JSON"));
    }

    #[test]
    fn test_builtin_fromjson_non_string_arg() {
        let result = builtin_fromjson(&[Value::Int(42)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string argument"));
    }

    #[test]
    fn test_builtin_fromjson_no_args() {
        let result = builtin_fromjson(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_builtin_fromjson_too_many_args() {
        let result = builtin_fromjson(&[
            Value::String("null".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_fromjson_registered_via_inventory() {
        println!("fromjson test running");
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("fromjson"));
    }
}
