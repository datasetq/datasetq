use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_has(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "has() expects 2 arguments",
        ));
    }

    let key = match &args[1] {
        Value::String(s) => s,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "has() key must be a string",
            ));
        }
    };

    match &args[0] {
        Value::Object(obj) => Ok(Value::Bool(obj.contains_key(key))),
        _ => Ok(Value::Bool(false)),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "has",
        func: builtin_has,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_has_object_true() {
        let mut obj = HashMap::new();
        obj.insert("key1".to_string(), Value::String("value1".to_string()));
        obj.insert("key2".to_string(), Value::Int(42));
        let result = builtin_has(&[Value::Object(obj), Value::String("key1".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_has_object_false() {
        let mut obj = HashMap::new();
        obj.insert("key1".to_string(), Value::String("value1".to_string()));
        let result = builtin_has(&[Value::Object(obj), Value::String("key2".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_has_non_object() {
        let result = builtin_has(&[
            Value::String("test".to_string()),
            Value::String("key".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_has_array() {
        let arr = vec![Value::Int(1), Value::Int(2)];
        let result = builtin_has(&[Value::Array(arr), Value::String("key".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_has_int() {
        let result = builtin_has(&[Value::Int(123), Value::String("key".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_has_null() {
        let result = builtin_has(&[Value::Null, Value::String("key".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_has_bool() {
        let result = builtin_has(&[Value::Bool(true), Value::String("key".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_has_float() {
        let result = builtin_has(&[
            Value::Float(std::f64::consts::PI),
            Value::String("key".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_has_empty_object() {
        let obj = HashMap::new();
        let result = builtin_has(&[Value::Object(obj), Value::String("key".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_has_key_not_string() {
        let mut obj = HashMap::new();
        obj.insert("key1".to_string(), Value::String("value1".to_string()));
        let result = builtin_has(&[Value::Object(obj), Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("key must be a string"));
    }

    #[test]
    fn test_has_no_args() {
        let result = builtin_has(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_has_one_arg() {
        let result = builtin_has(&[Value::String("test".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_has_three_args() {
        let result = builtin_has(&[
            Value::String("test".to_string()),
            Value::String("key".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_has_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("has"));
    }
}
