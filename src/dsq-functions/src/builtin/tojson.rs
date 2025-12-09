use crate::inventory;
use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;

inventory::submit! {
    FunctionRegistration {
        name: "tojson",
        func: builtin_tojson,
    }
}

pub fn builtin_tojson(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "tojson() expects 1 argument",
        ));
    }

    match serde_json::to_string(&args[0]) {
        Ok(json) => Ok(Value::String(json)),
        Err(_) => Err(dsq_shared::error::operation_error(
            "tojson() failed to serialize",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_tojson_string() {
        let input = Value::String("hello".to_string());
        let result = builtin_tojson(&[input]).unwrap();
        assert_eq!(result, Value::String("\"hello\"".to_string()));
    }

    #[test]
    fn test_tojson_int() {
        let input = Value::Int(42);
        let result = builtin_tojson(&[input]).unwrap();
        assert_eq!(result, Value::String("42".to_string()));
    }

    #[test]
    fn test_tojson_float() {
        let input = Value::Float(std::f64::consts::PI);
        let result = builtin_tojson(&[input]).unwrap();
        assert_eq!(result, Value::String("3.141592653589793".to_string()));
    }

    #[test]
    fn test_tojson_bool() {
        let input = Value::Bool(true);
        let result = builtin_tojson(&[input]).unwrap();
        assert_eq!(result, Value::String("true".to_string()));
    }

    #[test]
    fn test_tojson_null() {
        let input = Value::Null;
        let result = builtin_tojson(&[input]).unwrap();
        assert_eq!(result, Value::String("null".to_string()));
    }

    #[test]
    fn test_tojson_array() {
        let input = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let result = builtin_tojson(&[input]).unwrap();
        assert_eq!(result, Value::String("[1,2,3]".to_string()));
    }

    #[test]
    fn test_tojson_object() {
        let mut obj = std::collections::HashMap::new();
        obj.insert("key".to_string(), Value::String("value".to_string()));
        let input = Value::Object(obj);
        let result = builtin_tojson(&[input]).unwrap();
        assert_eq!(result, Value::String("{\"key\":\"value\"}".to_string()));
    }

    #[test]
    fn test_tojson_no_args() {
        let result = builtin_tojson(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_tojson_too_many_args() {
        let input1 = Value::Int(1);
        let input2 = Value::Int(2);
        let result = builtin_tojson(&[input1, input2]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_tojson_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("tojson"));
    }
}
