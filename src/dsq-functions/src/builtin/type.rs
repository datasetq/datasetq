use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_type(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "type() expects 1 argument",
        ));
    }

    let type_name = args[0].type_name();
    Ok(Value::String(type_name.to_string()))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "type",
        func: builtin_type,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_type_int() {
        let result = builtin_type(&[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::String("integer".to_string()));
    }

    #[test]
    fn test_builtin_type_float() {
        let result = builtin_type(&[Value::Float(3.14)]).unwrap();
        assert_eq!(result, Value::String("float".to_string()));
    }

    #[test]
    fn test_builtin_type_string() {
        let result = builtin_type(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(result, Value::String("string".to_string()));
    }

    #[test]
    fn test_builtin_type_bool() {
        let result = builtin_type(&[Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::String("boolean".to_string()));
    }

    #[test]
    fn test_builtin_type_null() {
        let result = builtin_type(&[Value::Null]).unwrap();
        assert_eq!(result, Value::String("null".to_string()));
    }

    #[test]
    fn test_builtin_type_array() {
        let arr = vec![Value::Int(1), Value::Int(2)];
        let result = builtin_type(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::String("array".to_string()));
    }

    #[test]
    fn test_builtin_type_object() {
        let mut obj = std::collections::HashMap::new();
        obj.insert("key".to_string(), Value::String("value".to_string()));
        let result = builtin_type(&[Value::Object(obj)]).unwrap();
        assert_eq!(result, Value::String("object".to_string()));
    }

    #[test]
    fn test_builtin_type_no_args() {
        let result = builtin_type(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_builtin_type_too_many_args() {
        let result = builtin_type(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_builtin_type_registered_via_inventory() {
        // Test that the function is registered via inventory
        let mut found = false;
        for func in inventory::iter::<crate::FunctionRegistration> {
            if func.name == "type" {
                found = true;
                // Test that the function works
                let result = (func.func)(&[Value::Int(42)]).unwrap();
                assert_eq!(result, Value::String("integer".to_string()));
                break;
            }
        }
        assert!(found, "type function not found in inventory");
    }
}
