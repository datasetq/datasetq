use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "tostring",
        func: builtin_tostring,
    }
}

pub fn builtin_tostring(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "tostring() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.clone())),
        Value::Int(i) => Ok(Value::String(i.to_string())),
        Value::Float(f) => Ok(Value::String(f.to_string())),
        Value::Bool(b) => Ok(Value::String(b.to_string())),
        Value::Null => Ok(Value::String("null".to_string())),
        Value::Array(arr) => {
            let strings: Vec<String> = arr.iter().map(|v| v.to_string()).collect();
            Ok(Value::String(format!("[{}]", strings.join(", "))))
        }
        Value::Object(obj) => {
            let pairs: Vec<String> = obj.iter().map(|(k, v)| format!("{}: {}", k, v)).collect();
            Ok(Value::String(format!("{{{}}}", pairs.join(", "))))
        }
        _ => Ok(Value::String(args[0].to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_tostring_registered_via_inventory() {
        let mut found = false;
        for func in inventory::iter::<FunctionRegistration> {
            if func.name == "tostring" {
                found = true;
                // Test that the function works
                let result = (func.func)(&[Value::Int(42)]).unwrap();
                assert_eq!(result, Value::String("42".to_string()));
                break;
            }
        }
        assert!(found, "tostring function not found in inventory");
    }

    #[test]
    fn test_tostring_int() {
        let result = builtin_tostring(&[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::String("42".to_string()));
    }

    #[test]
    fn test_tostring_float() {
        let result = builtin_tostring(&[Value::Float(3.14)]).unwrap();
        assert_eq!(result, Value::String("3.14".to_string()));
    }

    #[test]
    fn test_tostring_bool() {
        let result = builtin_tostring(&[Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::String("true".to_string()));
    }

    #[test]
    fn test_tostring_null() {
        let result = builtin_tostring(&[Value::Null]).unwrap();
        assert_eq!(result, Value::String("null".to_string()));
    }

    #[test]
    fn test_tostring_string() {
        let result = builtin_tostring(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(result, Value::String("hello".to_string()));
    }

    #[test]
    fn test_tostring_array() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_tostring(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::String("[1, 2, 3]".to_string()));
    }

    #[test]
    fn test_tostring_object() {
        let mut obj = std::collections::HashMap::new();
        obj.insert("key".to_string(), Value::String("value".to_string()));
        let result = builtin_tostring(&[Value::Object(obj)]).unwrap();
        assert_eq!(result, Value::String("{key: \"value\"}".to_string()));
    }

    #[test]
    fn test_tostring_wrong_args() {
        let result = builtin_tostring(&[]);
        assert!(result.is_err());
        let result = builtin_tostring(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
    }
}
