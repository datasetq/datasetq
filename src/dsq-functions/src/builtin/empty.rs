use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_empty(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "empty() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => Ok(Value::Bool(arr.is_empty())),
        Value::Object(obj) => Ok(Value::Bool(obj.is_empty())),
        Value::String(s) => Ok(Value::Bool(s.is_empty())),
        Value::Null => Ok(Value::Bool(true)),
        _ => Ok(Value::Bool(false)),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "empty",
        func: builtin_empty,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_empty_array() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_empty(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_empty_empty_array() {
        let arr: Vec<Value> = vec![];
        let result = builtin_empty(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_empty_string() {
        let result = builtin_empty(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_empty_empty_string() {
        let result = builtin_empty(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_empty_object() {
        let mut obj = HashMap::new();
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("b".to_string(), Value::Int(2));
        let result = builtin_empty(&[Value::Object(obj)]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_empty_empty_object() {
        let obj = HashMap::new();
        let result = builtin_empty(&[Value::Object(obj)]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_empty_null() {
        let result = builtin_empty(&[Value::Null]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_empty_other_values() {
        let result = builtin_empty(&[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Bool(false));

        let result = builtin_empty(&[Value::Float(3.14)]).unwrap();
        assert_eq!(result, Value::Bool(false));

        let result = builtin_empty(&[Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_empty_no_args() {
        let result = builtin_empty(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_empty_too_many_args() {
        let result = builtin_empty(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }
}
