use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_concat(args: &[Value]) -> Result<Value> {
    let mut result = String::new();
    for arg in args {
        match arg {
            Value::String(s) => result.push_str(s),
            _ => result.push_str(&arg.to_string()),
        }
    }
    Ok(Value::String(result))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "concat",
        func: builtin_concat,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_concat_strings() {
        let result = builtin_concat(&[
            Value::String("hello".to_string()),
            Value::String(" ".to_string()),
            Value::String("world".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    #[test]
    fn test_concat_mixed_types() {
        let result =
            builtin_concat(&[Value::String("count: ".to_string()), Value::Int(42)]).unwrap();
        assert_eq!(result, Value::String("count: 42".to_string()));
    }

    #[test]
    fn test_concat_single_arg() {
        let result = builtin_concat(&[Value::String("single".to_string())]).unwrap();
        assert_eq!(result, Value::String("single".to_string()));
    }

    #[test]
    fn test_concat_no_args() {
        let result = builtin_concat(&[]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_concat_arrays() {
        let arr = vec![Value::Int(1), Value::Int(2)];
        let result = builtin_concat(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::String("[1, 2]".to_string()));
    }

    #[test]
    fn test_concat_objects() {
        let mut obj = std::collections::HashMap::new();
        obj.insert("key".to_string(), Value::String("value".to_string()));
        let result = builtin_concat(&[Value::Object(obj)]).unwrap();
        // Object to_string might vary, but should contain the key-value
        let result_str = match result {
            Value::String(s) => s,
            _ => panic!("Expected string"),
        };
        assert!(result_str.contains("key"));
        assert!(result_str.contains("value"));
    }

    #[test]
    fn test_concat_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("concat"));
    }
}
