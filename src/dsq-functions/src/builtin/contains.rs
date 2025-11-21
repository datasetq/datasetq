use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_contains(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "contains() expects 2 arguments",
        ));
    }

    match (&args[0], &args[1]) {
        (Value::String(s), Value::String(sub)) => Ok(Value::Bool(s.contains(sub))),
        (Value::Array(arr), _) => Ok(Value::Bool(arr.contains(&args[1]))),
        _ => Ok(Value::Bool(false)),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "contains",
        func: builtin_contains,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_contains_string_true() {
        let result = builtin_contains(&[
            Value::String("hello world".to_string()),
            Value::String("world".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_contains_string_false() {
        let result = builtin_contains(&[
            Value::String("hello world".to_string()),
            Value::String("foo".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_contains_array_true() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_contains(&[Value::Array(arr), Value::Int(2)]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_contains_array_false() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_contains(&[Value::Array(arr), Value::Int(4)]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_contains_array_string() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let result =
            builtin_contains(&[Value::Array(arr), Value::String("a".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_contains_other_types() {
        let result = builtin_contains(&[Value::Int(123), Value::Int(1)]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_contains_no_args() {
        let result = builtin_contains(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_contains_one_arg() {
        let result = builtin_contains(&[Value::String("test".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_contains_three_args() {
        let result = builtin_contains(&[
            Value::String("test".to_string()),
            Value::String("t".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_contains_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("contains"));
    }
}
