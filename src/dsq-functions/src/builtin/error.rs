use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "error",
        func: builtin_error,
    }
}

pub fn builtin_error(args: &[Value]) -> Result<Value> {
    let message = if args.is_empty() {
        "error".to_string()
    } else {
        args[0].to_string()
    };

    Err(dsq_shared::error::operation_error(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_error_no_args() {
        let result = builtin_error(&[]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("error"));
    }

    #[test]
    fn test_error_with_string_arg() {
        let result = builtin_error(&[Value::String("custom error message".to_string())]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("custom error message"));
    }

    #[test]
    fn test_error_with_int_arg() {
        let result = builtin_error(&[Value::Int(42)]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("42"));
    }

    #[test]
    fn test_error_with_array_arg() {
        let arr = vec![Value::Int(1), Value::Int(2)];
        let result = builtin_error(&[Value::Array(arr)]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("[1, 2]"));
    }

    #[test]
    fn test_error_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("error"));
    }
}
