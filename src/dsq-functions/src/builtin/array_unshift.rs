use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_array_unshift(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(dsq_shared::error::operation_error(
            "array_unshift() expects at least 2 arguments",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let mut new_arr = args[1..].to_vec();
            new_arr.extend(arr.clone());
            Ok(Value::Int(new_arr.len() as i64))
        }
        _ => Err(dsq_shared::error::operation_error(
            "array_unshift() first argument must be an array",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "array_unshift",
        func: builtin_array_unshift,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_array_unshift_basic() {
        let args = vec![
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            Value::Int(0),
        ];
        let result = builtin_array_unshift(&args);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(3));
    }

    #[test]
    fn test_array_unshift_multiple_elements() {
        let args = vec![
            Value::Array(vec![Value::Int(3), Value::Int(4)]),
            Value::Int(1),
            Value::Int(2),
        ];
        let result = builtin_array_unshift(&args);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(4));
    }

    #[test]
    fn test_array_unshift_empty_array() {
        let args = vec![Value::Array(vec![]), Value::Int(1)];
        let result = builtin_array_unshift(&args);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(1));
    }

    #[test]
    fn test_array_unshift_wrong_args_count() {
        let args = vec![Value::Array(vec![Value::Int(1)])];
        let result = builtin_array_unshift(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_unshift_invalid_input() {
        let args = vec![Value::String("not an array".to_string()), Value::Int(1)];
        let result = builtin_array_unshift(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_unshift_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("array_unshift"));
    }
}
