use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_startswith(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "startswith() expects 2 arguments",
        ));
    }

    match (&args[0], &args[1]) {
        (Value::String(s), Value::String(prefix)) => Ok(Value::Bool(s.starts_with(prefix))),
        _ => Ok(Value::Bool(false)),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "startswith",
        func: builtin_startswith,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_startswith_basic() {
        let result = builtin_startswith(&[
            Value::String("hello world".to_string()),
            Value::String("hello".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_startswith_false() {
        let result = builtin_startswith(&[
            Value::String("hello world".to_string()),
            Value::String("world".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_startswith_empty_prefix() {
        let result = builtin_startswith(&[
            Value::String("hello".to_string()),
            Value::String("".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_startswith_empty_string() {
        let result = builtin_startswith(&[
            Value::String("".to_string()),
            Value::String("hello".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_startswith_both_empty() {
        let result =
            builtin_startswith(&[Value::String("".to_string()), Value::String("".to_string())])
                .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_startswith_prefix_longer() {
        let result = builtin_startswith(&[
            Value::String("hi".to_string()),
            Value::String("hello".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_startswith_same_string() {
        let result = builtin_startswith(&[
            Value::String("hello".to_string()),
            Value::String("hello".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_startswith_unicode() {
        let result = builtin_startswith(&[
            Value::String("héllo world".to_string()),
            Value::String("héllo".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_startswith_non_string_first_arg() {
        let result =
            builtin_startswith(&[Value::Int(123), Value::String("1".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_startswith_non_string_second_arg() {
        let result =
            builtin_startswith(&[Value::String("123".to_string()), Value::Int(1)]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_startswith_both_non_string() {
        let result = builtin_startswith(&[Value::Int(123), Value::Int(1)]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_startswith_no_args() {
        let result = builtin_startswith(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_startswith_one_arg() {
        let result = builtin_startswith(&[Value::String("hello".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_startswith_three_args() {
        let result = builtin_startswith(&[
            Value::String("hello".to_string()),
            Value::String("he".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }
}
