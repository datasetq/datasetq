use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_endswith(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "endswith() expects 2 arguments",
        ));
    }

    match (&args[0], &args[1]) {
        (Value::String(s), Value::String(suffix)) => Ok(Value::Bool(s.ends_with(suffix))),
        _ => Ok(Value::Bool(false)),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "endswith",
        func: builtin_endswith,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_endswith_basic() {
        let result = builtin_endswith(&[
            Value::String("hello world".to_string()),
            Value::String("world".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(true));

        let result = builtin_endswith(&[
            Value::String("hello world".to_string()),
            Value::String("hello".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_endswith_empty_suffix() {
        let result = builtin_endswith(&[
            Value::String("hello".to_string()),
            Value::String("".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_endswith_empty_string() {
        let result = builtin_endswith(&[
            Value::String("".to_string()),
            Value::String("world".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_endswith_non_string_args() {
        let result =
            builtin_endswith(&[Value::Int(123), Value::String("world".to_string())]).unwrap();
        assert_eq!(result, Value::Bool(false));

        let result =
            builtin_endswith(&[Value::String("hello".to_string()), Value::Int(123)]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_endswith_wrong_arg_count() {
        let result = builtin_endswith(&[Value::String("hello".to_string())]);
        assert!(result.is_err());

        let result = builtin_endswith(&[
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_endswith_registered_via_inventory() {
        let mut found = false;
        for func in inventory::iter::<crate::FunctionRegistration> {
            if func.name == "endswith" {
                found = true;
                let result = (func.func)(&[
                    Value::String("test".to_string()),
                    Value::String("st".to_string()),
                ])
                .unwrap();
                assert_eq!(result, Value::Bool(true));
                break;
            }
        }
        assert!(found, "endswith function not found in inventory");
    }
}
