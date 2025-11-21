use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_uppercase(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "uppercase() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.to_uppercase())),
        _ => Err(dsq_shared::error::operation_error(
            "uppercase() requires string argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "uppercase",
        func: builtin_uppercase,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_uppercase_basic() {
        let result = builtin_uppercase(&[Value::String("hello world".to_string())]).unwrap();
        assert_eq!(result, Value::String("HELLO WORLD".to_string()));
    }

    #[test]
    fn test_uppercase_already_upper() {
        let result = builtin_uppercase(&[Value::String("HELLO WORLD".to_string())]).unwrap();
        assert_eq!(result, Value::String("HELLO WORLD".to_string()));
    }

    #[test]
    fn test_uppercase_mixed_case() {
        let result = builtin_uppercase(&[Value::String("HeLLo WoRlD".to_string())]).unwrap();
        assert_eq!(result, Value::String("HELLO WORLD".to_string()));
    }

    #[test]
    fn test_uppercase_empty_string() {
        let result = builtin_uppercase(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_uppercase_unicode() {
        let result = builtin_uppercase(&[Value::String("héllo wörld".to_string())]).unwrap();
        assert_eq!(result, Value::String("HÉLLO WÖRLD".to_string()));
    }

    #[test]
    fn test_uppercase_numbers_and_symbols() {
        let result = builtin_uppercase(&[Value::String("123!@#abc".to_string())]).unwrap();
        assert_eq!(result, Value::String("123!@#ABC".to_string()));
    }

    #[test]
    fn test_uppercase_non_string() {
        let result = builtin_uppercase(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string argument"));
    }

    #[test]
    fn test_uppercase_no_args() {
        let result = builtin_uppercase(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_uppercase_multiple_args() {
        let result = builtin_uppercase(&[
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }
}
