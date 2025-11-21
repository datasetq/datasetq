use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_toupper(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "toupper() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.to_uppercase())),
        _ => Err(dsq_shared::error::operation_error(
            "toupper() requires string argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "toupper",
        func: builtin_toupper,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_toupper_basic() {
        let result = builtin_toupper(&[Value::String("hello world".to_string())]).unwrap();
        assert_eq!(result, Value::String("HELLO WORLD".to_string()));
    }

    #[test]
    fn test_toupper_already_upper() {
        let result = builtin_toupper(&[Value::String("HELLO WORLD".to_string())]).unwrap();
        assert_eq!(result, Value::String("HELLO WORLD".to_string()));
    }

    #[test]
    fn test_toupper_mixed_case() {
        let result = builtin_toupper(&[Value::String("HeLLo WoRlD".to_string())]).unwrap();
        assert_eq!(result, Value::String("HELLO WORLD".to_string()));
    }

    #[test]
    fn test_toupper_empty_string() {
        let result = builtin_toupper(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_toupper_unicode() {
        let result = builtin_toupper(&[Value::String("héllo wörld".to_string())]).unwrap();
        assert_eq!(result, Value::String("HÉLLO WÖRLD".to_string()));
    }

    #[test]
    fn test_toupper_numbers_and_symbols() {
        let result = builtin_toupper(&[Value::String("123!@#abc".to_string())]).unwrap();
        assert_eq!(result, Value::String("123!@#ABC".to_string()));
    }

    #[test]
    fn test_toupper_non_string() {
        let result = builtin_toupper(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string argument"));
    }

    #[test]
    fn test_toupper_no_args() {
        let result = builtin_toupper(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_toupper_multiple_args() {
        let result = builtin_toupper(&[
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
