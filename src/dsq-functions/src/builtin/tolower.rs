use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_tolower(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "tolower() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.to_lowercase())),
        _ => Err(dsq_shared::error::operation_error(
            "tolower() requires string argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "tolower",
        func: builtin_tolower,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_tolower_basic() {
        let result = builtin_tolower(&[Value::String("HELLO WORLD".to_string())]).unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    #[test]
    fn test_tolower_already_lower() {
        let result = builtin_tolower(&[Value::String("hello world".to_string())]).unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    #[test]
    fn test_tolower_mixed_case() {
        let result = builtin_tolower(&[Value::String("HeLLo WoRlD".to_string())]).unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    #[test]
    fn test_tolower_empty_string() {
        let result = builtin_tolower(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_tolower_unicode() {
        let result = builtin_tolower(&[Value::String("HÉLLO WÖRLD".to_string())]).unwrap();
        assert_eq!(result, Value::String("héllo wörld".to_string()));
    }

    #[test]
    fn test_tolower_numbers_and_symbols() {
        let result = builtin_tolower(&[Value::String("123!@#ABC".to_string())]).unwrap();
        assert_eq!(result, Value::String("123!@#abc".to_string()));
    }

    #[test]
    fn test_tolower_non_string() {
        let result = builtin_tolower(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string argument"));
    }

    #[test]
    fn test_tolower_no_args() {
        let result = builtin_tolower(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_tolower_multiple_args() {
        let result = builtin_tolower(&[
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
