use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "trim",
        func: builtin_trim,
    }
}

pub fn builtin_trim(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "trim() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.trim().to_string())),
        _ => Err(dsq_shared::error::operation_error(
            "trim() requires string argument",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_trim_string() {
        let result = builtin_trim(&[Value::String("  hello world  ".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("hello world".to_string()));
    }

    #[test]
    fn test_trim_no_whitespace() {
        let result = builtin_trim(&[Value::String("hello".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("hello".to_string()));
    }

    #[test]
    fn test_trim_empty_string() {
        let result = builtin_trim(&[Value::String("".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("".to_string()));
    }

    #[test]
    fn test_trim_whitespace_only() {
        let result = builtin_trim(&[Value::String("   ".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("".to_string()));
    }

    #[test]
    fn test_trim_tabs_and_spaces() {
        let result = builtin_trim(&[Value::String("\t  hello  \t".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("hello".to_string()));
    }

    #[test]
    fn test_trim_newlines() {
        let result = builtin_trim(&[Value::String("\nhello\n".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("hello".to_string()));
    }

    #[test]
    fn test_trim_no_args() {
        let result = builtin_trim(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("trim() expects 1 argument"));
    }

    #[test]
    fn test_trim_too_many_args() {
        let result = builtin_trim(&[
            Value::String("test".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("trim() expects 1 argument"));
    }

    #[test]
    fn test_trim_non_string() {
        let result = builtin_trim(&[Value::Int(42)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("trim() requires string argument"));
    }

    #[test]
    fn test_trim_registered_via_inventory() {
        // Test that the function is registered via inventory
        let mut found = false;
        for func in inventory::iter::<FunctionRegistration> {
            if func.name == "trim" {
                found = true;
                // Test that calling the function works
                let result = (func.func)(&[Value::String("  test  ".to_string())]);
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), Value::String("test".to_string()));
                break;
            }
        }
        assert!(found, "trim function not found in inventory");
    }
}
