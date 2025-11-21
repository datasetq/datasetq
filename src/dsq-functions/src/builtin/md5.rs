use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use md5;

pub fn builtin_md5(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "md5() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            let digest = md5::compute(s.as_bytes());
            Ok(Value::String(format!("{:x}", digest)))
        }
        _ => Err(dsq_shared::error::operation_error(
            "md5() requires string argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "md5",
        func: builtin_md5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_md5_basic() {
        let result = builtin_md5(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(
            result,
            Value::String("5d41402abc4b2a76b9719d911017c592".to_string())
        );
    }

    #[test]
    fn test_md5_empty_string() {
        let result = builtin_md5(&[Value::String("".to_string())]).unwrap();
        assert_eq!(
            result,
            Value::String("d41d8cd98f00b204e9800998ecf8427e".to_string())
        );
    }

    #[test]
    fn test_md5_non_string() {
        let result = builtin_md5(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string argument"));
    }

    #[test]
    fn test_md5_no_args() {
        let result = builtin_md5(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_md5_multiple_args() {
        let result = builtin_md5(&[
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_md5_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("md5"));
    }
}
