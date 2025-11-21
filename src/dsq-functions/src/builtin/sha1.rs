use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use sha1::{Digest, Sha1};

pub fn builtin_sha1(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "sha1() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            let mut hasher = Sha1::new();
            hasher.update(s.as_bytes());
            let result = hasher.finalize();
            Ok(Value::String(format!("{:x}", result)))
        }
        _ => Err(dsq_shared::error::operation_error(
            "sha1() requires string argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "sha1",
        func: builtin_sha1,
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "SHA1",
        func: builtin_sha1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_sha1_basic() {
        let result = builtin_sha1(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(
            result,
            Value::String("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d".to_string())
        );
    }

    #[test]
    fn test_sha1_empty_string() {
        let result = builtin_sha1(&[Value::String("".to_string())]).unwrap();
        assert_eq!(
            result,
            Value::String("da39a3ee5e6b4b0d3255bfef95601890afd80709".to_string())
        );
    }

    #[test]
    fn test_sha1_non_string() {
        let result = builtin_sha1(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string argument"));
    }

    #[test]
    fn test_sha1_no_args() {
        let result = builtin_sha1(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_sha1_multiple_args() {
        let result = builtin_sha1(&[
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
    fn test_sha1_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("sha1"));
        assert!(registry.functions.contains_key("SHA1"));
    }
}
