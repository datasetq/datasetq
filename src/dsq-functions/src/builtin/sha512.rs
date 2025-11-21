use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use sha2::{Digest, Sha512};

pub fn builtin_sha512(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "sha512() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            let mut hasher = Sha512::new();
            hasher.update(s.as_bytes());
            let result = hasher.finalize();
            Ok(Value::String(format!("{:x}", result)))
        }
        _ => Err(dsq_shared::error::operation_error(
            "sha512() requires string argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "sha512",
        func: builtin_sha512,
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "SHA512",
        func: builtin_sha512,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_sha512_basic() {
        let result = builtin_sha512(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(result, Value::String("9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043".to_string()));
    }

    #[test]
    fn test_sha512_empty_string() {
        let result = builtin_sha512(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::String("cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e".to_string()));
    }

    #[test]
    fn test_sha512_non_string() {
        let result = builtin_sha512(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string argument"));
    }

    #[test]
    fn test_sha512_no_args() {
        let result = builtin_sha512(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_sha512_multiple_args() {
        let result = builtin_sha512(&[
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
    fn test_sha512_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("sha512"));
        assert!(registry.functions.contains_key("SHA512"));
    }
}
