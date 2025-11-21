use base58::{FromBase58, ToBase58};
use dsq_shared::value::Value;
use dsq_shared::Result;

pub fn builtin_base58_decode(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "base58_decode() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            match s.from_base58() {
                Ok(bytes) => match String::from_utf8(bytes.clone()) {
                    Ok(decoded) => Ok(Value::String(decoded)),
                    Err(_) => {
                        // If not valid UTF-8, return as hex string
                        let hex_string = bytes
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>();
                        Ok(Value::String(hex_string))
                    }
                },
                Err(_) => Err(dsq_shared::error::operation_error(
                    "base58_decode() invalid base58",
                )),
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "base58_decode() requires string argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "base58_decode",
        func: builtin_base58_decode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_base58_decode_valid_utf8() {
        // Test decoding "hello world" which should encode to "StV1DL6CwTryKyV"
        let encoded = "StV1DL6CwTryKyV";
        let result = builtin_base58_decode(&[Value::String(encoded.to_string())]).unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    #[test]
    fn test_base58_decode_empty_string() {
        let result = builtin_base58_decode(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_base58_decode_invalid_base58() {
        let result = builtin_base58_decode(&[Value::String("invalid!@#".to_string())]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid base58"));
    }

    #[test]
    fn test_base58_decode_null_byte() {
        // Test with null byte - this is valid UTF-8
        let encoded = "1"; // This decodes to a single null byte 0x00
        let result = builtin_base58_decode(&[Value::String(encoded.to_string())]).unwrap();
        // Null byte is valid UTF-8, so it should return the string with null byte
        assert_eq!(result, Value::String("\0".to_string()));
    }

    #[test]
    fn test_base58_decode_wrong_arg_count() {
        // No arguments
        let result = builtin_base58_decode(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Too many arguments
        let result = builtin_base58_decode(&[
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
    fn test_base58_decode_wrong_arg_type() {
        let result = builtin_base58_decode(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string argument"));
    }

    #[test]
    fn test_base58_decode_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("base58_decode"));
    }

    #[test]
    fn test_base58_decode_roundtrip() {
        use base58::ToBase58;

        let original = "test string with spaces";
        let encoded = original.as_bytes().to_base58();
        let result = builtin_base58_decode(&[Value::String(encoded)]).unwrap();
        assert_eq!(result, Value::String(original.to_string()));
    }

    #[test]
    fn test_base58_decode_binary_data() {
        // Test with actual binary data that would not be valid UTF-8
        // Use bytes that form invalid UTF-8 sequence
        let binary_data = vec![0xff, 0xfe, 0xfd]; // Invalid UTF-8 bytes
        let encoded = binary_data.to_base58();
        let result = builtin_base58_decode(&[Value::String(encoded)]).unwrap();
        // Should return hex representation since it's not valid UTF-8
        match result {
            Value::String(s) => {
                // Should be hex representation
                assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
                assert_eq!(s.len(), 6); // 3 bytes * 2 hex chars each
                assert_eq!(s, "fffefd");
            }
            _ => panic!("Expected string result"),
        }
    }
}
