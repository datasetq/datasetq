use crate::FunctionRegistration;
use chrono::Utc;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "now",
        func: builtin_now,
    }
}

pub fn builtin_now(args: &[Value]) -> Result<Value> {
    if !args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "now() expects no arguments",
        ));
    }
    let now = Utc::now();
    Ok(Value::String(
        now.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_now_no_args() {
        let result = builtin_now(&[]);
        assert!(result.is_ok());
        let value = result.unwrap();
        match value {
            Value::String(s) => {
                // Check that the string matches the expected format
                // Should be something like "2025-10-02 12:34:56 UTC"
                assert!(s.len() == 23); // "YYYY-MM-DD HH:MM:SS UTC" is 23 chars
                assert!(s.ends_with(" UTC"));
                // Check date format
                let parts: Vec<&str> = s.split(' ').collect();
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[2], "UTC");
                let date_parts: Vec<&str> = parts[0].split('-').collect();
                assert_eq!(date_parts.len(), 3);
                let time_parts: Vec<&str> = parts[1].split(':').collect();
                assert_eq!(time_parts.len(), 3);
            }
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_now_with_args() {
        let result = builtin_now(&[Value::Int(1)]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("now() expects no arguments"));
    }

    #[test]
    fn test_now_registered_via_inventory() {
        // Test that the function is registered via inventory
        let mut found = false;
        for func in inventory::iter::<FunctionRegistration> {
            if func.name == "now" {
                found = true;
                // Test that calling the function works
                let result = (func.func)(&[]);
                assert!(result.is_ok());
                break;
            }
        }
        assert!(found, "now function not found in inventory");
    }
}
