use chrono::{NaiveDateTime, Utc};
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_mktime(args: &[Value]) -> Result<Value> {
    let datetime_str = if args.len() > 1 && matches!(&args[1], Value::String(_)) {
        match &args[1] {
            Value::String(s) => s,
            _ => unreachable!(),
        }
    } else {
        match &args[0] {
            Value::String(s) => s,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "mktime() argument must be a string",
                ))
            }
        }
    };

    let dt = NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S").map_err(|_| {
        dsq_shared::error::operation_error(
            "mktime() invalid datetime format, expected 'YYYY-MM-DD HH:MM:SS'",
        )
    })?;

    let utc_dt = Utc.from_utc_datetime(&dt);
    Ok(Value::Int(utc_dt.timestamp()))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "mktime",
        func: builtin_mktime,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_mktime_valid_datetime() {
        // Test with a valid datetime string
        let result = builtin_mktime(&[Value::String("2021-01-01 00:00:00".to_string())]).unwrap();
        // 2021-01-01 00:00:00 UTC is 1609459200
        assert_eq!(result, Value::Int(1609459200));
    }

    #[test]
    fn test_mktime_valid_datetime_with_time() {
        // Test with a datetime including time
        let result = builtin_mktime(&[Value::String("2021-06-15 14:30:45".to_string())]).unwrap();
        // This should be 1623767445
        assert_eq!(result, Value::Int(1623767445));
    }

    #[test]
    fn test_mktime_invalid_format() {
        // Test with invalid format
        let result = builtin_mktime(&[Value::String("2021-01-01".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid datetime format"));
    }

    #[test]
    fn test_mktime_non_string_argument() {
        // Test with non-string argument
        let result = builtin_mktime(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("argument must be a string"));
    }

    #[test]
    fn test_mktime_no_args() {
        // Test with no arguments
        let result = builtin_mktime(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("argument must be a string"));
    }

    #[test]
    fn test_mktime_second_arg() {
        // Test with second argument as string
        let result = builtin_mktime(&[
            Value::Null,
            Value::String("2021-01-01 00:00:00".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Int(1609459200));
    }

    #[test]
    fn test_mktime_registered_via_inventory() {
        // Test that the function is registered via inventory
        let mut found = false;
        for func in inventory::iter::<crate::FunctionRegistration> {
            if func.name == "mktime" {
                found = true;
                // Test that the function works
                let result =
                    (func.func)(&[Value::String("2021-01-01 00:00:00".to_string())]).unwrap();
                assert_eq!(result, Value::Int(1609459200));
                break;
            }
        }
        assert!(found, "mktime function not found in inventory");
    }
}
