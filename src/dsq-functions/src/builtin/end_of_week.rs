use chrono::{Datelike, Weekday};
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_end_of_week(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "end_of_week() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(_) | Value::Float(_) | Value::String(_) => {
            let dt = crate::extract_timestamp(&args[0])?;
            // Sunday is the end of the week (ISO 8601 standard where weeks start on Monday)
            let is_end_of_week = dt.weekday() == Weekday::Sun;
            Ok(Value::Bool(is_end_of_week))
        }
        Value::Array(arr) => {
            let results: Result<Vec<Value>> = arr
                .iter()
                .map(|v| {
                    if matches!(v, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                        let dt = crate::extract_timestamp(v)?;
                        let is_end_of_week = dt.weekday() == Weekday::Sun;
                        Ok(Value::Bool(is_end_of_week))
                    } else {
                        Ok(Value::Null)
                    }
                })
                .collect();
            Ok(Value::Array(results?))
        }
        _ => Err(dsq_shared::error::operation_error(
            "end_of_week() argument must be a date value or array",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "end_of_week",
        func: builtin_end_of_week,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_end_of_week() {
        // Test Sunday (end of week)
        let sunday = Value::String("2023-10-01".to_string()); // October 1, 2023 is a Sunday
        let result = builtin_end_of_week(&[sunday]).unwrap();
        assert_eq!(result, Value::Bool(true));

        // Test Monday (not end of week)
        let monday = Value::String("2023-10-02".to_string()); // October 2, 2023 is a Monday
        let result = builtin_end_of_week(&[monday]).unwrap();
        assert_eq!(result, Value::Bool(false));

        // Test Saturday (not end of week)
        let saturday = Value::String("2023-09-30".to_string()); // September 30, 2023 is a Saturday
        let result = builtin_end_of_week(&[saturday]).unwrap();
        assert_eq!(result, Value::Bool(false));

        // Test with array
        let dates = Value::Array(vec![
            Value::String("2023-10-01".to_string()), // Sunday
            Value::String("2023-10-02".to_string()), // Monday
            Value::String("2023-10-08".to_string()), // Sunday
        ]);
        let result = builtin_end_of_week(&[dates]).unwrap();
        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Value::Bool(true));
                assert_eq!(arr[1], Value::Bool(false));
                assert_eq!(arr[2], Value::Bool(true));
            }
            _ => panic!("Expected array result"),
        }

        // Test invalid arguments
        let result = builtin_end_of_week(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        let result = builtin_end_of_week(&[
            Value::String("2023-10-01".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }
}
