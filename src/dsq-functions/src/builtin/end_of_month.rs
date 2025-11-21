use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_end_of_month(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "end_of_month() expects 1 argument",
        ));
    }

    let dt = crate::extract_timestamp(&args[0])?;
    let year = dt.year();
    let month = dt.month();
    let last_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 31,
    };
    let end_of_month = Utc.from_utc_datetime(
        &NaiveDate::from_ymd_opt(year, month, last_day)
            .unwrap()
            .and_hms_opt(23, 59, 59)
            .unwrap(),
    );
    Ok(Value::Int(end_of_month.timestamp()))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "end_of_month",
        func: builtin_end_of_month,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_end_of_month_timestamp() {
        // Test with a timestamp (January 15, 2023 -> January 31, 2023)
        let timestamp = 1673740800; // 2023-01-15 00:00:00 UTC
        let expected = 1675209599; // 2023-01-31 23:59:59 UTC
        let result = builtin_end_of_month(&[Value::Int(timestamp)]).unwrap();
        assert_eq!(result, Value::Int(expected));
    }

    #[test]
    fn test_end_of_month_february_non_leap() {
        // February 10, 2023 (non-leap year) -> March 1, 2023
        let timestamp = 1675987200; // 2023-02-10 00:00:00 UTC
        let expected = 1677628799; // 2023-03-01 23:59:59 UTC
        let result = builtin_end_of_month(&[Value::Int(timestamp)]).unwrap();
        assert_eq!(result, Value::Int(expected));
    }

    #[test]
    fn test_end_of_month_february_leap() {
        // February 10, 2024 (leap year) -> March 1, 2024
        let timestamp = 1707523200; // 2024-02-10 00:00:00 UTC
        let expected = 1709251199; // 2024-03-01 23:59:59 UTC
        let result = builtin_end_of_month(&[Value::Int(timestamp)]).unwrap();
        assert_eq!(result, Value::Int(expected));
    }

    #[test]
    fn test_end_of_month_december() {
        // December 15, 2023 -> December 31, 2023
        let timestamp = 1702598400; // 2023-12-15 00:00:00 UTC
        let expected = 1704067199; // 2023-12-31 23:59:59 UTC
        let result = builtin_end_of_month(&[Value::Int(timestamp)]).unwrap();
        assert_eq!(result, Value::Int(expected));
    }

    #[test]
    fn test_end_of_month_string_date() {
        // Test with string date
        let date_str = "2023-01-15".to_string();
        let expected = 1675209599; // 2023-01-31 23:59:59 UTC
        let result = builtin_end_of_month(&[Value::String(date_str)]).unwrap();
        assert_eq!(result, Value::Int(expected));
    }

    #[test]
    fn test_end_of_month_no_args() {
        let result = builtin_end_of_month(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_end_of_month_too_many_args() {
        let result = builtin_end_of_month(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_end_of_month_invalid_date() {
        let result = builtin_end_of_month(&[Value::String("invalid".to_string())]);
        assert!(result.is_err());
    }
}
