use chrono::{Datelike, Timelike};
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::collections::HashMap;

pub fn builtin_gmtime(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "gmtime() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(_) | Value::Float(_) | Value::String(_) => {
            let dt = crate::extract_timestamp(&args[0])?;
            let mut result = HashMap::new();
            result.insert("year".to_string(), Value::Int(dt.year() as i64));
            result.insert("month".to_string(), Value::Int(dt.month() as i64));
            result.insert("day".to_string(), Value::Int(dt.day() as i64));
            result.insert("hour".to_string(), Value::Int(dt.hour() as i64));
            result.insert("minute".to_string(), Value::Int(dt.minute() as i64));
            result.insert("second".to_string(), Value::Int(dt.second() as i64));
            result.insert(
                "weekday".to_string(),
                Value::Int(dt.weekday().num_days_from_sunday() as i64),
            );
            result.insert("yearday".to_string(), Value::Int(dt.ordinal() as i64));
            Ok(Value::Object(result))
        }
        Value::Array(arr) => {
            let gmtimes: Result<Vec<Value>> = arr
                .iter()
                .map(|v| {
                    if matches!(v, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                        builtin_gmtime(std::slice::from_ref(v))
                    } else {
                        Ok(Value::Null)
                    }
                })
                .collect();
            Ok(Value::Array(gmtimes?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                        let mut gmtime_values = Vec::new();
                        for i in 0..series.len() {
                            if let Ok(val) = series.get(i) {
                                let value = crate::value_from_any_value(val).unwrap_or(Value::Null);
                                if matches!(
                                    value,
                                    Value::Int(_) | Value::Float(_) | Value::String(_)
                                ) {
                                    if let Ok(gmtime_obj) = builtin_gmtime(&[value]) {
                                        // For DataFrame, we might want to flatten or select a field
                                        // For now, just store as string representation
                                        gmtime_values.push(format!("{:?}", gmtime_obj));
                                    } else {
                                        gmtime_values.push("null".to_string());
                                    }
                                } else {
                                    gmtime_values.push("null".to_string());
                                }
                            } else {
                                gmtime_values.push("null".to_string());
                            }
                        }
                        let new_s =
                            Series::new(format!("{}_gmtime", col_name).into(), gmtime_values);
                        new_series.push(new_s.into());
                    } else {
                        let mut s = series.clone();
                        s.rename(col_name.clone());
                        new_series.push(s.into());
                    }
                }
            }
            match DataFrame::new(new_series) {
                Ok(new_df) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "gmtime() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            let mut gmtime_values = Vec::new();
            for i in 0..series.len() {
                if let Ok(val) = series.get(i) {
                    let value = crate::value_from_any_value(val).unwrap_or(Value::Null);
                    if matches!(value, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                        if let Ok(gmtime_obj) = builtin_gmtime(&[value]) {
                            gmtime_values.push(format!("{:?}", gmtime_obj));
                        } else {
                            gmtime_values.push("null".to_string());
                        }
                    } else {
                        gmtime_values.push("null".to_string());
                    }
                } else {
                    gmtime_values.push("null".to_string());
                }
            }
            let new_series = Series::new("gmtime".into(), gmtime_values);
            Ok(Value::Series(new_series))
        }
        _ => Err(dsq_shared::error::operation_error(
            "gmtime() requires numeric or string timestamp",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "gmtime",
        func: builtin_gmtime,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_gmtime_timestamp() {
        // Test with timestamp 1609459200 (2021-01-01 00:00:00 UTC)
        let result = builtin_gmtime(&[Value::Int(1609459200)]).unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.get("year"), Some(&Value::Int(2021)));
            assert_eq!(obj.get("month"), Some(&Value::Int(1)));
            assert_eq!(obj.get("day"), Some(&Value::Int(1)));
            assert_eq!(obj.get("hour"), Some(&Value::Int(0)));
            assert_eq!(obj.get("minute"), Some(&Value::Int(0)));
            assert_eq!(obj.get("second"), Some(&Value::Int(0)));
            assert_eq!(obj.get("weekday"), Some(&Value::Int(5))); // Friday
            assert_eq!(obj.get("yearday"), Some(&Value::Int(1)));
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_gmtime_date_string() {
        // Test with date string
        let result = builtin_gmtime(&[Value::String("2021-06-15".to_string())]).unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.get("year"), Some(&Value::Int(2021)));
            assert_eq!(obj.get("month"), Some(&Value::Int(6)));
            assert_eq!(obj.get("day"), Some(&Value::Int(15)));
            assert_eq!(obj.get("hour"), Some(&Value::Int(0)));
            assert_eq!(obj.get("minute"), Some(&Value::Int(0)));
            assert_eq!(obj.get("second"), Some(&Value::Int(0)));
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_gmtime_no_args() {
        let result = builtin_gmtime(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_gmtime_too_many_args() {
        let result = builtin_gmtime(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_gmtime_invalid_date() {
        let result = builtin_gmtime(&[Value::String("invalid".to_string())]);
        assert!(result.is_err());
    }
}
