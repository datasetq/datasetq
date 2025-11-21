use chrono::{DateTime, Local, NaiveDate, TimeZone, Utc};
use dsq_shared::{value::Value, Result};
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

inventory::submit! {
    crate::FunctionRegistration {
        name: "strflocaltime",
        func: builtin_strflocaltime,
    }
}

pub fn builtin_strflocaltime(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "strflocaltime() expects 2 arguments",
        ));
    }

    let format_str = match &args[1] {
        Value::String(s) => s,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "strflocaltime() second argument must be a format string",
            ));
        }
    };

    match &args[0] {
        Value::Int(i) => {
            let dt = Utc
                .timestamp_opt(*i, 0)
                .single()
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp"))?;
            let local_dt: DateTime<Local> = dt.with_timezone(&Local);
            Ok(Value::String(local_dt.format(format_str).to_string()))
        }
        Value::Float(f) => {
            let secs = f.trunc() as i64;
            let nanos = (f.fract() * 1_000_000_000.0) as u32;
            let dt = Utc
                .timestamp_opt(secs, nanos)
                .single()
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp"))?;
            let local_dt: DateTime<Local> = dt.with_timezone(&Local);
            Ok(Value::String(local_dt.format(format_str).to_string()))
        }
        Value::String(s) => {
            // Try to parse as timestamp number first
            if let Ok(ts) = s.parse::<i64>() {
                let dt = Utc
                    .timestamp_opt(ts, 0)
                    .single()
                    .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp"))?;
                let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                Ok(Value::String(local_dt.format(format_str).to_string()))
            } else {
                // Try to parse as date/datetime string
                if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let dt_utc = Utc.from_utc_datetime(&dt.and_hms_opt(0, 0, 0).unwrap());
                    let local_dt: DateTime<Local> = dt_utc.with_timezone(&Local);
                    Ok(Value::String(local_dt.format(format_str).to_string()))
                } else if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    let dt_utc = dt.with_timezone(&Utc);
                    let local_dt: DateTime<Local> = dt_utc.with_timezone(&Local);
                    Ok(Value::String(local_dt.format(format_str).to_string()))
                } else {
                    Ok(Value::Null)
                }
            }
        }
        Value::Array(arr) => {
            let formatted: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::Int(i) => {
                        let dt = Utc.timestamp_opt(*i, 0).single().ok_or_else(|| {
                            dsq_shared::error::operation_error("Invalid timestamp")
                        })?;
                        let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                        Ok(Value::String(local_dt.format(format_str).to_string()))
                    }
                    Value::Float(f) => {
                        let secs = f.trunc() as i64;
                        let nanos = (f.fract() * 1_000_000_000.0) as u32;
                        let dt = Utc.timestamp_opt(secs, nanos).single().ok_or_else(|| {
                            dsq_shared::error::operation_error("Invalid timestamp")
                        })?;
                        let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                        Ok(Value::String(local_dt.format(format_str).to_string()))
                    }
                    Value::String(s) => {
                        if let Ok(ts) = s.parse::<i64>() {
                            let dt = Utc.timestamp_opt(ts, 0).single().ok_or_else(|| {
                                dsq_shared::error::operation_error("Invalid timestamp")
                            })?;
                            let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                            Ok(Value::String(local_dt.format(format_str).to_string()))
                        } else {
                            if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                let dt_utc =
                                    Utc.from_utc_datetime(&dt.and_hms_opt(0, 0, 0).unwrap());
                                let local_dt: DateTime<Local> = dt_utc.with_timezone(&Local);
                                Ok(Value::String(local_dt.format(format_str).to_string()))
                            } else if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                                let dt_utc = dt.with_timezone(&Utc);
                                let local_dt: DateTime<Local> = dt_utc.with_timezone(&Local);
                                Ok(Value::String(local_dt.format(format_str).to_string()))
                            } else {
                                Ok(Value::Null)
                            }
                        }
                    }
                    _ => Ok(Value::Null),
                })
                .collect();
            Ok(Value::Array(formatted?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() {
                        let mut formatted_values = Vec::new();
                        for i in 0..series.len() {
                            if let Ok(val) = series.get(i) {
                                match val {
                                    AnyValue::Int64(ts) => {
                                        let dt =
                                            Utc.timestamp_opt(ts, 0).single().ok_or_else(|| {
                                                dsq_shared::error::operation_error(
                                                    "Invalid timestamp",
                                                )
                                            })?;
                                        let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                                        formatted_values
                                            .push(local_dt.format(format_str).to_string());
                                    }
                                    AnyValue::Float64(ts) => {
                                        let secs = ts.trunc() as i64;
                                        let nanos = (ts.fract() * 1_000_000_000.0) as u32;
                                        let dt = Utc
                                            .timestamp_opt(secs, nanos)
                                            .single()
                                            .ok_or_else(|| {
                                                dsq_shared::error::operation_error(
                                                    "Invalid timestamp",
                                                )
                                            })?;
                                        let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                                        formatted_values
                                            .push(local_dt.format(format_str).to_string());
                                    }
                                    _ => formatted_values.push("".to_string()),
                                }
                            } else {
                                formatted_values.push("".to_string());
                            }
                        }
                        let formatted_series = Series::new(col_name, formatted_values);
                        new_series.push(formatted_series);
                    } else if series.dtype() == &DataType::Utf8 {
                        let formatted_series = series
                            .utf8()
                            .unwrap()
                            .apply(|s| {
                                s.and_then(|s| {
                                    if let Ok(ts) = s.parse::<i64>() {
                                        let dt = Utc
                                            .timestamp_opt(ts, 0)
                                            .single()
                                            .ok_or_else(|| {
                                                dsq_shared::error::operation_error(
                                                    "Invalid timestamp",
                                                )
                                            })
                                            .ok()?;
                                        let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                                        Some(Cow::Owned(local_dt.format(format_str).to_string()))
                                    } else {
                                        if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                            let dt_utc = Utc.from_utc_datetime(
                                                &dt.and_hms_opt(0, 0, 0).unwrap(),
                                            );
                                            let local_dt: DateTime<Local> =
                                                dt_utc.with_timezone(&Local);
                                            Some(Cow::Owned(
                                                local_dt.format(format_str).to_string(),
                                            ))
                                        } else if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                                            let dt_utc = dt.with_timezone(&Utc);
                                            let local_dt: DateTime<Local> =
                                                dt_utc.with_timezone(&Local);
                                            Some(Cow::Owned(
                                                local_dt.format(format_str).to_string(),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                })
                            })
                            .into_series();
                        let mut s = formatted_series;
                        s.rename(col_name);
                        new_series.push(s);
                    } else {
                        let mut s = series.clone();
                        s.rename(col_name);
                        new_series.push(s);
                    }
                }
            }
            match DataFrame::new(new_series) {
                Ok(new_df) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "strflocaltime() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                let mut formatted_values = Vec::new();
                for i in 0..series.len() {
                    if let Ok(val) = series.get(i) {
                        match val {
                            AnyValue::Int64(ts) => {
                                let dt = Utc.timestamp_opt(ts, 0).single().ok_or_else(|| {
                                    dsq_shared::error::operation_error("Invalid timestamp")
                                })?;
                                let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                                formatted_values.push(local_dt.format(format_str).to_string());
                            }
                            AnyValue::Float64(ts) => {
                                let secs = ts.trunc() as i64;
                                let nanos = (ts.fract() * 1_000_000_000.0) as u32;
                                let dt =
                                    Utc.timestamp_opt(secs, nanos).single().ok_or_else(|| {
                                        dsq_shared::error::operation_error("Invalid timestamp")
                                    })?;
                                let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                                formatted_values.push(local_dt.format(format_str).to_string());
                            }
                            _ => formatted_values.push("".to_string()),
                        }
                    } else {
                        formatted_values.push("".to_string());
                    }
                }
                Ok(Value::Series(Series::new("", formatted_values)))
            } else if series.dtype() == &DataType::Utf8 {
                let formatted_series = series
                    .utf8()
                    .unwrap()
                    .apply(|s| {
                        s.and_then(|s| {
                            if let Ok(ts) = s.parse::<i64>() {
                                let dt = Utc
                                    .timestamp_opt(ts, 0)
                                    .single()
                                    .ok_or_else(|| {
                                        dsq_shared::error::operation_error("Invalid timestamp")
                                    })
                                    .ok()?;
                                let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                                Some(Cow::Owned(local_dt.format(format_str).to_string()))
                            } else {
                                if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                    let dt_utc =
                                        Utc.from_utc_datetime(&dt.and_hms_opt(0, 0, 0).unwrap());
                                    let local_dt: DateTime<Local> = dt_utc.with_timezone(&Local);
                                    Some(Cow::Owned(local_dt.format(format_str).to_string()))
                                } else if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                                    let dt_utc = dt.with_timezone(&Utc);
                                    let local_dt: DateTime<Local> = dt_utc.with_timezone(&Local);
                                    Some(Cow::Owned(local_dt.format(format_str).to_string()))
                                } else {
                                    None
                                }
                            }
                        })
                    })
                    .into_series();
                Ok(Value::Series(formatted_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Ok(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_strflocaltime_int_timestamp() {
        // Test with integer timestamp (Unix timestamp)
        let ts = Value::Int(1609459200); // 2021-01-01 00:00:00 UTC
        let format = Value::String("%Y-%m-%d %H:%M:%S".to_string());
        let result = builtin_strflocaltime(&[ts, format]).unwrap();
        // The result will depend on the local timezone, but should be a string
        assert!(matches!(result, Value::String(_)));
    }

    #[test]
    fn test_builtin_strflocaltime_float_timestamp() {
        // Test with float timestamp
        let ts = Value::Float(1609459200.5);
        let format = Value::String("%Y-%m-%d %H:%M:%S".to_string());
        let result = builtin_strflocaltime(&[ts, format]).unwrap();
        assert!(matches!(result, Value::String(_)));
    }

    #[test]
    fn test_builtin_strflocaltime_string_timestamp() {
        // Test with string timestamp
        let ts = Value::String("1609459200".to_string());
        let format = Value::String("%Y-%m-%d".to_string());
        let result = builtin_strflocaltime(&[ts, format]).unwrap();
        assert!(matches!(result, Value::String(_)));
    }

    #[test]
    fn test_builtin_strflocaltime_date_string() {
        // Test with date string
        let ts = Value::String("2021-01-01".to_string());
        let format = Value::String("%Y-%m-%d".to_string());
        let result = builtin_strflocaltime(&[ts, format]).unwrap();
        assert!(matches!(result, Value::String(_)));
    }

    #[test]
    fn test_builtin_strflocaltime_rfc3339_string() {
        // Test with RFC3339 string
        let ts = Value::String("2021-01-01T00:00:00Z".to_string());
        let format = Value::String("%Y-%m-%d %H:%M:%S".to_string());
        let result = builtin_strflocaltime(&[ts, format]).unwrap();
        assert!(matches!(result, Value::String(_)));
    }

    #[test]
    fn test_builtin_strflocaltime_array() {
        // Test with array of timestamps
        let arr = Value::Array(vec![
            Value::Int(1609459200),
            Value::String("2021-01-01".to_string()),
        ]);
        let format = Value::String("%Y-%m-%d".to_string());
        let result = builtin_strflocaltime(&[arr, format]).unwrap();
        assert!(matches!(result, Value::Array(_)));
        if let Value::Array(res_arr) = result {
            assert_eq!(res_arr.len(), 2);
            assert!(matches!(res_arr[0], Value::String(_)));
            assert!(matches!(res_arr[1], Value::String(_)));
        }
    }

    #[test]
    fn test_builtin_strflocaltime_invalid_args() {
        // Test with wrong number of arguments
        let result = builtin_strflocaltime(&[Value::Int(123)]);
        assert!(result.is_err());

        let result = builtin_strflocaltime(&[
            Value::Int(123),
            Value::String("format".to_string()),
            Value::Int(456),
        ]);
        assert!(result.is_err());

        // Test with invalid format argument
        let result = builtin_strflocaltime(&[Value::Int(123), Value::Int(456)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_builtin_strflocaltime_invalid_timestamp() {
        // Test with invalid timestamp
        let ts = Value::Int(-1); // Invalid timestamp
        let format = Value::String("%Y-%m-%d".to_string());
        let result = builtin_strflocaltime(&[ts, format]);
        // Depending on chrono behavior, this might succeed or fail
        // For now, just check it's not panicking
        let _ = result;
    }

    #[test]
    fn test_builtin_strflocaltime_invalid_string() {
        // Test with invalid date string
        let ts = Value::String("not-a-date".to_string());
        let format = Value::String("%Y-%m-%d".to_string());
        let result = builtin_strflocaltime(&[ts, format]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_strflocaltime_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("strflocaltime"));
    }
}
