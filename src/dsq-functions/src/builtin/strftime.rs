use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_strftime(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "strftime() expects 2 arguments",
        ));
    }

    let format_str = match &args[1] {
        Value::String(s) => s,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "strftime() second argument must be a format string",
            ));
        }
    };

    match &args[0] {
        Value::Int(i) => {
            let dt = Utc
                .timestamp_opt(*i, 0)
                .single()
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp"))?;
            Ok(Value::String(dt.format(format_str).to_string()))
        }
        Value::Float(f) => {
            let secs = f.trunc() as i64;
            let nanos = (f.fract() * 1_000_000_000.0) as u32;
            let dt = Utc
                .timestamp_opt(secs, nanos)
                .single()
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp"))?;
            Ok(Value::String(dt.format(format_str).to_string()))
        }
        Value::String(s) => {
            // Try to parse as timestamp number first
            if let Ok(ts) = s.parse::<i64>() {
                let dt = Utc
                    .timestamp_opt(ts, 0)
                    .single()
                    .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp"))?;
                Ok(Value::String(dt.format(format_str).to_string()))
            } else {
                // Try to parse as date/datetime string
                if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let dt_utc = Utc.from_utc_datetime(&dt.and_hms_opt(0, 0, 0).unwrap());
                    Ok(Value::String(dt_utc.format(format_str).to_string()))
                } else if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    let dt_utc = dt.with_timezone(&Utc);
                    Ok(Value::String(dt_utc.format(format_str).to_string()))
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
                        Ok(Value::String(dt.format(format_str).to_string()))
                    }
                    Value::Float(f) => {
                        let secs = f.trunc() as i64;
                        let nanos = (f.fract() * 1_000_000_000.0) as u32;
                        let dt = Utc.timestamp_opt(secs, nanos).single().ok_or_else(|| {
                            dsq_shared::error::operation_error("Invalid timestamp")
                        })?;
                        Ok(Value::String(dt.format(format_str).to_string()))
                    }
                    Value::String(s) => {
                        if let Ok(ts) = s.parse::<i64>() {
                            let dt = Utc.timestamp_opt(ts, 0).single().ok_or_else(|| {
                                dsq_shared::error::operation_error("Invalid timestamp")
                            })?;
                            Ok(Value::String(dt.format(format_str).to_string()))
                        } else {
                            if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                let dt_utc =
                                    Utc.from_utc_datetime(&dt.and_hms_opt(0, 0, 0).unwrap());
                                Ok(Value::String(dt_utc.format(format_str).to_string()))
                            } else if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                                let dt_utc = dt.with_timezone(&Utc);
                                Ok(Value::String(dt_utc.format(format_str).to_string()))
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
                                        formatted_values.push(dt.format(format_str).to_string());
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
                                        formatted_values.push(dt.format(format_str).to_string());
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
                                        Some(Cow::Owned(dt.format(format_str).to_string()))
                                    } else {
                                        if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                            let dt_utc = Utc.from_utc_datetime(
                                                &dt.and_hms_opt(0, 0, 0).unwrap(),
                                            );
                                            Some(Cow::Owned(dt_utc.format(format_str).to_string()))
                                        } else if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                                            let dt_utc = dt.with_timezone(&Utc);
                                            Some(Cow::Owned(dt_utc.format(format_str).to_string()))
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
                    "strftime() failed on DataFrame: {}",
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
                                formatted_values.push(dt.format(format_str).to_string());
                            }
                            AnyValue::Float64(ts) => {
                                let secs = ts.trunc() as i64;
                                let nanos = (ts.fract() * 1_000_000_000.0) as u32;
                                let dt =
                                    Utc.timestamp_opt(secs, nanos).single().ok_or_else(|| {
                                        dsq_shared::error::operation_error("Invalid timestamp")
                                    })?;
                                formatted_values.push(dt.format(format_str).to_string());
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
                                Some(Cow::Owned(dt.format(format_str).to_string()))
                            } else {
                                if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                    let dt_utc =
                                        Utc.from_utc_datetime(&dt.and_hms_opt(0, 0, 0).unwrap());
                                    Some(Cow::Owned(dt_utc.format(format_str).to_string()))
                                } else if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                                    let dt_utc = dt.with_timezone(&Utc);
                                    Some(Cow::Owned(dt_utc.format(format_str).to_string()))
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

inventory::submit! {
    crate::FunctionRegistration {
        name: "strftime",
        func: builtin_strftime,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_strftime_int_timestamp() {
        let result = builtin_strftime(&[
            Value::Int(1609459200),
            Value::String("%Y-%m-%d".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("2021-01-01".to_string()));
    }

    #[test]
    fn test_strftime_float_timestamp() {
        let result = builtin_strftime(&[
            Value::Float(1609459200.5),
            Value::String("%Y-%m-%d %H:%M:%S".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("2021-01-01 00:00:00".to_string()));
    }

    #[test]
    fn test_strftime_string_timestamp() {
        let result = builtin_strftime(&[
            Value::String("1609459200".to_string()),
            Value::String("%Y-%m-%d".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("2021-01-01".to_string()));
    }

    #[test]
    fn test_strftime_string_date() {
        let result = builtin_strftime(&[
            Value::String("2021-01-01".to_string()),
            Value::String("%Y-%m-%d".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("2021-01-01".to_string()));
    }

    #[test]
    fn test_strftime_rfc3339() {
        let result = builtin_strftime(&[
            Value::String("2021-01-01T00:00:00Z".to_string()),
            Value::String("%Y-%m-%d".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("2021-01-01".to_string()));
    }

    #[test]
    fn test_strftime_invalid_args() {
        let result = builtin_strftime(&[Value::Int(1609459200)]);
        assert!(result.is_err());

        let result = builtin_strftime(&[Value::Int(1609459200), Value::Int(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_strftime_array() {
        let arr = vec![
            Value::Int(1609459200),
            Value::String("2021-01-01".to_string()),
        ];
        let result =
            builtin_strftime(&[Value::Array(arr), Value::String("%Y-%m-%d".to_string())]).unwrap();
        match result {
            Value::Array(res) => {
                assert_eq!(res.len(), 2);
                assert_eq!(res[0], Value::String("2021-01-01".to_string()));
                assert_eq!(res[1], Value::String("2021-01-01".to_string()));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_strftime_registered_via_inventory() {
        let mut found = false;
        for func in inventory::iter::<crate::FunctionRegistration> {
            if func.name == "strftime" {
                found = true;
                let result = (func.func)(&[
                    Value::Int(1609459200),
                    Value::String("%Y-%m-%d".to_string()),
                ])
                .unwrap();
                assert_eq!(result, Value::String("2021-01-01".to_string()));
                break;
            }
        }
        assert!(found, "strftime function not found in inventory");
    }
}
