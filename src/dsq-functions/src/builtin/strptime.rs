use chrono::{DateTime, NaiveDate, NaiveDateTime};
use dsq_shared::{value::Value, Result};
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_strptime(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "strptime() expects 2 arguments",
        ));
    }

    let date_val = &args[0];
    let format_val = &args[1];

    // Helper function to parse a single date string with format
    let parse_single = |date_str: &str, format_str: &str| -> Result<Value> {
        if let Ok(dt) = NaiveDateTime::parse_from_str(date_str, format_str) {
            let timestamp = dt.and_utc().timestamp();
            Ok(Value::Int(timestamp))
        } else if let Ok(dt) = DateTime::parse_from_str(date_str, format_str) {
            let timestamp = dt.timestamp();
            Ok(Value::Int(timestamp))
        } else if let Ok(date) = NaiveDate::parse_from_str(date_str, format_str) {
            let dt = date.and_hms_opt(0, 0, 0).unwrap();
            let timestamp = dt.and_utc().timestamp();
            Ok(Value::Int(timestamp))
        } else {
            Ok(Value::Null)
        }
    };

    match date_val {
        Value::String(date_str) => match format_val {
            Value::String(format_str) => parse_single(date_str, format_str),
            Value::Array(_) => Err(dsq_shared::error::operation_error(
                "strptime() cannot use array of formats with single date",
            )),
            _ => Err(dsq_shared::error::operation_error(
                "strptime() second argument must be a string or array of strings",
            )),
        },
        Value::Array(date_arr) => match format_val {
            Value::String(format_str) => {
                let parsed: Result<Vec<Value>> = date_arr
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => parse_single(s, format_str),
                        _ => Ok(Value::Null),
                    })
                    .collect();
                Ok(Value::Array(parsed?))
            }
            Value::Array(format_arr) => {
                if date_arr.len() != format_arr.len() {
                    return Err(dsq_shared::error::operation_error(
                        "strptime() date and format arrays must have the same length",
                    ));
                }
                let parsed: Result<Vec<Value>> = date_arr
                    .iter()
                    .zip(format_arr.iter())
                    .map(|(d, f)| match (d, f) {
                        (Value::String(date_s), Value::String(format_s)) => {
                            parse_single(date_s, format_s)
                        }
                        _ => Ok(Value::Null),
                    })
                    .collect();
                Ok(Value::Array(parsed?))
            }
            _ => Err(dsq_shared::error::operation_error(
                "strptime() second argument must be a string or array of strings",
            )),
        },
        Value::DataFrame(df) => {
            // For DataFrame, assume the format is a string
            let format_str = match format_val {
                Value::String(s) => s,
                _ => {
                    return Err(dsq_shared::error::operation_error(
                        "strptime() second argument must be a string for DataFrame",
                    ));
                }
            };
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::String {
                        let parsed_series = series
                            .str()
                            .unwrap()
                            .apply(|s| {
                                s.and_then(|s| {
                                    if let Ok(dt) = NaiveDateTime::parse_from_str(s, format_str) {
                                        let timestamp = dt.and_utc().timestamp();
                                        Some(Cow::Owned(timestamp.to_string()))
                                    } else if let Ok(dt) = DateTime::parse_from_str(s, format_str) {
                                        let timestamp = dt.timestamp();
                                        Some(Cow::Owned(timestamp.to_string()))
                                    } else if let Ok(date) =
                                        NaiveDate::parse_from_str(s, format_str)
                                    {
                                        let dt = date.and_hms_opt(0, 0, 0).unwrap();
                                        let timestamp = dt.and_utc().timestamp();
                                        Some(Cow::Owned(timestamp.to_string()))
                                    } else {
                                        None
                                    }
                                })
                            })
                            .into_series();
                        let mut s = parsed_series;
                        s.rename(col_name.clone());
                        new_series.push(s.into());
                    } else {
                        let mut s = series.clone();
                        s.rename(col_name.clone());
                        new_series.push(s);
                    }
                }
            }
            match DataFrame::new(new_series) {
                Ok(new_df) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "strptime() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            let format_str = match format_val {
                Value::String(s) => s,
                _ => {
                    return Err(dsq_shared::error::operation_error(
                        "strptime() second argument must be a string for Series",
                    ));
                }
            };
            if series.dtype() == &DataType::String {
                let parsed_series = series
                    .str()
                    .unwrap()
                    .apply(|s| {
                        s.and_then(|s| {
                            if let Ok(dt) = NaiveDateTime::parse_from_str(s, format_str) {
                                let timestamp = dt.and_utc().timestamp();
                                Some(Cow::Owned(timestamp.to_string()))
                            } else if let Ok(dt) = DateTime::parse_from_str(s, format_str) {
                                let timestamp = dt.timestamp();
                                Some(Cow::Owned(timestamp.to_string()))
                            } else if let Ok(date) = NaiveDate::parse_from_str(s, format_str) {
                                let dt = date.and_hms_opt(0, 0, 0).unwrap();
                                let timestamp = dt.and_utc().timestamp();
                                Some(Cow::Owned(timestamp.to_string()))
                            } else {
                                None
                            }
                        })
                    })
                    .into_series();
                Ok(Value::Series(parsed_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        Value::LazyFrame(lf) => {
            // Collect the LazyFrame to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_strptime(&[Value::DataFrame(df), format_val.clone()])
        }
        _ => Err(dsq_shared::error::operation_error(
            "strptime() first argument must be a string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "strptime",
        func: builtin_strptime,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_strptime_string_valid() {
        let result = builtin_strptime(&[
            Value::String("2021-01-01 00:00:00".to_string()),
            Value::String("%Y-%m-%d %H:%M:%S".to_string()),
        ])
        .unwrap();
        assert!(matches!(result, Value::Int(_)));
    }

    #[test]
    fn test_strptime_string_date_only() {
        let result = builtin_strptime(&[
            Value::String("2021-01-01".to_string()),
            Value::String("%Y-%m-%d".to_string()),
        ])
        .unwrap();
        assert!(matches!(result, Value::Int(_)));
    }

    #[test]
    fn test_strptime_string_invalid() {
        let result = builtin_strptime(&[
            Value::String("invalid".to_string()),
            Value::String("%Y-%m-%d".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_strptime_array_valid() {
        let arr = vec![
            Value::String("2021-01-01".to_string()),
            Value::String("2021-01-02".to_string()),
        ];
        let result =
            builtin_strptime(&[Value::Array(arr), Value::String("%Y-%m-%d".to_string())]).unwrap();
        match result {
            Value::Array(results) => {
                assert_eq!(results.len(), 2);
                assert!(matches!(results[0], Value::Int(_)));
                assert!(matches!(results[1], Value::Int(_)));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_strptime_array_mixed() {
        let arr = vec![Value::String("2021-01-01".to_string()), Value::Int(123)];
        let result =
            builtin_strptime(&[Value::Array(arr), Value::String("%Y-%m-%d".to_string())]).unwrap();
        match result {
            Value::Array(results) => {
                assert_eq!(results.len(), 2);
                assert!(matches!(results[0], Value::Int(_)));
                assert_eq!(results[1], Value::Null);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_strptime_array_with_formats() {
        let dates = vec![
            Value::String("2021-01-01".to_string()),
            Value::String("2021-01-02 10:30:00".to_string()),
        ];
        let formats = vec![
            Value::String("%Y-%m-%d".to_string()),
            Value::String("%Y-%m-%d %H:%M:%S".to_string()),
        ];
        let result = builtin_strptime(&[Value::Array(dates), Value::Array(formats)]).unwrap();
        match result {
            Value::Array(results) => {
                assert_eq!(results.len(), 2);
                assert!(matches!(results[0], Value::Int(_)));
                assert!(matches!(results[1], Value::Int(_)));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_strptime_invalid_args() {
        // Too few args
        let result = builtin_strptime(&[Value::String("test".to_string())]);
        assert!(result.is_err());

        // Too many args
        let result = builtin_strptime(&[
            Value::String("test".to_string()),
            Value::String("%Y".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());

        // Wrong type for format
        let result = builtin_strptime(&[Value::String("2021-01-01".to_string()), Value::Int(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_strptime_array_format_mismatch() {
        let dates = vec![Value::String("2021-01-01".to_string())];
        let formats = vec![
            Value::String("%Y-%m-%d".to_string()),
            Value::String("%Y-%m-%d %H:%M:%S".to_string()),
        ];
        let result = builtin_strptime(&[Value::Array(dates), Value::Array(formats)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_strptime_single_date_with_array_format() {
        let result = builtin_strptime(&[
            Value::String("2021-01-01".to_string()),
            Value::Array(vec![Value::String("%Y-%m-%d".to_string())]),
        ]);
        assert!(result.is_err());
    }
}
