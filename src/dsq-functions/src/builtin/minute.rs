use chrono::Timelike;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_minute(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "minute() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(_) | Value::Float(_) | Value::String(_) => {
            let dt = crate::extract_timestamp(&args[0])?;
            Ok(Value::Int(dt.minute() as i64))
        }
        Value::Array(arr) => {
            let minutes: Result<Vec<Value>> = arr
                .iter()
                .map(|v| {
                    if matches!(v, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                        let dt = crate::extract_timestamp(v)?;
                        Ok(Value::Int(dt.minute() as i64))
                    } else {
                        Ok(Value::Null)
                    }
                })
                .collect();
            Ok(Value::Array(minutes?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                        let mut minute_values = Vec::new();
                        for i in 0..series.len() {
                            match series.get(i) {
                                Ok(val) => {
                                    let value =
                                        crate::value_from_any_value(val).unwrap_or(Value::Null);
                                    if matches!(
                                        value,
                                        Value::Int(_) | Value::Float(_) | Value::String(_)
                                    ) {
                                        match crate::extract_timestamp(&value) {
                                            Ok(dt) => {
                                                minute_values.push(dt.minute() as i64);
                                            }
                                            _ => {
                                                minute_values.push(0i64);
                                            }
                                        }
                                    } else {
                                        minute_values.push(0i64);
                                    }
                                }
                                _ => {
                                    minute_values.push(0i64);
                                }
                            }
                        }
                        let minute_series = Series::new(col_name.clone(), minute_values);
                        new_series.push(minute_series.into());
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
                    "minute() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                let mut minute_values = Vec::new();
                for i in 0..series.len() {
                    match series.get(i) {
                        Ok(val) => {
                            let value = crate::value_from_any_value(val).unwrap_or(Value::Null);
                            if matches!(value, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                                match crate::extract_timestamp(&value) {
                                    Ok(dt) => {
                                        minute_values.push(dt.minute() as i64);
                                    }
                                    _ => {
                                        minute_values.push(0i64);
                                    }
                                }
                            } else {
                                minute_values.push(0i64);
                            }
                        }
                        _ => {
                            minute_values.push(0i64);
                        }
                    }
                }
                Ok(Value::Series(Series::new(
                    series.name().clone(),
                    minute_values,
                )))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "minute() requires timestamp, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "minute",
        func: builtin_minute,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_minute_timestamp() {
        // Test with string datetime
        let datetime_str = Value::String("2021-06-15T14:30:45Z".to_string());
        let result = builtin_minute(&[datetime_str]).unwrap();
        assert_eq!(result, Value::Int(30));
    }

    #[test]
    fn test_minute_string_datetime() {
        // Test with string datetime
        let datetime_str = Value::String("2021-06-15T14:30:45Z".to_string());
        let result = builtin_minute(&[datetime_str]).unwrap();
        assert_eq!(result, Value::Int(30));
    }

    #[test]
    fn test_minute_no_args() {
        let result = builtin_minute(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_minute_too_many_args() {
        let result = builtin_minute(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_minute_invalid_date() {
        let result = builtin_minute(&[Value::String("invalid".to_string())]);
        assert!(result.is_err());
    }
}
