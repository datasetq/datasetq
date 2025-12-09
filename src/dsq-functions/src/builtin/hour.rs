use chrono::Timelike;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_hour(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "hour() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(_) | Value::Float(_) | Value::String(_) => {
            let dt = crate::extract_timestamp(&args[0])?;
            Ok(Value::Int(dt.hour() as i64))
        }
        Value::Array(arr) => {
            let hours: Result<Vec<Value>> = arr
                .iter()
                .map(|v| {
                    if matches!(v, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                        let dt = crate::extract_timestamp(v)?;
                        Ok(Value::Int(dt.hour() as i64))
                    } else {
                        Ok(Value::Null)
                    }
                })
                .collect();
            Ok(Value::Array(hours?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                        let mut hour_values = Vec::new();
                        for i in 0..series.len() {
                            if let Ok(val) = series.get(i) {
                                let value = crate::value_from_any_value(val).unwrap_or(Value::Null);
                                if matches!(
                                    value,
                                    Value::Int(_) | Value::Float(_) | Value::String(_)
                                ) {
                                    if let Ok(dt) = crate::extract_timestamp(&value) {
                                        hour_values.push(dt.hour() as i64);
                                    } else {
                                        hour_values.push(0i64);
                                    }
                                } else {
                                    hour_values.push(0i64);
                                }
                            } else {
                                hour_values.push(0i64);
                            }
                        }
                        let hour_series = Series::new(col_name.clone(), hour_values);
                        new_series.push(hour_series.into());
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
                    "hour() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                let mut hour_values = Vec::new();
                for i in 0..series.len() {
                    if let Ok(val) = series.get(i) {
                        let value = crate::value_from_any_value(val).unwrap_or(Value::Null);
                        if matches!(value, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                            if let Ok(dt) = crate::extract_timestamp(&value) {
                                hour_values.push(dt.hour() as i64);
                            } else {
                                hour_values.push(0i64);
                            }
                        } else {
                            hour_values.push(0i64);
                        }
                    } else {
                        hour_values.push(0i64);
                    }
                }
                Ok(Value::Series(Series::new(
                    series.name().clone(),
                    hour_values,
                )))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "hour() requires timestamp, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "hour",
        func: builtin_hour,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_hour_timestamp() {
        // Test with string datetime
        let datetime_str = Value::String("2021-06-15T14:30:45Z".to_string());
        let result = builtin_hour(&[datetime_str]).unwrap();
        assert_eq!(result, Value::Int(14));
    }

    #[test]
    fn test_hour_string_datetime() {
        // Test with string datetime
        let datetime_str = Value::String("2021-06-15T14:30:45Z".to_string());
        let result = builtin_hour(&[datetime_str]).unwrap();
        assert_eq!(result, Value::Int(14));
    }

    #[test]
    fn test_hour_no_args() {
        let result = builtin_hour(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_hour_too_many_args() {
        let result = builtin_hour(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_hour_invalid_date() {
        let result = builtin_hour(&[Value::String("invalid".to_string())]);
        assert!(result.is_err());
    }
}
