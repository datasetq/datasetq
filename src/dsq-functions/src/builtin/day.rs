use chrono::Datelike;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_day(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "day() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(_) | Value::Float(_) | Value::String(_) => {
            let dt = crate::extract_timestamp(&args[0])?;
            Ok(Value::Int(dt.day() as i64))
        }
        Value::Array(arr) => {
            let days: Result<Vec<Value>> = arr
                .iter()
                .map(|v| {
                    if matches!(v, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                        let dt = crate::extract_timestamp(v)?;
                        Ok(Value::Int(dt.day() as i64))
                    } else {
                        Ok(Value::Null)
                    }
                })
                .collect();
            Ok(Value::Array(days?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                        let mut day_values = Vec::new();
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
                                                day_values.push(dt.day() as i64);
                                            }
                                            _ => {
                                                day_values.push(0i64);
                                            }
                                        }
                                    } else {
                                        day_values.push(0i64);
                                    }
                                }
                                _ => {
                                    day_values.push(0i64);
                                }
                            }
                        }
                        let day_series = Series::new(col_name.clone(), day_values);
                        new_series.push(day_series.into());
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
                    "day() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                let mut day_values = Vec::new();
                for i in 0..series.len() {
                    match series.get(i) {
                        Ok(val) => {
                            let value = crate::value_from_any_value(val).unwrap_or(Value::Null);
                            if matches!(value, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                                match crate::extract_timestamp(&value) {
                                    Ok(dt) => {
                                        day_values.push(dt.day() as i64);
                                    }
                                    _ => {
                                        day_values.push(0i64);
                                    }
                                }
                            } else {
                                day_values.push(0i64);
                            }
                        }
                        _ => {
                            day_values.push(0i64);
                        }
                    }
                }
                Ok(Value::Series(Series::new(
                    series.name().clone(),
                    day_values,
                )))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        Value::LazyFrame(lf) => {
            // Collect the LazyFrame to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_day(&[Value::DataFrame(df)])
        }
        _ => Err(dsq_shared::error::operation_error(
            "day() requires timestamp, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "day",
        func: builtin_day,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_day_timestamp() {
        // Test with timestamp (January 15, 2023 -> 15)
        let timestamp = Value::Int(1673740800); // 2023-01-15 00:00:00 UTC
        let result = builtin_day(&[timestamp]).unwrap();
        assert_eq!(result, Value::Int(15));
    }

    #[test]
    fn test_day_string_date() {
        // Test with string date
        let date_str = Value::String("2023-01-15".to_string());
        let result = builtin_day(&[date_str]).unwrap();
        assert_eq!(result, Value::Int(15));
    }

    #[test]
    fn test_day_no_args() {
        let result = builtin_day(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_day_too_many_args() {
        let result = builtin_day(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_day_invalid_date() {
        let result = builtin_day(&[Value::String("invalid".to_string())]);
        assert!(result.is_err());
    }
}
