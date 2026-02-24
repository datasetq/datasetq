use chrono::Datelike;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_month(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "month() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(_) | Value::Float(_) | Value::String(_) => {
            let dt = crate::extract_timestamp(&args[0])?;
            Ok(Value::Int(dt.month() as i64))
        }
        Value::Array(arr) => {
            let months: Vec<Value> = arr
                .iter()
                .map(|v| {
                    if matches!(v, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                        match crate::extract_timestamp(v) {
                            Ok(dt) => Value::Int(dt.month() as i64),
                            Err(_) => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                })
                .collect();
            Ok(Value::Array(months))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                        let mut month_values = Vec::new();
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
                                                month_values.push(dt.month() as i64);
                                            }
                                            _ => {
                                                month_values.push(0i64);
                                            }
                                        }
                                    } else {
                                        month_values.push(0i64);
                                    }
                                }
                                _ => {
                                    month_values.push(0i64);
                                }
                            }
                        }
                        let month_series = Series::new(col_name.clone(), month_values);
                        new_series.push(month_series.into());
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
                    "month() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                let mut month_values = Vec::new();
                for i in 0..series.len() {
                    match series.get(i) {
                        Ok(val) => {
                            let value = crate::value_from_any_value(val).unwrap_or(Value::Null);
                            if matches!(value, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                                match crate::extract_timestamp(&value) {
                                    Ok(dt) => {
                                        month_values.push(dt.month() as i64);
                                    }
                                    _ => {
                                        month_values.push(0i64);
                                    }
                                }
                            } else {
                                month_values.push(0i64);
                            }
                        }
                        _ => {
                            month_values.push(0i64);
                        }
                    }
                }
                Ok(Value::Series(Series::new(
                    series.name().clone(),
                    month_values,
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
            builtin_month(&[Value::DataFrame(df)])
        }
        _ => Err(dsq_shared::error::operation_error(
            "month() requires timestamp, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "month",
        func: builtin_month,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_month_timestamp() {
        // Test with timestamp (January 15, 2023 -> 1)
        let timestamp = Value::Int(1673740800); // 2023-01-15 00:00:00 UTC
        let result = builtin_month(&[timestamp]).unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_month_string_date() {
        // Test with string date
        let date_str = Value::String("2023-06-15".to_string());
        let result = builtin_month(&[date_str]).unwrap();
        assert_eq!(result, Value::Int(6));
    }

    #[test]
    fn test_month_no_args() {
        let result = builtin_month(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_month_too_many_args() {
        let result = builtin_month(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_month_invalid_date() {
        let result = builtin_month(&[Value::String("invalid".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_month_array() {
        let arr = Value::Array(vec![
            Value::String("2023-01-15".to_string()),
            Value::String("2023-06-20".to_string()),
            Value::String("2023-12-25".to_string()),
        ]);
        let result = builtin_month(&[arr]).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![Value::Int(1), Value::Int(6), Value::Int(12)])
        );
    }

    #[test]
    fn test_month_array_with_invalid() {
        let arr = Value::Array(vec![
            Value::String("2023-01-15".to_string()),
            Value::String("invalid".to_string()),
            Value::String("2023-12-25".to_string()),
        ]);
        let result = builtin_month(&[arr]).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![Value::Int(1), Value::Null, Value::Int(12)])
        );
    }
}
