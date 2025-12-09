use chrono::{Datelike, NaiveDate};
use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_start_of_month(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "start_of_month() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(_) | Value::Float(_) | Value::String(_) => {
            let dt = crate::extract_timestamp(&args[0])?;
            let year = dt.year();
            let month = dt.month();
            let start_of_month = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
            Ok(Value::String(start_of_month.format("%Y-%m-%d").to_string()))
        }
        Value::Array(arr) => {
            let start_dates: Result<Vec<Value>> = arr
                .iter()
                .map(|v| {
                    if matches!(v, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                        let dt = crate::extract_timestamp(v)?;
                        let year = dt.year();
                        let month = dt.month();
                        let start_of_month = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
                        Ok(Value::String(start_of_month.format("%Y-%m-%d").to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                })
                .collect();
            Ok(Value::Array(start_dates?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                        let mut start_values = Vec::new();
                        for i in 0..series.len() {
                            match series.get(i) {
                                Ok(val) => {
                                    let value = value_from_any_value(val).unwrap_or(Value::Null);
                                    if matches!(
                                        value,
                                        Value::Int(_) | Value::Float(_) | Value::String(_)
                                    ) {
                                        match crate::extract_timestamp(&value) {
                                            Ok(dt) => {
                                                let year = dt.year();
                                                let month = dt.month();
                                                let start_of_month =
                                                    NaiveDate::from_ymd_opt(year, month, 1)
                                                        .unwrap();
                                                start_values.push(
                                                    start_of_month.format("%Y-%m-%d").to_string(),
                                                );
                                            }
                                            _ => {
                                                start_values.push("".to_string());
                                            }
                                        }
                                    } else {
                                        start_values.push("".to_string());
                                    }
                                }
                                _ => {
                                    start_values.push("".to_string());
                                }
                            }
                        }
                        let start_series = Series::new(col_name.clone(), start_values);
                        new_series.push(start_series.into());
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
                    "start_of_month() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                let mut start_values = Vec::new();
                for i in 0..series.len() {
                    match series.get(i) {
                        Ok(val) => {
                            let value = value_from_any_value(val).unwrap_or(Value::Null);
                            if matches!(value, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                                match crate::extract_timestamp(&value) {
                                    Ok(dt) => {
                                        let year = dt.year();
                                        let month = dt.month();
                                        let start_of_month =
                                            NaiveDate::from_ymd_opt(year, month, 1).unwrap();
                                        start_values
                                            .push(start_of_month.format("%Y-%m-%d").to_string());
                                    }
                                    _ => {
                                        start_values.push("".to_string());
                                    }
                                }
                            } else {
                                start_values.push("".to_string());
                            }
                        }
                        _ => {
                            start_values.push("".to_string());
                        }
                    }
                }
                Ok(Value::Series(Series::new(
                    series.name().clone(),
                    start_values,
                )))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "start_of_month() requires timestamp, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "start_of_month",
        func: builtin_start_of_month,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_start_of_month_timestamp() {
        // Test with timestamp in the middle of the month
        let ts = Value::Int(1612137600); // 2021-02-01 00:00:00 UTC (already start of month)
        let result = builtin_start_of_month(&[ts]).unwrap();
        assert_eq!(result, Value::String("2021-02-01".to_string()));
    }

    #[test]
    fn test_start_of_month_timestamp_middle_month() {
        // Test with timestamp later in the month
        let ts = Value::Int(1614556800); // 2021-03-01 00:00:00 UTC
        let result = builtin_start_of_month(&[ts]).unwrap();
        assert_eq!(result, Value::String("2021-03-01".to_string()));
    }

    #[test]
    fn test_start_of_month_timestamp_february() {
        // Test with timestamp in the middle of February
        let ts = Value::Int(1613347200); // 2021-02-15 00:00:00 UTC
        let result = builtin_start_of_month(&[ts]).unwrap();
        assert_eq!(result, Value::String("2021-02-01".to_string()));
    }

    #[test]
    fn test_start_of_month_string_date() {
        // Test with string date
        let date_str = Value::String("2021-06-15".to_string());
        let result = builtin_start_of_month(&[date_str]).unwrap();
        assert_eq!(result, Value::String("2021-06-01".to_string()));
    }

    #[test]
    fn test_start_of_month_no_args() {
        let result = builtin_start_of_month(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_start_of_month_too_many_args() {
        let result = builtin_start_of_month(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_start_of_month_invalid_date() {
        let result = builtin_start_of_month(&[Value::String("invalid".to_string())]);
        assert!(result.is_err());
    }
}
