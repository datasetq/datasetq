use chrono::Datelike;
use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_year(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "year() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(_) | Value::Float(_) | Value::String(_) => {
            let dt = crate::extract_timestamp(&args[0])?;
            Ok(Value::Int(dt.year() as i64))
        }
        Value::Array(arr) => {
            let years: Result<Vec<Value>> = arr
                .iter()
                .map(|v| {
                    if matches!(v, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                        let dt = crate::extract_timestamp(v)?;
                        Ok(Value::Int(dt.year() as i64))
                    } else {
                        Ok(Value::Null)
                    }
                })
                .collect();
            Ok(Value::Array(years?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                        let mut year_values = Vec::new();
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
                                                year_values.push(dt.year() as i64);
                                            }
                                            _ => {
                                                year_values.push(0i64);
                                            }
                                        }
                                    } else {
                                        year_values.push(0i64);
                                    }
                                }
                                _ => {
                                    year_values.push(0i64);
                                }
                            }
                        }
                        let year_series = Series::new(col_name.clone(), year_values);
                        new_series.push(year_series.into());
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
                    "year() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                let mut year_values = Vec::new();
                for i in 0..series.len() {
                    match series.get(i) {
                        Ok(val) => {
                            let value = value_from_any_value(val).unwrap_or(Value::Null);
                            if matches!(value, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                                match crate::extract_timestamp(&value) {
                                    Ok(dt) => {
                                        year_values.push(dt.year() as i64);
                                    }
                                    _ => {
                                        year_values.push(0i64);
                                    }
                                }
                            } else {
                                year_values.push(0i64);
                            }
                        }
                        _ => {
                            year_values.push(0i64);
                        }
                    }
                }
                Ok(Value::Series(Series::new(
                    series.name().clone(),
                    year_values,
                )))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "year() requires timestamp, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "year",
        func: builtin_year,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_year_timestamp() {
        // Test with timestamp 1609459200 (2021-01-01 00:00:00 UTC)
        let result = builtin_year(&[Value::Int(1609459200)]).unwrap();
        assert_eq!(result, Value::Int(2021));
    }

    #[test]
    fn test_year_date_string() {
        // Test with date string
        let result = builtin_year(&[Value::String("2021-06-15".to_string())]).unwrap();
        assert_eq!(result, Value::Int(2021));
    }

    #[test]
    fn test_year_rfc3339_string() {
        // Test with RFC3339 string
        let result = builtin_year(&[Value::String("2021-06-15T12:30:45Z".to_string())]).unwrap();
        assert_eq!(result, Value::Int(2021));
    }

    #[test]
    fn test_year_no_args() {
        let result = builtin_year(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_year_too_many_args() {
        let result = builtin_year(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_year_invalid_date() {
        let result = builtin_year(&[Value::String("invalid".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_year_array() {
        let arr = vec![
            Value::String("2021-01-01".to_string()),
            Value::String("2022-01-01".to_string()),
        ];
        let result = builtin_year(&[Value::Array(arr)]).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![Value::Int(2021), Value::Int(2022)])
        );
    }
}
