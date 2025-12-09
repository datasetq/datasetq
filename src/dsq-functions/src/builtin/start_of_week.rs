use chrono::Datelike;
use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_start_of_week(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "start_of_week() expects 1 or 2 arguments",
        ));
    }

    let start_day = if args.len() == 2 {
        match &args[1] {
            Value::String(s) => s.to_lowercase(),
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "start_of_week() second argument must be a string",
                ));
            }
        }
    } else {
        "monday".to_string()
    };

    match &args[0] {
        Value::Int(_) | Value::Float(_) | Value::String(_) => {
            let dt = crate::extract_timestamp(&args[0])?;
            let weekday = dt.weekday();
            let days_to_subtract = match start_day.as_str() {
                "monday" => weekday.num_days_from_monday() as i64,
                "sunday" => weekday.num_days_from_sunday() as i64,
                _ => {
                    return Err(dsq_shared::error::operation_error(
                        "start_of_week() start_day must be 'monday' or 'sunday'",
                    ));
                }
            };

            let week_start_date = dt.date_naive() - chrono::Duration::days(days_to_subtract);
            let week_start = week_start_date.and_hms_opt(0, 0, 0).unwrap().and_utc();
            Ok(Value::Int(week_start.timestamp()))
        }
        Value::Array(arr) => {
            let results: Result<Vec<Value>> = arr
                .iter()
                .map(|v| {
                    if matches!(v, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                        let dt = crate::extract_timestamp(v)?;
                        let weekday = dt.weekday();
                        let days_to_subtract = match start_day.as_str() {
                            "monday" => weekday.num_days_from_monday() as i64,
                            "sunday" => weekday.num_days_from_sunday() as i64,
                            _ => {
                                return Err(dsq_shared::error::operation_error(
                                    "start_of_week() start_day must be 'monday' or 'sunday'",
                                ));
                            }
                        };

                        let week_start_date =
                            dt.date_naive() - chrono::Duration::days(days_to_subtract);
                        let week_start = week_start_date.and_hms_opt(0, 0, 0).unwrap().and_utc();
                        Ok(Value::Int(week_start.timestamp()))
                    } else {
                        Ok(Value::Null)
                    }
                })
                .collect();
            Ok(Value::Array(results?))
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
                                                let weekday = dt.weekday();
                                                let days_to_subtract = match start_day.as_str() {
                                                    "monday" => {
                                                        weekday.num_days_from_monday() as i64
                                                    }
                                                    "sunday" => {
                                                        weekday.num_days_from_sunday() as i64
                                                    }
                                                    _ => {
                                                        return Err(dsq_shared::error::operation_error(
                                                    "start_of_week() start_day must be 'monday' or 'sunday'",
                                                ));
                                                    }
                                                };

                                                let week_start_date = dt.date_naive()
                                                    - chrono::Duration::days(days_to_subtract);
                                                let week_start = week_start_date
                                                    .and_hms_opt(0, 0, 0)
                                                    .unwrap()
                                                    .and_utc();
                                                start_values.push(week_start.timestamp());
                                            }
                                            _ => {
                                                start_values.push(0i64);
                                            }
                                        }
                                    } else {
                                        start_values.push(0i64);
                                    }
                                }
                                _ => {
                                    start_values.push(0i64);
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
                    "start_of_week() failed on DataFrame: {}",
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
                                        let weekday = dt.weekday();
                                        let days_to_subtract = match start_day.as_str() {
                                            "monday" => weekday.num_days_from_monday() as i64,
                                            "sunday" => weekday.num_days_from_sunday() as i64,
                                            _ => {
                                                return Err(dsq_shared::error::operation_error(
                                            "start_of_week() start_day must be 'monday' or 'sunday'",
                                        ));
                                            }
                                        };

                                        let week_start_date = dt.date_naive()
                                            - chrono::Duration::days(days_to_subtract);
                                        let week_start =
                                            week_start_date.and_hms_opt(0, 0, 0).unwrap().and_utc();
                                        start_values.push(week_start.timestamp());
                                    }
                                    _ => {
                                        start_values.push(0i64);
                                    }
                                }
                            } else {
                                start_values.push(0i64);
                            }
                        }
                        _ => {
                            start_values.push(0i64);
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
            "start_of_week() argument must be a date value, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "start_of_week",
        func: builtin_start_of_week,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_start_of_week_monday_default() {
        // Test with Wednesday, should go back to Monday
        let ts = Value::Int(1609718400); // 2021-01-04 00:00:00 UTC (Monday)
        let result = builtin_start_of_week(&[ts]).unwrap();
        assert_eq!(result, Value::Int(1609718400)); // Should be the same Monday
    }

    #[test]
    fn test_start_of_week_wednesday_to_monday() {
        // Test with Wednesday, should go back to Monday
        let ts = Value::Int(1609804800); // 2021-01-05 00:00:00 UTC (Tuesday)
        let result = builtin_start_of_week(&[ts]).unwrap();
        assert_eq!(result, Value::Int(1609718400)); // Should be Monday 2021-01-04
    }

    #[test]
    fn test_start_of_week_sunday_start() {
        // Test with Sunday start
        let ts = Value::Int(1609804800); // 2021-01-05 00:00:00 UTC (Tuesday)
        let result = builtin_start_of_week(&[ts, Value::String("sunday".to_string())]).unwrap();
        assert_eq!(result, Value::Int(1609632000)); // Should be Sunday 2021-01-03
    }

    #[test]
    fn test_start_of_week_array() {
        let arr = Value::Array(vec![
            Value::Int(1609804800), // Tuesday
            Value::Int(1609891200), // Wednesday
        ]);
        let result = builtin_start_of_week(&[arr]).unwrap();
        if let Value::Array(res_arr) = result {
            assert_eq!(res_arr.len(), 2);
            assert_eq!(res_arr[0], Value::Int(1609718400)); // Monday
            assert_eq!(res_arr[1], Value::Int(1609718400)); // Monday
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_start_of_week_invalid_start_day() {
        let ts = Value::Int(1609804800);
        let result = builtin_start_of_week(&[ts, Value::String("invalid".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_start_of_week_no_args() {
        let result = builtin_start_of_week(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_start_of_week_too_many_args() {
        let result = builtin_start_of_week(&[
            Value::Int(1),
            Value::String("monday".to_string()),
            Value::Int(2),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_start_of_week_invalid_second_arg() {
        let result = builtin_start_of_week(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
    }
}
