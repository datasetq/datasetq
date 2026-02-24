use chrono::Timelike;
use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_second(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "second() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(_) | Value::Float(_) | Value::String(_) => {
            let dt = crate::extract_timestamp(&args[0])?;
            Ok(Value::Int(dt.second() as i64))
        }
        Value::Array(arr) => {
            let seconds: Result<Vec<Value>> = arr
                .iter()
                .map(|v| {
                    if matches!(v, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                        let dt = crate::extract_timestamp(v)?;
                        Ok(Value::Int(dt.second() as i64))
                    } else {
                        Ok(Value::Null)
                    }
                })
                .collect();
            Ok(Value::Array(seconds?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                        let mut second_values = Vec::new();
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
                                                second_values.push(dt.second() as i64);
                                            }
                                            _ => {
                                                second_values.push(0i64);
                                            }
                                        }
                                    } else {
                                        second_values.push(0i64);
                                    }
                                }
                                _ => {
                                    second_values.push(0i64);
                                }
                            }
                        }
                        let second_series = Series::new(col_name.clone(), second_values);
                        new_series.push(second_series.into());
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
                    "second() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() || series.dtype() == &DataType::String {
                let mut second_values = Vec::new();
                for i in 0..series.len() {
                    match series.get(i) {
                        Ok(val) => {
                            let value = value_from_any_value(val).unwrap_or(Value::Null);
                            if matches!(value, Value::Int(_) | Value::Float(_) | Value::String(_)) {
                                match crate::extract_timestamp(&value) {
                                    Ok(dt) => {
                                        second_values.push(dt.second() as i64);
                                    }
                                    _ => {
                                        second_values.push(0i64);
                                    }
                                }
                            } else {
                                second_values.push(0i64);
                            }
                        }
                        _ => {
                            second_values.push(0i64);
                        }
                    }
                }
                Ok(Value::Series(Series::new(
                    series.name().clone(),
                    second_values,
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
            builtin_second(&[Value::DataFrame(df)])
        }
        _ => Err(dsq_shared::error::operation_error(
            "second() requires timestamp, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "second",
        func: builtin_second,
    }
}
