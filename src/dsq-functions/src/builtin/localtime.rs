use chrono::{DateTime, Local, TimeZone, Utc};
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_localtime(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "localtime() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(i) => {
            let dt = Utc
                .timestamp_opt(*i, 0)
                .single()
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp"))?;
            let local_dt: DateTime<Local> = dt.with_timezone(&Local);
            Ok(Value::String(
                local_dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            ))
        }
        Value::Float(f) => {
            let secs = f.trunc() as i64;
            let nanos = (f.fract() * 1_000_000_000.0) as u32;
            let dt = Utc
                .timestamp_opt(secs, nanos)
                .single()
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp"))?;
            let local_dt: DateTime<Local> = dt.with_timezone(&Local);
            Ok(Value::String(
                local_dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            ))
        }
        Value::String(s) => {
            // Try to parse as timestamp number
            if let Ok(ts) = s.parse::<i64>() {
                let dt = Utc
                    .timestamp_opt(ts, 0)
                    .single()
                    .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp"))?;
                let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                Ok(Value::String(
                    local_dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                ))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Array(arr) => {
            let localtimes: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::Int(i) => {
                        let dt = Utc.timestamp_opt(*i, 0).single().ok_or_else(|| {
                            dsq_shared::error::operation_error("Invalid timestamp")
                        })?;
                        let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                        Ok(Value::String(
                            local_dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                        ))
                    }
                    Value::Float(f) => {
                        let secs = f.trunc() as i64;
                        let nanos = (f.fract() * 1_000_000_000.0) as u32;
                        let dt = Utc.timestamp_opt(secs, nanos).single().ok_or_else(|| {
                            dsq_shared::error::operation_error("Invalid timestamp")
                        })?;
                        let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                        Ok(Value::String(
                            local_dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                        ))
                    }
                    Value::String(s) => {
                        if let Ok(ts) = s.parse::<i64>() {
                            let dt = Utc.timestamp_opt(ts, 0).single().ok_or_else(|| {
                                dsq_shared::error::operation_error("Invalid timestamp")
                            })?;
                            let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                            Ok(Value::String(
                                local_dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                            ))
                        } else {
                            Ok(Value::Null)
                        }
                    }
                    _ => Ok(Value::Null),
                })
                .collect();
            Ok(Value::Array(localtimes?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() {
                        let mut localtime_values = Vec::new();
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
                                        let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                                        localtime_values
                                            .push(local_dt.format("%Y-%m-%d %H:%M:%S").to_string());
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
                                        let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                                        localtime_values
                                            .push(local_dt.format("%Y-%m-%d %H:%M:%S").to_string());
                                    }
                                    _ => localtime_values.push("".to_string()),
                                }
                            } else {
                                localtime_values.push("".to_string());
                            }
                        }
                        let localtime_series = Series::new(col_name, localtime_values);
                        new_series.push(localtime_series);
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
                    "localtime() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                let mut localtime_values = Vec::new();
                for i in 0..series.len() {
                    if let Ok(val) = series.get(i) {
                        match val {
                            AnyValue::Int64(ts) => {
                                let dt = Utc.timestamp_opt(ts, 0).single().ok_or_else(|| {
                                    dsq_shared::error::operation_error("Invalid timestamp")
                                })?;
                                let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                                localtime_values
                                    .push(local_dt.format("%Y-%m-%d %H:%M:%S").to_string());
                            }
                            AnyValue::Float64(ts) => {
                                let secs = ts.trunc() as i64;
                                let nanos = (ts.fract() * 1_000_000_000.0) as u32;
                                let dt =
                                    Utc.timestamp_opt(secs, nanos).single().ok_or_else(|| {
                                        dsq_shared::error::operation_error("Invalid timestamp")
                                    })?;
                                let local_dt: DateTime<Local> = dt.with_timezone(&Local);
                                localtime_values
                                    .push(local_dt.format("%Y-%m-%d %H:%M:%S").to_string());
                            }
                            _ => localtime_values.push("".to_string()),
                        }
                    } else {
                        localtime_values.push("".to_string());
                    }
                }
                Ok(Value::Series(Series::new("", localtime_values)))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Ok(Value::Null),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "localtime",
        func: builtin_localtime,
    }
}
