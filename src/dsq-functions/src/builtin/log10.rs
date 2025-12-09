use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "log10",
        func: builtin_log10,
    }
}

pub fn builtin_log10(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "log10() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(i) => {
            let x = *i as f64;
            if x <= 0.0 {
                return Err(dsq_shared::error::operation_error(
                    "log10() domain error: argument must be positive",
                ));
            }
            Ok(Value::Float(x.log10()))
        }
        Value::Float(f) => {
            if *f <= 0.0 {
                return Err(dsq_shared::error::operation_error(
                    "log10() domain error: argument must be positive",
                ));
            }
            Ok(Value::Float(f.log10()))
        }
        Value::Array(arr) => {
            let mut result = Vec::new();
            for val in arr {
                match val {
                    Value::Int(i) => {
                        let x = *i as f64;
                        if x <= 0.0 {
                            return Err(dsq_shared::error::operation_error(
                                "log10() domain error: argument must be positive",
                            ));
                        }
                        result.push(Value::Float(x.log10()));
                    }
                    Value::Float(f) => {
                        if *f <= 0.0 {
                            return Err(dsq_shared::error::operation_error(
                                "log10() domain error: argument must be positive",
                            ));
                        }
                        result.push(Value::Float(f.log10()));
                    }
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "log10() requires numeric values in array",
                        ));
                    }
                }
            }
            Ok(Value::Array(result))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() {
                        let f64_series = series.f64().map_err(|e| {
                            dsq_shared::error::operation_error(format!(
                                "log10() failed to cast series to f64: {}",
                                e
                            ))
                        })?;
                        let log10_series = f64_series
                            .apply(|opt_f| {
                                opt_f.and_then(|f| if f <= 0.0 { None } else { Some(f.log10()) })
                            })
                            .into_series();
                        let mut s = log10_series;
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
                    "log10() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                let f64_series = series.f64().map_err(|e| {
                    dsq_shared::error::operation_error(format!(
                        "log10() failed to cast series to f64: {}",
                        e
                    ))
                })?;
                let log10_series = f64_series
                    .apply(|opt_f| {
                        opt_f.and_then(|f| if f <= 0.0 { None } else { Some(f.log10()) })
                    })
                    .into_series();
                Ok(Value::Series(log10_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "log10() requires numeric, array, DataFrame, or Series",
        )),
    }
}
