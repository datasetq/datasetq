use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "asin",
        func: builtin_asin,
    }
}

pub fn builtin_asin(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "asin() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(i) => {
            let x = *i as f64;
            if !(-1.0..=1.0).contains(&x) {
                return Err(dsq_shared::error::operation_error(
                    "asin() domain error: argument must be between -1 and 1",
                ));
            }
            Ok(Value::Float(x.asin()))
        }
        Value::Float(f) => {
            if *f < -1.0 || *f > 1.0 {
                return Err(dsq_shared::error::operation_error(
                    "asin() domain error: argument must be between -1 and 1",
                ));
            }
            Ok(Value::Float(f.asin()))
        }
        Value::Array(arr) => {
            let mut result = Vec::new();
            for val in arr {
                match val {
                    Value::Int(i) => {
                        let x = *i as f64;
                        if !(-1.0..=1.0).contains(&x) {
                            return Err(dsq_shared::error::operation_error(
                                "asin() domain error: argument must be between -1 and 1",
                            ));
                        }
                        result.push(Value::Float(x.asin()));
                    }
                    Value::Float(f) => {
                        if *f < -1.0 || *f > 1.0 {
                            return Err(dsq_shared::error::operation_error(
                                "asin() domain error: argument must be between -1 and 1",
                            ));
                        }
                        result.push(Value::Float(f.asin()));
                    }
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "asin() requires numeric values in array",
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
                                "asin() failed to cast series to f64: {}",
                                e
                            ))
                        })?;
                        let asin_series = f64_series
                            .apply(|opt_f| {
                                opt_f.and_then(|f| {
                                    if !(-1.0..=1.0).contains(&f) {
                                        None
                                    } else {
                                        Some(f.asin())
                                    }
                                })
                            })
                            .into_series();
                        let mut s = asin_series;
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
                    "asin() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                let f64_series = series.f64().map_err(|e| {
                    dsq_shared::error::operation_error(format!(
                        "asin() failed to cast series to f64: {}",
                        e
                    ))
                })?;
                let asin_series = f64_series
                    .apply(|opt_f| {
                        opt_f.and_then(|f| {
                            if !(-1.0..=1.0).contains(&f) {
                                None
                            } else {
                                Some(f.asin())
                            }
                        })
                    })
                    .into_series();
                Ok(Value::Series(asin_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "asin() requires numeric, array, DataFrame, or Series",
        )),
    }
}
