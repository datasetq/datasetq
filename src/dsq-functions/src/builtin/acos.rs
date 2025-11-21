use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "acos",
        func: builtin_acos,
    }
}

pub fn builtin_acos(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "acos() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(i) => {
            let x = *i as f64;
            if x < -1.0 || x > 1.0 {
                return Err(dsq_shared::error::operation_error(
                    "acos() domain error: argument must be between -1 and 1",
                ));
            }
            Ok(Value::Float(x.acos()))
        }
        Value::Float(f) => {
            if *f < -1.0 || *f > 1.0 {
                return Err(dsq_shared::error::operation_error(
                    "acos() domain error: argument must be between -1 and 1",
                ));
            }
            Ok(Value::Float(f.acos()))
        }
        Value::Array(arr) => {
            let mut result = Vec::new();
            for val in arr {
                match val {
                    Value::Int(i) => {
                        let x = *i as f64;
                        if x < -1.0 || x > 1.0 {
                            return Err(dsq_shared::error::operation_error(
                                "acos() domain error: argument must be between -1 and 1",
                            ));
                        }
                        result.push(Value::Float(x.acos()));
                    }
                    Value::Float(f) => {
                        if *f < -1.0 || *f > 1.0 {
                            return Err(dsq_shared::error::operation_error(
                                "acos() domain error: argument must be between -1 and 1",
                            ));
                        }
                        result.push(Value::Float(f.acos()));
                    }
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "acos() requires numeric values in array",
                        ))
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
                        let acos_series = series
                            .f64()
                            .unwrap()
                            .apply(|opt_f| {
                                opt_f.and_then(|f| {
                                    if f < -1.0 || f > 1.0 {
                                        None
                                    } else {
                                        Some(f.acos())
                                    }
                                })
                            })
                            .into_series();
                        let mut s = acos_series;
                        s.rename(col_name);
                        new_series.push(s);
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
                    "acos() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                let acos_series = series
                    .f64()
                    .unwrap()
                    .apply(|opt_f| {
                        opt_f.and_then(|f| {
                            if f < -1.0 || f > 1.0 {
                                None
                            } else {
                                Some(f.acos())
                            }
                        })
                    })
                    .into_series();
                Ok(Value::Series(acos_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "acos() requires numeric, array, DataFrame, or Series",
        )),
    }
}
