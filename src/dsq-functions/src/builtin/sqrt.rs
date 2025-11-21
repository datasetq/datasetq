use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "sqrt",
        func: builtin_sqrt,
    }
}

pub fn builtin_sqrt(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "sqrt() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(i) => {
            if *i < 0 {
                Err(dsq_shared::error::operation_error(
                    "sqrt() domain error: negative number",
                ))
            } else {
                Ok(Value::Float((*i as f64).sqrt()))
            }
        }
        Value::Float(f) => {
            if *f < 0.0 {
                Err(dsq_shared::error::operation_error(
                    "sqrt() domain error: negative number",
                ))
            } else {
                Ok(Value::Float(f.sqrt()))
            }
        }
        Value::Array(arr) => {
            let results: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::Int(i) => {
                        if *i < 0 {
                            Err(dsq_shared::error::operation_error(
                                "sqrt() domain error: negative number",
                            ))
                        } else {
                            Ok(Value::Float((*i as f64).sqrt()))
                        }
                    }
                    Value::Float(f) => {
                        if *f < 0.0 {
                            Err(dsq_shared::error::operation_error(
                                "sqrt() domain error: negative number",
                            ))
                        } else {
                            Ok(Value::Float(f.sqrt()))
                        }
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "sqrt() requires numeric values in array",
                    )),
                })
                .collect();
            Ok(Value::Array(results?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() {
                        let f64_series = series.f64().map_err(|e| {
                            dsq_shared::error::operation_error(format!(
                                "sqrt() failed to cast series to f64: {}",
                                e
                            ))
                        })?;
                        let sqrt_series = f64_series.apply(|v| v.map(|v| v.sqrt())).into_series();
                        let mut s = sqrt_series;
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
                    "sqrt() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                let f64_series = series.f64().map_err(|e| {
                    dsq_shared::error::operation_error(format!(
                        "sqrt() failed to cast series to f64: {}",
                        e
                    ))
                })?;
                let sqrt_series = f64_series.apply(|v| v.map(|v| v.sqrt())).into_series();
                Ok(Value::Series(sqrt_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "sqrt() requires numeric argument, array, DataFrame, or Series",
        )),
    }
}
