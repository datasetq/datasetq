use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "cos",
        func: builtin_cos,
    }
}

pub fn builtin_cos(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "cos() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(i) => Ok(Value::Float((*i as f64).cos())),
        Value::Float(f) => Ok(Value::Float(f.cos())),
        Value::Array(arr) => {
            let mut result = Vec::new();
            for val in arr {
                match val {
                    Value::Int(i) => result.push(Value::Float((*i as f64).cos())),
                    Value::Float(f) => result.push(Value::Float(f.cos())),
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "cos() requires numeric values in array",
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
                                "cos() failed to cast series to f64: {}",
                                e
                            ))
                        })?;
                        let cos_series = f64_series
                            .apply(|opt_f| opt_f.map(|f| f.cos()))
                            .into_series();
                        let mut s = cos_series;
                        s.rename(col_name.clone());
                        new_series.push(s.into());
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
                    "cos() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                let f64_series = series.f64().map_err(|e| {
                    dsq_shared::error::operation_error(format!(
                        "cos() failed to cast series to f64: {}",
                        e
                    ))
                })?;
                let cos_series = f64_series
                    .apply(|opt_f| opt_f.map(|f| f.cos()))
                    .into_series();
                Ok(Value::Series(cos_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "cos() requires numeric, array, DataFrame, or Series",
        )),
    }
}
