use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "sin",
        func: builtin_sin,
    }
}

pub fn builtin_sin(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "sin() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(i) => Ok(Value::Float((*i as f64).sin())),
        Value::Float(f) => Ok(Value::Float(f.sin())),
        Value::Array(arr) => {
            let mut result = Vec::new();
            for val in arr {
                match val {
                    Value::Int(i) => result.push(Value::Float((*i as f64).sin())),
                    Value::Float(f) => result.push(Value::Float(f.sin())),
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "sin() requires numeric values in array",
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
                        let sin_series = series
                            .f64()
                            .unwrap()
                            .apply(|opt_f| opt_f.map(|f| f.sin()))
                            .into_series();
                        let mut s = sin_series;
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
                    "sin() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                let sin_series = series
                    .f64()
                    .unwrap()
                    .apply(|opt_f| opt_f.map(|f| f.sin()))
                    .into_series();
                Ok(Value::Series(sin_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "sin() requires numeric, array, DataFrame, or Series",
        )),
    }
}
