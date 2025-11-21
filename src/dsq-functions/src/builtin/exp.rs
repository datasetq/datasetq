use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "exp",
        func: builtin_exp,
    }
}

pub fn builtin_exp(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "exp() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(i) => Ok(Value::Float((*i as f64).exp())),
        Value::Float(f) => Ok(Value::Float(f.exp())),
        Value::Array(arr) => {
            let mut result = Vec::new();
            for val in arr {
                match val {
                    Value::Int(i) => result.push(Value::Float((*i as f64).exp())),
                    Value::Float(f) => result.push(Value::Float(f.exp())),
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "exp() requires numeric values in array",
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
                        let exp_series = series
                            .f64()
                            .unwrap()
                            .apply(|opt_f| opt_f.map(|f| f.exp()))
                            .into_series();
                        let mut s = exp_series;
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
                    "exp() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                let exp_series = series
                    .f64()
                    .unwrap()
                    .apply(|opt_f| opt_f.map(|f| f.exp()))
                    .into_series();
                Ok(Value::Series(exp_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "exp() requires numeric, array, DataFrame, or Series",
        )),
    }
}
