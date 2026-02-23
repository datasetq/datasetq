use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use std::collections::HashMap;

pub fn builtin_mean(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "mean() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(Value::Null);
            }
            let mut sum = 0.0;
            let mut count = 0;
            for val in arr {
                match val {
                    Value::Int(i) => {
                        sum += *i as f64;
                        count += 1;
                    }
                    Value::Float(f) => {
                        sum += *f;
                        count += 1;
                    }
                    Value::BigInt(bi) => {
                        sum += bi.to_string().parse::<f64>().unwrap_or(0.0);
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Float(sum / count as f64))
            }
        }
        Value::DataFrame(df) => {
            let mut means = HashMap::new();
            for col_name in df.get_column_names() {
                if let Ok(column) = df.column(col_name) {
                    let series = column.as_materialized_series();
                    if series.dtype().is_numeric() {
                        if let Some(mean_val) = series.mean() {
                            means.insert(col_name.to_string(), Value::Float(mean_val));
                        }
                    }
                }
            }
            Ok(Value::Object(means))
        }
        Value::LazyFrame(lf) => {
            // Collect to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_mean(&[Value::DataFrame(df)])
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                Ok(series.mean().map(Value::Float).unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "mean() requires array, DataFrame, LazyFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "mean",
        func: builtin_mean,
    }
}
