use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use std::collections::HashMap;

pub fn builtin_median(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "median() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let mut values: Vec<f64> = arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(Value::Null);
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let mid = values.len() / 2;
            if values.len().is_multiple_of(2) {
                Ok(Value::Float((values[mid - 1] + values[mid]) / 2.0))
            } else {
                Ok(Value::Float(values[mid]))
            }
        }
        Value::DataFrame(df) => {
            let mut medians = HashMap::new();
            for col_name in df.get_column_names() {
                if let Ok(column) = df.column(col_name) {
                    let series = column.as_materialized_series();
                    if series.dtype().is_numeric() {
                        if let Some(median_val) = series.median() {
                            medians.insert(col_name.to_string(), Value::Float(median_val));
                        }
                    }
                }
            }
            Ok(Value::Object(medians))
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                Ok(series.median().map(Value::Float).unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "median() requires array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "median",
        func: builtin_median,
    }
}
