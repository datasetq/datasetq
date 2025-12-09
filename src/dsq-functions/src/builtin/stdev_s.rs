use dsq_shared::value::Value;
use dsq_shared::Result;
use std::collections::HashMap;

pub fn builtin_stdev_s(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "stdev_s() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let values: Vec<f64> = arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            if values.len() < 2 {
                return Ok(Value::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            Ok(Value::Float(variance.sqrt()))
        }
        Value::DataFrame(df) => {
            let mut stds = HashMap::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() {
                        // std calculation for DataFrame columns - placeholder
                        stds.insert(col_name.to_string(), Value::Null);
                    }
                }
            }
            Ok(Value::Object(stds))
        }
        Value::Series(_series) => {
            Ok(Value::Null) // Placeholder - std calculation for series
        }
        _ => Err(dsq_shared::error::operation_error(
            "stdev_s() requires array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "stdev_s",
        func: builtin_stdev_s,
    }
}
