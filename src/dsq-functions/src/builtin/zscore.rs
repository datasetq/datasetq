use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Z-score normalization
///
/// Computes z-score: (x - mean) / std_dev
///
/// # Arguments
/// * `values` - Array of numeric values
///
/// # Returns
/// Array of z-scores
pub fn builtin_zscore(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "zscore() expects 1 argument (array of values)",
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

            if values.is_empty() {
                return Ok(Value::Array(vec![]));
            }

            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;
            let std_dev = variance.sqrt();

            if std_dev == 0.0 {
                return Err(dsq_shared::error::operation_error(
                    "Standard deviation is zero, cannot compute z-score",
                ));
            }

            let zscores: Vec<Value> = values
                .iter()
                .map(|&x| Value::Float((x - mean) / std_dev))
                .collect();

            Ok(Value::Array(zscores))
        }
        _ => Err(dsq_shared::error::operation_error(
            "zscore() requires an array of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "zscore",
        func: builtin_zscore,
    }
}
