use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_correl(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "correl() expects 2 arguments",
        ));
    }

    match (&args[0], &args[1]) {
        (Value::Array(arr1), Value::Array(arr2)) if arr1.len() == arr2.len() => {
            let values1: Vec<f64> = arr1
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            let values2: Vec<f64> = arr2
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            if values1.len() != values2.len() || values1.len() < 2 {
                return Ok(Value::Null);
            }
            // Simple correlation calculation
            let mean1 = values1.iter().sum::<f64>() / values1.len() as f64;
            let mean2 = values2.iter().sum::<f64>() / values2.len() as f64;
            let mut numerator = 0.0;
            let mut sum_sq1 = 0.0;
            let mut sum_sq2 = 0.0;
            for i in 0..values1.len() {
                let diff1 = values1[i] - mean1;
                let diff2 = values2[i] - mean2;
                numerator += diff1 * diff2;
                sum_sq1 += diff1 * diff1;
                sum_sq2 += diff2 * diff2;
            }
            if sum_sq1 == 0.0 || sum_sq2 == 0.0 {
                Ok(Value::Float(0.0))
            } else {
                Ok(Value::Float(numerator / (sum_sq1 * sum_sq2).sqrt()))
            }
        }
        (Value::Series(series1), Value::Series(series2)) => {
            if series1.dtype().is_numeric() && series2.dtype().is_numeric() {
                // Use Polars correlation if available
                Ok(Value::Null) // Placeholder
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "correl() requires two arrays or two series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "correl",
        func: builtin_correl,
    }
}
