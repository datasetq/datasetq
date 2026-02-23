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
        (Value::LazyFrame(lf1), Value::LazyFrame(lf2)) => {
            // Collect both LazyFrames to DataFrames
            let df1 = lf1.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!(
                    "Failed to collect first LazyFrame: {}",
                    e
                ))
            })?;
            let df2 = lf2.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!(
                    "Failed to collect second LazyFrame: {}",
                    e
                ))
            })?;

            // Get first column from each DataFrame as Series
            let series1 = df1
                .get_columns()
                .first()
                .ok_or_else(|| {
                    dsq_shared::error::operation_error("First LazyFrame has no columns")
                })?
                .as_materialized_series()
                .clone();
            let series2 = df2
                .get_columns()
                .first()
                .ok_or_else(|| {
                    dsq_shared::error::operation_error("Second LazyFrame has no columns")
                })?
                .as_materialized_series()
                .clone();

            // Recursively call with Series
            builtin_correl(&[Value::Series(series1), Value::Series(series2)])
        }
        (Value::LazyFrame(lf), other) | (other, Value::LazyFrame(lf)) => {
            // Collect LazyFrame and convert to Series
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            let series = df
                .get_columns()
                .first()
                .ok_or_else(|| dsq_shared::error::operation_error("LazyFrame has no columns"))?
                .as_materialized_series()
                .clone();

            // Recursively call with Series and the other argument
            if matches!(&args[0], Value::LazyFrame(_)) {
                builtin_correl(&[Value::Series(series), other.clone()])
            } else {
                builtin_correl(&[other.clone(), Value::Series(series)])
            }
        }
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
            "correl() requires two arrays, two series, or two LazyFrames",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "correl",
        func: builtin_correl,
    }
}
