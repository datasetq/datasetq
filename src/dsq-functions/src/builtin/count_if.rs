use dsq_shared::value::{is_truthy, Value};
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_count_if(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "count_if() expects 2 arguments: collection and mask",
        ));
    }

    let collection = &args[0];
    let mask = &args[1];

    match (collection, mask) {
        (Value::LazyFrame(lf), Value::Series(mask_series)) => {
            // Collect LazyFrame to DataFrame
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;

            // Recursively call with the collected DataFrame
            builtin_count_if(&[Value::DataFrame(df), Value::Series(mask_series.clone())])
        }
        (Value::Array(arr), Value::Array(mask_arr)) => {
            if arr.len() != mask_arr.len() {
                return Err(dsq_shared::error::operation_error(
                    "count_if() collection and mask arrays must have same length",
                ));
            }
            let count = arr
                .iter()
                .zip(mask_arr.iter())
                .filter(|(_, m)| is_truthy(m))
                .count();
            Ok(Value::Int(count as i64))
        }
        (Value::DataFrame(df), Value::Series(mask_series)) => {
            if df.height() != mask_series.len() {
                return Err(dsq_shared::error::operation_error(
                    "count_if() DataFrame and mask series must have same length",
                ));
            }
            let count = (0..mask_series.len())
                .filter(|&i| {
                    mask_series
                        .get(i)
                        .ok()
                        .map(|v| matches!(v, AnyValue::Boolean(true)))
                        .unwrap_or(false)
                })
                .count();
            Ok(Value::Int(count as i64))
        }
        (Value::Series(series), Value::Series(mask_series)) => {
            if series.len() != mask_series.len() {
                return Err(dsq_shared::error::operation_error(
                    "count_if() series and mask series must have same length",
                ));
            }
            let count = (0..mask_series.len())
                .filter(|&i| {
                    mask_series
                        .get(i)
                        .ok()
                        .map(|v| matches!(v, AnyValue::Boolean(true)))
                        .unwrap_or(false)
                })
                .count();
            Ok(Value::Int(count as i64))
        }
        _ => Err(dsq_shared::error::operation_error(
            "count_if() requires (array, array) or (dataframe/lazyframe/series, series)",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "count_if",
        func: builtin_count_if,
    }
}
