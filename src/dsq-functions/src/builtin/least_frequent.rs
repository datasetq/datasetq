use dsq_shared::{
    value::{value_from_any_value, Value},
    Result,
};
use inventory;
use polars::prelude::*;
use serde_json;
use std::collections::HashMap;

use crate::FunctionRegistration;

pub fn builtin_least_frequent(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "least_frequent() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::LazyFrame(lf) => {
            // Select only the first column before collecting to avoid materializing entire LazyFrame
            let schema = (**lf).clone().collect_schema().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get LazyFrame schema: {}", e))
            })?;

            let col_names: Vec<_> = schema.iter_names().map(|s| s.as_str()).collect();
            if col_names.is_empty() {
                return Ok(Value::Null);
            }

            let first_col = col_names[0];
            let df = lf.clone().select([col(first_col)]).collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;

            // Recursively call with the collected DataFrame
            builtin_least_frequent(&[Value::DataFrame(df)])
        }
        Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(Value::Null);
            }
            let mut counts: HashMap<String, (Value, usize)> = HashMap::new();
            for val in arr {
                let key = serde_json::to_string(val).unwrap_or_default();
                let entry = counts.entry(key).or_insert_with(|| (val.clone(), 0));
                entry.1 += 1;
            }
            let min_count = counts.values().map(|(_, count)| *count).min().unwrap_or(0);
            let least_frequent: Vec<&Value> = counts
                .values()
                .filter(|(_, count)| *count == min_count)
                .map(|(val, _)| val)
                .collect();
            // Return the first one (arbitrary choice if multiple have same frequency)
            Ok(least_frequent.first().map_or(Value::Null, |v| (*v).clone()))
        }
        Value::DataFrame(df) => {
            if df.height() == 0 {
                return Ok(Value::Null);
            }
            // For DataFrame, find least frequent value in the first column
            let col_names = df.get_column_names();
            if col_names.is_empty() {
                return Ok(Value::Null);
            }

            let series = df.column(col_names[0]).map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get first column: {}", e))
            })?;

            // Track counts and maintain insertion order
            let mut counts: HashMap<String, usize> = HashMap::new();
            let mut value_order: Vec<Value> = Vec::new();
            let mut seen: HashMap<String, bool> = HashMap::new();

            for i in 0..series.len() {
                if let Ok(val) = series.get(i) {
                    let value = value_from_any_value(val).unwrap_or(Value::Null);
                    let key = serde_json::to_string(&value).unwrap_or_default();
                    *counts.entry(key.clone()).or_insert(0) += 1;
                    seen.entry(key).or_insert_with(|| {
                        value_order.push(value);
                        true
                    });
                }
            }

            if value_order.is_empty() {
                return Ok(Value::Null);
            }

            let min_count = counts.values().min().copied().unwrap_or(0);

            // Find the first value in order that has the minimum count
            for value in value_order.iter() {
                let key = serde_json::to_string(&value).unwrap_or_default();
                if counts.get(&key) == Some(&min_count) {
                    return Ok(value.clone());
                }
            }

            Ok(Value::Null)
        }
        Value::Series(series) => {
            if series.is_empty() {
                return Ok(Value::Null);
            }
            let mut counts: HashMap<String, (Value, usize)> = HashMap::new();
            for i in 0..series.len() {
                if let Ok(val) = series.get(i) {
                    let value = value_from_any_value(val).unwrap_or(Value::Null);
                    let key = serde_json::to_string(&value).unwrap_or_default();
                    let entry = counts.entry(key).or_insert_with(|| (value, 0));
                    entry.1 += 1;
                }
            }
            let min_count = counts.values().map(|(_, count)| *count).min().unwrap_or(0);
            let least_frequent: Vec<&Value> = counts
                .values()
                .filter(|(_, count)| *count == min_count)
                .map(|(val, _)| val)
                .collect();
            Ok(least_frequent.first().map_or(Value::Null, |v| (*v).clone()))
        }
        _ => Err(dsq_shared::error::operation_error(
            "least_frequent() requires array, DataFrame, LazyFrame, or Series",
        )),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "least_frequent",
        func: builtin_least_frequent,
    }
}
