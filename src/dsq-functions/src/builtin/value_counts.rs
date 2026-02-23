use dsq_shared::{
    value::{value_from_any_value, Value},
    Result,
};
use inventory;
use polars::prelude::*;
use serde_json;
use std::collections::HashMap;

use crate::FunctionRegistration;

pub fn builtin_value_counts(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "value_counts() expects 1 or 2 arguments (column, sort)",
        ));
    }

    let sort = if args.len() == 2 {
        match &args[1] {
            Value::Bool(b) => *b,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "value_counts() second argument must be a boolean",
                ))
            }
        }
    } else {
        false // default: don't sort
    };

    match &args[0] {
        Value::LazyFrame(lf) => {
            // Select only the first column before collecting to avoid materializing entire LazyFrame
            let schema = (**lf).clone().collect_schema().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get LazyFrame schema: {}", e))
            })?;

            let col_names: Vec<_> = schema.iter_names().map(|s| s.as_str()).collect();
            if col_names.is_empty() {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

            let first_col = col_names[0];
            let df = lf.clone().select([col(first_col)]).collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;

            // Recursively call with the collected DataFrame
            builtin_value_counts(&[
                Value::DataFrame(df),
                args.get(1).cloned().unwrap_or(Value::Bool(false)),
            ])
        }
        Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

            let mut counts: HashMap<String, (Value, usize)> = HashMap::new();
            let mut value_order: Vec<Value> = Vec::new();
            let mut seen: HashMap<String, bool> = HashMap::new();

            for val in arr {
                let key = serde_json::to_string(val).unwrap_or_default();
                if let Some(entry) = counts.get_mut(&key) {
                    entry.1 += 1;
                } else {
                    counts.insert(key.clone(), (val.clone(), 1));
                }
                seen.entry(key).or_insert_with(|| {
                    value_order.push(val.clone());
                    true
                });
            }

            let mut results: Vec<(Value, usize)> = value_order
                .iter()
                .map(|v| {
                    let key = serde_json::to_string(v).unwrap_or_default();
                    let count = counts.get(&key).map(|(_, c)| *c).unwrap_or(0);
                    (v.clone(), count)
                })
                .collect();

            if sort {
                results.sort_by(|a, b| b.1.cmp(&a.1));
            }

            // Convert to DataFrame with two columns: value and count
            let values: Vec<String> = results
                .iter()
                .map(|(v, _)| serde_json::to_string(v).unwrap_or_default())
                .collect();
            let counts_vec: Vec<u64> = results.iter().map(|(_, c)| *c as u64).collect();

            let value_series = Series::new("value".into(), values);
            let count_series = Series::new("count".into(), counts_vec);

            let df =
                DataFrame::new(vec![value_series.into(), count_series.into()]).map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to create DataFrame: {}", e))
                })?;

            Ok(Value::DataFrame(df))
        }
        Value::DataFrame(df) => {
            if df.height() == 0 {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

            let col_names = df.get_column_names();
            if col_names.is_empty() {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

            let series = df.column(col_names[0]).map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get first column: {}", e))
            })?;

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

            let mut results: Vec<(Value, usize)> = value_order
                .iter()
                .map(|v| {
                    let key = serde_json::to_string(v).unwrap_or_default();
                    let count = *counts.get(&key).unwrap_or(&0);
                    (v.clone(), count)
                })
                .collect();

            if sort {
                results.sort_by(|a, b| b.1.cmp(&a.1));
            }

            let values: Vec<String> = results
                .iter()
                .map(|(v, _)| serde_json::to_string(v).unwrap_or_default())
                .collect();
            let counts_vec: Vec<u64> = results.iter().map(|(_, c)| *c as u64).collect();

            let value_series = Series::new("value".into(), values);
            let count_series = Series::new("count".into(), counts_vec);

            let result_df = DataFrame::new(vec![value_series.into(), count_series.into()])
                .map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to create DataFrame: {}", e))
                })?;

            Ok(Value::DataFrame(result_df))
        }
        Value::Series(series) => {
            if series.is_empty() {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

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

            let mut results: Vec<(Value, usize)> = value_order
                .iter()
                .map(|v| {
                    let key = serde_json::to_string(v).unwrap_or_default();
                    let count = *counts.get(&key).unwrap_or(&0);
                    (v.clone(), count)
                })
                .collect();

            if sort {
                results.sort_by(|a, b| b.1.cmp(&a.1));
            }

            let values: Vec<String> = results
                .iter()
                .map(|(v, _)| serde_json::to_string(v).unwrap_or_default())
                .collect();
            let counts_vec: Vec<u64> = results.iter().map(|(_, c)| *c as u64).collect();

            let value_series = Series::new("value".into(), values);
            let count_series = Series::new("count".into(), counts_vec);

            let df =
                DataFrame::new(vec![value_series.into(), count_series.into()]).map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to create DataFrame: {}", e))
                })?;

            Ok(Value::DataFrame(df))
        }
        _ => Err(dsq_shared::error::operation_error(
            "value_counts() requires array, DataFrame, LazyFrame, or Series",
        )),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "value_counts",
        func: builtin_value_counts,
    }
}
