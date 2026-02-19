use dsq_shared::{
    value::{value_from_any_value, Value},
    Result,
};
use inventory;
use polars::prelude::*;
use serde_json;
use std::cmp::Ordering;

use crate::FunctionRegistration;

pub fn builtin_topk(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(dsq_shared::error::operation_error(
            "topk() expects 2 or 3 arguments (column, k, descending)",
        ));
    }

    let k = match &args[1] {
        Value::Int(i) => {
            if *i < 0 {
                return Err(dsq_shared::error::operation_error(
                    "topk() k must be a positive integer",
                ));
            }
            *i as usize
        }
        Value::Float(f) => {
            if *f < 0.0 {
                return Err(dsq_shared::error::operation_error(
                    "topk() k must be a positive integer",
                ));
            }
            *f as usize
        }
        _ => {
            return Err(dsq_shared::error::operation_error(
                "topk() k must be a number",
            ))
        }
    };

    let descending = if args.len() == 3 {
        match &args[2] {
            Value::Bool(b) => *b,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "topk() descending must be a boolean",
                ))
            }
        }
    } else {
        true // default: descending (top k largest)
    };

    match &args[0] {
        Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(Value::Array(vec![]));
            }

            let mut indexed_values: Vec<(usize, &Value)> = arr.iter().enumerate().collect();

            indexed_values.sort_by(|a, b| {
                let cmp = compare_values(a.1, b.1);
                if descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            });

            let result: Vec<Value> = indexed_values
                .iter()
                .take(k)
                .map(|(_, v)| (*v).clone())
                .collect();

            Ok(Value::Array(result))
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

            let mut indexed_values: Vec<(usize, Value)> = Vec::new();
            for i in 0..series.len() {
                if let Ok(val) = series.get(i) {
                    let value = value_from_any_value(val).unwrap_or(Value::Null);
                    indexed_values.push((i, value));
                }
            }

            indexed_values.sort_by(|a, b| {
                let cmp = compare_values(&a.1, &b.1);
                if descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            });

            let indices: Vec<u32> = indexed_values
                .iter()
                .take(k)
                .map(|(i, _)| *i as u32)
                .collect();

            let result_df = df.take(&IdxCa::new("".into(), &indices)).map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to select rows: {}", e))
            })?;

            Ok(Value::DataFrame(result_df))
        }
        Value::Series(series) => {
            if series.is_empty() {
                return Ok(Value::Array(vec![]));
            }

            let mut indexed_values: Vec<(usize, Value)> = Vec::new();
            for i in 0..series.len() {
                if let Ok(val) = series.get(i) {
                    let value = value_from_any_value(val).unwrap_or(Value::Null);
                    indexed_values.push((i, value));
                }
            }

            indexed_values.sort_by(|a, b| {
                let cmp = compare_values(&a.1, &b.1);
                if descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            });

            let result: Vec<Value> = indexed_values
                .iter()
                .take(k)
                .map(|(_, v)| v.clone())
                .collect();

            Ok(Value::Array(result))
        }
        _ => Err(dsq_shared::error::operation_error(
            "topk() requires array, DataFrame, or Series",
        )),
    }
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Int(i1), Value::Int(i2)) => i1.cmp(i2),
        (Value::Float(f1), Value::Float(f2)) => f1.partial_cmp(f2).unwrap_or(Ordering::Equal),
        (Value::Int(i), Value::Float(f)) => (*i as f64).partial_cmp(f).unwrap_or(Ordering::Equal),
        (Value::Float(f), Value::Int(i)) => f.partial_cmp(&(*i as f64)).unwrap_or(Ordering::Equal),
        (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
        (Value::Bool(b1), Value::Bool(b2)) => b1.cmp(b2),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        // For other types, compare as JSON strings
        _ => {
            let s1 = serde_json::to_string(a).unwrap_or_default();
            let s2 = serde_json::to_string(b).unwrap_or_default();
            s1.cmp(&s2)
        }
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "topk",
        func: builtin_topk,
    }
}
