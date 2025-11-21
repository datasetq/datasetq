use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::collections::HashMap;

pub fn builtin_histogram(args: &[Value]) -> Result<Value> {
    if args.len() < 1 || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "histogram() expects 1 or 2 arguments",
        ));
    }

    let bins = if args.len() == 2 {
        match &args[1] {
            Value::Int(i) if *i > 0 => *i as usize,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "histogram() bins must be positive integer",
                ));
            }
        }
    } else {
        10
    };

    let values = match &args[0] {
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| match v {
                Value::Int(i) => Some(*i as f64),
                Value::Float(f) => Some(*f),
                _ => None,
            })
            .collect::<Vec<f64>>(),
        Value::Series(series) => {
            if let Ok(float_chunked) = series.cast(&DataType::Float64) {
                float_chunked
                    .f64()
                    .unwrap()
                    .into_iter()
                    .flatten()
                    .collect::<Vec<f64>>()
            } else if let Ok(int_chunked) = series.cast(&DataType::Int64) {
                int_chunked
                    .i64()
                    .unwrap()
                    .into_iter()
                    .flatten()
                    .map(|x| x as f64)
                    .collect::<Vec<f64>>()
            } else {
                return Err(dsq_shared::error::operation_error(
                    "histogram() requires numeric series",
                ));
            }
        }
        _ => {
            return Err(dsq_shared::error::operation_error(
                "histogram() requires array or series",
            ));
        }
    };

    if values.is_empty() {
        let mut obj = HashMap::new();
        obj.insert("counts".to_string(), Value::Array(vec![]));
        obj.insert("bins".to_string(), Value::Array(vec![]));
        return Ok(Value::Object(obj));
    }

    let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    let bin_width = (max - min) / bins as f64;
    let mut counts = vec![0; bins];
    for &val in &values {
        let bin = if bin_width == 0.0 {
            0
        } else {
            ((val - min) / bin_width).floor() as usize
        };
        let bin = bin.min(bins - 1);
        counts[bin] += 1;
    }

    let bin_edges: Vec<Value> = (0..=bins)
        .map(|i| Value::Float(min + i as f64 * bin_width))
        .collect();
    let counts_values: Vec<Value> = counts.into_iter().map(|c| Value::Int(c as i64)).collect();

    let mut obj = HashMap::new();
    obj.insert("counts".to_string(), Value::Array(counts_values));
    obj.insert("bins".to_string(), Value::Array(bin_edges));
    Ok(Value::Object(obj))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "histogram",
        func: builtin_histogram,
    }
}
