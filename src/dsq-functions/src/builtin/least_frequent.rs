use dsq_shared::{
    value::{value_from_any_value, Value},
    Result,
};
use inventory;
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
            let min_count = counts.values().map(|(_, count)| *count).min().unwrap();
            let least_frequent: Vec<&Value> = counts
                .values()
                .filter(|(_, count)| *count == min_count)
                .map(|(val, _)| val)
                .collect();
            // Return the first one (arbitrary choice if multiple have same frequency)
            Ok((*least_frequent.first().unwrap()).clone())
        }
        Value::DataFrame(df) => {
            if df.height() == 0 {
                return Ok(Value::Null);
            }
            // For DataFrame, find least frequent value across all columns
            let mut all_values = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    for i in 0..series.len() {
                        if let Ok(val) = series.get(i) {
                            let value = value_from_any_value(val).unwrap_or(Value::Null);
                            all_values.push(value);
                        }
                    }
                }
            }
            let mut counts: HashMap<String, (Value, usize)> = HashMap::new();
            for val in &all_values {
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
            Ok(least_frequent.first().map_or(Value::Null, |v| (*v).clone()))
        }
        Value::Series(series) => {
            if series.len() == 0 {
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
            "least_frequent() requires array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "least_frequent",
        func: builtin_least_frequent,
    }
}
