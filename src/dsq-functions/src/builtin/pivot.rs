use dsq_shared::Result;
use dsq_shared::value::{Value, value_from_any_value};
use inventory;
use polars::prelude::*;
use std::collections::HashMap;

pub fn builtin_pivot(args: &[Value]) -> Result<Value> {
    if args.len() < 4 || args.len() > 5 {
        return Err(dsq_shared::error::operation_error(
            "pivot() expects 4 or 5 arguments: dataframe, index_columns, pivot_column, value_column, optional agg_function",
        ));
    }

    let df = match &args[0] {
        Value::DataFrame(df) => df,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "pivot() first argument must be a DataFrame",
            ));
        }
    };

    let index_columns = match &args[1] {
        Value::Array(arr) => {
            let mut cols = Vec::new();
            for v in arr {
                if let Value::String(s) = v {
                    cols.push(s.clone());
                } else {
                    return Err(dsq_shared::error::operation_error(
                        "pivot() index_columns must be an array of strings",
                    ));
                }
            }
            cols
        }
        _ => {
            return Err(dsq_shared::error::operation_error(
                "pivot() second argument must be an array of column names",
            ));
        }
    };

    let pivot_column = match &args[2] {
        Value::String(s) => s,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "pivot() third argument must be a string (pivot column name)",
            ));
        }
    };

    let value_column = match &args[3] {
        Value::String(s) => s,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "pivot() fourth argument must be a string (value column name)",
            ));
        }
    };

    let agg_function = if args.len() == 5 {
        match &args[4] {
            Value::String(s) => Some(s.as_str()),
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "pivot() fifth argument must be a string (aggregation function)",
                ));
            }
        }
    } else {
        None
    };

    // Check if columns exist
    if !df.get_column_names().contains(&pivot_column.as_str()) {
        return Err(dsq_shared::error::operation_error(format!(
            "pivot() pivot column '{}' not found",
            pivot_column
        )));
    }
    if !df.get_column_names().contains(&value_column.as_str()) {
        return Err(dsq_shared::error::operation_error(format!(
            "pivot() value column '{}' not found",
            value_column
        )));
    }
    for col in &index_columns {
        if !df.get_column_names().contains(&col.as_str()) {
            return Err(dsq_shared::error::operation_error(format!(
                "pivot() index column '{}' not found",
                col
            )));
        }
    }

    // Get unique values in pivot column
    let pivot_series = df
        .column(pivot_column)
        .map_err(|_| dsq_shared::error::operation_error("Failed to get pivot column"))?;
    let mut pivot_values = Vec::new();
    for i in 0..pivot_series.len() {
        if let Ok(val) = pivot_series.get(i) {
            let value = value_from_any_value(val).unwrap_or(Value::Null);
            if !pivot_values.contains(&value) {
                pivot_values.push(value);
            }
        }
    }

    // Sort pivot values for consistent output
    pivot_values.sort_by(|a, b| format!("{}", a).cmp(&format!("{}", b)));

    // Create result columns: index columns + pivoted columns
    let mut result_columns = Vec::new();

    // Add index columns
    for col_name in &index_columns {
        if let Ok(series) = df.column(col_name) {
            let mut s = series.clone();
            s.rename(col_name);
            result_columns.push(s);
        }
    }

    // For each pivot value, create a column with aggregated values
    for pivot_val in &pivot_values {
        let mut aggregated_values = Vec::new();

        // Group by index columns and aggregate for this pivot value
        let mut groups: HashMap<Vec<Value>, Vec<Value>> = HashMap::new();

        for i in 0..df.height() {
            // Get index values
            let mut index_key = Vec::new();
            for col_name in &index_columns {
                if let Ok(series) = df.column(col_name) {
                    if let Ok(val) = series.get(i) {
                        let value = value_from_any_value(val).unwrap_or(Value::Null);
                        index_key.push(value);
                    }
                }
            }

            // Get pivot value for this row
            let row_pivot_val = if let Ok(val) = pivot_series.get(i) {
                value_from_any_value(val).unwrap_or(Value::Null)
            } else {
                Value::Null
            };

            // Get value for this row
            let row_value = if let Ok(series) = df.column(value_column) {
                if let Ok(val) = series.get(i) {
                    value_from_any_value(val).unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            };

            // Only include if pivot value matches
            if format!("{}", row_pivot_val) == format!("{}", pivot_val) {
                groups
                    .entry(index_key)
                    .or_insert_with(Vec::new)
                    .push(row_value);
            }
        }

        // For each group, aggregate the values
        let mut group_keys: Vec<Vec<Value>> = groups.keys().cloned().collect();
        group_keys.sort_by(|a, b| {
            for (va, vb) in a.iter().zip(b.iter()) {
                let cmp = format!("{}", va).cmp(&format!("{}", vb));
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        for key in &group_keys {
            if let Some(values) = groups.get(key) {
                let aggregated = aggregate_values(values, agg_function)?;
                aggregated_values.push(aggregated);
            }
        }

        // Create series for this pivot value
        let column_name = format!("{}_{}", pivot_column, pivot_val);
        let series = Series::new(&column_name, &aggregated_values);
        result_columns.push(series);
    }

    // Create result DataFrame
    match DataFrame::new(result_columns) {
        Ok(result_df) => Ok(Value::DataFrame(result_df)),
        Err(e) => Err(dsq_shared::error::operation_error(format!(
            "pivot() failed to create result DataFrame: {}",
            e
        ))),
    }
}

fn aggregate_values(values: &[Value], agg_function: Option<&str>) -> Result<AnyValue<'static>> {
    if values.is_empty() {
        return Ok(AnyValue::Null);
    }

    match agg_function {
        Some("sum") | None => {
            let mut sum = 0.0;
            for v in values {
                match v {
                    Value::Int(i) => sum += *i as f64,
                    Value::Float(f) => sum += *f,
                    _ => {}
                }
            }
            Ok(AnyValue::Float64(sum))
        }
        Some("mean") | Some("avg") => {
            let mut sum = 0.0;
            let mut count = 0;
            for v in values {
                match v {
                    Value::Int(i) => {
                        sum += *i as f64;
                        count += 1;
                    }
                    Value::Float(f) => {
                        sum += *f;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count == 0 {
                Ok(AnyValue::Null)
            } else {
                Ok(AnyValue::Float64(sum / count as f64))
            }
        }
        Some("count") => Ok(AnyValue::UInt32(values.len() as u32)),
        Some("min") => {
            let mut min_val = f64::INFINITY;
            for v in values {
                match v {
                    Value::Int(i) => min_val = min_val.min(*i as f64),
                    Value::Float(f) => min_val = min_val.min(*f),
                    _ => {}
                }
            }
            if min_val == f64::INFINITY {
                Ok(AnyValue::Null)
            } else {
                Ok(AnyValue::Float64(min_val))
            }
        }
        Some("max") => {
            let mut max_val = f64::NEG_INFINITY;
            for v in values {
                match v {
                    Value::Int(i) => max_val = max_val.max(*i as f64),
                    Value::Float(f) => max_val = max_val.max(*f),
                    _ => {}
                }
            }
            if max_val == f64::NEG_INFINITY {
                Ok(AnyValue::Null)
            } else {
                Ok(AnyValue::Float64(max_val))
            }
        }
        Some("first") => {
            if let Some(first) = values.first() {
                match first {
                    Value::Int(i) => Ok(AnyValue::Int64(*i as i64)),
                    Value::Float(f) => Ok(AnyValue::Float64(*f)),
                    Value::String(s) => Ok(AnyValue::Utf8(&s)),
                    Value::Bool(b) => Ok(AnyValue::Boolean(*b)),
                    _ => Ok(AnyValue::Null),
                }
            } else {
                Ok(AnyValue::Null)
            }
        }
        Some("last") => {
            if let Some(last) = values.last() {
                match last {
                    Value::Int(i) => Ok(AnyValue::Int64(*i as i64)),
                    Value::Float(f) => Ok(AnyValue::Float64(*f)),
                    Value::String(s) => Ok(AnyValue::Utf8(&s)),
                    Value::Bool(b) => Ok(AnyValue::Boolean(*b)),
                    _ => Ok(AnyValue::Null),
                }
            } else {
                Ok(AnyValue::Null)
            }
        }
        _ => Err(dsq_shared::error::operation_error(format!(
            "Unsupported aggregation function: {}",
            agg_function.unwrap_or("")
        ))),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "pivot",
        func: builtin_pivot,
    }
}
