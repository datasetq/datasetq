use crate::{Error, Result, TypeError, Value};
use polars::prelude::*;
use smallvec::SmallVec;
use std::collections::HashMap;

use super::{compare_values_for_ordering, df_to_array, AggregationFunction};

pub fn group_by(value: &Value, columns: &[String]) -> Result<Value> {
    if columns.is_empty() {
        return Err(Error::operation("Group by requires at least one column"));
    }

    match value {
        Value::DataFrame(df) => {
            // Convert DataFrame to array of objects, then group
            let arr = df_to_array(df)?;
            group_by(&Value::Array(arr), columns)
        }
        Value::LazyFrame(lf) => {
            let grouped = lf
                .clone()
                .group_by(columns.iter().map(col).collect::<Vec<_>>())
                .agg([col("*").count().alias("count")]);
            Ok(Value::LazyFrame(Box::new(grouped)))
        }
        Value::Array(arr) => {
            // Group array of objects by specified fields
            let mut groups: std::collections::BTreeMap<String, Vec<Value>> =
                std::collections::BTreeMap::new();

            for item in arr {
                if let Value::Object(obj) = item {
                    // Create group key from specified columns
                    let mut key_parts = Vec::new();
                    for col in columns {
                        if let Some(val) = obj.get(col) {
                            key_parts.push(format!("{val:?}"));
                        } else {
                            key_parts.push("null".to_string());
                        }
                    }
                    let key = key_parts.join("|");

                    groups.entry(key).or_default().push(item.clone());
                } else {
                    return Err(TypeError::UnsupportedOperation {
                        operation: "group_by".to_string(),
                        typ: item.type_name().to_string(),
                    }
                    .into());
                }
            }

            // Convert groups to array of arrays
            let grouped: Vec<Value> = groups.into_values().map(Value::Array).collect();

            Ok(Value::Array(grouped))
        }
        _ => Err(TypeError::UnsupportedOperation {
            operation: "group_by".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}

/// Apply aggregation functions to grouped data
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::group_by::{group_by_agg, AggregationFunction};
/// use dsq_core::value::Value;
///
/// let group_cols = vec!["department".to_string()];
/// let agg_funcs = vec![
///     AggregationFunction::Sum("salary".to_string()),
///     AggregationFunction::Mean("age".to_string()),
///     AggregationFunction::Count,
/// ];
/// let result = group_by_agg(&dataframe_value, &group_cols, &agg_funcs).unwrap();
/// ```
pub fn group_by_agg(
    value: &Value,
    group_columns: &[String],
    aggregations: &[AggregationFunction],
) -> Result<Value> {
    if group_columns.is_empty() {
        return Err(Error::operation("Group by requires at least one column"));
    }

    if aggregations.is_empty() {
        return Err(Error::operation(
            "Aggregation requires at least one function",
        ));
    }

    match value {
        Value::DataFrame(df) => {
            let group_exprs: Vec<Expr> = group_columns.iter().map(col).collect();
            let agg_exprs: Vec<Expr> = aggregations
                .iter()
                .map(AggregationFunction::to_polars_expr)
                .collect::<crate::Result<Vec<_>>>()?;

            let grouped = df
                .clone()
                .lazy()
                .group_by(group_exprs)
                .agg(agg_exprs)
                .collect()
                .map_err(Error::from)?;

            Ok(Value::DataFrame(grouped))
        }
        Value::LazyFrame(lf) => {
            let group_exprs: Vec<Expr> = group_columns.iter().map(col).collect();
            let agg_exprs: Vec<Expr> = aggregations
                .iter()
                .map(AggregationFunction::to_polars_expr)
                .collect::<crate::Result<Vec<_>>>()?;

            let grouped = lf.clone().group_by(group_exprs).agg(agg_exprs);

            Ok(Value::LazyFrame(Box::new(grouped)))
        }
        Value::Array(arr) => group_by_agg_array(arr, group_columns, aggregations),
        _ => Err(TypeError::UnsupportedOperation {
            operation: "group_by_agg".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}

/// Apply aggregations to array of objects (jq-style)
fn group_by_agg_array(
    arr: &[Value],
    group_columns: &[String],
    aggregations: &[AggregationFunction],
) -> Result<Value> {
    // First group the data
    let mut groups: std::collections::BTreeMap<String, Vec<&Value>> =
        std::collections::BTreeMap::new();

    for item in arr {
        match item {
            Value::Object(obj) => {
                // Create group key from specified columns
                let mut key_parts: SmallVec<[String; 8]> = SmallVec::new();
                for col in group_columns {
                    if let Some(val) = obj.get(col) {
                        let key_part = match val {
                            Value::String(s) => s.clone(),
                            Value::Int(i) => i.to_string(),
                            Value::BigInt(bi) => bi.to_string(),
                            Value::Float(f) => f.to_string(),
                            Value::Bool(b) => b.to_string(),
                            Value::Null => "null".to_string(),
                            _ => format!("{val:?}"), // For complex types, use debug
                        };
                        key_parts.push(key_part);
                    } else {
                        key_parts.push("null".to_string());
                    }
                }
                let key = key_parts.join("|");

                groups.entry(key).or_default().push(item);
            }
            _ => {
                return Err(TypeError::UnsupportedOperation {
                    operation: "group_by_agg".to_string(),
                    typ: item.type_name().to_string(),
                }
                .into());
            }
        }
    }

    // Apply aggregations to each group
    let mut result_rows = Vec::new();

    for (group_key, group_items) in groups {
        let mut result_row = HashMap::new();

        // Add group key columns
        let key_parts: Vec<&str> = group_key.split('|').collect();
        for (i, col) in group_columns.iter().enumerate() {
            if let Some(key_part) = key_parts.get(i) {
                // Try to parse back the original value type
                let value = if *key_part == "null" {
                    Value::Null
                } else if let Ok(int_val) = key_part.parse::<i64>() {
                    Value::Int(int_val)
                } else if let Ok(float_val) = key_part.parse::<f64>() {
                    Value::Float(float_val)
                } else if *key_part == "true" {
                    Value::Bool(true)
                } else if *key_part == "false" {
                    Value::Bool(false)
                } else {
                    // Remove quotes if present
                    let cleaned = key_part.trim_matches('"');
                    Value::String(cleaned.to_string())
                };
                result_row.insert(col.clone(), value);
            }
        }

        // Apply each aggregation
        for agg in aggregations {
            let agg_result = apply_aggregation_to_group(agg, &group_items)?;
            let col_name = agg.output_column_name();
            result_row.insert(col_name, agg_result);
        }

        result_rows.push(Value::Object(result_row));
    }

    Ok(Value::Array(result_rows))
}

/// Apply a single aggregation function to a group of objects
pub(super) fn apply_aggregation_to_group(
    agg: &AggregationFunction,
    group_items: &[&Value],
) -> Result<Value> {
    match agg {
        AggregationFunction::Count => Ok(Value::Int(
            i64::try_from(group_items.len()).unwrap_or(i64::MAX),
        )),
        AggregationFunction::Sum(col_name) => {
            let mut sum = 0.0;
            let mut count = 0;

            for item in group_items {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        match val {
                            Value::Int(i) => {
                                #[allow(clippy::cast_precision_loss)]
                                {
                                    sum += *i as f64;
                                }
                                count += 1;
                            }
                            Value::Float(f) => {
                                sum += f;
                                count += 1;
                            }
                            Value::Null => {} // Skip nulls
                            _ => {
                                return Err(TypeError::UnsupportedOperation {
                                    operation: "sum".to_string(),
                                    typ: val.type_name().to_string(),
                                }
                                .into());
                            }
                        }
                    }
                }
            }

            if count == 0 {
                Ok(Value::Null)
            } else {
                #[allow(clippy::cast_precision_loss)]
                if sum.fract() == 0.0 && sum <= i64::MAX as f64 && sum >= i64::MIN as f64 {
                    #[allow(clippy::cast_possible_truncation)]
                    Ok(Value::Int(sum as i64))
                } else {
                    Ok(Value::Float(sum))
                }
            }
        }
        AggregationFunction::Mean(col_name) => {
            let mut sum = 0.0;
            let mut count = 0;

            for item in group_items {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        match val {
                            Value::Int(i) => {
                                #[allow(clippy::cast_precision_loss)]
                                {
                                    sum += *i as f64;
                                }
                                count += 1;
                            }
                            Value::Float(f) => {
                                sum += f;
                                count += 1;
                            }
                            Value::Null => {} // Skip nulls
                            _ => {
                                return Err(TypeError::UnsupportedOperation {
                                    operation: "mean".to_string(),
                                    typ: val.type_name().to_string(),
                                }
                                .into());
                            }
                        }
                    }
                }
            }

            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Float(sum / f64::from(count)))
            }
        }
        AggregationFunction::Min(col_name) => {
            let mut min_val: Option<&Value> = None;

            for item in group_items {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        if !matches!(val, Value::Null) {
                            match min_val {
                                None => min_val = Some(val),
                                Some(current_min) => {
                                    if compare_values_for_ordering(val, current_min)
                                        == std::cmp::Ordering::Less
                                    {
                                        min_val = Some(val);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Ok(min_val.map_or(Value::Null, Clone::clone))
        }
        AggregationFunction::Max(col_name) => {
            let mut max_val: Option<&Value> = None;

            for item in group_items {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        if !matches!(val, Value::Null) {
                            match max_val {
                                None => max_val = Some(val),
                                Some(current_max) => {
                                    if compare_values_for_ordering(val, current_max)
                                        == std::cmp::Ordering::Greater
                                    {
                                        max_val = Some(val);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Ok(max_val.map_or(Value::Null, Clone::clone))
        }
        AggregationFunction::First(col_name) => {
            for item in group_items {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        return Ok(val.clone());
                    }
                }
            }
            Ok(Value::Null)
        }
        AggregationFunction::Last(col_name) => {
            for item in group_items.iter().rev() {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        return Ok(val.clone());
                    }
                }
            }
            Ok(Value::Null)
        }
        AggregationFunction::List(col_name) => {
            let mut values: SmallVec<[Value; 16]> = SmallVec::new();

            for item in group_items {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        values.push(val.clone());
                    } else {
                        values.push(Value::Null);
                    }
                }
            }

            Ok(Value::Array(values.into_vec()))
        }
        AggregationFunction::CountUnique(col_name) => {
            let mut unique_values = std::collections::HashSet::new();

            for item in group_items {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        unique_values.insert(format!("{val:?}"));
                    }
                }
            }

            #[allow(clippy::cast_possible_wrap)]
            {
                Ok(Value::Int(unique_values.len() as i64))
            }
        }
        AggregationFunction::StringConcat(col_name, separator) => {
            let mut string_values: SmallVec<[String; 16]> = SmallVec::new();
            let sep = separator.as_deref().unwrap_or(",");

            for item in group_items {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        match val {
                            Value::String(s) => string_values.push(s.clone()),
                            Value::Null => {} // Skip nulls
                            _ => string_values.push(val.to_string()),
                        }
                    }
                }
            }

            Ok(Value::String(string_values.join(sep)))
        }
        AggregationFunction::Median(col_name) => {
            let mut numeric_values = Vec::with_capacity(group_items.len());

            for item in group_items {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        match val {
                            Value::Int(i) => {
                                #[allow(clippy::cast_precision_loss)]
                                {
                                    numeric_values.push(*i as f64);
                                }
                            }
                            Value::Float(f) => numeric_values.push(*f),
                            Value::Null => {} // Skip nulls
                            _ => {
                                return Err(TypeError::UnsupportedOperation {
                                    operation: "median".to_string(),
                                    typ: val.type_name().to_string(),
                                }
                                .into());
                            }
                        }
                    }
                }
            }

            if numeric_values.is_empty() {
                return Ok(Value::Null);
            }

            numeric_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            let median = if numeric_values.len() % 2 == 0 {
                let mid = numeric_values.len() / 2;
                f64::midpoint(numeric_values[mid - 1], numeric_values[mid])
            } else {
                numeric_values[numeric_values.len() / 2]
            };

            Ok(Value::Float(median))
        }
        AggregationFunction::Std(col_name) => {
            let mut numeric_values = Vec::with_capacity(group_items.len());

            for item in group_items {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        match val {
                            Value::Int(i) => {
                                #[allow(clippy::cast_precision_loss)]
                                {
                                    numeric_values.push(*i as f64);
                                }
                            }
                            Value::Float(f) => numeric_values.push(*f),
                            Value::Null => {} // Skip nulls
                            _ => {
                                return Err(TypeError::UnsupportedOperation {
                                    operation: "std".to_string(),
                                    typ: val.type_name().to_string(),
                                }
                                .into());
                            }
                        }
                    }
                }
            }

            if numeric_values.len() <= 1 {
                return Ok(Value::Null);
            }

            #[allow(clippy::cast_precision_loss)]
            let mean = numeric_values.iter().sum::<f64>() / numeric_values.len() as f64;
            #[allow(clippy::cast_precision_loss)]
            let variance = numeric_values
                .iter()
                .map(|x| (x - mean).powi(2))
                .sum::<f64>()
                / (numeric_values.len() - 1) as f64;

            Ok(Value::Float(variance.sqrt()))
        }
        AggregationFunction::Var(col_name) => {
            let mut numeric_values = Vec::with_capacity(group_items.len());

            for item in group_items {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(col_name) {
                        match val {
                            Value::Int(i) => {
                                #[allow(clippy::cast_precision_loss)]
                                {
                                    numeric_values.push(*i as f64);
                                }
                            }
                            Value::Float(f) => numeric_values.push(*f),
                            Value::Null => {} // Skip nulls
                            _ => {
                                return Err(TypeError::UnsupportedOperation {
                                    operation: "var".to_string(),
                                    typ: val.type_name().to_string(),
                                }
                                .into());
                            }
                        }
                    }
                }
            }

            if numeric_values.len() <= 1 {
                return Ok(Value::Null);
            }

            #[allow(clippy::cast_precision_loss)]
            let mean = numeric_values.iter().sum::<f64>() / numeric_values.len() as f64;
            #[allow(clippy::cast_precision_loss)]
            let variance = numeric_values
                .iter()
                .map(|x| (x - mean).powi(2))
                .sum::<f64>()
                / (numeric_values.len() - 1) as f64;

            Ok(Value::Float(variance))
        }
    }
}
