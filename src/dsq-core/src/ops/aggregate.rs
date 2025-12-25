//! Aggregation operations for dsq
//!
//! This module provides aggregation functions for `DataFrames` including:
//! - Group by operations
//! - Statistical aggregations (sum, mean, count, etc.)
//! - Window functions
//! - Pivot and unpivot operations
//!
//! These operations correspond to common SQL aggregations and jq's `group_by`
//! functionality, adapted for tabular data processing.

use crate::{Error, Result, TypeError, Value};
use polars::prelude::*;
use smallvec::SmallVec;
use std::collections::HashMap;

/// Helper function to convert `AnyValue` to Value
fn any_value_to_value(any_val: &AnyValue) -> Result<Value> {
    use serde_json::Value as JsonValue;
    let json_val = match any_val {
        AnyValue::Null => JsonValue::Null,
        AnyValue::Boolean(b) => JsonValue::Bool(*b),
        AnyValue::Int8(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::Int16(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::Int32(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::Int64(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::UInt8(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::UInt16(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::UInt32(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::UInt64(i) => JsonValue::Number(serde_json::Number::from(*i)),
        AnyValue::Float32(f) => JsonValue::Number(
            serde_json::Number::from_f64(f64::from(*f))
                .ok_or_else(|| Error::operation("Invalid float"))?,
        ),
        AnyValue::Float64(f) => JsonValue::Number(
            serde_json::Number::from_f64(*f).ok_or_else(|| Error::operation("Invalid float"))?,
        ),
        AnyValue::String(s) => JsonValue::String((*s).to_string()),
        _ => return Err(Error::operation("Unsupported AnyValue type")),
    };
    Ok(Value::from_json(json_val))
}

/// Helper function to convert `DataFrame` to Array of Objects
fn df_to_array(df: &DataFrame) -> Result<Vec<Value>> {
    let columns = df.get_column_names();
    let mut result = Vec::with_capacity(df.height());

    for row_idx in 0..df.height() {
        let mut obj = std::collections::HashMap::new();
        for col_name in &columns {
            let series = df.column(col_name).map_err(Error::from)?;
            let any_val = series.get(row_idx).map_err(Error::from)?;
            let value = any_value_to_value(&any_val)?;
            obj.insert(col_name.to_string(), value);
        }
        result.push(Value::Object(obj));
    }

    Ok(result)
}

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
/// use dsq_core::ops::aggregate::{group_by_agg, AggregationFunction};
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

/// Aggregation functions that can be applied to grouped data
#[derive(Debug, Clone)]
pub enum AggregationFunction {
    /// Count of rows in each group
    Count,
    /// Sum of values in specified column
    Sum(String),
    /// Mean/average of values in specified column
    Mean(String),
    /// Median of values in specified column
    Median(String),
    /// Minimum value in specified column
    Min(String),
    /// Maximum value in specified column
    Max(String),
    /// Standard deviation of values in specified column
    Std(String),
    /// Variance of values in specified column
    Var(String),
    /// First value in specified column (within each group)
    First(String),
    /// Last value in specified column (within each group)
    Last(String),
    /// Collect all values in specified column into a list
    List(String),
    /// Count unique values in specified column
    CountUnique(String),
    /// Concatenate string values in specified column
    StringConcat(String, Option<String>), // column, separator
}

impl AggregationFunction {
    /// Convert to Polars expression
    pub fn to_polars_expr(&self) -> Result<Expr> {
        match self {
            AggregationFunction::Count => Ok(len().alias("count")),
            AggregationFunction::Sum(col_name) => {
                Ok(col(col_name).sum().alias(format!("{col_name}_sum")))
            }
            AggregationFunction::Mean(col_name) => {
                Ok(col(col_name).mean().alias(format!("{col_name}_mean")))
            }
            AggregationFunction::Median(col_name) => {
                Ok(col(col_name).median().alias(format!("{col_name}_median")))
            }
            AggregationFunction::Min(col_name) => {
                Ok(col(col_name).min().alias(format!("{col_name}_min")))
            }
            AggregationFunction::Max(col_name) => {
                Ok(col(col_name).max().alias(format!("{col_name}_max")))
            }
            AggregationFunction::Std(col_name) => {
                Ok(col(col_name).std(1).alias(format!("{col_name}_std")))
            }
            AggregationFunction::Var(col_name) => {
                Ok(col(col_name).var(1).alias(format!("{col_name}_var")))
            }
            AggregationFunction::First(col_name) => {
                Ok(col(col_name).first().alias(format!("{col_name}_first")))
            }
            AggregationFunction::Last(col_name) => {
                Ok(col(col_name).last().alias(format!("{col_name}_last")))
            }
            AggregationFunction::List(col_name) => {
                Ok(col(col_name).alias(format!("{col_name}_list")))
            }
            AggregationFunction::CountUnique(col_name) => Ok(col(col_name)
                .n_unique()
                .alias(format!("{col_name}_nunique"))),
            AggregationFunction::StringConcat(col_name, separator) => {
                let _sep = separator.as_deref().unwrap_or(",");
                // String concatenation in groupby context requires custom aggregation
                // For now, we'll collect into a list and handle concatenation in array processing
                Ok(col(col_name).alias(format!("{col_name}_concat")))
            }
        }
    }

    /// Get the output column name for this aggregation
    #[must_use]
    pub fn output_column_name(&self) -> String {
        match self {
            AggregationFunction::Count => "count".to_string(),
            AggregationFunction::Sum(col_name) => format!("{col_name}_sum"),
            AggregationFunction::Mean(col_name) => format!("{col_name}_mean"),
            AggregationFunction::Median(col_name) => format!("{col_name}_median"),
            AggregationFunction::Min(col_name) => format!("{col_name}_min"),
            AggregationFunction::Max(col_name) => format!("{col_name}_max"),
            AggregationFunction::Std(col_name) => format!("{col_name}_std"),
            AggregationFunction::Var(col_name) => format!("{col_name}_var"),
            AggregationFunction::First(col_name) => format!("{col_name}_first"),
            AggregationFunction::Last(col_name) => format!("{col_name}_last"),
            AggregationFunction::List(col_name) => format!("{col_name}_list"),
            AggregationFunction::CountUnique(col_name) => format!("{col_name}_nunique"),
            AggregationFunction::StringConcat(col_name, _) => format!("{col_name}_concat"),
        }
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
fn apply_aggregation_to_group(agg: &AggregationFunction, group_items: &[&Value]) -> Result<Value> {
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

/// Compare values for ordering (used in min/max)
fn compare_values_for_ordering(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,

        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),

        // Cross-type numeric comparisons
        #[allow(clippy::cast_precision_loss)]
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal),
        #[allow(clippy::cast_precision_loss)]
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),

        // For complex types, compare string representations
        _ => a.to_string().cmp(&b.to_string()),
    }
}

/// Pivot a `DataFrame` (convert rows to columns)
///
/// Equivalent to SQL's PIVOT operation or Excel's pivot tables.
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::aggregate::pivot;
/// use dsq_core::value::Value;
///
/// let result = pivot(
///     &dataframe_value,
///     &["id".to_string()],           // index columns
///     "category",                     // column to pivot
///     "value",                       // values to aggregate
///     Some("sum")                    // aggregation function
/// ).unwrap();
/// ```
pub fn pivot(
    value: &Value,
    index_columns: &[String],
    _pivot_column: &str,
    value_column: &str,
    agg_function: Option<&str>,
) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            let agg_expr = match agg_function {
                Some("sum") => col(value_column).sum().alias("value_sum"),
                Some("mean") => col(value_column).mean().alias("value_mean"),
                Some("count") => col(value_column).count().alias("value_count"),
                Some("min") => col(value_column).min().alias("value_min"),
                Some("max") => col(value_column).max().alias("value_max"),
                Some("first") | None => col(value_column).first().alias("value_first"),
                Some("last") => col(value_column).last().alias("value_last"),
                _ => {
                    return Err(Error::operation(format!(
                        "Unsupported aggregation function: {}",
                        agg_function.unwrap_or("")
                    )));
                }
            };

            // Pivot operation using group_by and aggregation
            // This is a simplified implementation - full pivot would require more complex logic
            let pivoted = df
                .clone()
                .lazy()
                .group_by(index_columns.iter().map(col).collect::<Vec<_>>())
                .agg([agg_expr])
                .collect()
                .map_err(Error::from)?;

            Ok(Value::DataFrame(pivoted))
        }
        Value::LazyFrame(lf) => {
            let agg_expr = match agg_function {
                Some("sum") => col(value_column).sum().alias("value_sum"),
                Some("mean") => col(value_column).mean(),
                Some("count") => col(value_column).count(),
                Some("min") => col(value_column).min(),
                Some("max") => col(value_column).max(),
                Some("first") | None => col(value_column).first(),
                Some("last") => col(value_column).last(),
                _ => {
                    return Err(Error::operation(format!(
                        "Unsupported aggregation function: {}",
                        agg_function.unwrap_or("")
                    )));
                }
            };

            // Pivot operation using group_by and aggregation
            // This is a simplified implementation - full pivot would require more complex logic
            let pivoted = lf
                .clone()
                .group_by(index_columns.iter().map(col).collect::<Vec<_>>())
                .agg([agg_expr]);

            Ok(Value::LazyFrame(Box::new(pivoted)))
        }
        _ => Err(TypeError::UnsupportedOperation {
            operation: "pivot".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}

/// Unpivot a `DataFrame` (convert columns to rows)
///
/// Equivalent to SQL's UNPIVOT operation or pandas' melt function.
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::aggregate::unpivot;
/// use dsq_core::value::Value;
///
/// let result = unpivot(
///     &dataframe_value,
///     &["id".to_string()],           // columns to keep as identifiers
///     &["col1".to_string(), "col2".to_string()], // columns to unpivot
///     "variable",                    // name for the variable column
///     "value"                        // name for the value column
/// ).unwrap();
/// ```
pub fn unpivot(
    value: &Value,
    id_columns: &[String],
    value_columns: &[String],
    variable_name: &str,
    value_name: &str,
) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            // Use unpivot method from UnpivotDF trait
            let mut unpivoted = if id_columns.is_empty() {
                df.clone()
                    .unpivot([] as [&str; 0], value_columns)
                    .map_err(Error::from)?
            } else {
                df.clone()
                    .unpivot(id_columns, value_columns)
                    .map_err(Error::from)?
            };
            unpivoted
                .rename("variable", variable_name.into())
                .map_err(Error::from)?;
            unpivoted
                .rename("value", value_name.into())
                .map_err(Error::from)?;

            Ok(Value::DataFrame(unpivoted))
        }
        Value::LazyFrame(lf) => {
            let df = lf.clone().collect().map_err(Error::from)?;
            unpivot(
                &Value::DataFrame(df),
                id_columns,
                value_columns,
                variable_name,
                value_name,
            )
        }
        _ => Err(TypeError::UnsupportedOperation {
            operation: "unpivot".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}

/// Rolling window aggregations
///
/// Apply aggregation functions over a rolling window of rows.
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::aggregate::{rolling_agg, WindowFunction};
/// use dsq_core::value::Value;
///
/// let result = rolling_agg(
///     &dataframe_value,
///     "value",                       // column to aggregate
///     WindowFunction::Sum,           // aggregation function
///     3,                            // window size
///     None                          // min_periods (optional)
/// ).unwrap();
/// ```
pub fn rolling_agg(
    value: &Value,
    _column: &str,
    _function: WindowFunction,
    window_size: usize,
    min_periods: Option<usize>,
) -> Result<Value> {
    let _min_periods = min_periods.unwrap_or(window_size);

    match value {
        Value::DataFrame(_df) => {
            // Rolling functions are not available in Polars 0.35 Expr API
            // Use a simple implementation for now
            Err(Error::operation(
                "Rolling window functions not yet implemented",
            ))
        }
        Value::LazyFrame(_lf) => {
            // Rolling functions are not available in Polars 0.35 Expr API
            // Use a simple implementation for now
            Err(Error::operation(
                "Rolling window functions not yet implemented",
            ))
        }
        _ => Err(TypeError::UnsupportedOperation {
            operation: "rolling_agg".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}

/// Window functions for rolling aggregations
#[derive(Debug, Clone)]
pub enum WindowFunction {
    /// Sum of values
    Sum,
    /// Mean (average) of values
    Mean,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Count of values
    Count,
    /// Standard deviation
    Std,
    /// Variance
    Var,
}

/// Exponentially Weighted Moving Average (EWMA) calculation
///
/// Apply exponentially weighted moving average over a column.
/// The smoothing factor (alpha) controls how quickly older values decay.
/// alpha = 2 / (span + 1), where span is the number of periods for the EMA
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::aggregate::ewma;
/// use dsq_core::value::Value;
///
/// let result = ewma(
///     &dataframe_value,
///     "value",                       // column to calculate EWMA for
///     0.3,                           // smoothing factor (alpha)
///     None                           // min_periods (optional)
/// ).unwrap();
/// ```
pub fn ewma(value: &Value, column: &str, alpha: f64, min_periods: Option<usize>) -> Result<Value> {
    if !(0.0..=1.0).contains(&alpha) {
        return Err(Error::operation("Alpha must be between 0 and 1"));
    }

    let min_periods = min_periods.unwrap_or(1);

    match value {
        Value::DataFrame(df) => {
            // Get the column to operate on
            let series = df.column(column).map_err(Error::from)?;

            // Convert to Vec<f64> for EWMA calculation
            let mut values: Vec<Option<f64>> = Vec::with_capacity(series.len());
            for i in 0..series.len() {
                let val = series.get(i).map_err(Error::from)?;
                let numeric_val = match val {
                    AnyValue::Int8(i) => Some(f64::from(i)),
                    AnyValue::Int16(i) => Some(f64::from(i)),
                    AnyValue::Int32(i) => Some(f64::from(i)),
                    AnyValue::Int64(i) =>
                    {
                        #[allow(clippy::cast_precision_loss)]
                        Some(i as f64)
                    }
                    AnyValue::UInt8(i) => Some(f64::from(i)),
                    AnyValue::UInt16(i) => Some(f64::from(i)),
                    AnyValue::UInt32(i) => Some(f64::from(i)),
                    AnyValue::UInt64(i) =>
                    {
                        #[allow(clippy::cast_precision_loss)]
                        Some(i as f64)
                    }
                    AnyValue::Float32(f) => Some(f64::from(f)),
                    AnyValue::Float64(f) => Some(f),
                    AnyValue::Null => None,
                    _ => {
                        return Err(TypeError::UnsupportedOperation {
                            operation: "ewma".to_string(),
                            typ: format!("{val:?}"),
                        }
                        .into());
                    }
                };
                values.push(numeric_val);
            }

            // Calculate EWMA
            let mut result_values: Vec<Option<f64>> = Vec::with_capacity(values.len());
            let mut ewma_val: Option<f64> = None;
            let mut count = 0;

            for val_opt in &values {
                if let Some(val) = val_opt {
                    count += 1;
                    ewma_val = match ewma_val {
                        None => Some(*val),
                        Some(prev_ewma) => Some(alpha * val + (1.0 - alpha) * prev_ewma),
                    };

                    if count >= min_periods {
                        result_values.push(ewma_val);
                    } else {
                        result_values.push(None);
                    }
                } else {
                    // Propagate the previous EWMA for null values
                    if count >= min_periods {
                        result_values.push(ewma_val);
                    } else {
                        result_values.push(None);
                    }
                }
            }

            // Create a new series with the result
            let result_series = Series::new(format!("{column}_ewma").into(), result_values);

            // Clone the dataframe and add the new column
            let mut result_df = df.clone();
            result_df.with_column(result_series).map_err(Error::from)?;

            Ok(Value::DataFrame(result_df))
        }
        Value::LazyFrame(lf) => {
            // For LazyFrame, we need to collect first
            let df = lf.clone().collect().map_err(Error::from)?;
            ewma(&Value::DataFrame(df), column, alpha, Some(min_periods))
        }
        Value::Array(arr) => {
            // For arrays, extract values from array of objects
            let mut values: Vec<Option<f64>> = Vec::with_capacity(arr.len());

            for item in arr {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(column) {
                        let numeric_val = match val {
                            Value::Int(i) =>
                            {
                                #[allow(clippy::cast_precision_loss)]
                                Some(*i as f64)
                            }
                            Value::Float(f) => Some(*f),
                            Value::Null => None,
                            _ => {
                                return Err(TypeError::UnsupportedOperation {
                                    operation: "ewma".to_string(),
                                    typ: val.type_name().to_string(),
                                }
                                .into());
                            }
                        };
                        values.push(numeric_val);
                    } else {
                        values.push(None);
                    }
                } else {
                    return Err(TypeError::UnsupportedOperation {
                        operation: "ewma".to_string(),
                        typ: item.type_name().to_string(),
                    }
                    .into());
                }
            }

            // Calculate EWMA
            let mut result_arr = Vec::with_capacity(arr.len());
            let mut ewma_val: Option<f64> = None;
            let mut count = 0;

            for (i, item) in arr.iter().enumerate() {
                if let Some(val) = values[i] {
                    count += 1;
                    ewma_val = match ewma_val {
                        None => Some(val),
                        Some(prev_ewma) => Some(alpha * val + (1.0 - alpha) * prev_ewma),
                    };
                }

                let ewma_result = if count >= min_periods {
                    ewma_val.map_or(Value::Null, Value::Float)
                } else {
                    Value::Null
                };

                // Clone the object and add the ewma field
                if let Value::Object(obj) = item {
                    let mut new_obj = obj.clone();
                    new_obj.insert(format!("{column}_ewma"), ewma_result);
                    result_arr.push(Value::Object(new_obj));
                }
            }

            Ok(Value::Array(result_arr))
        }
        _ => Err(TypeError::UnsupportedOperation {
            operation: "ewma".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}

/// Rolling standard deviation calculation
///
/// Apply rolling standard deviation over a window of rows.
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::aggregate::rolling_std;
/// use dsq_core::value::Value;
///
/// let result = rolling_std(
///     &dataframe_value,
///     "value",                       // column to calculate std for
///     3,                            // window size
///     None                          // min_periods (optional)
/// ).unwrap();
/// ```
pub fn rolling_std(
    value: &Value,
    column: &str,
    window_size: usize,
    min_periods: Option<usize>,
) -> Result<Value> {
    let min_periods = min_periods.unwrap_or(window_size);

    match value {
        Value::DataFrame(df) => {
            // Get the column to operate on
            let series = df.column(column).map_err(Error::from)?;

            // Convert to Vec<f64> for manual rolling calculation
            let mut values: Vec<Option<f64>> = Vec::with_capacity(series.len());
            for i in 0..series.len() {
                let val = series.get(i).map_err(Error::from)?;
                let numeric_val = match val {
                    AnyValue::Int8(i) => Some(f64::from(i)),
                    AnyValue::Int16(i) => Some(f64::from(i)),
                    AnyValue::Int32(i) => Some(f64::from(i)),
                    AnyValue::Int64(i) =>
                    {
                        #[allow(clippy::cast_precision_loss)]
                        Some(i as f64)
                    }
                    AnyValue::UInt8(i) => Some(f64::from(i)),
                    AnyValue::UInt16(i) => Some(f64::from(i)),
                    AnyValue::UInt32(i) => Some(f64::from(i)),
                    AnyValue::UInt64(i) =>
                    {
                        #[allow(clippy::cast_precision_loss)]
                        Some(i as f64)
                    }
                    AnyValue::Float32(f) => Some(f64::from(f)),
                    AnyValue::Float64(f) => Some(f),
                    AnyValue::Null => None,
                    _ => {
                        return Err(TypeError::UnsupportedOperation {
                            operation: "rolling_std".to_string(),
                            typ: format!("{val:?}"),
                        }
                        .into());
                    }
                };
                values.push(numeric_val);
            }

            // Calculate rolling std
            let mut result_values: Vec<Option<f64>> = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                let window_start = if i + 1 >= window_size {
                    i + 1 - window_size
                } else {
                    0
                };
                let window = &values[window_start..=i];

                // Filter out None values
                let valid_values: Vec<f64> = window.iter().filter_map(|&v| v).collect();

                if valid_values.len() >= min_periods {
                    // Calculate std
                    #[allow(clippy::cast_precision_loss)]
                    let mean = valid_values.iter().sum::<f64>() / valid_values.len() as f64;
                    #[allow(clippy::cast_precision_loss)]
                    let variance = valid_values.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                        / (valid_values.len() - 1).max(1) as f64;
                    result_values.push(Some(variance.sqrt()));
                } else {
                    result_values.push(None);
                }
            }

            // Create a new series with the result
            let result_series = Series::new(format!("{column}_rolling_std").into(), result_values);

            // Clone the dataframe and add the new column
            let mut result_df = df.clone();
            result_df.with_column(result_series).map_err(Error::from)?;

            Ok(Value::DataFrame(result_df))
        }
        Value::LazyFrame(lf) => {
            // For LazyFrame, we need to collect first
            let df = lf.clone().collect().map_err(Error::from)?;
            rolling_std(
                &Value::DataFrame(df),
                column,
                window_size,
                Some(min_periods),
            )
        }
        Value::Array(arr) => {
            // For arrays, we can implement a similar logic
            // Extract values from array of objects
            let mut values: Vec<Option<f64>> = Vec::with_capacity(arr.len());

            for item in arr {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(column) {
                        let numeric_val = match val {
                            Value::Int(i) =>
                            {
                                #[allow(clippy::cast_precision_loss)]
                                Some(*i as f64)
                            }
                            Value::Float(f) => Some(*f),
                            Value::Null => None,
                            _ => {
                                return Err(TypeError::UnsupportedOperation {
                                    operation: "rolling_std".to_string(),
                                    typ: val.type_name().to_string(),
                                }
                                .into());
                            }
                        };
                        values.push(numeric_val);
                    } else {
                        values.push(None);
                    }
                } else {
                    return Err(TypeError::UnsupportedOperation {
                        operation: "rolling_std".to_string(),
                        typ: item.type_name().to_string(),
                    }
                    .into());
                }
            }

            // Calculate rolling std
            let mut result_arr = Vec::with_capacity(arr.len());
            for (i, item) in arr.iter().enumerate() {
                let window_start = if i + 1 >= window_size {
                    i + 1 - window_size
                } else {
                    0
                };
                let window = &values[window_start..=i];

                // Filter out None values
                let valid_values: Vec<f64> = window.iter().filter_map(|&v| v).collect();

                let rolling_std_val = if valid_values.len() >= min_periods {
                    // Calculate std
                    #[allow(clippy::cast_precision_loss)]
                    let mean = valid_values.iter().sum::<f64>() / valid_values.len() as f64;
                    #[allow(clippy::cast_precision_loss)]
                    let variance = valid_values.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                        / (valid_values.len() - 1).max(1) as f64;
                    Value::Float(variance.sqrt())
                } else {
                    Value::Null
                };

                // Clone the object and add the rolling_std field
                if let Value::Object(obj) = item {
                    let mut new_obj = obj.clone();
                    new_obj.insert(format!("{column}_rolling_std"), rolling_std_val);
                    result_arr.push(Value::Object(new_obj));
                }
            }

            Ok(Value::Array(result_arr))
        }
        _ => Err(TypeError::UnsupportedOperation {
            operation: "rolling_std".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}

impl WindowFunction {
    /// Get the function name as a string
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            WindowFunction::Sum => "sum",
            WindowFunction::Mean => "mean",
            WindowFunction::Min => "min",
            WindowFunction::Max => "max",
            WindowFunction::Count => "count",
            WindowFunction::Std => "std",
            WindowFunction::Var => "var",
        }
    }
}

/// Cumulative aggregations
///
/// Apply cumulative aggregation functions (running totals, etc.).
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::aggregate::{cumulative_agg, WindowFunction};
/// use dsq_core::value::Value;
///
/// let result = cumulative_agg(
///     &dataframe_value,
///     "value",                       // column to aggregate
///     WindowFunction::Sum            // cumulative sum
/// ).unwrap();
/// ```
#[allow(clippy::needless_pass_by_value)]
pub fn cumulative_agg(value: &Value, _column: &str, function: WindowFunction) -> Result<Value> {
    match value {
        Value::DataFrame(_df) => {
            // Cumulative functions need special window handling in polars
            // For now, return an error indicating they're not implemented
            Err(Error::operation(format!(
                "Cumulative {} not yet implemented",
                function.name()
            )))
        }
        Value::LazyFrame(_lf) => {
            // Cumulative functions need special window handling in polars
            // For now, return an error indicating they're not implemented
            Err(Error::operation(format!(
                "Cumulative {} not yet implemented",
                function.name()
            )))
        }
        _ => Err(TypeError::UnsupportedOperation {
            operation: "cumulative_agg".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_dataframe() -> DataFrame {
        df! {
            "department" => ["Sales", "Sales", "Marketing", "Marketing", "Engineering"],
            "employee" => ["Alice", "Bob", "Charlie", "Dave", "Eve"],
            "salary" => [50000, 55000, 60000, 65000, 80000],
            "age" => [25, 30, 35, 28, 32]
        }
        .unwrap()
    }

    fn create_test_object(key: &str, value: Value) -> Value {
        Value::Object(HashMap::from([(key.to_string(), value)]))
    }

    #[test]
    fn test_aggregation_functions() {
        // Test min/max with different types
        let test_values = vec![
            &Value::Int(10),
            &Value::Int(5),
            &Value::Int(20),
            &Value::Int(15),
        ];

        // Test finding minimum
        let mut min_val: Option<&Value> = None;
        for val in &test_values {
            match min_val {
                None => min_val = Some(val),
                Some(current_min) => {
                    if compare_values_for_ordering(val, current_min) == std::cmp::Ordering::Less {
                        min_val = Some(val);
                    }
                }
            }
        }

        assert_eq!(min_val, Some(&Value::Int(5)));
    }

    #[test]
    fn test_pivot_unpivot() {
        let df = df! {
            "id" => [1, 2, 3],
            "category" => ["A", "B", "A"],
            "value" => [10, 20, 30]
        }
        .unwrap();

        let value = Value::DataFrame(df);

        // Test pivot
        let pivoted = pivot(
            &value,
            &["id".to_string()],
            "category",
            "value",
            Some("sum"),
        )
        .unwrap();

        match pivoted {
            Value::DataFrame(df) => {
                assert!(df.width() >= 2); // At least id column and pivoted columns
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_aggregation_function_names() {
        let agg = AggregationFunction::Sum("salary".to_string());
        assert_eq!(agg.output_column_name(), "salary_sum");

        let agg = AggregationFunction::Mean("age".to_string());
        assert_eq!(agg.output_column_name(), "age_mean");

        let agg = AggregationFunction::Count;
        assert_eq!(agg.output_column_name(), "count");
    }

    // #[test]
    // fn test_group_by_with_map_and_aggregation() {
    //     // Test the pattern from example_081: group_by(.department) | map({dept: .[0].department, count: length, avg_salary: (map(.salary) | add / length)})
    //     let df = df! {
    //         "id" => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    //         "name" => ["Alice Johnson", "Bob Smith", "Carol Williams", "David Brown", "Eve Davis", "Frank Miller", "Grace Wilson", "Henry Moore", "Ivy Taylor", "Jack Anderson"],
    //         "age" => [28, 34, 29, 41, 26, 38, 31, 45, 27, 33],
    //         "city" => ["New York", "Los Angeles", "Chicago", "Boston", "Miami", "Seattle", "Denver", "Austin", "Nashville", "Portland"],
    //         "salary" => [75000, 82000, 68000, 95000, 62000, 88000, 71000, 102000, 65000, 79000],
    //         "department" => ["Engineering", "Sales", "Marketing", "Engineering", "HR", "Sales", "Marketing", "Engineering", "HR", "Sales"]
    //     }.unwrap();

    //     let value = Value::DataFrame(df);

    //     // First, group by department
    //     let columns = vec!["department".to_string()];
    //     let grouped = group_by(&value, &columns).unwrap();

    //     match grouped {
    //         Value::Array(groups) => {
    //             assert_eq!(groups.len(), 4); // Engineering, Sales, Marketing, HR

    //             // For each group, simulate the map operation: {dept: .[0].department, count: length, avg_salary: (map(.salary) | add / length)}
    //             let mut results = Vec::new();
    //             for group in groups {
    //                 if let Value::Array(items) = group {
    //                     // Get department from first item
    //                     let dept = if let Some(Value::Object(first_obj)) = items.first() {
    //                         if let Some(Value::String(dept_str)) = first_obj.get("department") {
    //                             dept_str.clone()
    //                         } else {
    //                             continue;
    //                         }
    //                     } else {
    //                         continue;
    //                     };

    //                     let count = items.len();

    //                     // Calculate average salary
    //                     let mut total_salary = 0.0;
    //                     for item in &items {
    //                         if let Value::Object(obj) = item {
    //                             if let Some(Value::Int(salary)) = obj.get("salary") {
    //                                 total_salary += *salary as f64;
    //                             }
    //                         }
    //                     }
    //                     let avg_salary = total_salary / count as f64;

    //                     results.push((dept, count, avg_salary));
    //                 }
    //             }

    //             // Sort results by department for consistent testing
    //             results.sort_by(|a, b| a.0.cmp(&b.0));

    //             // Verify results have correct structure and departments
    //             assert_eq!(results.len(), 4);
    //             let depts: Vec<&str> = results.iter().map(|(dept, _, _)| dept.as_str()).collect();
    //             assert!(depts.contains(&"Engineering".into()));
    //             assert!(depts.contains(&"HR".into()));
    //             assert!(depts.contains(&"Marketing".into()));
    //             assert!(depts.contains(&"Sales".into()));

    //             // Check counts
    //             let eng_result = results
    //                 .iter()
    //                 .find(|(dept, _, _)| dept == "Engineering")
    //                 .unwrap();
    //             assert_eq!(eng_result.1, 3); // 3 engineers
    //             let hr_result = results.iter().find(|(dept, _, _)| dept == "HR").unwrap();
    //             assert_eq!(hr_result.1, 2); // 2 HR
    //         }
    //         _ => panic!("Expected Array"),
    //     }
    // }

    #[test]
    fn test_string_concatenation() {
        let alice = Value::Object(HashMap::from([(
            "name".to_string(),
            Value::String("Alice".to_string()),
        )]));
        let bob = Value::Object(HashMap::from([(
            "name".to_string(),
            Value::String("Bob".to_string()),
        )]));
        let charlie = Value::Object(HashMap::from([(
            "name".to_string(),
            Value::String("Charlie".to_string()),
        )]));

        let group_items = vec![&alice, &bob, &charlie];

        let agg = AggregationFunction::StringConcat("name".to_string(), Some(", ".to_string()));
        let result = apply_aggregation_to_group(&agg, &group_items).unwrap();

        assert_eq!(result, Value::String("Alice, Bob, Charlie".to_string()));
    }

    #[test]
    fn test_median_aggregation() {
        let obj1 = Value::Object(HashMap::from([("value".to_string(), Value::Int(1))]));
        let obj2 = Value::Object(HashMap::from([("value".to_string(), Value::Int(3))]));
        let obj3 = Value::Object(HashMap::from([("value".to_string(), Value::Int(2))]));
        let items = vec![&obj1, &obj2, &obj3];

        let agg = AggregationFunction::Median("value".to_string());
        let result = apply_aggregation_to_group(&agg, &items).unwrap();
        assert_eq!(result, Value::Float(2.0));

        // Even number of items
        let obj4 = create_test_object("value", Value::Int(1));
        let obj5 = create_test_object("value", Value::Int(2));
        let obj6 = create_test_object("value", Value::Int(3));
        let obj7 = create_test_object("value", Value::Int(4));
        let items_even = vec![&obj4, &obj5, &obj6, &obj7];

        let agg_even = AggregationFunction::Median("value".to_string());
        let result_even = apply_aggregation_to_group(&agg_even, &items_even).unwrap();
        assert_eq!(result_even, Value::Float(2.5));

        let first_agg = AggregationFunction::First("value".to_string());
        let first_result = apply_aggregation_to_group(&first_agg, &items).unwrap();
        assert_eq!(first_result, Value::Int(1));

        let last_agg = AggregationFunction::Last("value".to_string());
        let last_result = apply_aggregation_to_group(&last_agg, &items).unwrap();
        assert_eq!(last_result, Value::Int(2)); // Last item in [1, 3, 2] is 2

        // Empty group
        let empty_items: Vec<&Value> = vec![];
        let first_empty = apply_aggregation_to_group(&first_agg, &empty_items).unwrap();
        assert_eq!(first_empty, Value::Null);

        let last_empty = apply_aggregation_to_group(&last_agg, &empty_items).unwrap();
        assert_eq!(last_empty, Value::Null);
    }

    #[test]
    fn test_list_aggregation() {
        let obj1 = create_test_object("value", Value::Int(1));
        let obj2 = create_test_object("value", Value::Int(2));
        let obj3 = create_test_object("value", Value::Null);
        let items = vec![&obj1, &obj2, &obj3];

        let list_agg = AggregationFunction::List("value".to_string());
        let result = apply_aggregation_to_group(&list_agg, &items).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Value::Int(1));
                assert_eq!(arr[1], Value::Int(2));
                assert_eq!(arr[2], Value::Null);
            }
            _ => panic!("Expected Array"),
        }

        // Missing column
        let missing_obj = Value::Object(HashMap::from([("other".to_string(), Value::Int(1))]));
        let items_missing = vec![&missing_obj];
        let result_missing = apply_aggregation_to_group(&list_agg, &items_missing).unwrap();
        match result_missing {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], Value::Null);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_count_unique_aggregation() {
        let obj1 = Value::Object(HashMap::from([("value".to_string(), Value::Int(1))]));
        let obj2 = Value::Object(HashMap::from([("value".to_string(), Value::Int(2))]));
        let obj3 = Value::Object(HashMap::from([("value".to_string(), Value::Int(1))]));
        let obj4 = Value::Object(HashMap::from([(
            "value".to_string(),
            Value::String("test".to_string()),
        )]));
        let items = vec![&obj1, &obj2, &obj3, &obj4];

        let count_unique_agg = AggregationFunction::CountUnique("value".to_string());
        let result = apply_aggregation_to_group(&count_unique_agg, &items).unwrap();
        assert_eq!(result, Value::Int(3)); // 1, 2, "test"

        // Empty group
        let empty_items: Vec<&Value> = vec![];
        let result_empty = apply_aggregation_to_group(&count_unique_agg, &empty_items).unwrap();
        assert_eq!(result_empty, Value::Int(0));
    }

    #[test]
    fn test_sum_mean_with_nulls_and_mixed_types() {
        let v1 = Value::Object(HashMap::from([("value".to_string(), Value::Int(10))]));
        let v2 = Value::Object(HashMap::from([("value".to_string(), Value::Null)]));
        let v3 = Value::Object(HashMap::from([("value".to_string(), Value::Float(20.5))]));
        let v4 = Value::Object(HashMap::from([("value".to_string(), Value::Int(5))]));
        let items = vec![&v1, &v2, &v3, &v4];

        let sum_agg = AggregationFunction::Sum("value".to_string());
        let sum_result = apply_aggregation_to_group(&sum_agg, &items).unwrap();
        assert_eq!(sum_result, Value::Float(35.5)); // 10 + 20.5 + 5

        let mean_agg = AggregationFunction::Mean("value".to_string());
        let mean_result = apply_aggregation_to_group(&mean_agg, &items).unwrap();
        assert_eq!(mean_result, Value::Float(11.833333333333334)); // 35.5 / 3

        // All nulls
        let null1 = Value::Object(HashMap::from([("value".to_string(), Value::Null)]));
        let null2 = Value::Object(HashMap::from([("value".to_string(), Value::Null)]));
        let null_items = vec![&null1, &null2];
        let sum_null = apply_aggregation_to_group(&sum_agg, &null_items).unwrap();
        assert_eq!(sum_null, Value::Null);

        let mean_null = apply_aggregation_to_group(&mean_agg, &null_items).unwrap();
        assert_eq!(mean_null, Value::Null);
    }

    #[test]
    fn test_min_max_with_different_types() {
        let v1 = Value::Object(HashMap::from([("int_val".to_string(), Value::Int(10))]));
        let v2 = Value::Object(HashMap::from([("int_val".to_string(), Value::Int(5))]));
        let v3 = Value::Object(HashMap::from([(
            "float_val".to_string(),
            Value::Float(7.5),
        )]));
        let v4 = Value::Object(HashMap::from([(
            "float_val".to_string(),
            Value::Float(12.3),
        )]));
        let v5 = Value::Object(HashMap::from([(
            "str_val".to_string(),
            Value::String("apple".to_string()),
        )]));
        let v6 = Value::Object(HashMap::from([(
            "str_val".to_string(),
            Value::String("banana".to_string()),
        )]));
        let items = vec![&v1, &v2, &v3, &v4, &v5, &v6];

        let min_int = AggregationFunction::Min("int_val".to_string());
        let min_int_result = apply_aggregation_to_group(&min_int, &items).unwrap();
        assert_eq!(min_int_result, Value::Int(5));

        let max_float = AggregationFunction::Max("float_val".to_string());
        let max_float_result = apply_aggregation_to_group(&max_float, &items).unwrap();
        assert_eq!(max_float_result, Value::Float(12.3));

        let min_str = AggregationFunction::Min("str_val".to_string());
        let min_str_result = apply_aggregation_to_group(&min_str, &items).unwrap();
        assert_eq!(min_str_result, Value::String("apple".to_string()));

        let max_str = AggregationFunction::Max("str_val".to_string());
        let max_str_result = apply_aggregation_to_group(&max_str, &items).unwrap();
        assert_eq!(max_str_result, Value::String("banana".to_string()));
    }

    #[test]
    fn test_group_by_multiple_columns() {
        let array_value = Value::Array(vec![
            Value::Object(HashMap::from([
                ("dept".to_string(), Value::String("Sales".to_string())),
                ("region".to_string(), Value::String("North".to_string())),
                ("salary".to_string(), Value::Int(50000)),
            ])),
            Value::Object(HashMap::from([
                ("dept".to_string(), Value::String("Sales".to_string())),
                ("region".to_string(), Value::String("South".to_string())),
                ("salary".to_string(), Value::Int(55000)),
            ])),
            Value::Object(HashMap::from([
                ("dept".to_string(), Value::String("Sales".to_string())),
                ("region".to_string(), Value::String("North".to_string())),
                ("salary".to_string(), Value::Int(60000)),
            ])),
        ]);

        let group_cols = vec!["dept".to_string(), "region".to_string()];
        let agg_funcs = vec![AggregationFunction::Sum("salary".to_string())];

        let result = group_by_agg(&array_value, &group_cols, &agg_funcs).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2); // Two groups: Sales-North and Sales-South

                let mut found_north = false;
                let mut found_south = false;

                for item in &arr {
                    if let Value::Object(obj) = item {
                        if let Some(Value::String(dept)) = obj.get("dept") {
                            if let Some(Value::String(region)) = obj.get("region") {
                                if let Some(Value::Int(sum)) = obj.get("salary_sum") {
                                    if *dept == "Sales" && *region == "North" && *sum == 110000 {
                                        found_north = true;
                                    } else if *dept == "Sales"
                                        && *region == "South"
                                        && *sum == 55000
                                    {
                                        found_south = true;
                                    }
                                }
                            }
                        }
                    }
                }

                assert!(found_north, "North group not found or incorrect");
                assert!(found_south, "South group not found or incorrect");
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_error_conditions() {
        // Empty group columns
        let array_value = Value::Array(vec![Value::Object(HashMap::from([(
            "value".to_string(),
            Value::Int(1),
        )]))]);

        let result = group_by(&array_value, &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one column"));

        let result_agg = group_by_agg(&array_value, &[], &[]);
        assert!(result_agg.is_err());

        // Empty aggregations
        let result_agg_empty = group_by_agg(&array_value, &["value".to_string()], &[]);
        assert!(result_agg_empty.is_err());

        // Unsupported type for group_by
        let int_value = Value::Int(42);
        let result_unsupported = group_by(&int_value, &["test".to_string()]);
        assert!(result_unsupported.is_err());

        // Unsupported aggregation type
        let bool_val = Value::Object(HashMap::from([("value".to_string(), Value::Bool(true))]));
        let items = vec![&bool_val];
        let sum_agg = AggregationFunction::Sum("value".to_string());
        let result_type_error = apply_aggregation_to_group(&sum_agg, &items);
        assert!(result_type_error.is_err());
    }

    #[test]
    fn test_pivot_current_behavior() {
        // Test that pivot currently just does group_by with aggregation
        let df = df! {
            "id" => [1, 2, 3],
            "category" => ["A", "B", "A"],
            "value" => [10, 20, 30]
        }
        .unwrap();

        let value = Value::DataFrame(df);

        let pivoted = pivot(
            &value,
            &["id".to_string()],
            "category",
            "value",
            Some("sum"),
        )
        .unwrap();

        // Currently just returns grouped data, not actually pivoted
        match pivoted {
            Value::DataFrame(df) => {
                // Should have id and value_sum columns
                assert!(df
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "id"));
                assert!(df
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "value_sum"));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_unpivot() {
        let df = df! {
            "id" => [1, 2],
            "A" => [10, 20],
            "B" => [30, 40]
        }
        .unwrap();

        let value = Value::DataFrame(df);

        let unpivoted = unpivot(
            &value,
            &["id".to_string()],
            &["A".to_string(), "B".to_string()],
            "category",
            "value",
        )
        .unwrap();

        match unpivoted {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2); // Current unpivot behavior
                assert!(df
                    .get_column_names()
                    .contains(&&PlSmallStr::from("category")));
                assert!(df.get_column_names().contains(&&PlSmallStr::from("value")));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_rolling_agg_not_implemented() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);

        let result = rolling_agg(&value, "salary", WindowFunction::Sum, 3, None);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    #[test]
    fn test_cumulative_agg_not_implemented() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);

        let result = cumulative_agg(&value, "salary", WindowFunction::Sum);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    #[test]
    fn test_aggregation_function_to_polars_expr() {
        let sum_agg = AggregationFunction::Sum("salary".to_string());
        let _expr = sum_agg.to_polars_expr().unwrap();
        // Just check it doesn't panic and returns an expr

        let count_agg = AggregationFunction::Count;
        let _expr_count = count_agg.to_polars_expr().unwrap();

        let string_concat_agg =
            AggregationFunction::StringConcat("name".to_string(), Some(",".to_string()));
        let _expr_concat = string_concat_agg.to_polars_expr().unwrap();
    }

    #[test]
    fn test_compare_values_for_ordering() {
        assert_eq!(
            compare_values_for_ordering(&Value::Int(1), &Value::Int(2)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values_for_ordering(&Value::Float(1.0), &Value::Float(2.0)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values_for_ordering(
                &Value::String("a".to_string()),
                &Value::String("b".to_string())
            ),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values_for_ordering(&Value::Bool(false), &Value::Bool(true)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values_for_ordering(&Value::Null, &Value::Int(1)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values_for_ordering(&Value::Int(1), &Value::Null),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            compare_values_for_ordering(&Value::Null, &Value::Null),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_values_for_ordering(&Value::Int(1), &Value::Float(1.0)),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn test_rolling_std() {
        // Test with DataFrame
        let df = df! {
            "value" => [1.0, 2.0, 3.0, 4.0, 5.0]
        }
        .unwrap();

        let value = Value::DataFrame(df);

        let result = rolling_std(&value, "value", 3, None).unwrap();

        match result {
            Value::DataFrame(df) => {
                assert!(df.column("value_rolling_std").is_ok());
                let rolling_std_col = df.column("value_rolling_std").unwrap();
                assert_eq!(rolling_std_col.len(), 5);

                // First two values should be null (not enough data for window of 3)
                let val0 = rolling_std_col.get(0).unwrap();
                let val1 = rolling_std_col.get(1).unwrap();
                assert!(matches!(val0, AnyValue::Null));
                assert!(matches!(val1, AnyValue::Null));

                // Third value should be std of [1, 2, 3]
                let val2 = rolling_std_col.get(2).unwrap();
                if let AnyValue::Float64(f) = val2 {
                    assert!((f - 1.0).abs() < 0.01); // std([1,2,3]) = 1.0
                }
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_rolling_std_array() {
        // Test with Array
        let array_value = Value::Array(vec![
            Value::Object(HashMap::from([("value".to_string(), Value::Int(1))])),
            Value::Object(HashMap::from([("value".to_string(), Value::Int(2))])),
            Value::Object(HashMap::from([("value".to_string(), Value::Int(3))])),
            Value::Object(HashMap::from([("value".to_string(), Value::Int(4))])),
            Value::Object(HashMap::from([("value".to_string(), Value::Int(5))])),
        ]);

        let result = rolling_std(&array_value, "value", 3, None).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 5);

                // Check first item (should have null rolling_std)
                if let Value::Object(obj) = &arr[0] {
                    assert_eq!(obj.get("value_rolling_std"), Some(&Value::Null));
                }

                // Check third item (should have std of [1, 2, 3])
                if let Value::Object(obj) = &arr[2] {
                    if let Some(Value::Float(f)) = obj.get("value_rolling_std") {
                        assert!((f - 1.0).abs() < 0.01); // std([1,2,3]) = 1.0
                    } else {
                        panic!("Expected Float for rolling_std at index 2");
                    }
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_rolling_std_min_periods() {
        // Test with custom min_periods
        let df = df! {
            "value" => [1.0, 2.0, 3.0, 4.0, 5.0]
        }
        .unwrap();

        let value = Value::DataFrame(df);

        // Window size 3, but min_periods 2
        let result = rolling_std(&value, "value", 3, Some(2)).unwrap();

        match result {
            Value::DataFrame(df) => {
                let rolling_std_col = df.column("value_rolling_std").unwrap();

                // First value should be null (only 1 value)
                assert!(matches!(rolling_std_col.get(0).unwrap(), AnyValue::Null));

                // Second value should have a result (2 values >= min_periods)
                assert!(matches!(
                    rolling_std_col.get(1).unwrap(),
                    AnyValue::Float64(_)
                ));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_ewma_dataframe() {
        // Test EWMA with DataFrame
        let df = df! {
            "value" => [10.0, 20.0, 30.0, 40.0, 50.0]
        }
        .unwrap();

        let value = Value::DataFrame(df);

        // Alpha = 0.5 means equal weight to current and previous
        let result = ewma(&value, "value", 0.5, None).unwrap();

        match result {
            Value::DataFrame(df) => {
                assert!(df.column("value_ewma").is_ok());
                let ewma_col = df.column("value_ewma").unwrap();
                assert_eq!(ewma_col.len(), 5);

                // First value should be the original value
                if let AnyValue::Float64(f) = ewma_col.get(0).unwrap() {
                    assert!((f - 10.0).abs() < 0.01);
                }

                // Second value: 0.5 * 20 + 0.5 * 10 = 15
                if let AnyValue::Float64(f) = ewma_col.get(1).unwrap() {
                    assert!((f - 15.0).abs() < 0.01);
                }

                // Third value: 0.5 * 30 + 0.5 * 15 = 22.5
                if let AnyValue::Float64(f) = ewma_col.get(2).unwrap() {
                    assert!((f - 22.5).abs() < 0.01);
                }
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_ewma_array() {
        // Test EWMA with Array
        let array_value = Value::Array(vec![
            Value::Object(HashMap::from([("value".to_string(), Value::Int(10))])),
            Value::Object(HashMap::from([("value".to_string(), Value::Int(20))])),
            Value::Object(HashMap::from([("value".to_string(), Value::Int(30))])),
        ]);

        let result = ewma(&array_value, "value", 0.5, None).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);

                // Check first item
                if let Value::Object(obj) = &arr[0] {
                    if let Some(Value::Float(f)) = obj.get("value_ewma") {
                        assert!((f - 10.0).abs() < 0.01);
                    } else {
                        panic!("Expected Float for ewma at index 0");
                    }
                }

                // Check second item: 0.5 * 20 + 0.5 * 10 = 15
                if let Value::Object(obj) = &arr[1] {
                    if let Some(Value::Float(f)) = obj.get("value_ewma") {
                        assert!((f - 15.0).abs() < 0.01);
                    } else {
                        panic!("Expected Float for ewma at index 1");
                    }
                }

                // Check third item: 0.5 * 30 + 0.5 * 15 = 22.5
                if let Value::Object(obj) = &arr[2] {
                    if let Some(Value::Float(f)) = obj.get("value_ewma") {
                        assert!((f - 22.5).abs() < 0.01);
                    } else {
                        panic!("Expected Float for ewma at index 2");
                    }
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_ewma_min_periods() {
        // Test EWMA with min_periods
        let df = df! {
            "value" => [10.0, 20.0, 30.0, 40.0]
        }
        .unwrap();

        let value = Value::DataFrame(df);

        // min_periods = 3 means first 2 values should be null
        let result = ewma(&value, "value", 0.3, Some(3)).unwrap();

        match result {
            Value::DataFrame(df) => {
                let ewma_col = df.column("value_ewma").unwrap();

                // First two values should be null
                assert!(matches!(ewma_col.get(0).unwrap(), AnyValue::Null));
                assert!(matches!(ewma_col.get(1).unwrap(), AnyValue::Null));

                // Third value should have a result
                assert!(matches!(ewma_col.get(2).unwrap(), AnyValue::Float64(_)));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_ewma_with_nulls() {
        // Test EWMA with null values
        let array_value = Value::Array(vec![
            Value::Object(HashMap::from([("value".to_string(), Value::Int(10))])),
            Value::Object(HashMap::from([("value".to_string(), Value::Null)])),
            Value::Object(HashMap::from([("value".to_string(), Value::Int(30))])),
        ]);

        let result = ewma(&array_value, "value", 0.5, None).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);

                // First value
                if let Value::Object(obj) = &arr[0] {
                    assert!(matches!(obj.get("value_ewma"), Some(Value::Float(_))));
                }

                // Second value (null input, should propagate previous EWMA)
                if let Value::Object(obj) = &arr[1] {
                    assert!(matches!(obj.get("value_ewma"), Some(Value::Float(_))));
                }

                // Third value
                if let Value::Object(obj) = &arr[2] {
                    assert!(matches!(obj.get("value_ewma"), Some(Value::Float(_))));
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_ewma_invalid_alpha() {
        let df = df! {
            "value" => [1.0, 2.0, 3.0]
        }
        .unwrap();

        let value = Value::DataFrame(df);

        // Alpha > 1 should error
        let result = ewma(&value, "value", 1.5, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Alpha must be between 0 and 1"));

        // Alpha < 0 should error
        let result = ewma(&value, "value", -0.1, None);
        assert!(result.is_err());
    }
}
