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

use crate::{Error, Result, Value};
use polars::prelude::*;

mod cumulative;
mod ewma;
mod group_by;
mod pivot;
mod rolling;
mod unpivot;

pub use cumulative::cumulative_agg;
pub use ewma::ewma;
pub use group_by::{group_by, group_by_agg};
pub use pivot::pivot;
pub use rolling::{rolling_agg, rolling_std, WindowFunction};
pub use unpivot::unpivot;

/// Helper function to convert `AnyValue` to Value
pub(super) fn any_value_to_value(any_val: &AnyValue) -> Result<Value> {
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
pub(super) fn df_to_array(df: &DataFrame) -> Result<Vec<Value>> {
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

/// Compare values for ordering (used in min/max)
pub(super) fn compare_values_for_ordering(a: &Value, b: &Value) -> std::cmp::Ordering {
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

#[cfg(test)]
mod tests;
