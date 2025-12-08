//! Basic data operations for dsq
//!
//! This module provides fundamental operations like selection, filtering,
//! mapping, and basic transformations that form the building blocks of
//! more complex data processing pipelines.

use std::collections::HashMap;

use polars::prelude::*;
#[cfg(feature = "rand")]
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};

use crate::error::{Error, Result};
use crate::Value;

/// Sort options for specifying column and direction
#[derive(Debug, Clone, PartialEq)]
pub struct SortOptions {
    /// Column name to sort by
    pub column: String,
    /// Whether to sort in descending order
    pub descending: bool,
}

impl SortOptions {
    /// Create ascending sort options for a column
    #[must_use]
    pub fn asc(column: String) -> Self {
        Self {
            column,
            descending: false,
        }
    }

    /// Create descending sort options for a column
    #[must_use]
    pub fn desc(column: String) -> Self {
        Self {
            column,
            descending: true,
        }
    }
}

/// Select specific columns from a `DataFrame`
pub fn select(df: &DataFrame, columns: &[String]) -> Result<DataFrame> {
    let selected = df
        .select(columns)
        .map_err(|e| Error::operation(format!("Failed to select columns: {e}")))?;
    Ok(selected)
}

/// Select columns by index
pub fn select_by_index(df: &DataFrame, indices: &[usize]) -> Result<DataFrame> {
    let column_names: Vec<String> = indices
        .iter()
        .filter_map(|&idx| df.get_column_names().get(idx).map(|s| (*s).to_string()))
        .collect();

    if column_names.len() != indices.len() {
        return Err(Error::operation("Some column indices are out of bounds"));
    }

    select(df, &column_names)
}

/// Filter rows based on a predicate
pub fn filter(df: &DataFrame, mask: &Series) -> Result<DataFrame> {
    if mask.dtype() != &DataType::Boolean {
        return Err(Error::operation("Filter mask must be boolean"));
    }

    let boolean_mask = mask
        .bool()
        .map_err(|e| Error::operation(format!("Failed to convert mask to boolean: {e}")))?;

    let filtered = df
        .filter(boolean_mask)
        .map_err(|e| Error::operation(format!("Failed to filter: {e}")))?;
    Ok(filtered)
}

/// Get the first n rows from a `DataFrame`
#[must_use]
pub fn head_df(df: &DataFrame, n: usize) -> DataFrame {
    df.head(Some(n))
}

/// Get the last n rows from a `DataFrame`
#[must_use]
pub fn tail_df(df: &DataFrame, n: usize) -> DataFrame {
    df.tail(Some(n))
}

/// Get a slice of rows from a `DataFrame`
#[must_use]
pub fn slice_df(df: &DataFrame, offset: i64, length: usize) -> DataFrame {
    df.slice(offset, length)
}

/// Sort `DataFrame` by columns
pub fn sort(df: &DataFrame, by: &[String], descending: Vec<bool>) -> Result<DataFrame> {
    let sorted = df
        .sort(by, descending, false)
        .map_err(|e| Error::operation(format!("Failed to sort: {e}")))?;
    Ok(sorted)
}

/// Get unique rows from `DataFrame`
pub fn unique_df(
    df: &DataFrame,
    subset: Option<&[String]>,
    keep: UniqueKeepStrategy,
) -> Result<DataFrame> {
    let unique_df = df
        .unique(subset, keep, None)
        .map_err(|e| Error::operation(format!("Failed to get unique rows: {e}")))?;
    Ok(unique_df)
}

/// Drop null values
pub fn drop_nulls(df: &DataFrame, subset: Option<&[String]>) -> Result<DataFrame> {
    let result = df
        .drop_nulls(subset)
        .map_err(|e| Error::operation(format!("Failed to drop nulls: {e}")))?;
    Ok(result)
}

/// Fill null values with a constant
pub fn fill_null(df: &DataFrame, _value: &Value) -> Result<DataFrame> {
    let mut filled = df.clone();

    for column_name in df.get_column_names() {
        let column = df
            .column(column_name)
            .map_err(|e| Error::operation(format!("Failed to get column: {e}")))?;

        if column.null_count() > 0 {
            // For now, just use forward fill strategy instead of custom values
            let filled_column = column
                .fill_null(FillNullStrategy::Forward(None))
                .map_err(|e| Error::operation(format!("Failed to fill nulls: {e}")))?;

            filled = filled
                .with_column(filled_column)
                .map_err(|e| Error::operation(format!("Failed to update column: {e}")))?
                .clone();
        }
    }

    Ok(filled)
}

/// Rename columns
#[allow(clippy::implicit_hasher)]
pub fn rename(df: &DataFrame, mapping: &HashMap<String, String>) -> Result<DataFrame> {
    let mut renamed = df.clone();

    for (old_name, new_name) in mapping {
        renamed = renamed
            .rename(old_name, new_name)
            .map_err(|e| Error::operation(format!("Failed to rename column: {e}")))?
            .clone();
    }

    Ok(renamed)
}

/// Add a new column with a constant value
pub fn with_column(df: &DataFrame, name: &str, value: &Value) -> Result<DataFrame> {
    let series = value_to_series(name, value, df.height())?;

    let mut result = df.clone();
    result
        .with_column(series)
        .map_err(|e| Error::operation(format!("Failed to add column: {e}")))?;

    Ok(result)
}

/// Drop columns
pub fn drop(df: &DataFrame, columns: &[String]) -> Result<DataFrame> {
    let dropped = df.drop_many(columns);
    Ok(dropped)
}

/// Cast column types
pub fn cast(df: &DataFrame, column: &str, dtype: &DataType) -> Result<DataFrame> {
    let casted_column = df
        .column(column)
        .map_err(|e| Error::operation(format!("Column not found: {e}")))?
        .cast(dtype)
        .map_err(|e| Error::operation(format!("Failed to cast column: {e}")))?;

    let mut result = df.clone();
    result
        .with_column(casted_column)
        .map_err(|e| Error::operation(format!("Failed to update column: {e}")))?;

    Ok(result)
}

/// Apply a function to each element in a column
pub fn map_column<F>(df: &DataFrame, column: &str, f: F) -> Result<DataFrame>
where
    F: Fn(&Value) -> Result<Value>,
{
    let col = df
        .column(column)
        .map_err(|e| Error::operation(format!("Column not found: {e}")))?;

    let values: Vec<Value> = series_to_values(col)?;
    let mapped_values: Result<Vec<Value>> = values.iter().map(f).collect();
    let mapped_values = mapped_values?;

    let mapped_series = values_to_series(column, &mapped_values)?;

    let mut result = df.clone();
    result
        .with_column(mapped_series)
        .map_err(|e| Error::operation(format!("Failed to update column: {e}")))?;

    Ok(result)
}

/// Transpose `DataFrame`
pub fn transpose(
    df: &DataFrame,
    _include_header: bool,
    header_name: Option<&str>,
) -> Result<DataFrame> {
    // The Polars transpose API has changed, using a simpler version for now
    let transposed = df
        .transpose(header_name, None)
        .map_err(|e| Error::operation(format!("Failed to transpose: {e}")))?;
    Ok(transposed)
}

/// Melt `DataFrame` from wide to long format
pub fn melt(
    df: &DataFrame,
    id_vars: &[String],
    value_vars: &[String],
    _variable_name: Option<&str>,
    _value_name: Option<&str>,
) -> Result<DataFrame> {
    let melted = df
        .melt(id_vars, value_vars)
        .map_err(|e| Error::operation(format!("Failed to melt: {e}")))?;

    Ok(melted)
}

/// Pivot `DataFrame` from long to wide format (placeholder)
pub fn pivot(
    _df: &DataFrame,
    _values: &[String],
    _index: &[String],
    _columns: &[String],
    _aggregate_fn: Option<&str>,
) -> Result<DataFrame> {
    // For now, return an error as pivot requires more complex implementation
    Err(Error::operation("Pivot functionality not yet implemented"))
}

/// Sample rows from `DataFrame`
#[allow(unused_variables)]
pub fn sample(
    df: &DataFrame,
    n: Option<usize>,
    frac: Option<f64>,
    _with_replacement: bool,
    seed: Option<u64>,
) -> Result<DataFrame> {
    // For now, implement a simple sampling approach
    if let Some(n) = n {
        let total_rows = df.height();
        let sample_size = n.min(total_rows);

        #[cfg(feature = "rand")]
        {
            use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
            let mut rng = if let Some(seed) = seed {
                StdRng::seed_from_u64(seed)
            } else {
                StdRng::from_os_rng()
            };

            #[allow(clippy::cast_possible_truncation)]
            let mut indices: Vec<u32> = (0..total_rows as u32).collect();
            indices.shuffle(&mut rng);
            indices.truncate(sample_size);

            let idx_ca = polars::prelude::UInt32Chunked::new("idx", indices);
            let sampled = df
                .take(&idx_ca)
                .map_err(|e| Error::operation(format!("Failed to sample: {e}")))?;
            Ok(sampled)
        }
        #[cfg(not(feature = "rand"))]
        {
            Err(Error::operation("Sampling requires rand feature"))
        }
    } else if let Some(frac_value) = frac {
        #[cfg(feature = "rand")]
        {
            let total_rows = df.height();
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let sample_size = ((total_rows as f64) * frac_value).round() as usize;

            let mut rng = if let Some(seed) = seed {
                StdRng::seed_from_u64(seed)
            } else {
                StdRng::from_os_rng()
            };

            #[allow(clippy::cast_possible_truncation)]
            let mut indices: Vec<u32> = (0..total_rows as u32).collect();
            indices.shuffle(&mut rng);
            indices.truncate(sample_size);

            let idx_ca = polars::prelude::UInt32Chunked::new("idx", indices);
            let sampled = df
                .take(&idx_ca)
                .map_err(|e| Error::operation(format!("Failed to sample: {e}")))?;
            Ok(sampled)
        }
        #[cfg(not(feature = "rand"))]
        {
            Err(Error::operation("Sampling requires rand feature"))
        }
    } else {
        Err(Error::operation(
            "Either n or frac must be specified for sampling",
        ))
    }
}

/// Explode list columns into separate rows
pub fn explode(df: &DataFrame, columns: &[String]) -> Result<DataFrame> {
    let exploded = df
        .explode(columns)
        .map_err(|e| Error::operation(format!("Failed to explode: {e}")))?;
    Ok(exploded)
}

/// Select columns from a Value (works with `DataFrame`, Array, Object)
pub fn select_columns(value: &Value, columns: &[String]) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            let selected = select(df, columns)?;
            Ok(Value::DataFrame(selected))
        }
        Value::Array(arr) => {
            // For arrays of objects, select specified fields
            let selected_objects: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::Object(obj) => {
                        let mut selected_obj = std::collections::HashMap::new();
                        for column in columns {
                            if let Some(val) = obj.get(column) {
                                selected_obj.insert(column.clone(), val.clone());
                            }
                        }
                        Ok(Value::Object(selected_obj))
                    }
                    _ => Ok(v.clone()),
                })
                .collect();
            Ok(Value::Array(selected_objects?))
        }
        Value::Object(obj) => {
            let mut selected_obj = std::collections::HashMap::new();
            for column in columns {
                if let Some(val) = obj.get(column) {
                    selected_obj.insert(column.clone(), val.clone());
                }
            }
            Ok(Value::Object(selected_obj))
        }
        _ => Err(Error::operation(
            "Cannot select columns from this value type".to_string(),
        )),
    }
}

/// Filter rows based on a predicate function
pub fn filter_rows(value: &Value, mask: &Value) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            if let Value::Array(mask_arr) = mask {
                let bool_mask: Result<Vec<bool>> = mask_arr
                    .iter()
                    .map(|v| match v {
                        Value::Bool(b) => Ok(*b),
                        _ => Err(Error::operation("Filter mask must be boolean")),
                    })
                    .collect();
                let mask_series = Series::new("mask", bool_mask?);
                let filtered = filter(df, &mask_series)?;
                Ok(Value::DataFrame(filtered))
            } else {
                Err(Error::operation("Filter mask must be array of booleans"))
            }
        }
        Value::Array(arr) => {
            if let Value::Array(mask_arr) = mask {
                if mask_arr.len() != arr.len() {
                    return Err(Error::operation(
                        "Mask length must match array length".to_string(),
                    ));
                }
                let filtered: Result<Vec<Value>> = arr
                    .iter()
                    .zip(mask_arr.iter())
                    .filter_map(|(val, mask_val)| match mask_val {
                        Value::Bool(true) => Some(Ok(val.clone())),
                        Value::Bool(false) => None,
                        _ => Some(Err(Error::operation("Filter mask must be boolean"))),
                    })
                    .collect();
                Ok(Value::Array(filtered?))
            } else {
                Err(Error::operation("Filter mask must be array of booleans"))
            }
        }
        _ => Err(Error::operation(
            "Cannot filter this value type".to_string(),
        )),
    }
}

/// Filter values based on a predicate function
pub fn filter_values<F>(value: &Value, predicate: F) -> Result<Value>
where
    F: Fn(&Value) -> Result<bool>,
{
    match value {
        Value::Array(arr) => {
            let filtered: Result<Vec<Value>> = arr
                .iter()
                .filter_map(|v| match predicate(v) {
                    Ok(true) => Some(Ok(v.clone())),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                })
                .collect();
            Ok(Value::Array(filtered?))
        }
        Value::DataFrame(df) => {
            // For DataFrames, we need to convert each row to a Value and apply the predicate
            let mut mask = Vec::new();
            for i in 0..df.height() {
                let row_value = df_row_to_value(df, i)?;
                mask.push(predicate(&row_value)?);
            }
            let mask_series = Series::new("mask", mask);
            let filtered = filter(df, &mask_series)?;
            Ok(Value::DataFrame(filtered))
        }
        _ => {
            if predicate(value)? {
                Ok(value.clone())
            } else {
                Ok(Value::Null)
            }
        }
    }
}

/// Sort by columns with sort options
pub fn sort_by_columns(value: &Value, options: &[SortOptions]) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            let columns: Vec<String> = options.iter().map(|opt| opt.column.clone()).collect();
            let descending: Vec<bool> = options.iter().map(|opt| opt.descending).collect();
            let sorted = sort(df, &columns, descending)?;
            Ok(Value::DataFrame(sorted))
        }
        Value::Array(arr) => {
            if options.is_empty() {
                return Ok(value.clone());
            }

            let mut sorted_arr = arr.clone();
            sorted_arr.sort_by(|a, b| {
                for opt in options {
                    let cmp = match (a, b) {
                        (Value::Object(obj_a), Value::Object(obj_b)) => {
                            let val_a = obj_a.get(&opt.column).unwrap_or(&Value::Null);
                            let val_b = obj_b.get(&opt.column).unwrap_or(&Value::Null);
                            compare_values(val_a, val_b)
                        }
                        _ => std::cmp::Ordering::Equal,
                    };

                    let final_cmp = if opt.descending { cmp.reverse() } else { cmp };
                    if final_cmp != std::cmp::Ordering::Equal {
                        return final_cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
            Ok(Value::Array(sorted_arr))
        }
        _ => Err(Error::operation("Cannot sort this value type".to_string())),
    }
}

/// Add a column to a Value
pub fn add_column(value: &Value, name: &str, column_value: &Value) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            let new_df = with_column(df, name, column_value)?;
            Ok(Value::DataFrame(new_df))
        }
        _ => Err(Error::operation(
            "Cannot add column to this value type".to_string(),
        )),
    }
}

/// Drop columns from a Value
pub fn drop_columns(value: &Value, columns: &[String]) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            let dropped = drop(df, columns)?;
            Ok(Value::DataFrame(dropped))
        }
        _ => Err(Error::operation(
            "Cannot drop columns from this value type".to_string(),
        )),
    }
}

/// Rename columns in a Value
#[allow(clippy::implicit_hasher)]
pub fn rename_columns(value: &Value, mapping: &HashMap<String, String>) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            let renamed = rename(df, mapping)?;
            Ok(Value::DataFrame(renamed))
        }
        _ => Err(Error::operation(
            "Cannot rename columns in this value type".to_string(),
        )),
    }
}

/// Head operation on Value
pub fn head(value: &Value, n: usize) -> Result<Value> {
    match value {
        Value::DataFrame(df) => Ok(Value::DataFrame(df.head(Some(n)))),
        Value::Array(arr) => {
            let take = n.min(arr.len());
            Ok(Value::Array(arr[..take].to_vec()))
        }
        _ => Ok(value.clone()),
    }
}

/// Tail operation on Value
pub fn tail(value: &Value, n: usize) -> Result<Value> {
    match value {
        Value::DataFrame(df) => Ok(Value::DataFrame(df.tail(Some(n)))),
        Value::Array(arr) => {
            let len = arr.len();
            let start = len.saturating_sub(n);
            Ok(Value::Array(arr[start..].to_vec()))
        }
        _ => Ok(value.clone()),
    }
}

/// Slice operation on Value
pub fn slice(value: &Value, offset: i64, length: usize) -> Result<Value> {
    match value {
        Value::DataFrame(df) => Ok(Value::DataFrame(df.slice(offset, length))),
        Value::Array(arr) => {
            #[allow(
                clippy::cast_sign_loss,
                clippy::cast_possible_truncation,
                clippy::cast_possible_wrap
            )]
            let start = if offset < 0 {
                (arr.len() as i64 + offset).max(0) as usize
            } else {
                (offset as usize).min(arr.len())
            };
            let end = (start + length).min(arr.len());
            Ok(Value::Array(arr[start..end].to_vec()))
        }
        _ => Ok(value.clone()),
    }
}

/// Reverse operation on Value
pub fn reverse(value: &Value) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            #[allow(clippy::cast_possible_truncation)]
            let indices: Vec<u32> = (0..df.height() as u32).rev().collect();
            let idx_ca = polars::prelude::UInt32Chunked::new("idx", indices);
            let reversed = df
                .take(&idx_ca)
                .map_err(|e| Error::operation(format!("Failed to reverse DataFrame: {e}")))?;
            Ok(Value::DataFrame(reversed))
        }
        Value::Array(arr) => {
            let mut reversed = arr.clone();
            reversed.reverse();
            Ok(Value::Array(reversed))
        }
        _ => Ok(value.clone()),
    }
}

/// Unique operation on Value
pub fn unique(value: &Value) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            let unique_df = df
                .unique(None, UniqueKeepStrategy::First, None)
                .map_err(|e| Error::operation(format!("Failed to get unique values: {e}")))?;
            Ok(Value::DataFrame(unique_df))
        }
        Value::Array(arr) => {
            let mut unique_vals: Vec<Value> = Vec::new();
            for val in arr {
                if !unique_vals.contains(val) {
                    unique_vals.push(val.clone());
                }
            }
            Ok(Value::Array(unique_vals))
        }
        _ => Ok(value.clone()),
    }
}

/// Count operation on Value
#[allow(clippy::cast_possible_wrap)]
pub fn count(value: &Value) -> Result<Value> {
    let count = match value {
        Value::DataFrame(df) => df.height() as i64,
        Value::Array(arr) => arr.len() as i64,
        Value::Object(obj) => obj.len() as i64,
        Value::String(s) => s.len() as i64,
        Value::Null => 0,
        _ => 1,
    };
    Ok(Value::Int(count))
}

// Helper functions

fn df_row_to_value(df: &DataFrame, row_idx: usize) -> Result<Value> {
    let mut obj = std::collections::HashMap::new();

    for col_name in df.get_column_names() {
        let series = df
            .column(col_name)
            .map_err(|e| Error::operation(format!("Failed to get column: {e}")))?;
        let value = series_value_at(series, row_idx)?;
        obj.insert(col_name.to_string(), value);
    }

    Ok(Value::Object(obj))
}

fn series_value_at(series: &Series, idx: usize) -> Result<Value> {
    if idx >= series.len() {
        return Ok(Value::Null);
    }

    match series.dtype() {
        DataType::Boolean => {
            let ca = series
                .bool()
                .map_err(|e| Error::operation(format!("Failed to get bool: {e}")))?;
            Ok(ca.get(idx).map_or(Value::Null, Value::Bool))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let ca = series
                .i64()
                .map_err(|e| Error::operation(format!("Failed to get int: {e}")))?;
            Ok(ca.get(idx).map_or(Value::Null, Value::Int))
        }
        DataType::Float32 | DataType::Float64 => {
            let ca = series
                .f64()
                .map_err(|e| Error::operation(format!("Failed to get float: {e}")))?;
            Ok(ca.get(idx).map_or(Value::Null, Value::Float))
        }
        DataType::Utf8 => {
            let ca = series
                .utf8()
                .map_err(|e| Error::operation(format!("Failed to get string: {e}")))?;
            Ok(ca
                .get(idx)
                .map_or(Value::Null, |s| Value::String(s.to_string())))
        }
        _ => Ok(Value::Null),
    }
}

#[allow(clippy::cast_precision_loss)]
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

/// Helper function to convert Value to Series
fn value_to_series(name: &str, value: &Value, length: usize) -> Result<Series> {
    match value {
        Value::Null => Ok(Series::new_null(name, length)),
        Value::Bool(b) => Ok(Series::new(name, vec![*b; length])),
        Value::Int(i) => Ok(Series::new(name, vec![*i; length])),
        Value::Float(f) => Ok(Series::new(name, vec![*f; length])),
        Value::String(s) => Ok(Series::new(name, vec![s.as_str(); length])),
        Value::Array(arr) => {
            if arr.len() != length {
                return Err(Error::operation("Array length must match DataFrame height"));
            }
            values_to_series(name, arr)
        }
        _ => Err(Error::operation("Cannot convert value to series")),
    }
}

/// Helper function to convert Series to Values
fn series_to_values(series: &Series) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(series.len());

    match series.dtype() {
        DataType::Boolean => {
            let ca = series
                .bool()
                .map_err(|e| Error::operation(format!("Failed to get bool array: {e}")))?;
            for opt_val in ca {
                values.push(opt_val.map_or(Value::Null, Value::Bool));
            }
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let ca = series
                .i64()
                .map_err(|e| Error::operation(format!("Failed to get int array: {e}")))?;
            for opt_val in ca {
                values.push(opt_val.map_or(Value::Null, Value::Int));
            }
        }
        DataType::Float32 | DataType::Float64 => {
            let ca = series
                .f64()
                .map_err(|e| Error::operation(format!("Failed to get float array: {e}")))?;
            for opt_val in ca {
                values.push(opt_val.map_or(Value::Null, Value::Float));
            }
        }
        DataType::Utf8 => {
            let ca = series
                .utf8()
                .map_err(|e| Error::operation(format!("Failed to get string array: {e}")))?;
            for opt_val in ca {
                values.push(opt_val.map_or(Value::Null, |s| Value::String(s.to_string())));
            }
        }
        _ => {
            return Err(Error::operation(format!(
                "Unsupported data type: {:?}",
                series.dtype()
            )));
        }
    }

    Ok(values)
}

/// Helper function to convert Values to Series
#[allow(clippy::unnecessary_wraps, clippy::cast_precision_loss)]
fn values_to_series(name: &str, values: &[Value]) -> Result<Series> {
    if values.is_empty() {
        return Ok(Series::new_empty(name, &DataType::Null));
    }

    // Determine the data type from the first non-null value
    let dtype = values
        .iter()
        .find(|v| !v.is_null())
        .map_or(DataType::Null, |v| match v {
            Value::Bool(_) => DataType::Boolean,
            Value::Int(_) => DataType::Int64,
            Value::Float(_) => DataType::Float64,
            Value::String(_) => DataType::Utf8,
            _ => DataType::Null,
        });

    match dtype {
        DataType::Boolean => {
            let vec: Vec<Option<bool>> = values
                .iter()
                .map(|v| match v {
                    Value::Bool(b) => Some(*b),
                    _ => None,
                })
                .collect();
            Ok(Series::new(name, vec))
        }
        DataType::Int64 => {
            let vec: Vec<Option<i64>> = values
                .iter()
                .map(|v| match v {
                    Value::Int(i) => Some(*i),
                    _ => None,
                })
                .collect();
            Ok(Series::new(name, vec))
        }
        DataType::Float64 => {
            let vec: Vec<Option<f64>> = values
                .iter()
                .map(|v| match v {
                    Value::Float(f) => Some(*f),
                    Value::Int(i) => Some(*i as f64),
                    _ => None,
                })
                .collect();
            Ok(Series::new(name, vec))
        }
        DataType::Utf8 => {
            let vec: Vec<Option<&str>> = values
                .iter()
                .map(|v| match v {
                    Value::String(s) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Ok(Series::new(name, vec))
        }
        _ => Ok(Series::new_null(name, values.len())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select() {
        let df = DataFrame::new(vec![
            Series::new("a", vec![1, 2, 3]),
            Series::new("b", vec![4, 5, 6]),
            Series::new("c", vec![7, 8, 9]),
        ])
        .unwrap();

        let selected = select(&df, &["a".to_string(), "c".to_string()]).unwrap();
        assert_eq!(selected.width(), 2);
        assert!(selected.get_column_names().contains(&"a"));
        assert!(selected.get_column_names().contains(&"c"));
    }

    #[test]
    fn test_filter() {
        let df = DataFrame::new(vec![
            Series::new("a", vec![1, 2, 3, 4, 5]),
            Series::new("b", vec![10, 20, 30, 40, 50]),
        ])
        .unwrap();

        let mask = Series::new("mask", vec![true, false, true, false, true]);
        let filtered = filter(&df, &mask).unwrap();

        assert_eq!(filtered.height(), 3);
        assert_eq!(filtered.column("a").unwrap().i32().unwrap().get(0), Some(1));
        assert_eq!(filtered.column("a").unwrap().i32().unwrap().get(1), Some(3));
        assert_eq!(filtered.column("a").unwrap().i32().unwrap().get(2), Some(5));
    }

    #[test]
    fn test_sort() {
        let df = DataFrame::new(vec![
            Series::new("a", vec![3, 1, 4, 1, 5]),
            Series::new("b", vec!["c", "a", "d", "b", "e"]),
        ])
        .unwrap();

        let sorted = sort(&df, &["a".to_string()], vec![false]).unwrap();
        let col_a = sorted.column("a").unwrap().i32().unwrap();

        assert_eq!(col_a.get(0), Some(1));
        assert_eq!(col_a.get(1), Some(1));
        assert_eq!(col_a.get(2), Some(3));
        assert_eq!(col_a.get(3), Some(4));
        assert_eq!(col_a.get(4), Some(5));
    }

    #[test]
    fn test_rename() {
        let df = DataFrame::new(vec![
            Series::new("old_name", vec![1, 2, 3]),
            Series::new("keep_name", vec![4, 5, 6]),
        ])
        .unwrap();

        let mut mapping = HashMap::new();
        mapping.insert("old_name".to_string(), "new_name".to_string());

        let renamed = rename(&df, &mapping).unwrap();
        assert!(renamed.get_column_names().contains(&"new_name"));
        assert!(renamed.get_column_names().contains(&"keep_name"));
        assert!(!renamed.get_column_names().contains(&"old_name"));
    }
}
