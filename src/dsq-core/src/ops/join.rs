//! Join operations for dsq
//!
//! This module provides join operations for `DataFrames` including:
//! - Inner joins
//! - Left outer joins  
//! - Right outer joins
//! - Full outer joins
//! - Cross joins
//! - Semi joins
//! - Anti joins
//!
//! These operations correspond to SQL JOIN operations and allow combining
//! data from multiple `DataFrames` based on common keys.

use std::collections::HashMap;

use polars::prelude::*;

use crate::error::{Error, Result};
use crate::Value;

/// Types of join operations supported
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Inner join - returns only matching rows from both `DataFrames`
    Inner,
    /// Left outer join - returns all rows from left `DataFrame`, matching rows from right
    Left,
    /// Right outer join - returns all rows from right `DataFrame`, matching rows from left
    Right,
    /// Full outer join - returns all rows from both `DataFrames`
    Outer,
    /// Cross join - cartesian product of both `DataFrames`
    Cross,
    /// Semi join - returns rows from left `DataFrame` that have matches in right
    Semi,
    /// Anti join - returns rows from left `DataFrame` that have no matches in right
    Anti,
}

impl JoinType {
    /// Convert to Polars `JoinType`
    pub fn to_polars(&self) -> Result<polars::prelude::JoinType> {
        match self {
            JoinType::Inner => Ok(polars::prelude::JoinType::Inner),
            JoinType::Left => Ok(polars::prelude::JoinType::Left),
            JoinType::Right => Err(Error::operation(
                "Right join not supported in this Polars version, so cannot convert to Polars",
            )),
            JoinType::Outer => Ok(polars::prelude::JoinType::Full),
            JoinType::Cross => Ok(polars::prelude::JoinType::Cross),
            JoinType::Semi => Err(Error::operation(
                "Semi join not supported in this Polars version, so cannot convert to Polars",
            )),
            JoinType::Anti => Err(Error::operation(
                "Anti join not supported in this Polars version, so cannot convert to Polars",
            )),
        }
    }

    /// Get the string representation
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            JoinType::Inner => "inner",
            JoinType::Left => "left",
            JoinType::Right => "right",
            JoinType::Outer => "outer",
            JoinType::Cross => "cross",
            JoinType::Semi => "semi",
            JoinType::Anti => "anti",
        }
    }

    /// Parse from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "inner" => Ok(JoinType::Inner),
            "left" | "left_outer" => Ok(JoinType::Left),
            "right" | "right_outer" => Ok(JoinType::Right),
            "outer" | "full" | "full_outer" => Ok(JoinType::Outer),
            "cross" => Ok(JoinType::Cross),
            "semi" => Ok(JoinType::Semi),
            "anti" => Ok(JoinType::Anti),
            _ => Err(Error::operation(format!("Unknown join type: {s}"))),
        }
    }
}

/// Options for join operations
#[derive(Debug, Clone)]
pub struct JoinOptions {
    /// Type of join to perform
    pub join_type: JoinType,
    /// Suffix to add to duplicate column names from the right `DataFrame`
    pub suffix: String,
    /// Whether to validate that join keys are unique (for performance)
    pub validate: JoinValidation,
    /// Whether to sort the result by join keys
    pub sort: bool,
    /// Whether to coalesce join keys (combine left and right key columns)
    pub coalesce: bool,
}

impl Default for JoinOptions {
    fn default() -> Self {
        Self {
            join_type: JoinType::Inner,
            suffix: "_right".to_string(),
            validate: JoinValidation::None,
            sort: false,
            coalesce: false,
        }
    }
}

/// Join validation options
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinValidation {
    /// No validation
    None,
    /// Validate that left keys are unique
    OneToMany,
    /// Validate that right keys are unique  
    ManyToOne,
    /// Validate that both left and right keys are unique
    OneToOne,
}

impl JoinValidation {
    /// Convert to Polars `JoinValidation`
    #[must_use]
    pub fn to_polars(&self) -> polars::prelude::JoinValidation {
        match self {
            // None not available in Polars 0.35, default to OneToMany
            JoinValidation::None | JoinValidation::OneToMany => {
                polars::prelude::JoinValidation::OneToMany
            }
            JoinValidation::ManyToOne => polars::prelude::JoinValidation::ManyToOne,
            JoinValidation::OneToOne => polars::prelude::JoinValidation::OneToOne,
        }
    }
}

/// Join specification for columns
#[derive(Debug, Clone)]
pub enum JoinKeys {
    /// Join on columns with the same name
    On(Vec<String>),
    /// Join with different column names for left and right
    LeftRight {
        /// Column names from left `DataFrame`
        left: Vec<String>,
        /// Column names from right `DataFrame`
        right: Vec<String>,
    },
}

impl JoinKeys {
    /// Create join keys for columns with the same name
    #[must_use]
    pub fn on(columns: Vec<String>) -> Self {
        JoinKeys::On(columns)
    }

    /// Create join keys with different left and right column names
    #[must_use]
    pub fn left_right(left: Vec<String>, right: Vec<String>) -> Self {
        JoinKeys::LeftRight { left, right }
    }

    /// Get the left column names
    #[must_use]
    pub fn left_columns(&self) -> &[String] {
        match self {
            JoinKeys::On(cols) => cols,
            JoinKeys::LeftRight { left, .. } => left,
        }
    }

    /// Get the right column names
    #[must_use]
    pub fn right_columns(&self) -> &[String] {
        match self {
            JoinKeys::On(cols) => cols,
            JoinKeys::LeftRight { right, .. } => right,
        }
    }
}

/// Join two `DataFrames`
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::join::{join, JoinKeys, JoinOptions, JoinType};
/// use dsq_core::value::Value;
///
/// let keys = JoinKeys::on(vec!["id".to_string()]);
/// let options = JoinOptions {
///     join_type: JoinType::Inner,
///     ..Default::default()
/// };
/// let result = join(&left_df, &right_df, &keys, &options).unwrap();
/// ```
pub fn join(left: &Value, right: &Value, keys: &JoinKeys, options: &JoinOptions) -> Result<Value> {
    match (left, right) {
        (Value::DataFrame(left_df), Value::DataFrame(right_df)) => {
            join_dataframes(left_df, right_df, keys, options)
        }
        (Value::LazyFrame(left_lf), Value::LazyFrame(right_lf)) => {
            join_lazy_frames(left_lf, right_lf, keys, options)
        }
        (Value::DataFrame(left_df), Value::LazyFrame(right_lf)) => {
            let right_df = right_lf.clone().collect().map_err(Error::from)?;
            join_dataframes(left_df, &right_df, keys, options)
        }
        (Value::LazyFrame(left_lf), Value::DataFrame(right_df)) => {
            let left_df = left_lf.clone().collect().map_err(Error::from)?;
            join_dataframes(&left_df, right_df, keys, options)
        }
        (Value::Array(left_arr), Value::Array(right_arr)) => {
            join_arrays(left_arr, right_arr, keys, options)
        }
        (left_val, right_val) => {
            // Try to convert to DataFrames
            let left_df = left_val.to_dataframe()?;
            let right_df = right_val.to_dataframe()?;
            join_dataframes(&left_df, &right_df, keys, options)
        }
    }
}

/// Join two `DataFrames` using Polars
fn join_dataframes(
    left_df: &DataFrame,
    right_df: &DataFrame,
    keys: &JoinKeys,
    options: &JoinOptions,
) -> Result<Value> {
    let left_on: Vec<Expr> = keys
        .left_columns()
        .iter()
        .map(|column_name| col(column_name))
        .collect();
    let right_on: Vec<Expr> = keys
        .right_columns()
        .iter()
        .map(|column_name| col(column_name))
        .collect();

    let join_args = JoinArgs {
        how: options.join_type.to_polars()?,
        suffix: Some(options.suffix.clone().into()),
        validation: options.validate.to_polars(),
        slice: None,
        coalesce: polars::prelude::JoinCoalesce::KeepColumns,
        maintain_order: polars::prelude::MaintainOrderJoin::None,
        nulls_equal: false,
    };

    let mut join_builder =
        left_df
            .clone()
            .lazy()
            .join(right_df.clone().lazy(), left_on, right_on, join_args);

    if options.sort {
        // Sort by the join keys
        let sort_exprs: Vec<Expr> = keys
            .left_columns()
            .iter()
            .map(|column_name| col(column_name))
            .collect();
        join_builder = join_builder.sort_by_exprs(sort_exprs, SortMultipleOptions::default());
    }

    let result_df = join_builder.collect().map_err(Error::from)?;
    Ok(Value::DataFrame(result_df))
}

/// Join two `LazyFrames`
fn join_lazy_frames(
    left_lf: &LazyFrame,
    right_lf: &LazyFrame,
    keys: &JoinKeys,
    options: &JoinOptions,
) -> Result<Value> {
    let left_on: Vec<Expr> = keys
        .left_columns()
        .iter()
        .map(|column_name| col(column_name))
        .collect();
    let right_on: Vec<Expr> = keys
        .right_columns()
        .iter()
        .map(|column_name| col(column_name))
        .collect();

    let join_args = JoinArgs {
        how: options.join_type.to_polars()?,
        suffix: Some(options.suffix.clone().into()),
        validation: options.validate.to_polars(),
        slice: None,
        coalesce: polars::prelude::JoinCoalesce::KeepColumns,
        maintain_order: polars::prelude::MaintainOrderJoin::None,
        nulls_equal: false,
    };

    let mut join_builder = left_lf
        .clone()
        .join(right_lf.clone(), left_on, right_on, join_args);

    if options.sort {
        // Sort by the join keys
        let sort_exprs: Vec<Expr> = keys
            .left_columns()
            .iter()
            .map(|column_name| col(column_name))
            .collect();
        join_builder = join_builder.sort_by_exprs(sort_exprs, SortMultipleOptions::default());
    }

    Ok(Value::LazyFrame(Box::new(join_builder)))
}

/// Join two arrays of objects (jq-style)
fn join_arrays(
    left_arr: &[Value],
    right_arr: &[Value],
    keys: &JoinKeys,
    options: &JoinOptions,
) -> Result<Value> {
    let mut result = match options.join_type {
        JoinType::Inner => inner_join_arrays(left_arr, right_arr, keys, &options.suffix)?,
        JoinType::Left => left_join_arrays(left_arr, right_arr, keys, &options.suffix)?,
        JoinType::Right => right_join_arrays(left_arr, right_arr, keys, &options.suffix)?,
        JoinType::Outer => outer_join_arrays(left_arr, right_arr, keys, &options.suffix)?,
        JoinType::Cross => cross_join_arrays(left_arr, right_arr, &options.suffix)?,
        JoinType::Semi => semi_join_arrays(left_arr, right_arr, keys)?,
        JoinType::Anti => anti_join_arrays(left_arr, right_arr, keys)?,
    };

    if options.sort {
        // Sort by the first join key
        if let Some(first_key) = keys.left_columns().first() {
            result.sort_by(|a, b| {
                let a_val = match a {
                    Value::Object(obj) => obj.get(first_key).unwrap_or(&Value::Null),
                    _ => &Value::Null,
                };
                let b_val = match b {
                    Value::Object(obj) => obj.get(first_key).unwrap_or(&Value::Null),
                    _ => &Value::Null,
                };
                compare_values_for_sorting(a_val, b_val)
            });
        }
    }

    Ok(Value::Array(result))
}

/// Inner join for arrays
fn inner_join_arrays(
    left_arr: &[Value],
    right_arr: &[Value],
    keys: &JoinKeys,
    suffix: &str,
) -> Result<Vec<Value>> {
    let mut result = Vec::new();

    for left_item in left_arr {
        if let Value::Object(left_obj) = left_item {
            for right_item in right_arr {
                if let Value::Object(right_obj) = right_item {
                    if objects_match_on_keys(left_obj, right_obj, keys)? {
                        let joined = merge_objects(
                            left_obj,
                            right_obj,
                            suffix,
                            false,
                            &std::collections::HashSet::new(),
                        )?;
                        result.push(Value::Object(joined));
                    }
                }
            }
        }
    }

    Ok(result)
}

/// Left join for arrays
fn left_join_arrays(
    left_arr: &[Value],
    right_arr: &[Value],
    keys: &JoinKeys,
    suffix: &str,
) -> Result<Vec<Value>> {
    let right_keys: std::collections::HashSet<String> = right_arr
        .iter()
        .filter_map(|v| {
            if let Value::Object(o) = v {
                Some(o.keys().cloned().collect::<Vec<_>>())
            } else {
                None
            }
        })
        .flatten()
        .collect();

    let mut result = Vec::new();

    for left_item in left_arr {
        if let Value::Object(left_obj) = left_item {
            let mut found_match = false;

            for right_item in right_arr {
                if let Value::Object(right_obj) = right_item {
                    if objects_match_on_keys(left_obj, right_obj, keys)? {
                        let joined = merge_objects(
                            left_obj,
                            right_obj,
                            suffix,
                            false,
                            &std::collections::HashSet::new(),
                        )?;
                        result.push(Value::Object(joined));
                        found_match = true;
                    }
                }
            }

            if !found_match {
                // Add left row with nulls for right columns
                let joined = merge_objects(left_obj, &HashMap::new(), suffix, true, &right_keys)?;
                result.push(Value::Object(joined));
            }
        }
    }

    Ok(result)
}

/// Right join for arrays
fn right_join_arrays(
    left_arr: &[Value],
    right_arr: &[Value],
    keys: &JoinKeys,
    suffix: &str,
) -> Result<Vec<Value>> {
    let left_keys: std::collections::HashSet<String> = left_arr
        .iter()
        .filter_map(|v| {
            if let Value::Object(o) = v {
                Some(o.keys().cloned().collect::<Vec<_>>())
            } else {
                None
            }
        })
        .flatten()
        .collect();

    let mut result = Vec::new();

    for right_item in right_arr {
        if let Value::Object(right_obj) = right_item {
            let mut found_match = false;

            for left_item in left_arr {
                if let Value::Object(left_obj) = left_item {
                    if objects_match_on_keys(left_obj, right_obj, keys)? {
                        let joined = merge_objects(
                            left_obj,
                            right_obj,
                            suffix,
                            false,
                            &std::collections::HashSet::new(),
                        )?;
                        result.push(Value::Object(joined));
                        found_match = true;
                    }
                }
            }

            if !found_match {
                // Add right row with nulls for left columns
                let joined = merge_objects(&HashMap::new(), right_obj, suffix, true, &left_keys)?;
                result.push(Value::Object(joined));
            }
        }
    }

    Ok(result)
}

/// Full outer join for arrays
fn outer_join_arrays(
    left_arr: &[Value],
    right_arr: &[Value],
    keys: &JoinKeys,
    suffix: &str,
) -> Result<Vec<Value>> {
    let left_keys: std::collections::HashSet<String> = left_arr
        .iter()
        .filter_map(|v| {
            if let Value::Object(o) = v {
                Some(o.keys().cloned().collect::<Vec<_>>())
            } else {
                None
            }
        })
        .flatten()
        .collect();
    let right_keys: std::collections::HashSet<String> = right_arr
        .iter()
        .filter_map(|v| {
            if let Value::Object(o) = v {
                Some(o.keys().cloned().collect::<Vec<_>>())
            } else {
                None
            }
        })
        .flatten()
        .collect();

    let mut result = Vec::new();
    let mut right_matched = vec![false; right_arr.len()];

    // First pass: left join
    for left_item in left_arr {
        if let Value::Object(left_obj) = left_item {
            let mut found_match = false;

            for (right_idx, right_item) in right_arr.iter().enumerate() {
                if let Value::Object(right_obj) = right_item {
                    if objects_match_on_keys(left_obj, right_obj, keys)? {
                        let joined = merge_objects(
                            left_obj,
                            right_obj,
                            suffix,
                            false,
                            &std::collections::HashSet::new(),
                        )?;
                        result.push(Value::Object(joined));
                        right_matched[right_idx] = true;
                        found_match = true;
                    }
                }
            }

            if !found_match {
                // Add left row with nulls for right columns
                let joined = merge_objects(left_obj, &HashMap::new(), suffix, true, &right_keys)?;
                result.push(Value::Object(joined));
            }
        }
    }

    // Second pass: add unmatched right rows
    for (right_idx, right_item) in right_arr.iter().enumerate() {
        if !right_matched[right_idx] {
            if let Value::Object(right_obj) = right_item {
                let joined = merge_objects(&HashMap::new(), right_obj, suffix, true, &left_keys)?;
                result.push(Value::Object(joined));
            }
        }
    }

    Ok(result)
}

/// Cross join for arrays
fn cross_join_arrays(left_arr: &[Value], right_arr: &[Value], suffix: &str) -> Result<Vec<Value>> {
    let mut result = Vec::new();

    for left_item in left_arr {
        if let Value::Object(left_obj) = left_item {
            for right_item in right_arr {
                if let Value::Object(right_obj) = right_item {
                    let joined = merge_objects(
                        left_obj,
                        right_obj,
                        suffix,
                        false,
                        &std::collections::HashSet::new(),
                    )?;
                    result.push(Value::Object(joined));
                }
            }
        }
    }

    Ok(result)
}

/// Semi join for arrays - returns left rows that have matches in right
fn semi_join_arrays(
    left_arr: &[Value],
    right_arr: &[Value],
    keys: &JoinKeys,
) -> Result<Vec<Value>> {
    let mut result = Vec::new();

    for left_item in left_arr {
        if let Value::Object(left_obj) = left_item {
            for right_item in right_arr {
                if let Value::Object(right_obj) = right_item {
                    if objects_match_on_keys(left_obj, right_obj, keys)? {
                        result.push(left_item.clone());
                        break; // Only add once per left row
                    }
                }
            }
        }
    }

    Ok(result)
}

/// Anti join for arrays - returns left rows that have no matches in right
fn anti_join_arrays(
    left_arr: &[Value],
    right_arr: &[Value],
    keys: &JoinKeys,
) -> Result<Vec<Value>> {
    let mut result = Vec::new();

    for left_item in left_arr {
        if let Value::Object(left_obj) = left_item {
            let mut found_match = false;

            for right_item in right_arr {
                if let Value::Object(right_obj) = right_item {
                    if objects_match_on_keys(left_obj, right_obj, keys)? {
                        found_match = true;
                        break;
                    }
                }
            }

            if !found_match {
                result.push(left_item.clone());
            }
        }
    }

    Ok(result)
}

/// Check if two objects match on the specified join keys
fn objects_match_on_keys(
    left_obj: &HashMap<String, Value>,
    right_obj: &HashMap<String, Value>,
    keys: &JoinKeys,
) -> Result<bool> {
    let left_keys = keys.left_columns();
    let right_keys = keys.right_columns();

    if left_keys.len() != right_keys.len() {
        return Err(Error::operation(
            "Left and right join keys must have the same length",
        ));
    }

    for (left_key, right_key) in left_keys.iter().zip(right_keys.iter()) {
        let left_val = left_obj.get(left_key).unwrap_or(&Value::Null);
        let right_val = right_obj.get(right_key).unwrap_or(&Value::Null);

        if !values_equal_for_join(left_val, right_val) {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Check if two values are equal for join purposes
fn values_equal_for_join(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
        (Value::String(a), Value::String(b)) => a == b,
        // Cross-type numeric comparisons
        #[allow(clippy::cast_precision_loss)]
        (Value::Int(a), Value::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
        #[allow(clippy::cast_precision_loss)]
        (Value::Float(a), Value::Int(b)) => (a - *b as f64).abs() < f64::EPSILON,
        _ => false,
    }
}

/// Merge two objects, handling column name conflicts
#[allow(clippy::unnecessary_wraps)]
fn merge_objects(
    left_obj: &HashMap<String, Value>,
    right_obj: &HashMap<String, Value>,
    suffix: &str,
    fill_nulls: bool,
    null_keys: &std::collections::HashSet<String>,
) -> Result<HashMap<String, Value>> {
    let mut result = left_obj.clone();

    for (right_key, right_val) in right_obj {
        let key = if result.contains_key(right_key) {
            // Column name conflict - add suffix to right column
            format!("{right_key}{suffix}")
        } else {
            right_key.clone()
        };
        result.insert(key, right_val.clone());
    }

    if fill_nulls {
        for key in null_keys {
            if result.contains_key(key) {
                // If conflict, suffix
                let suffixed = format!("{key}{suffix}");
                result.entry(suffixed).or_insert(Value::Null);
            } else {
                result.insert(key.clone(), Value::Null);
            }
        }
    }

    Ok(result)
}

/// Compare values for sorting
fn compare_values_for_sorting(a: &Value, b: &Value) -> std::cmp::Ordering {
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

/// Convenience function for inner join
pub fn inner_join(left: &Value, right: &Value, keys: &JoinKeys) -> Result<Value> {
    let options = JoinOptions {
        join_type: JoinType::Inner,
        ..Default::default()
    };
    join(left, right, keys, &options)
}

/// Convenience function for left join
pub fn left_join(left: &Value, right: &Value, keys: &JoinKeys) -> Result<Value> {
    let options = JoinOptions {
        join_type: JoinType::Left,
        ..Default::default()
    };
    join(left, right, keys, &options)
}

/// Convenience function for right join
pub fn right_join(left: &Value, right: &Value, keys: &JoinKeys) -> Result<Value> {
    let options = JoinOptions {
        join_type: JoinType::Right,
        ..Default::default()
    };
    join(left, right, keys, &options)
}

/// Convenience function for outer join
pub fn outer_join(left: &Value, right: &Value, keys: &JoinKeys) -> Result<Value> {
    let options = JoinOptions {
        join_type: JoinType::Outer,
        ..Default::default()
    };
    join(left, right, keys, &options)
}

/// Join multiple `DataFrames` in sequence
///
/// Performs a series of joins on multiple `DataFrames` using the same join keys.
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::join::{join_multiple, JoinKeys, JoinOptions, JoinType};
/// use dsq_core::value::Value;
///
/// let dataframes = vec![df1, df2, df3];
/// let keys = JoinKeys::on(vec!["id".to_string()]);
/// let options = JoinOptions {
///     join_type: JoinType::Inner,
///     ..Default::default()
/// };
/// let result = join_multiple(&dataframes, &keys, &options).unwrap();
/// ```
pub fn join_multiple(
    dataframes: &[Value],
    keys: &JoinKeys,
    options: &JoinOptions,
) -> Result<Value> {
    if dataframes.is_empty() {
        return Err(Error::operation("No DataFrames provided for join"));
    }

    if dataframes.len() == 1 {
        return Ok(dataframes[0].clone());
    }

    let mut result = dataframes[0].clone();

    for (i, df) in dataframes.iter().enumerate().skip(1) {
        // Adjust suffix for each join to avoid conflicts
        let mut join_options = options.clone();
        join_options.suffix = format!("_right_{i}");

        result = join(&result, df, keys, &join_options)?;
    }

    Ok(result)
}

/// Perform a join with a custom condition
///
/// This allows for more complex join conditions beyond simple equality.
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::join::{join_with_condition, JoinType};
/// use dsq_core::value::Value;
/// use polars::prelude::*;
///
/// let condition = col("left.price").gt(col("right.min_price"))
///     .and(col("left.price").lt(col("right.max_price")));
/// let result = join_with_condition(
///     &left_df,
///     &right_df,
///     condition,
///     JoinType::Inner,
///     "_right"
/// ).unwrap();
/// ```
#[allow(clippy::used_underscore_binding)]
pub fn join_with_condition(
    left: &Value,
    right: &Value,
    condition: Expr,
    _join_type: JoinType,
    _suffix: &str,
) -> Result<Value> {
    match (left, right) {
        (Value::DataFrame(left_df), Value::DataFrame(right_df)) => {
            // For complex conditions, we need to use a cross join followed by a filter
            // This is less efficient but more flexible

            let how = JoinType::Cross.to_polars()?;
            let join_args = JoinArgs {
                how,
                suffix: Some("_right".to_string().into()),
                validation: JoinValidation::None.to_polars(),
                slice: None,
                coalesce: polars::prelude::JoinCoalesce::KeepColumns,
                maintain_order: polars::prelude::MaintainOrderJoin::None,
                nulls_equal: false,
            };

            let cross_joined =
                left_df
                    .clone()
                    .lazy()
                    .join(right_df.clone().lazy(), vec![], vec![], join_args);

            let filtered = cross_joined.filter(condition);

            let result_df = filtered.collect().map_err(Error::from)?;
            Ok(Value::DataFrame(result_df))
        }
        (Value::LazyFrame(left_lf), Value::LazyFrame(right_lf)) => {
            let how = JoinType::Cross.to_polars()?;
            let join_args = JoinArgs {
                how,
                suffix: Some("_right".to_string().into()),
                validation: JoinValidation::None.to_polars(),
                slice: None,
                coalesce: polars::prelude::JoinCoalesce::KeepColumns,
                maintain_order: polars::prelude::MaintainOrderJoin::None,
                nulls_equal: false,
            };

            let cross_joined = left_lf
                .clone()
                .join(*right_lf.clone(), vec![], vec![], join_args);

            let filtered = cross_joined.filter(condition);
            Ok(Value::LazyFrame(Box::new(filtered)))
        }
        _ => {
            // Convert to DataFrames and retry
            let left_df = left.to_dataframe()?;
            let right_df = right.to_dataframe()?;
            join_with_condition(
                &Value::DataFrame(left_df),
                &Value::DataFrame(right_df),
                condition,
                _join_type,
                _suffix,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn create_left_dataframe() -> DataFrame {
        let id = Series::new("id", &[1, 2, 3, 4]);
        let name = Series::new("name", &["Alice", "Bob", "Charlie", "Dave"]);
        let dept_id = Series::new("dept_id", &[10, 20, 10, 30]);
        DataFrame::new(vec![id, name, dept_id]).unwrap()
    }

    fn create_right_dataframe() -> DataFrame {
        let id = Series::new("id", &[10, 20, 40]);
        let dept_name = Series::new("dept_name", &["Engineering", "Sales", "Marketing"]);
        let budget = Series::new("budget", &[100000, 50000, 75000]);
        DataFrame::new(vec![id, dept_name, budget]).unwrap()
    }

    #[test]
    fn test_inner_join() {
        let left_df = create_left_dataframe();
        let right_df = create_right_dataframe();

        let keys = JoinKeys::left_right(vec!["dept_id".to_string()], vec!["id".to_string()]);

        let result = inner_join(
            &Value::DataFrame(left_df),
            &Value::DataFrame(right_df),
            &keys,
        )
        .unwrap();

        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.shape().0, 3); // Alice, Bob, and Charlie should match
                assert!(df.get_column_names().contains(&"name"));
                assert!(df.get_column_names().contains(&"dept_name"));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_left_join() {
        let left_df = create_left_dataframe();
        let right_df = create_right_dataframe();

        let keys = JoinKeys::left_right(vec!["dept_id".to_string()], vec!["id".to_string()]);

        let result = left_join(
            &Value::DataFrame(left_df),
            &Value::DataFrame(right_df),
            &keys,
        )
        .unwrap();

        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 4); // All left rows should be present
                assert!(df.get_column_names().contains(&"name"));
                assert!(df.get_column_names().contains(&"dept_name"));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    #[ignore = "Right join not supported in this Polars version"]
    fn test_right_join() {
        let left_df = create_left_dataframe();
        let right_df = create_right_dataframe();

        let keys = JoinKeys::left_right(vec!["dept_id".to_string()], vec!["id".to_string()]);

        let result = right_join(
            &Value::DataFrame(left_df),
            &Value::DataFrame(right_df),
            &keys,
        )
        .unwrap();

        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 3); // All right rows should be present (10,20,40), but 40 has no match
                assert!(df.get_column_names().contains(&"name"));
                assert!(df.get_column_names().contains(&"dept_name"));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_outer_join() {
        let left_df = create_left_dataframe();
        let right_df = create_right_dataframe();

        let keys = JoinKeys::left_right(vec!["dept_id".to_string()], vec!["id".to_string()]);

        let result = outer_join(
            &Value::DataFrame(left_df),
            &Value::DataFrame(right_df),
            &keys,
        )
        .unwrap();

        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 5); // 4 left + 1 unmatched right (40)
                assert!(df.get_column_names().contains(&"name"));
                assert!(df.get_column_names().contains(&"dept_name"));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_array_join() {
        let left_array = Value::Array(vec![
            Value::Object(HashMap::from([
                ("id".to_string(), Value::Int(1)),
                ("name".to_string(), Value::String("Alice".to_string())),
            ])),
            Value::Object(HashMap::from([
                ("id".to_string(), Value::Int(2)),
                ("name".to_string(), Value::String("Bob".to_string())),
            ])),
        ]);

        let right_array = Value::Array(vec![
            Value::Object(HashMap::from([
                ("id".to_string(), Value::Int(1)),
                ("age".to_string(), Value::Int(30)),
            ])),
            Value::Object(HashMap::from([
                ("id".to_string(), Value::Int(3)),
                ("age".to_string(), Value::Int(25)),
            ])),
        ]);

        let keys = JoinKeys::on(vec!["id".to_string()]);
        let result = inner_join(&left_array, &right_array, &keys).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 1); // Only Alice should match
                if let Value::Object(obj) = &arr[0] {
                    assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
                    assert_eq!(obj.get("age"), Some(&Value::Int(30)));
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_join_types() {
        assert_eq!(JoinType::from_str("inner").unwrap(), JoinType::Inner);
        assert_eq!(JoinType::from_str("left_outer").unwrap(), JoinType::Left);
        assert_eq!(JoinType::from_str("full").unwrap(), JoinType::Outer);
        assert_eq!(JoinType::from_str("cross").unwrap(), JoinType::Cross);

        assert!(JoinType::from_str("invalid").is_err());
    }

    #[test]
    fn test_join_keys() {
        let keys = JoinKeys::on(vec!["id".to_string(), "name".to_string()]);
        assert_eq!(keys.left_columns(), &["id", "name"]);
        assert_eq!(keys.right_columns(), &["id", "name"]);

        let keys = JoinKeys::left_right(vec!["left_id".to_string()], vec!["right_id".to_string()]);
        assert_eq!(keys.left_columns(), &["left_id"]);
        assert_eq!(keys.right_columns(), &["right_id"]);
    }

    #[test]
    fn test_semi_join() {
        let left_array = Value::Array(vec![
            Value::Object(HashMap::from([
                ("id".to_string(), Value::Int(1)),
                ("name".to_string(), Value::String("Alice".to_string())),
            ])),
            Value::Object(HashMap::from([
                ("id".to_string(), Value::Int(2)),
                ("name".to_string(), Value::String("Bob".to_string())),
            ])),
            Value::Object(HashMap::from([
                ("id".to_string(), Value::Int(3)),
                ("name".to_string(), Value::String("Charlie".to_string())),
            ])),
        ]);

        let right_array = Value::Array(vec![
            Value::Object(HashMap::from([("id".to_string(), Value::Int(1))])),
            Value::Object(HashMap::from([("id".to_string(), Value::Int(3))])),
        ]);

        let keys = JoinKeys::on(vec!["id".to_string()]);
        let options = JoinOptions {
            join_type: JoinType::Semi,
            ..Default::default()
        };

        let result = join(&left_array, &right_array, &keys, &options).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2); // Alice and Charlie should be returned
                                          // Should only contain left columns
                if let Value::Object(obj) = &arr[0] {
                    assert!(obj.contains_key("name"));
                    assert!(!obj.contains_key("age")); // No right columns
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_anti_join() {
        let left_array = Value::Array(vec![
            Value::Object(HashMap::from([
                ("id".to_string(), Value::Int(1)),
                ("name".to_string(), Value::String("Alice".to_string())),
            ])),
            Value::Object(HashMap::from([
                ("id".to_string(), Value::Int(2)),
                ("name".to_string(), Value::String("Bob".to_string())),
            ])),
        ]);

        let right_array = Value::Array(vec![Value::Object(HashMap::from([(
            "id".to_string(),
            Value::Int(1),
        )]))]);

        let keys = JoinKeys::on(vec!["id".to_string()]);
        let options = JoinOptions {
            join_type: JoinType::Anti,
            ..Default::default()
        };

        let result = join(&left_array, &right_array, &keys, &options).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 1); // Only Bob should be returned
                if let Value::Object(obj) = &arr[0] {
                    assert_eq!(obj.get("name"), Some(&Value::String("Bob".to_string())));
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_cross_join() {
        let left_array = Value::Array(vec![
            Value::Object(HashMap::from([(
                "name".to_string(),
                Value::String("Alice".to_string()),
            )])),
            Value::Object(HashMap::from([(
                "name".to_string(),
                Value::String("Bob".to_string()),
            )])),
        ]);

        let right_array = Value::Array(vec![
            Value::Object(HashMap::from([(
                "color".to_string(),
                Value::String("Red".to_string()),
            )])),
            Value::Object(HashMap::from([(
                "color".to_string(),
                Value::String("Blue".to_string()),
            )])),
        ]);

        let keys = JoinKeys::on(vec![]); // No join keys for cross join
        let options = JoinOptions {
            join_type: JoinType::Cross,
            ..Default::default()
        };

        let result = join(&left_array, &right_array, &keys, &options).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 4); // 2 x 2 = 4 combinations
                                          // Each result should have both name and color
                for item in &arr {
                    if let Value::Object(obj) = item {
                        assert!(obj.contains_key("name"));
                        assert!(obj.contains_key("color"));
                    }
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_join_multiple() {
        let df1 = DataFrame::new(vec![
            Series::new("id", &[1, 2]),
            Series::new("name", &["Alice", "Bob"]),
        ])
        .unwrap();

        let df2 = DataFrame::new(vec![
            Series::new("id", &[1, 2]),
            Series::new("age", &[30, 25]),
        ])
        .unwrap();

        let df3 = DataFrame::new(vec![
            Series::new("id", &[1, 2]),
            Series::new("city", &["NYC", "LA"]),
        ])
        .unwrap();

        let dataframes = vec![
            Value::DataFrame(df1),
            Value::DataFrame(df2),
            Value::DataFrame(df3),
        ];

        let keys = JoinKeys::on(vec!["id".to_string()]);
        let options = JoinOptions {
            join_type: JoinType::Inner,
            ..Default::default()
        };

        let result = join_multiple(&dataframes, &keys, &options).unwrap();

        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                assert!(df.get_column_names().contains(&"name"));
                assert!(df.get_column_names().contains(&"age"));
                assert!(df.get_column_names().contains(&"city"));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_join_with_options() {
        let left_df = create_left_dataframe();
        let right_df = create_right_dataframe();

        let keys = JoinKeys::left_right(vec!["dept_id".to_string()], vec!["id".to_string()]);

        let options = JoinOptions {
            join_type: JoinType::Inner,
            suffix: "_right".to_string(),
            validate: JoinValidation::OneToMany,
            sort: true,
            coalesce: true,
        };

        let result = join(
            &Value::DataFrame(left_df),
            &Value::DataFrame(right_df),
            &keys,
            &options,
        )
        .unwrap();

        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 3);
                // Check that columns are present
                assert!(df.get_column_names().contains(&"name"));
                assert!(df.get_column_names().contains(&"dept_name"));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_join_lazy_frames() {
        let left_df = create_left_dataframe();
        let right_df = create_right_dataframe();

        let keys = JoinKeys::left_right(vec!["dept_id".to_string()], vec!["id".to_string()]);

        let options = JoinOptions {
            join_type: JoinType::Inner,
            ..Default::default()
        };

        let result = join(
            &Value::LazyFrame(Box::new(left_df.lazy())),
            &Value::LazyFrame(Box::new(right_df.lazy())),
            &keys,
            &options,
        )
        .unwrap();

        match result {
            Value::LazyFrame(_) => {
                // Just check it's a LazyFrame
            }
            _ => panic!("Expected LazyFrame"),
        }
    }

    #[test]
    fn test_join_mixed_types() {
        let left_df = create_left_dataframe();
        let right_lf = create_right_dataframe().lazy();

        let keys = JoinKeys::left_right(vec!["dept_id".to_string()], vec!["id".to_string()]);

        let options = JoinOptions {
            join_type: JoinType::Inner,
            ..Default::default()
        };

        let result = join(
            &Value::DataFrame(left_df),
            &Value::LazyFrame(Box::new(right_lf)),
            &keys,
            &options,
        )
        .unwrap();

        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 3);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_join_with_suffix() {
        let left_array = Value::Array(vec![Value::Object(HashMap::from([
            ("id".to_string(), Value::Int(1)),
            ("name".to_string(), Value::String("Alice".to_string())),
        ]))]);

        let right_array = Value::Array(vec![Value::Object(HashMap::from([
            ("id".to_string(), Value::Int(1)),
            ("name".to_string(), Value::String("Bob".to_string())), // Conflicting column
        ]))]);

        let keys = JoinKeys::on(vec!["id".to_string()]);
        let options = JoinOptions {
            join_type: JoinType::Inner,
            suffix: "_right".to_string(),
            ..Default::default()
        };

        let result = join(&left_array, &right_array, &keys, &options).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 1);
                if let Value::Object(obj) = &arr[0] {
                    assert!(obj.contains_key("name"));
                    assert!(obj.contains_key("name_right"));
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_join_empty_arrays() {
        let left_array = Value::Array(vec![]);
        let right_array = Value::Array(vec![]);

        let keys = JoinKeys::on(vec!["id".to_string()]);
        let options = JoinOptions {
            join_type: JoinType::Inner,
            ..Default::default()
        };

        let result = join(&left_array, &right_array, &keys, &options).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 0);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_join_invalid_keys() {
        let left_df = create_left_dataframe();
        let right_df = create_right_dataframe();

        let keys = JoinKeys::on(vec!["nonexistent".to_string()]);

        let result = join(
            &Value::DataFrame(left_df),
            &Value::DataFrame(right_df),
            &keys,
            &JoinOptions::default(),
        );

        assert!(result.is_err()); // Should fail due to invalid column
    }

    #[test]
    fn test_join_type_parsing() {
        assert_eq!(JoinType::from_str("inner").unwrap(), JoinType::Inner);
        assert_eq!(JoinType::from_str("left").unwrap(), JoinType::Left);
        assert_eq!(JoinType::from_str("right").unwrap(), JoinType::Right);
        assert_eq!(JoinType::from_str("outer").unwrap(), JoinType::Outer);
        assert_eq!(JoinType::from_str("full").unwrap(), JoinType::Outer);
        assert_eq!(JoinType::from_str("cross").unwrap(), JoinType::Cross);
        assert_eq!(JoinType::from_str("semi").unwrap(), JoinType::Semi);
        assert_eq!(JoinType::from_str("anti").unwrap(), JoinType::Anti);
        assert!(JoinType::from_str("invalid").is_err());
    }

    #[test]
    fn test_join_validation() {
        assert_eq!(
            JoinValidation::None.to_polars(),
            polars::prelude::JoinValidation::OneToMany
        );
        assert_eq!(
            JoinValidation::OneToMany.to_polars(),
            polars::prelude::JoinValidation::OneToMany
        );
        assert_eq!(
            JoinValidation::ManyToOne.to_polars(),
            polars::prelude::JoinValidation::ManyToOne
        );
        assert_eq!(
            JoinValidation::OneToOne.to_polars(),
            polars::prelude::JoinValidation::OneToOne
        );
    }

    #[test]
    fn test_join_keys_methods() {
        let keys = JoinKeys::on(vec!["a".to_string(), "b".to_string()]);
        assert_eq!(keys.left_columns(), &["a", "b"]);
        assert_eq!(keys.right_columns(), &["a", "b"]);

        let keys = JoinKeys::left_right(vec!["la".to_string()], vec!["ra".to_string()]);
        assert_eq!(keys.left_columns(), &["la"]);
        assert_eq!(keys.right_columns(), &["ra"]);
    }

    #[test]
    fn test_join_options_default() {
        let options = JoinOptions::default();
        assert_eq!(options.join_type, JoinType::Inner);
        assert_eq!(options.suffix, "_right");
        assert_eq!(options.validate, JoinValidation::None);
        assert!(!options.sort);
        assert!(!options.coalesce);
    }
}
