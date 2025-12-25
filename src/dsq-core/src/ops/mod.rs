#![allow(missing_docs)]

//! Operations module for dsq
//!
//! This module provides core operations for manipulating `DataFrames` and other data structures
//! in dsq. It includes basic operations like selection and filtering, aggregation operations
//! like group by and statistical functions, join operations for combining datasets, and
//! transformation operations for reshaping and converting data.
//!
//! The operations are designed to work with both Polars `DataFrames` and jq-style arrays
//! and objects, providing a unified interface that bridges the gap between structured
//! and semi-structured data processing.
//!
//! # Examples
//!
//! Basic operations:
//! ```rust,ignore
//! use dsq_core::ops::basic::{select_columns, filter_values, sort_by_columns, SortOptions};
//! use dsq_core::value::Value;
//!
//! // Select specific columns
//! let columns = vec!["name".to_string(), "age".to_string()];
//! let result = select_columns(&dataframe_value, &columns).unwrap();
//!
//! // Sort by multiple columns
//! let sort_opts = vec![
//!     SortOptions::desc("age"),
//!     SortOptions::asc("name"),
//! ];
//! let sorted = sort_by_columns(&result, &sort_opts).unwrap();
//! ```
//!
//! Aggregation operations:
//! ```rust,ignore
//! use dsq_core::ops::aggregate::{group_by_agg, AggregationFunction};
//! use dsq_core::value::Value;
//!
//! let group_cols = vec!["department".to_string()];
//! let agg_funcs = vec![
//!     AggregationFunction::Sum("salary".to_string()),
//!     AggregationFunction::Mean("age".to_string()),
//!     AggregationFunction::Count,
//! ];
//! let result = group_by_agg(&dataframe_value, &group_cols, &agg_funcs).unwrap();
//! ```
//!
//! Join operations:
//! ```rust,ignore
//! use dsq_core::ops::join::{inner_join, JoinKeys};
//! use dsq_core::value::Value;
//!
//! let keys = JoinKeys::on(vec!["id".to_string()]);
//! let result = inner_join(&left_df, &right_df, &keys).unwrap();
//! ```
//!
//! Transformation operations:
//! ```rust,ignore
//! use dsq_core::ops::transform::{transpose, string::to_uppercase};
//! use dsq_core::value::Value;
//!
//! let transposed = transpose(&dataframe_value).unwrap();
//! let uppercase = to_uppercase(&dataframe_value, "name").unwrap();
//! ```
//!
//! # Architecture
//!
//! The operations module is organized into four main submodules:
//!
//! - [`basic`] - Fundamental operations like selection, filtering, sorting
//! - [`aggregate`] - Grouping and aggregation operations
//! - `join` - Operations for combining multiple datasets
//! - [`transform`] - Data transformation and reshaping operations
//!
//! Each operation is designed to work with the [`Value`] enum, which can represent
//! `DataFrames`, `LazyFrames`, arrays, objects, or scalar values. This unified approach
//! allows operations to work seamlessly across different data representations.
//!
//! # Error Handling
//!
//! All operations return [`Result<Value>`] where errors are represented by the
//! `Error` type. Common error scenarios include:
//!
//! - Type mismatches (e.g., trying to sort non-comparable values)
//! - Missing columns or fields
//! - Schema incompatibilities in joins
//! - Invalid operation parameters
//!
//! Operations will attempt to handle mixed data types gracefully where possible,
//! but will return descriptive errors when operations cannot be completed.
//!
//! # Performance Considerations
//!
//! Operations are optimized for different data representations:
//!
//! - **`DataFrame` operations** leverage Polars' optimized columnar processing
//! - **`LazyFrame` operations** benefit from query optimization and lazy evaluation
//! - **Array operations** use efficient in-memory processing for jq-style data
//! - **Mixed operations** automatically convert between representations as needed
//!
//! For large datasets, prefer using `LazyFrame` operations when possible to take
//! advantage of query optimization and memory-efficient processing.

pub mod access_ops;
pub mod aggregate;
pub mod arithmetic_ops;
pub mod assignment_ops;
pub mod basic;
pub mod comparison_ops;
pub mod construct_ops;
pub mod join;
pub mod logical_ops;
pub mod pipeline;
pub mod selection_ops;
#[cfg(test)]
pub mod tests;
/// Data transformation operations
pub mod transform;
pub mod utils;

// Re-export commonly used types and functions for convenience
pub use basic::{
    add_column, count, drop_columns, filter_rows, filter_values, head, rename_columns, reverse,
    select_columns, slice, sort_by_columns, tail, unique, SortOptions,
};

pub use aggregate::{
    cumulative_agg, group_by, group_by_agg, pivot, rolling_agg, rolling_std, unpivot,
    AggregationFunction, WindowFunction,
};

pub use join::{
    inner_join, join, join_multiple, left_join, outer_join, right_join, JoinKeys, JoinOptions,
    JoinType, JoinValidation,
};

pub use transform::Transform;

pub use pipeline::{
    apply_operations, apply_operations_mut, apply_operations_owned, OperationPipeline,
};

pub use utils::{recommended_batch_size, supports_operation, OperationType};

// Re-export operation types from filter_ops modules
pub use access_ops::{
    FieldAccessOperation, IdentityOperation, IndexOperation, IterateOperation, SliceOperation,
};

pub use construct_ops::{
    ArrayConstructOperation, LiteralOperation, ObjectConstructOperation, VariableOperation,
};

pub use arithmetic_ops::{AddOperation, DivOperation, MulOperation, SubOperation};

pub use comparison_ops::{
    EqOperation, GeOperation, GtOperation, LeOperation, LtOperation, NeOperation,
};

pub use logical_ops::{AndOperation, NegationOperation, OrOperation};

pub use assignment_ops::{AssignAddOperation, AssignUpdateOperation};

pub use selection_ops::SelectConditionOperation;

use crate::error::Result;
use crate::Value;

/// Trait for operations that can be applied to values
///
/// This trait provides a common interface for all data operations,
/// allowing them to be composed and chained together.
pub trait Operation {
    /// Apply the operation to a value
    fn apply(&self, value: &Value) -> Result<Value>;

    /// Get a description of what this operation does
    fn description(&self) -> String;

    /// Check if this operation can be applied to the given value type
    fn is_applicable(&self, value: &Value) -> bool {
        // Default implementation: try to apply and see if it works
        self.apply(value).is_ok()
    }
}
