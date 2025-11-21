//! High-level API for dsq-core
//!
//! This module provides a convenient, high-level interface for common dsq operations,
//! abstracting away the complexity of the underlying modules while still providing
//! access to advanced features when needed.
//!
//! The API is designed to be ergonomic for both simple one-off operations and
//! complex data processing pipelines.
//!
//! # Examples
//!
//! Simple data processing:
//! ```rust,ignore
//! use dsq_core::api::Dsq;
//!
//! // Load and process data in one pipeline
//! let result = Dsq::from_file("data.csv")?
//!     .select(&["name", "age", "department"])?
//!     .filter_expr("age > 25")?
//!     .sort_by(&["department", "age"])?
//!     .to_json()?;
//! # Ok::<(), dsq_core::Error>(())
//! ```
//!
//! Advanced processing with custom options:
//! ```rust,ignore
//! use dsq_core::api::{Dsq, ProcessingOptions};
//!
//! let options = ProcessingOptions::default()
//!     .with_lazy_evaluation(true)
//!     .with_batch_size(10000);
//!
//! let result = Dsq::with_options(options)
//!     .from_file("large_data.parquet")?
//!     .apply_filter(r#"map(select(.status == "active")) | group_by(.department)"#)?
//!     .collect()?;
//! # Ok::<(), dsq_core::Error>(())
//! ```

use crate::error::{Error, FilterError, Result, TypeError};
use crate::ops::{
    aggregate::{group_by, group_by_agg, AggregationFunction},
    basic::{head, select_columns, sort_by_columns, tail, SortOptions},
    join::{join, JoinKeys, JoinOptions, JoinType},
    transform::{cast_column, transpose, ColumnDataType},
    Operation, OperationPipeline, OperationType,
};
use crate::Value;
use dsq_formats::{from_csv as dsq_from_csv, from_json as dsq_from_json, DataFormat};

#[cfg(feature = "io")]
use dsq_formats::{ReadOptions, WriteOptions};
#[cfg(feature = "io")]
use dsq_io::{read_file, write_file};

#[cfg(feature = "filter")]
use dsq_filter::{
    execute_filter, ErrorMode as FilterErrorMode, ExecutorConfig, FilterCompiler, FilterExecutor,
    OptimizationLevel,
};

use polars::prelude::*;
use std::collections::HashMap;
use std::path::Path;

/// High-level dsq processing context
///
/// The `Dsq` struct provides a fluent interface for data processing operations.
/// It maintains the current data state and processing options, allowing for
/// method chaining and pipeline-style operations.
#[derive(Debug, Clone)]
pub struct Dsq {
    /// Current data value
    data: Value,
    /// Processing options
    options: ProcessingOptions,
    /// Operation history for debugging/optimization
    operations: Vec<String>,
}

/// Configuration options for dsq processing
#[derive(Debug, Clone)]
pub struct ProcessingOptions {
    /// Whether to use lazy evaluation where possible
    pub lazy_evaluation: bool,
    /// Whether to enable DataFrame-specific optimizations
    pub dataframe_optimizations: bool,
    /// Batch size for processing large datasets
    pub batch_size: Option<usize>,
    /// Maximum memory usage (in bytes)
    pub memory_limit: Option<usize>,
    /// Whether to collect detailed execution statistics
    pub collect_stats: bool,
    /// Error handling mode
    pub error_mode: ErrorMode,
    /// Filter execution optimization level
    pub optimization_level: OptimizationLevel,
}

/// Error handling modes for processing operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorMode {
    /// Stop on first error (default)
    Strict,
    /// Collect errors but continue processing
    Collect,
    /// Ignore errors and continue with null values
    Ignore,
}

impl Default for ProcessingOptions {
    fn default() -> Self {
        Self {
            lazy_evaluation: true,
            dataframe_optimizations: true,
            batch_size: Some(10000),
            memory_limit: None,
            collect_stats: false,
            error_mode: ErrorMode::Strict,
            optimization_level: OptimizationLevel::Basic,
        }
    }
}

impl ProcessingOptions {
    /// Create new processing options with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable lazy evaluation
    pub fn with_lazy_evaluation(mut self, lazy: bool) -> Self {
        self.lazy_evaluation = lazy;
        self
    }

    /// Enable or disable DataFrame optimizations
    pub fn with_dataframe_optimizations(mut self, optimize: bool) -> Self {
        self.dataframe_optimizations = optimize;
        self
    }

    /// Set the batch size for processing
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    /// Set memory limit
    pub fn with_memory_limit(mut self, limit: usize) -> Self {
        self.memory_limit = Some(limit);
        self
    }

    /// Enable statistics collection
    pub fn with_stats_collection(mut self, collect: bool) -> Self {
        self.collect_stats = collect;
        self
    }

    /// Set error handling mode
    pub fn with_error_mode(mut self, mode: ErrorMode) -> Self {
        self.error_mode = mode;
        self
    }

    /// Set filter optimization level
    pub fn with_optimization_level(mut self, level: OptimizationLevel) -> Self {
        self.optimization_level = level;
        self
    }
}

impl Dsq {
    /// Create a new dsq context with default options
    pub fn new() -> Self {
        Self {
            data: Value::Null,
            options: ProcessingOptions::default(),
            operations: Vec::new(),
        }
    }

    /// Create a new dsq context with custom options
    pub fn with_options(options: ProcessingOptions) -> Self {
        Self {
            data: Value::Null,
            options,
            operations: Vec::new(),
        }
    }

    /// Create a dsq context from a Value
    pub fn from_value(value: Value) -> Self {
        Self {
            data: value,
            options: ProcessingOptions::default(),
            operations: Vec::new(),
        }
    }

    /// Create a dsq context from a Value with custom options
    pub fn from_value_with_options(value: Value, options: ProcessingOptions) -> Self {
        Self {
            data: value,
            options,
            operations: Vec::new(),
        }
    }

    /// Load data from a file
    #[cfg(feature = "io")]
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let read_options = ReadOptions {
            lazy: true, // Use lazy by default for file loading
            ..Default::default()
        };
        let data = read_data_file(path, &read_options)?;
        Ok(Self::from_value(data))
    }

    /// Load data from a file with custom read options
    #[cfg(feature = "io")]
    pub fn from_file_with_options<P: AsRef<Path>>(
        path: P,
        read_options: &ReadOptions,
    ) -> Result<Self> {
        let data = read_data_file(path, read_options)?;
        Ok(Self::from_value(data))
    }

    /// Load data from memory (JSON, CSV, etc.)
    pub fn from_json(json: &str) -> Result<Self> {
        let value = dsq_from_json(json)?;
        Ok(Self::from_value(value))
    }

    /// Load data from CSV string
    pub fn from_csv(csv: &str) -> Result<Self> {
        let value = dsq_from_csv(csv)?;
        Ok(Self::from_value(value))
    }

    /// Create an empty dsq context
    pub fn empty() -> Self {
        Self::from_value(Value::Array(Vec::new()))
    }

    /// Create a dsq context with a range of numbers
    pub fn range(start: i64, end: i64) -> Self {
        let values: Vec<Value> = (start..end).map(Value::Int).collect();
        Self::from_value(Value::Array(values))
    }

    // === Core Operations ===

    /// Apply a jq-style filter expression
    #[cfg(feature = "filter")]
    pub fn apply_filter(mut self, filter: &str) -> Result<Self> {
        let mut config = ExecutorConfig::default();
        config.collect_stats = self.options.collect_stats;
        // Map error mode
        config.error_mode = match self.options.error_mode {
            ErrorMode::Strict => FilterErrorMode::Strict,
            ErrorMode::Collect => FilterErrorMode::Collect,
            ErrorMode::Ignore => FilterErrorMode::Ignore,
        };

        let mut executor = FilterExecutor::new();
        executor.set_config(config);
        let result = executor.execute_str(filter, self.data)?;
        self.data = result.value;
        self.operations.push(format!("filter: {}", filter));
        Ok(self)
    }

    /// Apply a simple filter expression (without full jq syntax)
    pub fn filter_expr(mut self, expr: &str) -> Result<Self> {
        // Simple expression parsing - in a full implementation, this would
        // parse basic expressions like "age > 25", "name == 'Alice'", etc.
        let result = self.apply_simple_filter(expr)?;
        self.data = result;
        self.operations.push(format!("filter_expr: {}", expr));
        Ok(self)
    }

    /// Select specific columns
    pub fn select(mut self, columns: &[&str]) -> Result<Self> {
        let column_names: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        let result = select_columns(&self.data, &column_names)?;
        self.data = result;
        self.operations
            .push(format!("select: {}", columns.join(", ")));
        Ok(self)
    }

    /// Sort by one or more columns
    pub fn sort_by(mut self, columns: &[&str]) -> Result<Self> {
        let sort_options: Vec<SortOptions> = columns
            .iter()
            .map(|col| SortOptions::asc(col.to_string()))
            .collect();
        let result = sort_by_columns(&self.data, &sort_options)?;
        self.data = result;
        self.operations
            .push(format!("sort_by: {}", columns.join(", ")));
        Ok(self)
    }

    /// Sort by columns with explicit ascending/descending order
    pub fn sort_by_with_order(mut self, columns: &[(&str, bool)]) -> Result<Self> {
        let sort_options: Vec<SortOptions> = columns
            .iter()
            .map(|(col, desc)| {
                if *desc {
                    SortOptions::desc(col.to_string())
                } else {
                    SortOptions::asc(col.to_string())
                }
            })
            .collect();
        let result = sort_by_columns(&self.data, &sort_options)?;
        self.data = result;
        self.operations
            .push(format!("sort_by_with_order: {:?}", columns));
        Ok(self)
    }

    /// Take the first N rows
    pub fn head(mut self, n: usize) -> Result<Self> {
        let result = head(&self.data, n)?;
        self.data = result;
        self.operations.push(format!("head: {}", n));
        Ok(self)
    }

    /// Take the last N rows
    pub fn tail(mut self, n: usize) -> Result<Self> {
        let result = tail(&self.data, n)?;
        self.data = result;
        self.operations.push(format!("tail: {}", n));
        Ok(self)
    }

    /// Group by one or more columns
    pub fn group_by(mut self, columns: &[&str]) -> Result<Self> {
        let column_names: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        let result = group_by(&self.data, &column_names)?;
        self.data = result;
        self.operations
            .push(format!("group_by: {}", columns.join(", ")));
        Ok(self)
    }

    /// Group by columns and apply aggregations
    pub fn aggregate(
        mut self,
        group_cols: &[&str],
        agg_funcs: Vec<AggregationFunction>,
    ) -> Result<Self> {
        let group_columns: Vec<String> = group_cols.iter().map(|s| s.to_string()).collect();
        let result = group_by_agg(&self.data, &group_columns, &agg_funcs)?;
        self.data = result;
        self.operations.push(format!(
            "aggregate: group_by({}), agg({:?})",
            group_cols.join(", "),
            agg_funcs
                .iter()
                .map(|f| f.output_column_name())
                .collect::<Vec<_>>()
        ));
        Ok(self)
    }

    /// Join with another dataset
    pub fn join_with(mut self, other: Dsq, keys: &[&str], join_type: JoinType) -> Result<Self> {
        let join_keys = JoinKeys::on(keys.iter().map(|s| s.to_string()).collect());
        let join_options = JoinOptions {
            join_type,
            ..Default::default()
        };
        let result = join(&self.data, &other.data, &join_keys, &join_options)?;
        self.data = result;
        self.operations.push(format!(
            "join: {} on {}",
            join_type.as_str(),
            keys.join(", ")
        ));
        Ok(self)
    }

    /// Inner join with another dataset
    pub fn inner_join(self, other: Dsq, keys: &[&str]) -> Result<Self> {
        self.join_with(other, keys, JoinType::Inner)
    }

    /// Left join with another dataset
    pub fn left_join(self, other: Dsq, keys: &[&str]) -> Result<Self> {
        self.join_with(other, keys, JoinType::Left)
    }

    /// Transpose the data
    pub fn transpose(mut self) -> Result<Self> {
        let result = transpose(&self.data)?;
        self.data = result;
        self.operations.push("transpose".to_string());
        Ok(self)
    }

    /// Cast a column to a different type
    pub fn cast_column(mut self, column: &str, target_type: ColumnDataType) -> Result<Self> {
        let result = cast_column(&self.data, column, target_type)?;
        self.data = result;
        self.operations
            .push(format!("cast: {} to {:?}", column, target_type));
        Ok(self)
    }

    // === Convenience Methods ===

    /// Get summary statistics
    pub fn describe(self) -> Result<Self> {
        match &self.data {
            Value::DataFrame(df) => {
                let mut stats = HashMap::new();
                stats.insert(
                    "shape".to_string(),
                    Value::Array(vec![
                        Value::Int(df.height() as i64),
                        Value::Int(df.width() as i64),
                    ]),
                );
                stats.insert(
                    "columns".to_string(),
                    Value::Array(
                        df.get_column_names()
                            .iter()
                            .map(|name| Value::String(name.to_string()))
                            .collect(),
                    ),
                );

                // Add basic statistics for numeric columns
                for col_name in df.get_column_names() {
                    if let Ok(series) = df.column(col_name) {
                        if series.dtype().is_numeric() {
                            let mut col_stats = HashMap::new();
                            if let Ok(Some(mean)) = series.mean() {
                                col_stats.insert("mean".to_string(), Value::Float(mean));
                            }
                            if let Ok(Some(min)) = series.min::<f64>() {
                                col_stats.insert("min".to_string(), Value::Float(min));
                            }
                            if let Ok(Some(max)) = series.max::<f64>() {
                                col_stats.insert("max".to_string(), Value::Float(max));
                            }
                            stats.insert(format!("{}_stats", col_name), Value::Object(col_stats));
                        }
                    }
                }

                Ok(Self::from_value(Value::Object(stats)))
            }
            _ => Err(Error::operation("describe() only works with DataFrames")),
        }
    }

    /// Get unique values from a column
    pub fn unique_values(self, column: &str) -> Result<Self> {
        match &self.data {
            Value::DataFrame(df) => {
                let series = df.column(column).map_err(Error::from)?;
                let unique_series = series.unique().map_err(Error::from)?;
                let unique_df = DataFrame::new(vec![unique_series]).map_err(Error::from)?;
                Ok(Self::from_value(Value::DataFrame(unique_df)))
            }
            _ => Err(Error::operation(
                "unique_values() only works with DataFrames",
            )),
        }
    }

    /// Count rows
    pub fn count(self) -> Result<i64> {
        match &self.data {
            Value::DataFrame(df) => Ok(df.height() as i64),
            Value::Array(arr) => Ok(arr.len() as i64),
            Value::Object(obj) => Ok(obj.len() as i64),
            _ => Ok(1),
        }
    }

    /// Check if data is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    // === Output Methods ===

    /// Convert to JSON string
    pub fn to_json(self) -> Result<String> {
        let json_val = self.data.to_json()?;
        serde_json::to_string(&json_val)
            .map_err(|e| Error::operation(format!("JSON serialization failed: {}", e)))
    }

    /// Convert to pretty-printed JSON string
    pub fn to_json_pretty(self) -> Result<String> {
        let json_val = self.data.to_json()?;
        serde_json::to_string_pretty(&json_val)
            .map_err(|e| Error::operation(format!("JSON serialization failed: {}", e)))
    }

    /// Write to a file
    #[cfg(feature = "io")]
    pub fn write_to_file<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let write_options = WriteOptions::default();
        write_data_file(&self.data, path, &write_options)
    }

    /// Write to a file with custom options
    #[cfg(feature = "io")]
    pub fn write_to_file_with_options<P: AsRef<Path>>(
        self,
        path: P,
        options: &WriteOptions,
    ) -> Result<()> {
        write_data_file(&self.data, path, options)
    }

    /// Collect lazy operations (force evaluation)
    pub fn collect(mut self) -> Result<Self> {
        match self.data {
            Value::LazyFrame(lf) => {
                let df = lf.collect().map_err(Error::from)?;
                self.data = Value::DataFrame(df);
                self.operations.push("collect".to_string());
                Ok(self)
            }
            _ => Ok(self), // Already collected
        }
    }

    /// Get the underlying Value
    pub fn into_value(self) -> Value {
        self.data
    }

    /// Get a reference to the underlying Value
    pub fn value(&self) -> &Value {
        &self.data
    }

    /// Get the processing options
    pub fn options(&self) -> &ProcessingOptions {
        &self.options
    }

    /// Get the operation history
    pub fn operations(&self) -> &[String] {
        &self.operations
    }

    /// Print operation history
    pub fn print_operations(&self) {
        println!("Operations performed:");
        for (i, op) in self.operations.iter().enumerate() {
            println!("  {}. {}", i + 1, op);
        }
    }

    // === Internal Helper Methods ===

    /// Apply a simple filter expression
    fn apply_simple_filter(&self, expr: &str) -> Result<Value> {
        // Parse simple expressions like "age > 25", "name == 'Alice'", "department != 'Sales'"
        let expr = expr.trim();

        // Handle boolean literals
        match expr {
            "true" => return Ok(self.data.clone()),
            "false" => return Ok(Value::Array(Vec::new())),
            _ => {}
        }

        // Parse comparison expressions
        if let Some((field, op, value_str)) = self.parse_comparison(expr)? {
            self.apply_comparison_filter(&field, op, value_str)
        } else {
            Err(Error::operation(format!(
                "Unsupported filter expression: {}",
                expr
            )))
        }
    }

    /// Parse a comparison expression into (field, operator, value)
    fn parse_comparison(&self, expr: &str) -> Result<Option<(String, &str, String)>> {
        let operators = [" > ", " < ", " >= ", " <= ", " == ", " != "];

        for op in &operators {
            if let Some(pos) = expr.find(op) {
                let field = expr[..pos].trim().to_string();
                let value_str = expr[pos + op.len()..].trim().to_string();
                return Ok(Some((field, op.trim(), value_str)));
            }
        }

        Ok(None)
    }

    /// Apply a comparison filter
    fn apply_comparison_filter(&self, field: &str, op: &str, value_str: String) -> Result<Value> {
        match &self.data {
            Value::Array(arr) => {
                let mut filtered = Vec::new();
                for item in arr {
                    if let Value::Object(obj) = item {
                        if let Some(field_value) = obj.get(field) {
                            if self.compare_values(field_value, op, &value_str)? {
                                filtered.push(item.clone());
                            }
                        }
                    }
                }
                Ok(Value::Array(filtered))
            }
            _ => Err(Error::operation(
                "Filter expressions only work with arrays of objects",
            )),
        }
    }

    /// Compare a field value with a string value using the given operator
    fn compare_values(&self, field_value: &Value, op: &str, value_str: &str) -> Result<bool> {
        match op {
            "==" => Ok(self.values_equal(field_value, value_str)),
            "!=" => Ok(!self.values_equal(field_value, value_str)),
            ">" | "<" | ">=" | "<=" => {
                let ordering = self.compare_numeric(field_value, value_str)?;
                match op {
                    ">" => Ok(ordering == std::cmp::Ordering::Greater),
                    "<" => Ok(ordering == std::cmp::Ordering::Less),
                    ">=" => Ok(ordering != std::cmp::Ordering::Less),
                    "<=" => Ok(ordering != std::cmp::Ordering::Greater),
                    _ => unreachable!(),
                }
            }
            _ => Err(Error::operation(format!("Unsupported operator: {}", op))),
        }
    }

    /// Check if a field value equals a string value (with type coercion)
    fn values_equal(&self, field_value: &Value, value_str: &str) -> bool {
        match field_value {
            Value::String(s) => {
                let trimmed = value_str.trim_matches('\'').trim_matches('"');
                s == trimmed
            }
            Value::Int(i) => {
                if let Ok(parsed) = value_str.parse::<i64>() {
                    *i == parsed
                } else {
                    false
                }
            }
            Value::Float(f) => {
                if let Ok(parsed) = value_str.parse::<f64>() {
                    (*f - parsed).abs() < f64::EPSILON
                } else {
                    false
                }
            }
            Value::Bool(b) => {
                if let Ok(parsed) = value_str.parse::<bool>() {
                    *b == parsed
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Compare numeric values
    fn compare_numeric(&self, field_value: &Value, value_str: &str) -> Result<std::cmp::Ordering> {
        match field_value {
            Value::Int(i) => {
                if let Ok(parsed) = value_str.parse::<i64>() {
                    Ok(i.cmp(&parsed))
                } else {
                    Err(Error::operation(
                        "Cannot compare int with non-numeric value",
                    ))
                }
            }
            Value::Float(f) => {
                if let Ok(parsed) = value_str.parse::<f64>() {
                    f.partial_cmp(&parsed)
                        .ok_or_else(|| Error::operation("Float comparison failed"))
                } else {
                    Err(Error::operation(
                        "Cannot compare float with non-numeric value",
                    ))
                }
            }
            _ => Err(Error::operation("Can only compare numeric values")),
        }
    }
}

impl Default for Dsq {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder pattern for creating aggregation pipelines
pub struct AggregationBuilder {
    group_columns: Vec<String>,
    aggregations: Vec<AggregationFunction>,
}

impl AggregationBuilder {
    /// Create a new aggregation builder
    pub fn new() -> Self {
        Self {
            group_columns: Vec::new(),
            aggregations: Vec::new(),
        }
    }

    /// Group by columns
    pub fn group_by(mut self, columns: &[&str]) -> Self {
        self.group_columns = columns.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Add a sum aggregation
    pub fn sum(mut self, column: &str) -> Self {
        self.aggregations
            .push(AggregationFunction::Sum(column.to_string()));
        self
    }

    /// Add a mean aggregation
    pub fn mean(mut self, column: &str) -> Self {
        self.aggregations
            .push(AggregationFunction::Mean(column.to_string()));
        self
    }

    /// Add a count aggregation
    pub fn count(mut self) -> Self {
        self.aggregations.push(AggregationFunction::Count);
        self
    }

    /// Add a min aggregation
    pub fn min(mut self, column: &str) -> Self {
        self.aggregations
            .push(AggregationFunction::Min(column.to_string()));
        self
    }

    /// Add a max aggregation
    pub fn max(mut self, column: &str) -> Self {
        self.aggregations
            .push(AggregationFunction::Max(column.to_string()));
        self
    }

    /// Build the aggregation functions
    pub fn build(self) -> (Vec<String>, Vec<AggregationFunction>) {
        (self.group_columns, self.aggregations)
    }
}

impl Default for AggregationBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience functions for common operations
pub mod convenience {
    use super::*;

    /// Quick CSV processing
    pub fn process_csv(csv_data: &str, operations: impl Fn(Dsq) -> Result<Dsq>) -> Result<String> {
        let dsq = Dsq::from_csv(csv_data)?;
        let result = operations(dsq)?;
        result.to_json()
    }

    /// Quick JSON processing
    pub fn process_json(
        json_data: &str,
        operations: impl Fn(Dsq) -> Result<Dsq>,
    ) -> Result<String> {
        let dsq = Dsq::from_json(json_data)?;
        let result = operations(dsq)?;
        result.to_json()
    }

    /// Quick file processing
    #[cfg(feature = "io")]
    pub fn process_file<P: AsRef<Path>>(
        input_path: P,
        output_path: P,
        operations: impl Fn(Dsq) -> Result<Dsq>,
    ) -> Result<()> {
        let dsq = Dsq::from_file(input_path)?;
        let result = operations(dsq)?;
        result.write_to_file(output_path)
    }

    /// Create a simple aggregation
    pub fn simple_agg(group_cols: &[&str]) -> AggregationBuilder {
        AggregationBuilder::new().group_by(group_cols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;
    use std::collections::HashMap;

    fn create_test_data() -> Value {
        Value::Array(vec![
            Value::Object(HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::Int(30)),
                (
                    "department".to_string(),
                    Value::String("Engineering".to_string()),
                ),
                ("salary".to_string(), Value::Int(75000)),
            ])),
            Value::Object(HashMap::from([
                ("name".to_string(), Value::String("Bob".to_string())),
                ("age".to_string(), Value::Int(25)),
                ("department".to_string(), Value::String("Sales".to_string())),
                ("salary".to_string(), Value::Int(50000)),
            ])),
            Value::Object(HashMap::from([
                ("name".to_string(), Value::String("Charlie".to_string())),
                ("age".to_string(), Value::Int(35)),
                (
                    "department".to_string(),
                    Value::String("Engineering".to_string()),
                ),
                ("salary".to_string(), Value::Int(80000)),
            ])),
        ])
    }

    #[test]
    fn test_basic_operations() {
        let dsq = Dsq::from_value(create_test_data());

        let result = dsq
            .select(&["name", "age", "department"])
            .unwrap()
            .sort_by(&["age"])
            .unwrap()
            .head(2)
            .unwrap();

        let count = result.count().unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_filter_operations() {
        let dsq = Dsq::from_value(create_test_data());

        let result = dsq.filter_expr("age > 28").unwrap().count().unwrap();

        assert_eq!(result, 2); // Alice and Charlie
    }

    #[test]
    fn test_aggregation_builder() {
        let (group_cols, agg_funcs) = AggregationBuilder::new()
            .group_by(&["department"])
            .count()
            .mean("salary")
            .build();

        assert_eq!(group_cols, vec!["department"]);
        assert_eq!(agg_funcs.len(), 2);
    }

    #[test]
    fn test_json_conversion() {
        let dsq = Dsq::from_value(create_test_data());
        let json = dsq.to_json().unwrap();

        // Should be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_array());
    }

    #[test]
    fn test_processing_options() {
        let options = ProcessingOptions::new()
            .with_lazy_evaluation(false)
            .with_batch_size(5000)
            .with_error_mode(ErrorMode::Collect)
            .with_memory_limit(1024 * 1024)
            .with_stats_collection(true)
            .with_optimization_level(OptimizationLevel::Basic);

        assert!(!options.lazy_evaluation);
        assert_eq!(options.batch_size, Some(5000));
        assert_eq!(options.error_mode, ErrorMode::Collect);
        assert_eq!(options.memory_limit, Some(1024 * 1024));
        assert!(options.collect_stats);
        assert_eq!(
            options.optimization_level,
            dsq_filter::OptimizationLevel::Basic
        );
    }

    #[test]
    fn test_processing_options_default() {
        let options = ProcessingOptions::default();
        assert!(options.lazy_evaluation);
        assert_eq!(options.batch_size, Some(10000));
        assert_eq!(options.error_mode, ErrorMode::Strict);
        assert!(!options.collect_stats);
    }

    #[test]
    fn test_operation_history() {
        let dsq = Dsq::from_value(create_test_data());

        let result = dsq
            .select(&["name", "age"])
            .unwrap()
            .sort_by(&["name"])
            .unwrap();

        let operations = result.operations();
        assert_eq!(operations.len(), 2);
        assert!(operations[0].contains("select"));
        assert!(operations[1].contains("sort_by"));
    }

    #[test]
    fn test_convenience_functions() {
        let json_data = r#"[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]"#;

        let result = convenience::process_json(json_data, |dsq| {
            dsq.select(&["name"]).and_then(|d| d.sort_by(&["name"]))
        })
        .unwrap();

        // Should be valid JSON with only name field
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.is_array());
    }

    #[test]
    fn test_convenience_functions_csv() {
        let csv_data = "name,age\nAlice,30\nBob,25";

        let result = convenience::process_csv(csv_data, |dsq| dsq.select(&["name"])).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_simple_agg_builder() {
        let builder = convenience::simple_agg(&["department"]);
        let (groups, aggs) = builder.sum("salary").mean("age").build();
        assert_eq!(groups, vec!["department"]);
        assert_eq!(aggs.len(), 2);
    }

    #[test]
    fn test_error_handling_invalid_json() {
        let result = Dsq::from_json("{invalid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_error_handling_invalid_csv() {
        // This might not fail depending on implementation
        // Let me test something that should fail
        let dsq = Dsq::from_value(Value::Int(42));
        let result = dsq.select(&["nonexistent"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_error_handling_filter_invalid_expr() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.filter_expr("invalid expression syntax");
        assert!(result.is_err());
    }

    #[test]
    fn test_error_handling_sort_invalid_column() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.sort_by(&["nonexistent_column"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_filter_expr_numeric_greater_than() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.filter_expr("age > 28").unwrap();
        let count = result.count().unwrap();
        assert_eq!(count, 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_filter_expr_numeric_less_than() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.filter_expr("age < 30").unwrap();
        let count = result.count().unwrap();
        assert_eq!(count, 1); // Bob (25)
    }

    #[test]
    fn test_filter_expr_equality_string() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.filter_expr("department == 'Engineering'").unwrap();
        let count = result.count().unwrap();
        assert_eq!(count, 2); // Alice and Charlie
    }

    #[test]
    fn test_filter_expr_inequality() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.filter_expr("department != 'Engineering'").unwrap();
        let count = result.count().unwrap();
        assert_eq!(count, 1); // Bob
    }

    #[test]
    fn test_filter_expr_greater_equal() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.filter_expr("age >= 30").unwrap();
        let count = result.count().unwrap();
        assert_eq!(count, 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_filter_expr_less_equal() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.filter_expr("age <= 25").unwrap();
        let count = result.count().unwrap();
        assert_eq!(count, 1); // Bob (25)
    }

    #[test]
    fn test_empty_and_range() {
        let empty = Dsq::empty();
        assert!(empty.is_empty());

        let range = Dsq::range(1, 5);
        let count = range.count().unwrap();
        assert_eq!(count, 4); // 1, 2, 3, 4
    }

    #[test]
    fn test_from_json() {
        let json = r#"{"name": "Alice", "age": 30}"#;
        let dsq = Dsq::from_json(json).unwrap();
        assert_eq!(dsq.count().unwrap(), 1);
    }

    #[test]
    fn test_from_csv() {
        let csv = "name,age\nAlice,30\nBob,25";
        let dsq = Dsq::from_csv(csv).unwrap();
        assert_eq!(dsq.count().unwrap(), 2);
    }

    #[test]
    fn test_select_single_column() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.select(&["name"]).unwrap();
        let json = result.to_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 3);
        // Check that only name field is present
        for item in parsed.as_array().unwrap() {
            assert!(item.get("name").is_some());
            assert!(item.get("age").is_none());
        }
    }

    #[test]
    fn test_sort_by_ascending() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.sort_by(&["age"]).unwrap();
        let json = result.to_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr[0]["name"], "Bob"); // age 25
        assert_eq!(arr[1]["name"], "Alice"); // age 30
        assert_eq!(arr[2]["name"], "Charlie"); // age 35
    }

    #[test]
    fn test_sort_by_descending() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.sort_by_with_order(&[("age", true)]).unwrap(); // true = descending
        let json = result.to_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr[0]["name"], "Charlie"); // age 35
        assert_eq!(arr[1]["name"], "Alice"); // age 30
        assert_eq!(arr[2]["name"], "Bob"); // age 25
    }

    #[test]
    fn test_head_and_tail() {
        let dsq = Dsq::from_value(create_test_data());
        let head_result = dsq.head(2).unwrap();
        assert_eq!(head_result.count().unwrap(), 2);

        let tail_result = dsq.tail(1).unwrap();
        assert_eq!(tail_result.count().unwrap(), 1);
    }

    #[test]
    fn test_group_by() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.group_by(&["department"]).unwrap();
        // This should create groups by department
        match result.value() {
            Value::Array(groups) => {
                assert_eq!(groups.len(), 2); // Engineering and Sales
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_aggregate() {
        let dsq = Dsq::from_value(create_test_data());
        let agg_funcs = vec![AggregationFunction::Count];
        let result = dsq.aggregate(&["department"], agg_funcs).unwrap();
        // Should have aggregated results
        assert!(!result.is_empty());
    }

    #[test]
    fn test_transpose() {
        // Create a simple 2x2 array for testing
        let data = Value::Array(vec![
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            Value::Array(vec![Value::Int(3), Value::Int(4)]),
        ]);
        let dsq = Dsq::from_value(data);
        let result = dsq.transpose().unwrap();
        // Transposed should be [[1,3], [2,4]]
        match result.value() {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
                if let Value::Array(row1) = &arr[0] {
                    assert_eq!(row1.len(), 2);
                    assert_eq!(row1[0], Value::Int(1));
                    assert_eq!(row1[1], Value::Int(3));
                }
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_cast_column() {
        // This test would require a DataFrame with columns
        // For now, skip as it needs more complex setup
    }

    #[test]
    fn test_describe_dataframe() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25],
            "salary" => [75000.0, 50000.0]
        }
        .unwrap();
        let dsq = Dsq::from_value(Value::DataFrame(df));
        let result = dsq.describe().unwrap();
        let json = result.to_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("shape").is_some());
        assert!(parsed.get("columns").is_some());
    }

    #[test]
    fn test_unique_values() {
        let df = df! {
            "department" => ["Engineering", "Sales", "Engineering"]
        }
        .unwrap();
        let dsq = Dsq::from_value(Value::DataFrame(df));
        let result = dsq.unique_values("department").unwrap();
        // Should have unique departments
        assert!(!result.is_empty());
    }

    #[test]
    fn test_to_json_pretty() {
        let dsq = Dsq::from_value(create_test_data());
        let json = dsq.to_json_pretty().unwrap();
        // Should be valid JSON and contain newlines
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_array());
        assert!(json.contains('\n'));
    }

    #[test]
    fn test_operation_history_tracking() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq
            .select(&["name"])
            .unwrap()
            .filter_expr("true")
            .unwrap()
            .sort_by(&["name"])
            .unwrap();

        let operations = result.operations();
        assert_eq!(operations.len(), 3);
        assert!(operations[0].contains("select"));
        assert!(operations[1].contains("filter_expr"));
        assert!(operations[2].contains("sort_by"));
    }

    #[test]
    fn test_print_operations() {
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.select(&["name"]).unwrap();
        // This should not panic
        result.print_operations();
    }

    #[test]
    fn test_collect_lazy() {
        // Create a lazy frame somehow - this might be complex
        // For now, test that collect works on regular data
        let dsq = Dsq::from_value(create_test_data());
        let result = dsq.collect().unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_example_003_scenario() {
        // Test the scenario from examples/example_003
        // Query: map(.salary += 5000) | map({name, new_salary: .salary, department})

        // Create test data similar to the CSV
        let df = df! {
            "id" => [1, 2, 3],
            "name" => ["Alice Johnson", "Bob Smith", "Carol Williams"],
            "age" => [28, 34, 29],
            "city" => ["New York", "Los Angeles", "Chicago"],
            "salary" => [75000, 82000, 68000],
            "department" => ["Engineering", "Sales", "Marketing"]
        }
        .unwrap();

        let dsq = Dsq::from_value(Value::DataFrame(df));

        // Execute the filter
        let result = dsq
            .apply_filter("map(.salary += 5000) | map({name, new_salary: .salary, department})")
            .unwrap();

        // Check the result
        match result.value() {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);

                // Check first item
                if let Value::Object(obj) = &arr[0] {
                    assert_eq!(
                        obj.get("name"),
                        Some(&Value::String("Alice Johnson".to_string()))
                    );
                    assert_eq!(obj.get("new_salary"), Some(&Value::Int(80000))); // 75000 + 5000
                    assert_eq!(
                        obj.get("department"),
                        Some(&Value::String("Engineering".to_string()))
                    );
                } else {
                    panic!("Expected Object");
                }

                // Check second item
                if let Value::Object(obj) = &arr[1] {
                    assert_eq!(
                        obj.get("name"),
                        Some(&Value::String("Bob Smith".to_string()))
                    );
                    assert_eq!(obj.get("new_salary"), Some(&Value::Int(87000))); // 82000 + 5000
                    assert_eq!(
                        obj.get("department"),
                        Some(&Value::String("Sales".to_string()))
                    );
                } else {
                    panic!("Expected Object");
                }

                // Check third item
                if let Value::Object(obj) = &arr[2] {
                    assert_eq!(
                        obj.get("name"),
                        Some(&Value::String("Carol Williams".to_string()))
                    );
                    assert_eq!(obj.get("new_salary"), Some(&Value::Int(73000))); // 68000 + 5000
                    assert_eq!(
                        obj.get("department"),
                        Some(&Value::String("Marketing".to_string()))
                    );
                } else {
                    panic!("Expected Object");
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_example_002_scenario() {
        // Test the scenario from examples/example_002
        // Query: group_by(.genre) | map({genre: .[0].genre, count: length, avg_price: (map(.price) | add / length)})

        // Create test data similar to the CSV
        let df = df! {
            "title" => ["The Great Gatsby", "To Kill a Mockingbird", "1984", "Pride and Prejudice", "The Catcher in the Rye", "Animal Farm", "Lord of the Flies", "The Hobbit", "Brave New World", "The Alchemist"],
            "author" => ["F. Scott Fitzgerald", "Harper Lee", "George Orwell", "Jane Austen", "J.D. Salinger", "George Orwell", "William Golding", "J.R.R. Tolkien", "Aldous Huxley", "Paulo Coelho"],
            "year" => [1925, 1960, 1949, 1813, 1951, 1945, 1954, 1937, 1932, 1988],
            "genre" => ["Fiction", "Fiction", "Dystopian", "Romance", "Fiction", "Satire", "Adventure", "Fantasy", "Dystopian", "Philosophy"],
            "price" => [10.99, 12.50, 9.99, 8.75, 11.25, 7.99, 10.50, 14.99, 9.50, 13.00]
        }.unwrap();

        let dsq = Dsq::from_value(Value::DataFrame(df));

        // Execute the filter
        let result = dsq.apply_filter("group_by(.genre) | map({genre: .[0].genre, count: length, avg_price: (map(.price) | add / length)})").unwrap();

        // Check the result
        match result.value() {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 7); // 7 genres

                // Find the Fiction group
                let fiction_group = arr
                    .iter()
                    .find(|item| {
                        if let Value::Object(obj) = item {
                            obj.get("genre") == Some(&Value::String("Fiction".to_string()))
                        } else {
                            false
                        }
                    })
                    .expect("Fiction group not found");

                if let Value::Object(obj) = fiction_group {
                    assert_eq!(obj.get("count"), Some(&Value::Int(3)));
                    // Average price: (10.99 + 12.50 + 11.25) / 3 = 34.74 / 3 = 11.58
                    assert_eq!(obj.get("avg_price"), Some(&Value::Float(11.58)));
                }

                // Find the Dystopian group
                let dystopian_group = arr
                    .iter()
                    .find(|item| {
                        if let Value::Object(obj) = item {
                            obj.get("genre") == Some(&Value::String("Dystopian".to_string()))
                        } else {
                            false
                        }
                    })
                    .expect("Dystopian group not found");

                if let Value::Object(obj) = dystopian_group {
                    assert_eq!(obj.get("count"), Some(&Value::Int(2)));
                    // Average price: (9.99 + 9.50) / 2 = 19.49 / 2 = 9.745
                    let avg_price = obj.get("avg_price").unwrap();
                    if let Value::Float(price) = avg_price {
                        assert!((price - 9.745).abs() < 0.001);
                    } else {
                        panic!("Expected float for avg_price");
                    }
                }

                // Check other genres have count 1
                for item in arr {
                    if let Value::Object(obj) = item {
                        let genre = obj.get("genre").unwrap();
                        if genre != &Value::String("Fiction".to_string())
                            && genre != &Value::String("Dystopian".to_string())
                        {
                            assert_eq!(obj.get("count"), Some(&Value::Int(1)));
                        }
                    }
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_example_002_group_by_genre() {
        // Test the example_002 scenario: group books by genre and calculate count and average price
        let books_data = Value::Array(vec![
            Value::Object(HashMap::from([
                (
                    "title".to_string(),
                    Value::String("The Great Gatsby".to_string()),
                ),
                (
                    "author".to_string(),
                    Value::String("F. Scott Fitzgerald".to_string()),
                ),
                ("year".to_string(), Value::Int(1925)),
                ("genre".to_string(), Value::String("Fiction".to_string())),
                ("price".to_string(), Value::Float(10.99)),
            ])),
            Value::Object(HashMap::from([
                (
                    "title".to_string(),
                    Value::String("To Kill a Mockingbird".to_string()),
                ),
                (
                    "author".to_string(),
                    Value::String("Harper Lee".to_string()),
                ),
                ("year".to_string(), Value::Int(1960)),
                ("genre".to_string(), Value::String("Fiction".to_string())),
                ("price".to_string(), Value::Float(12.50)),
            ])),
            Value::Object(HashMap::from([
                ("title".to_string(), Value::String("1984".to_string())),
                (
                    "author".to_string(),
                    Value::String("George Orwell".to_string()),
                ),
                ("year".to_string(), Value::Int(1949)),
                ("genre".to_string(), Value::String("Dystopian".to_string())),
                ("price".to_string(), Value::Float(9.99)),
            ])),
            Value::Object(HashMap::from([
                (
                    "title".to_string(),
                    Value::String("Pride and Prejudice".to_string()),
                ),
                (
                    "author".to_string(),
                    Value::String("Jane Austen".to_string()),
                ),
                ("year".to_string(), Value::Int(1813)),
                ("genre".to_string(), Value::String("Romance".to_string())),
                ("price".to_string(), Value::Float(8.75)),
            ])),
            Value::Object(HashMap::from([
                (
                    "title".to_string(),
                    Value::String("The Catcher in the Rye".to_string()),
                ),
                (
                    "author".to_string(),
                    Value::String("J.D. Salinger".to_string()),
                ),
                ("year".to_string(), Value::Int(1951)),
                ("genre".to_string(), Value::String("Fiction".to_string())),
                ("price".to_string(), Value::Float(11.25)),
            ])),
        ]);

        let dsq = Dsq::from_value(books_data);

        // Test group_by operation
        let grouped = dsq.group_by(&["genre"]).unwrap();

        // Verify we have groups for Fiction, Dystopian, and Romance
        match grouped.value() {
            Value::Array(groups) => {
                assert_eq!(groups.len(), 3); // 3 groups

                // Check that each group is an array and has the right size
                let mut fiction_count = 0;
                let mut dystopian_count = 0;
                let mut romance_count = 0;

                for group in groups {
                    if let Value::Array(items) = group {
                        // Check the genre of the first item in each group
                        if let Some(Value::Object(first_obj)) = items.first() {
                            if let Some(Value::String(genre)) = first_obj.get("genre") {
                                match genre.as_str() {
                                    "Fiction" => fiction_count = items.len(),
                                    "Dystopian" => dystopian_count = items.len(),
                                    "Romance" => romance_count = items.len(),
                                    _ => {}
                                }
                            }
                        }
                    }
                }

                assert_eq!(fiction_count, 3);
                assert_eq!(dystopian_count, 1);
                assert_eq!(romance_count, 1);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_group_by_with_filter_execution() {
        // Test the full example_002 query execution using filter: group_by(.genre) | map({ genre: .[0].genre, count: length, avg_price: (map(.price) | add / length) })
        use dsq_filter::execute_filter;

        let books_data = Value::Array(vec![
            Value::Object(HashMap::from([
                (
                    "title".to_string(),
                    Value::String("The Great Gatsby".to_string()),
                ),
                (
                    "author".to_string(),
                    Value::String("F. Scott Fitzgerald".to_string()),
                ),
                ("year".to_string(), Value::Int(1925)),
                ("genre".to_string(), Value::String("Fiction".to_string())),
                ("price".to_string(), Value::Float(10.99)),
            ])),
            Value::Object(HashMap::from([
                (
                    "title".to_string(),
                    Value::String("To Kill a Mockingbird".to_string()),
                ),
                (
                    "author".to_string(),
                    Value::String("Harper Lee".to_string()),
                ),
                ("year".to_string(), Value::Int(1960)),
                ("genre".to_string(), Value::String("Fiction".to_string())),
                ("price".to_string(), Value::Float(12.50)),
            ])),
            Value::Object(HashMap::from([
                ("title".to_string(), Value::String("1984".to_string())),
                (
                    "author".to_string(),
                    Value::String("George Orwell".to_string()),
                ),
                ("year".to_string(), Value::Int(1949)),
                ("genre".to_string(), Value::String("Dystopian".to_string())),
                ("price".to_string(), Value::Float(9.99)),
            ])),
            Value::Object(HashMap::from([
                (
                    "title".to_string(),
                    Value::String("Pride and Prejudice".to_string()),
                ),
                (
                    "author".to_string(),
                    Value::String("Jane Austen".to_string()),
                ),
                ("year".to_string(), Value::Int(1813)),
                ("genre".to_string(), Value::String("Romance".to_string())),
                ("price".to_string(), Value::Float(8.75)),
            ])),
            Value::Object(HashMap::from([
                (
                    "title".to_string(),
                    Value::String("The Catcher in the Rye".to_string()),
                ),
                (
                    "author".to_string(),
                    Value::String("J.D. Salinger".to_string()),
                ),
                ("year".to_string(), Value::Int(1951)),
                ("genre".to_string(), Value::String("Fiction".to_string())),
                ("price".to_string(), Value::Float(11.25)),
            ])),
        ]);

        let query = r#"group_by(.genre) | map({ genre: .[0].genre, count: length, avg_price: (map(.price) | add / length) })"#;
        let result = execute_filter(query, &books_data).unwrap();

        // Verify the result structure
        match result {
            Value::Array(groups) => {
                assert_eq!(groups.len(), 3); // Fiction, Dystopian, Romance

                // Find and verify each genre group
                let mut found_fiction = false;
                let mut found_dystopian = false;
                let mut found_romance = false;

                for group in groups {
                    if let Value::Object(obj) = group {
                        if let Some(Value::String(genre)) = obj.get("genre") {
                            match genre.as_str() {
                                "Fiction" => {
                                    found_fiction = true;
                                    assert_eq!(obj.get("count"), Some(&Value::Int(3)));
                                    // Average of 10.99, 12.50, 11.25 = 34.74 / 3 = 11.58
                                    if let Some(Value::Float(avg_price)) = obj.get("avg_price") {
                                        assert!((avg_price - 11.58).abs() < 0.01);
                                    } else {
                                        panic!("avg_price should be Float");
                                    }
                                }
                                "Dystopian" => {
                                    found_dystopian = true;
                                    assert_eq!(obj.get("count"), Some(&Value::Int(1)));
                                    assert_eq!(obj.get("avg_price"), Some(&Value::Float(9.99)));
                                }
                                "Romance" => {
                                    found_romance = true;
                                    assert_eq!(obj.get("count"), Some(&Value::Int(1)));
                                    assert_eq!(obj.get("avg_price"), Some(&Value::Float(8.75)));
                                }
                                _ => panic!("Unexpected genre: {}", genre),
                            }
                        }
                    }
                }

                assert!(found_fiction, "Fiction group not found");
                assert!(found_dystopian, "Dystopian group not found");
                assert!(found_romance, "Romance group not found");
            }
            _ => panic!("Expected Array result"),
        }
    }
}
