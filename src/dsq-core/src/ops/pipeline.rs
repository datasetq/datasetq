use super::Operation;
use crate::error::Result;
use crate::ops::aggregate::{group_by, group_by_agg, AggregationFunction};
use crate::ops::basic::{filter_values, select_columns, sort_by_columns, SortOptions};
use crate::ops::join::{join, JoinKeys, JoinOptions};
use crate::Value;

/// A pipeline of operations that can be applied sequentially
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::{OperationPipeline, basic::SortOptions};
/// use dsq_core::value::Value;
///
/// let mut pipeline = OperationPipeline::new();
/// pipeline
///     .select(vec!["name".to_string(), "age".to_string()])
///     .sort(vec![SortOptions::desc("age")])
///     .head(10);
///
/// let result = pipeline.execute(&input_value).unwrap();
/// ```
pub struct OperationPipeline {
    operations: Vec<Box<dyn Operation + Send + Sync>>,
}

impl OperationPipeline {
    /// Create a new empty operation pipeline
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Add a generic operation to the pipeline
    pub fn add_operation(mut self, op: Box<dyn Operation + Send + Sync>) -> Self {
        self.operations.push(op);
        self
    }

    /// Add a select columns operation
    pub fn select(self, columns: Vec<String>) -> Self {
        self.add_operation(Box::new(SelectOperation { columns }))
    }

    /// Add a filter operation
    pub fn filter<F>(self, predicate: F) -> Self
    where
        F: Fn(&Value) -> Result<bool> + Send + Sync + 'static,
    {
        self.add_operation(Box::new(FilterOperation {
            predicate: Box::new(predicate),
        }))
    }

    /// Add a sort operation
    pub fn sort(self, options: Vec<SortOptions>) -> Self {
        self.add_operation(Box::new(SortOperation { options }))
    }

    /// Add a head operation (take first N rows)
    pub fn head(self, n: usize) -> Self {
        self.add_operation(Box::new(HeadOperation { n }))
    }

    /// Add a tail operation (take last N rows)
    pub fn tail(self, n: usize) -> Self {
        self.add_operation(Box::new(TailOperation { n }))
    }

    /// Add a group by operation
    pub fn group_by(self, columns: Vec<String>) -> Self {
        self.add_operation(Box::new(GroupByOperation { columns }))
    }

    /// Add an aggregation operation
    pub fn aggregate(
        self,
        group_columns: Vec<String>,
        agg_functions: Vec<AggregationFunction>,
    ) -> Self {
        self.add_operation(Box::new(AggregateOperation {
            group_columns,
            agg_functions,
        }))
    }

    /// Add a join operation
    pub fn join(self, right: Value, keys: JoinKeys, options: JoinOptions) -> Self {
        self.add_operation(Box::new(JoinOperation {
            right,
            keys,
            options,
        }))
    }

    /// Execute the pipeline on a value
    pub fn execute(&self, mut value: Value) -> Result<Value> {
        for operation in &self.operations {
            value = operation.apply(&value)?;
        }
        Ok(value)
    }

    /// Execute the pipeline on a value by reference, avoiding clones where possible
    /// This is more efficient when the caller doesn't need to keep the original value
    pub fn execute_mut(&self, value: &mut Value) -> Result<()> {
        for operation in &self.operations {
            *value = operation.apply(value)?;
        }
        Ok(())
    }

    /// Get the number of operations in the pipeline
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if the pipeline is empty
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Get descriptions of all operations in the pipeline
    pub fn describe(&self) -> Vec<String> {
        self.operations.iter().map(|op| op.description()).collect()
    }
}

impl Default for OperationPipeline {
    fn default() -> Self {
        Self::new()
    }
}

// Concrete operation implementations for the pipeline

struct SelectOperation {
    columns: Vec<String>,
}

impl Operation for SelectOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        select_columns(value, &self.columns)
    }

    fn description(&self) -> String {
        format!("select columns: {}", self.columns.join(", "))
    }
}

pub struct FilterOperation {
    pub predicate: Box<dyn Fn(&Value) -> Result<bool> + Send + Sync>,
}

impl std::fmt::Debug for FilterOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterOperation").finish()
    }
}

impl Operation for FilterOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        filter_values(value, &self.predicate)
    }

    fn description(&self) -> String {
        "filter with custom predicate".to_string()
    }
}

struct SortOperation {
    options: Vec<SortOptions>,
}

impl Operation for SortOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        sort_by_columns(value, &self.options)
    }

    fn description(&self) -> String {
        let sort_desc: Vec<String> = self
            .options
            .iter()
            .map(|opt| {
                format!(
                    "{} {}",
                    opt.column,
                    if opt.descending { "desc" } else { "asc" }
                )
            })
            .collect();
        format!("sort by: {}", sort_desc.join(", "))
    }
}

struct HeadOperation {
    n: usize,
}

impl Operation for HeadOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        crate::ops::basic::head(value, self.n)
    }

    fn description(&self) -> String {
        format!("head {}", self.n)
    }
}

struct TailOperation {
    n: usize,
}

impl Operation for TailOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        crate::ops::basic::tail(value, self.n)
    }

    fn description(&self) -> String {
        format!("tail {}", self.n)
    }
}

struct GroupByOperation {
    columns: Vec<String>,
}

impl Operation for GroupByOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        group_by(value, &self.columns)
    }

    fn description(&self) -> String {
        format!("group by: {}", self.columns.join(", "))
    }
}

struct AggregateOperation {
    group_columns: Vec<String>,
    agg_functions: Vec<AggregationFunction>,
}

impl Operation for AggregateOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        group_by_agg(value, &self.group_columns, &self.agg_functions)
    }

    fn description(&self) -> String {
        let agg_desc: Vec<String> = self
            .agg_functions
            .iter()
            .map(|f| f.output_column_name())
            .collect();
        format!(
            "aggregate by {} with functions: {}",
            self.group_columns.join(", "),
            agg_desc.join(", ")
        )
    }
}

struct JoinOperation {
    right: Value,
    keys: JoinKeys,
    options: JoinOptions,
}

impl Operation for JoinOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        join(value, &self.right, &self.keys, &self.options)
    }

    fn description(&self) -> String {
        format!(
            "{} join on: {}",
            self.options.join_type.as_str(),
            self.keys.left_columns().join(", ")
        )
    }
}

/// Apply a series of operations to a value
///
/// This is a convenience function that creates a temporary pipeline
/// and executes it.
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::{apply_operations, basic::SortOptions};
/// use dsq_core::value::Value;
///
/// let operations = vec![
///     Box::new(SelectOperation { columns: vec!["name".to_string()] }),
///     Box::new(SortOperation { options: vec![SortOptions::asc("name")] }),
/// ];
///
/// let result = apply_operations(&input_value, operations).unwrap();
/// ```
pub fn apply_operations(
    value: &Value,
    operations: Vec<Box<dyn Operation + Send + Sync>>,
) -> Result<Value> {
    let mut pipeline = OperationPipeline::new();
    for op in operations {
        pipeline = pipeline.add_operation(op);
    }
    // Use execute which already clones internally if needed
    pipeline.execute(value.clone())
}

/// Apply a series of operations to an owned value (consumes the value)
///
/// More efficient than apply_operations when you don't need to keep the original value.
pub fn apply_operations_owned(
    mut value: Value,
    operations: Vec<Box<dyn Operation + Send + Sync>>,
) -> Result<Value> {
    let mut pipeline = OperationPipeline::new();
    for op in operations {
        pipeline = pipeline.add_operation(op);
    }
    pipeline.execute_mut(&mut value)?;
    Ok(value)
}

/// Apply a series of operations to a value in place
///
/// This is more efficient than apply_operations when the caller doesn't need
/// to preserve the original value.
pub fn apply_operations_mut(
    value: &mut Value,
    operations: Vec<Box<dyn Operation + Send + Sync>>,
) -> Result<()> {
    let mut pipeline = OperationPipeline::new();
    for op in operations {
        pipeline = pipeline.add_operation(op);
    }
    pipeline.execute_mut(value)
}
