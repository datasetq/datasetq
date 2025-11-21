//! Lazy pipeline operations that work with LazyFrame for optimal performance
//!
//! This module provides a pipeline that operates exclusively on LazyFrame,
//! allowing Polars to optimize the entire query plan before execution.

use crate::Value;
use crate::error::{Error, Result};
use polars::prelude::*;

/// A pipeline that operates on LazyFrame for optimal query optimization
///
/// Unlike OperationPipeline which can work with any Value type,
/// LazyPipeline only works with LazyFrame and converts at the end.
/// This allows Polars to:
/// - Apply predicate pushdown
/// - Apply projection pushdown
/// - Optimize join strategies
/// - Minimize memory usage through streaming
pub struct LazyPipeline {
    operations: Vec<Box<dyn Fn(LazyFrame) -> Result<LazyFrame> + Send + Sync>>,
}

impl LazyPipeline {
    /// Create a new empty lazy pipeline
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Add a select columns operation
    pub fn select(mut self, columns: Vec<String>) -> Self {
        self.operations.push(Box::new(move |lf: LazyFrame| {
            let cols: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
            Ok(lf.select(&cols))
        }));
        self
    }

    /// Add a filter operation using Polars expression
    pub fn filter(mut self, predicate: Expr) -> Self {
        self.operations.push(Box::new(move |lf: LazyFrame| {
            Ok(lf.filter(predicate.clone()))
        }));
        self
    }

    /// Add a sort operation
    pub fn sort(mut self, column: String, descending: bool) -> Self {
        self.operations.push(Box::new(move |lf: LazyFrame| {
            let options = SortOptions {
                descending,
                nulls_last: false,
                multithreaded: true,
                maintain_order: false,
            };
            Ok(lf.sort(&column, options))
        }));
        self
    }

    /// Add a head operation (take first N rows)
    pub fn head(mut self, n: u32) -> Self {
        self.operations
            .push(Box::new(move |lf: LazyFrame| Ok(lf.limit(n))));
        self
    }

    /// Add a tail operation (take last N rows)
    pub fn tail(mut self, n: u32) -> Self {
        self.operations
            .push(Box::new(move |lf: LazyFrame| Ok(lf.tail(n))));
        self
    }

    /// Add a group by operation
    pub fn group_by(mut self, columns: Vec<String>) -> Self {
        self.operations.push(Box::new(move |lf: LazyFrame| {
            let group_cols: Vec<Expr> = columns.iter().map(|c| col(c)).collect();
            Ok(lf.group_by(&group_cols).agg(&[]))
        }));
        self
    }

    /// Add a with_column operation (add or modify a column)
    pub fn with_column(mut self, name: String, expr: Expr) -> Self {
        self.operations.push(Box::new(move |lf: LazyFrame| {
            Ok(lf.with_column(expr.clone().alias(&name)))
        }));
        self
    }

    /// Add a drop_columns operation
    pub fn drop_columns(mut self, columns: Vec<String>) -> Self {
        self.operations.push(Box::new(move |lf: LazyFrame| {
            let cols_to_keep: Vec<Expr> = lf
                .schema()
                .map_err(|e| Error::operation(format!("Schema error: {}", e)))?
                .iter_names()
                .filter(|name| !columns.contains(&name.to_string()))
                .map(|name| col(name))
                .collect();
            Ok(lf.select(&cols_to_keep))
        }));
        self
    }

    /// Add a rename operation
    pub fn rename(mut self, old_name: String, new_name: String) -> Self {
        self.operations.push(Box::new(move |lf: LazyFrame| {
            Ok(lf.rename([&old_name], [&new_name]))
        }));
        self
    }

    /// Execute the pipeline and return a LazyFrame
    ///
    /// This doesn't actually execute the query - it builds the optimized plan.
    /// Call collect() on the result to materialize the data.
    pub fn build(&self, input: LazyFrame) -> Result<LazyFrame> {
        let mut lf = input;
        for operation in &self.operations {
            lf = operation(lf)?;
        }
        Ok(lf)
    }

    /// Execute the pipeline and collect into a DataFrame
    pub fn execute(&self, input: LazyFrame) -> Result<DataFrame> {
        let lf = self.build(input)?;
        lf.collect()
            .map_err(|e| Error::operation(format!("Polars error: {e}")))
    }

    /// Execute the pipeline and return as Value
    pub fn execute_as_value(&self, input: Value) -> Result<Value> {
        match input {
            Value::LazyFrame(lf) => {
                let result_lf = self.build(*lf)?;
                Ok(Value::lazy_frame(result_lf))
            }
            Value::DataFrame(df) => {
                let lf = df.lazy();
                let result_lf = self.build(lf)?;
                Ok(Value::lazy_frame(result_lf))
            }
            _ => Err(Error::operation(
                "LazyPipeline requires DataFrame or LazyFrame input",
            )),
        }
    }

    /// Execute the pipeline and collect as Value::DataFrame
    pub fn execute_and_collect(&self, input: Value) -> Result<Value> {
        match input {
            Value::LazyFrame(lf) => {
                let df = self.execute(*lf)?;
                Ok(Value::dataframe(df))
            }
            Value::DataFrame(df) => {
                let lf = df.lazy();
                let result_df = self.execute(lf)?;
                Ok(Value::dataframe(result_df))
            }
            _ => Err(Error::operation(
                "LazyPipeline requires DataFrame or LazyFrame input",
            )),
        }
    }

    /// Get the number of operations in the pipeline
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if the pipeline is empty
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

impl Default for LazyPipeline {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn test_lazy_pipeline_basic() {
        let df = DataFrame::new(vec![
            Series::new("id", vec![1i64, 2, 3, 4, 5]),
            Series::new("value", vec![10i64, 20, 30, 40, 50]),
        ])
        .unwrap();

        let pipeline = LazyPipeline::new()
            .filter(col("value").gt(lit(20)))
            .sort("value".to_string(), false)
            .head(2);

        let result = pipeline.execute(df.lazy()).unwrap();
        assert_eq!(result.height(), 2);
        assert_eq!(result.width(), 2);
    }

    #[test]
    fn test_lazy_pipeline_select() {
        let df = DataFrame::new(vec![
            Series::new("id", vec![1i64, 2, 3]),
            Series::new("value", vec![10i64, 20, 30]),
            Series::new("extra", vec!["a", "b", "c"]),
        ])
        .unwrap();

        let pipeline = LazyPipeline::new().select(vec!["id".to_string(), "value".to_string()]);

        let result = pipeline.execute(df.lazy()).unwrap();
        assert_eq!(result.width(), 2);
        assert!(result.get_column_names().contains(&"id"));
        assert!(result.get_column_names().contains(&"value"));
        assert!(!result.get_column_names().contains(&"extra"));
    }

    #[test]
    fn test_lazy_pipeline_with_value() {
        let df = DataFrame::new(vec![
            Series::new("id", vec![1i64, 2, 3]),
            Series::new("value", vec![10i64, 20, 30]),
        ])
        .unwrap();

        let input = Value::dataframe(df);
        let pipeline = LazyPipeline::new().filter(col("value").gt(lit(10))).head(2);

        let result = pipeline.execute_and_collect(input).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }
}
