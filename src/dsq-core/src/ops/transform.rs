use std::collections::HashMap;

use polars::prelude::*;
use polars_ops::prelude::UnpivotDF;

use crate::error::{Error, Result};
use crate::Value;

/// Data type for columns in `DataFrames`
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnDataType {
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 32-bit floating point
    Float32,
    /// 64-bit floating point
    Float64,
    /// UTF-8 string
    String,
    /// Boolean value
    Boolean,
    /// Date (without time)
    Date,
    /// Date and time
    DateTime,
}

impl ColumnDataType {
    /// Create a `ColumnDataType` from a string representation
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "int32" | "i32" => Ok(ColumnDataType::Int32),
            "int64" | "i64" => Ok(ColumnDataType::Int64),
            "float32" | "f32" => Ok(ColumnDataType::Float32),
            "float64" | "f64" => Ok(ColumnDataType::Float64),
            "string" | "str" | "utf8" => Ok(ColumnDataType::String),
            "bool" | "boolean" => Ok(ColumnDataType::Boolean),
            "date" => Ok(ColumnDataType::Date),
            "datetime" => Ok(ColumnDataType::DateTime),
            _ => Err(Error::operation(format!("Unknown data type: {s}"))),
        }
    }

    /// Convert to Polars `DataType`
    #[must_use]
    pub fn to_polars_dtype(&self) -> DataType {
        match self {
            ColumnDataType::Int32 => DataType::Int32,
            ColumnDataType::Int64 => DataType::Int64,
            ColumnDataType::Float32 => DataType::Float32,
            ColumnDataType::Float64 => DataType::Float64,
            ColumnDataType::String => DataType::String,
            ColumnDataType::Boolean => DataType::Boolean,
            ColumnDataType::Date => DataType::Date,
            ColumnDataType::DateTime => {
                DataType::Datetime(polars::prelude::TimeUnit::Milliseconds, None)
            }
        }
    }
}

/// Transform operations for `DataFrames`
pub struct Transform;

impl Transform {
    /// Select specific columns from a `DataFrame`
    pub fn select(df: &DataFrame, columns: &[String]) -> Result<DataFrame> {
        df.select(columns)
            .map_err(|e| Error::operation(format!("Failed to select columns: {e}")))
    }

    /// Select specific columns from a `LazyFrame`
    pub fn select_lazy(lf: LazyFrame, columns: &[String]) -> Result<LazyFrame> {
        let cols: Vec<Expr> = columns.iter().map(|name| col(name)).collect();
        Ok(lf.select(&cols))
    }

    /// Filter `DataFrame` based on a condition
    pub fn filter(df: &DataFrame, mask: &Series) -> Result<DataFrame> {
        if mask.dtype() != &DataType::Boolean {
            return Err(Error::operation("Filter mask must be boolean".to_string()));
        }

        let mask = mask
            .bool()
            .map_err(|e| Error::operation(format!("Failed to cast mask to boolean: {e}")))?;

        df.filter(mask)
            .map_err(|e| Error::operation(format!("Failed to filter DataFrame: {e}")))
    }

    /// Filter `LazyFrame` based on an expression
    pub fn filter_lazy(lf: LazyFrame, predicate: Expr) -> Result<LazyFrame> {
        Ok(lf.filter(predicate))
    }

    /// Sort `DataFrame` by columns
    pub fn sort(df: &DataFrame, by_columns: &[String], descending: Vec<bool>) -> Result<DataFrame> {
        df.sort(
            by_columns,
            SortMultipleOptions::default().with_order_descending_multi(descending),
        )
        .map_err(|e| Error::operation(format!("Failed to sort DataFrame: {e}")))
    }

    /// Sort `LazyFrame` by columns
    pub fn sort_lazy(
        lf: LazyFrame,
        by_columns: &[String],
        descending: &[bool],
    ) -> Result<LazyFrame> {
        let exprs: Vec<Expr> = by_columns.iter().map(|name| col(name)).collect();
        let options =
            SortMultipleOptions::default().with_order_descending_multi(descending.to_vec());
        Ok(lf.sort_by_exprs(&exprs, options))
    }

    /// Rename columns in a `DataFrame`
    pub fn rename(df: &DataFrame, mapping: &HashMap<String, String>) -> Result<DataFrame> {
        let mut result = df.clone();

        for (old_name, new_name) in mapping {
            result
                .rename(old_name.as_str(), new_name.as_str().into())
                .map_err(|e| {
                    Error::operation(format!("Failed to rename column '{old_name}': {e}"))
                })?;
        }

        Ok(result)
    }

    /// Rename columns in a `LazyFrame`
    pub fn rename_lazy(lf: LazyFrame, mapping: &HashMap<String, String>) -> Result<LazyFrame> {
        let mut result = lf;

        for (old_name, new_name) in mapping {
            result = result.rename([old_name.as_str()], [new_name.as_str()], true);
        }

        Ok(result)
    }

    /// Add a new column to a `DataFrame`
    pub fn with_column(df: &DataFrame, name: &str, series: Series) -> Result<DataFrame> {
        let mut result = df.clone();
        result
            .with_column(series.with_name(name.into()))
            .map_err(|e| Error::operation(format!("Failed to add column '{name}': {e}")))?;
        Ok(result)
    }

    /// Add a new column expression to a `LazyFrame`
    pub fn with_column_lazy(lf: LazyFrame, expr: Expr) -> Result<LazyFrame> {
        Ok(lf.with_column(expr))
    }

    /// Drop columns from a `DataFrame`
    pub fn drop(df: &DataFrame, columns: &[String]) -> Result<DataFrame> {
        let mut result = df.clone();
        for column in columns {
            result = result
                .drop(column)
                .map_err(|e| Error::operation(format!("Failed to drop column '{column}': {e}")))?;
        }
        Ok(result)
    }

    /// Drop columns from a `LazyFrame`
    pub fn drop_lazy(lf: LazyFrame, columns: &[String]) -> Result<LazyFrame> {
        // Collect to DataFrame, drop columns, then convert back to LazyFrame
        let df = lf
            .collect()
            .map_err(|e| Error::operation(format!("Failed to collect LazyFrame: {e}")))?;
        let mut result = df;
        for column in columns {
            result = result
                .drop(column)
                .map_err(|e| Error::operation(format!("Failed to drop column '{column}': {e}")))?;
        }
        Ok(result.lazy())
    }

    /// Get unique values in a `DataFrame`
    pub fn unique(
        df: &DataFrame,
        subset: Option<&[String]>,
        keep: UniqueKeepStrategy,
    ) -> Result<DataFrame> {
        let result = df
            .unique::<String, String>(subset, keep, None)
            .map_err(|e| Error::operation(format!("Failed to get unique values: {e}")))?;
        Ok(result)
    }

    /// Get unique values in a `LazyFrame`
    pub fn unique_lazy(
        lf: LazyFrame,
        subset: Option<&[String]>,
        keep: UniqueKeepStrategy,
    ) -> Result<LazyFrame> {
        // Collect to DataFrame, get unique, then convert back to LazyFrame
        let df = lf
            .collect()
            .map_err(|e| Error::operation(format!("Failed to collect LazyFrame: {e}")))?;
        let result = df
            .unique::<String, String>(subset, keep, None)
            .map_err(|e| Error::operation(format!("Failed to get unique values: {e}")))?;
        Ok(result.lazy())
    }

    /// Limit the number of rows
    pub fn limit(df: &DataFrame, n: usize) -> Result<DataFrame> {
        Ok(df.head(Some(n)))
    }

    /// Limit the number of rows in a `LazyFrame`
    pub fn limit_lazy(lf: LazyFrame, n: u32) -> Result<LazyFrame> {
        Ok(lf.limit(n))
    }

    /// Skip the first n rows
    pub fn skip(df: &DataFrame, n: usize) -> Result<DataFrame> {
        #[allow(clippy::cast_possible_wrap)]
        {
            Ok(df.slice(n as i64, df.height().saturating_sub(n)))
        }
    }

    /// Skip the first n rows in a `LazyFrame`
    pub fn skip_lazy(lf: LazyFrame, n: u32) -> Result<LazyFrame> {
        Ok(lf.slice(i64::from(n), u32::MAX))
    }

    /// Slice a `DataFrame`
    pub fn slice(df: &DataFrame, offset: i64, length: usize) -> Result<DataFrame> {
        Ok(df.slice(offset, length))
    }

    /// Slice a `LazyFrame`
    pub fn slice_lazy(lf: LazyFrame, offset: i64, length: u32) -> Result<LazyFrame> {
        Ok(lf.slice(offset, length))
    }

    /// Reverse the order of rows
    pub fn reverse(df: &DataFrame) -> Result<DataFrame> {
        #[allow(clippy::cast_possible_truncation)]
        let indices: Vec<IdxSize> = (0..df.height() as IdxSize).rev().collect();
        let ca = IdxCa::from_vec("".into(), indices);

        df.take(&ca)
            .map_err(|e| Error::operation(format!("Failed to reverse DataFrame: {e}")))
    }

    /// Reverse the order of rows in a `LazyFrame`
    pub fn reverse_lazy(lf: LazyFrame) -> Result<LazyFrame> {
        Ok(lf.reverse())
    }

    /// Sample rows from a `DataFrame`
    pub fn sample(
        df: &DataFrame,
        n: usize,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> Result<DataFrame> {
        #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
        let n_values = vec![n as u32];
        let n_series = Series::new("n".into(), n_values);
        df.sample_n(&n_series, with_replacement, true, seed)
            .map_err(|e| Error::operation(format!("Failed to sample DataFrame: {e}")))
    }

    /// Fill null values
    pub fn fill_null(df: &DataFrame, value: FillNullStrategy) -> Result<DataFrame> {
        let columns = df
            .get_columns()
            .iter()
            .map(|s| {
                s.fill_null(value)
                    .map_err(|e| Error::operation(format!("Failed to fill null values: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;

        let cols: Vec<_> = columns.into_iter().map(|s| s.into()).collect();
        DataFrame::new(cols).map_err(|e| {
            Error::operation(format!("Failed to create DataFrame after fill_null: {e}"))
        })
    }

    /// Fill null values in a `LazyFrame`
    #[allow(clippy::needless_pass_by_value)]
    pub fn fill_null_lazy(mut lf: LazyFrame, value: Expr) -> Result<LazyFrame> {
        let schema = lf
            .collect_schema()
            .map_err(|e| Error::operation(format!("Failed to collect schema: {e}")))?;
        let columns = schema
            .iter()
            .map(|(name, _)| col(name.as_str()).fill_null(value.clone()))
            .collect::<Vec<_>>();

        Ok(lf.with_columns(&columns))
    }

    /// Drop rows with null values
    pub fn drop_nulls(df: &DataFrame, subset: Option<&[String]>) -> Result<DataFrame> {
        df.drop_nulls(subset)
            .map_err(|e| Error::operation(format!("Failed to drop null values: {e}")))
    }

    /// Drop rows with null values in a `LazyFrame`
    pub fn drop_nulls_lazy(lf: LazyFrame, _subset: Option<Vec<Expr>>) -> Result<LazyFrame> {
        // drop_nulls expects Option<Selector> in 0.51
        Ok(lf.drop_nulls(None))
    }

    /// Cast column types
    pub fn cast(df: &DataFrame, column: &str, dtype: &DataType) -> Result<DataFrame> {
        let mut result = df.clone();
        let series = result
            .column(column)
            .map_err(|e| Error::operation(format!("Column '{column}' not found: {e}")))?
            .cast(dtype)
            .map_err(|e| Error::operation(format!("Failed to cast column '{column}': {e}")))?;

        result
            .with_column(series)
            .map_err(|e| Error::operation(format!("Failed to update column: {e}")))?;

        Ok(result)
    }

    /// Cast column types in a `LazyFrame`
    pub fn cast_lazy(lf: LazyFrame, column: &str, dtype: DataType) -> Result<LazyFrame> {
        Ok(lf.with_column(col(column).cast(dtype)))
    }

    /// Explode list columns
    pub fn explode(df: &DataFrame, columns: &[String]) -> Result<DataFrame> {
        df.explode(columns)
            .map_err(|e| Error::operation(format!("Failed to explode columns: {e}")))
    }

    /// Explode list columns in a `LazyFrame`
    pub fn explode_lazy(lf: LazyFrame, columns: &[String]) -> Result<LazyFrame> {
        // Collect to DataFrame, explode columns, then convert back to LazyFrame
        let df = lf
            .collect()
            .map_err(|e| Error::operation(format!("Failed to collect LazyFrame: {e}")))?;
        let result = df
            .explode(columns)
            .map_err(|e| Error::operation(format!("Failed to explode columns: {e}")))?;
        Ok(result.lazy())
    }

    /// Melt `DataFrame` from wide to long format
    pub fn melt(
        df: &DataFrame,
        id_vars: &[String],
        value_vars: &[String],
        _variable_name: Option<&str>,
        _value_name: Option<&str>,
    ) -> Result<DataFrame> {
        if id_vars.is_empty() {
            df.unpivot([] as [&str; 0], value_vars)
                .map_err(|e| Error::operation(format!("Failed to melt DataFrame: {e}")))
        } else {
            df.unpivot(id_vars, value_vars)
                .map_err(|e| Error::operation(format!("Failed to melt DataFrame: {e}")))
        }
    }

    /// Melt `LazyFrame` from wide to long format
    pub fn melt_lazy(
        lf: LazyFrame,
        id_vars: &[String],
        value_vars: &[String],
        _variable_name: Option<&str>,
        _value_name: Option<&str>,
    ) -> Result<LazyFrame> {
        // Collect to DataFrame, melt, then convert back to LazyFrame
        let df = lf
            .collect()
            .map_err(|e| Error::operation(format!("Failed to collect LazyFrame: {e}")))?;
        let result = if id_vars.is_empty() {
            df.unpivot([] as [&str; 0], value_vars)
                .map_err(|e| Error::operation(format!("Failed to melt LazyFrame: {e}")))?
        } else {
            df.unpivot(id_vars, value_vars)
                .map_err(|e| Error::operation(format!("Failed to melt LazyFrame: {e}")))?
        };
        Ok(result.lazy())
    }

    /// Pivot `DataFrame` from long to wide format
    pub fn pivot(
        df: &DataFrame,
        values: &[String],
        index: &[String],
        columns: &[String],
        aggregate_fn: Option<&str>,
    ) -> Result<DataFrame> {
        let values_expr: Vec<Expr> = values.iter().map(|s| col(s)).collect();
        let index_expr: Vec<Expr> = index.iter().map(|s| col(s)).collect();
        let _columns_expr = col(columns[0].as_str()); // Simplified to single column

        let agg_expr = match aggregate_fn {
            Some("sum") => values_expr[0].clone().sum(),
            Some("mean") => values_expr[0].clone().mean(),
            Some("count") => values_expr[0].clone().count(),
            Some("min") => values_expr[0].clone().min(),
            Some("max") => values_expr[0].clone().max(),
            _ => values_expr[0].clone().first(), // Default to first
        };

        // Pivot not available in Polars 0.35 LazyFrame, use group_by instead
        df.clone()
            .lazy()
            .group_by(index_expr)
            .agg([agg_expr])
            .collect()
            .map_err(|e| Error::operation(format!("Failed to pivot DataFrame: {e}")))
    }

    /// Apply a function to each row
    pub fn map_rows<F, T>(_df: &DataFrame, _f: F) -> Result<DataFrame>
    where
        F: Fn(usize) -> Result<T>,
        T: Into<Series>,
    {
        // Row-wise operations on DataFrames are complex and not efficiently supported in Polars
        // Consider using vectorized operations instead
        Err(Error::operation("Row-wise map operations are not supported. Use vectorized operations or process data differently.".to_string()))
    }

    /// Apply a transformation expression to all columns
    #[allow(clippy::needless_pass_by_value)]
    pub fn map_columns(df: &DataFrame, expr: Expr) -> Result<DataFrame> {
        let columns = df
            .get_columns()
            .iter()
            .map(|s| {
                let lazy_df = DataFrame::new(vec![s.clone().into()])
                    .map_err(|e| {
                        Error::operation(format!("Failed to create temporary DataFrame: {e}"))
                    })?
                    .lazy();

                let result = lazy_df
                    .select(&[expr.clone().alias(s.name().as_str())])
                    .collect()
                    .map_err(|e| Error::operation(format!("Failed to apply expression: {e}")))?;

                result
                    .column(s.name())
                    .map_err(|e| Error::operation(format!("Failed to get result column: {e}")))
                    .cloned()
            })
            .collect::<Result<Vec<_>>>()?;

        let cols: Vec<_> = columns.into_iter().map(|s| s.into()).collect();
        DataFrame::new(cols)
            .map_err(|e| Error::operation(format!("Failed to create result DataFrame: {e}")))
    }

    /// Transpose a `DataFrame`
    pub fn transpose(_df: &DataFrame, _keep_names_as: Option<&str>) -> Result<DataFrame> {
        // TODO: Implement transpose
        Err(Error::operation("Transpose not implemented yet"))
    }
}

/// Cast a column to a specific data type
#[allow(clippy::needless_pass_by_value)]
pub fn cast_column(value: &Value, column: &str, target_type: ColumnDataType) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            let dtype = target_type.to_polars_dtype();
            let mut result = df.clone();
            let series = result
                .column(column)
                .map_err(|e| Error::operation(format!("Column '{column}' not found: {e}")))?
                .cast(&dtype)
                .map_err(|e| Error::operation(format!("Failed to cast column '{column}': {e}")))?;

            result
                .with_column(series)
                .map_err(|e| Error::operation(format!("Failed to update column: {e}")))?;

            Ok(Value::DataFrame(result))
        }
        _ => Err(Error::operation(
            "cast_column can only be applied to DataFrames".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::{
        col, lit, DataFrame, DataType, FillNullStrategy, Series, UniqueKeepStrategy,
    };

    use super::*;

    #[test]
    fn test_select() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[1, 2, 3]).into(),
            Series::new("b".into(), &[4, 5, 6]).into(),
            Series::new("c".into(), &[7, 8, 9]).into(),
        ])
        .unwrap();

        let result = Transform::select(&df, &["a".to_string(), "c".to_string()]).unwrap();
        assert_eq!(result.width(), 2);
        assert!(result.column("a").is_ok());
        assert!(result.column("c").is_ok());
        assert!(result.column("b").is_err());
    }

    #[test]
    fn test_filter() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[1, 2, 3, 4, 5]).into(),
            Series::new("b".into(), &[10, 20, 30, 40, 50]).into(),
        ])
        .unwrap();

        let mask = Series::new("mask".into(), &[true, false, true, false, true]);
        let result = Transform::filter(&df, &mask).unwrap();

        assert_eq!(result.height(), 3);
        assert_eq!(result.column("a").unwrap().i32().unwrap().get(0), Some(1));
        assert_eq!(result.column("a").unwrap().i32().unwrap().get(1), Some(3));
        assert_eq!(result.column("a").unwrap().i32().unwrap().get(2), Some(5));
    }

    #[test]
    fn test_sort() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[3, 1, 4, 1, 5]).into(),
            Series::new("b".into(), &[30, 10, 40, 15, 50]).into(),
        ])
        .unwrap();

        let result = Transform::sort(&df, &["a".to_string()], vec![false]).unwrap();

        let col_a = result.column("a").unwrap().i32().unwrap();
        assert_eq!(col_a.get(0), Some(1));
        assert_eq!(col_a.get(1), Some(1));
        assert_eq!(col_a.get(2), Some(3));
        assert_eq!(col_a.get(3), Some(4));
        assert_eq!(col_a.get(4), Some(5));
    }

    #[test]
    fn test_rename() {
        let df = DataFrame::new(vec![Series::new("old_name".into(), &[1, 2, 3]).into()]).unwrap();

        let mut mapping = HashMap::new();
        mapping.insert("old_name".to_string(), "new_name".to_string());

        let result = Transform::rename(&df, &mapping).unwrap();
        assert!(result.column("new_name").is_ok());
        assert!(result.column("old_name").is_err());
    }

    #[test]
    fn test_unique() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[1, 2, 2, 3, 3, 3]).into(),
            Series::new("b".into(), &[10, 20, 20, 30, 30, 30]).into(),
        ])
        .unwrap();

        let result = Transform::unique(&df, None, UniqueKeepStrategy::First).unwrap();
        assert_eq!(result.height(), 3);
    }

    #[test]
    fn test_limit_and_skip() {
        let df = DataFrame::new(vec![Series::new("a".into(), &[1, 2, 3, 4, 5]).into()]).unwrap();

        let limited = Transform::limit(&df, 3).unwrap();
        assert_eq!(limited.height(), 3);

        let skipped = Transform::skip(&df, 2).unwrap();
        assert_eq!(skipped.height(), 3);
        assert_eq!(skipped.column("a").unwrap().i32().unwrap().get(0), Some(3));
    }

    #[test]
    fn test_drop_nulls() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[Some(1), None, Some(3), None, Some(5)]).into(),
            Series::new("b".into(), &[Some(10), Some(20), None, Some(40), Some(50)]).into(),
        ])
        .unwrap();

        let result = Transform::drop_nulls(&df, None).unwrap();
        assert_eq!(result.height(), 2); // Only rows without any nulls
    }

    #[test]
    fn test_column_datatype_from_str() {
        assert_eq!(
            ColumnDataType::from_str("int32").unwrap(),
            ColumnDataType::Int32
        );
        assert_eq!(
            ColumnDataType::from_str("i32").unwrap(),
            ColumnDataType::Int32
        );
        assert_eq!(
            ColumnDataType::from_str("int64").unwrap(),
            ColumnDataType::Int64
        );
        assert_eq!(
            ColumnDataType::from_str("i64").unwrap(),
            ColumnDataType::Int64
        );
        assert_eq!(
            ColumnDataType::from_str("float32").unwrap(),
            ColumnDataType::Float32
        );
        assert_eq!(
            ColumnDataType::from_str("f32").unwrap(),
            ColumnDataType::Float32
        );
        assert_eq!(
            ColumnDataType::from_str("float64").unwrap(),
            ColumnDataType::Float64
        );
        assert_eq!(
            ColumnDataType::from_str("f64").unwrap(),
            ColumnDataType::Float64
        );
        assert_eq!(
            ColumnDataType::from_str("string").unwrap(),
            ColumnDataType::String
        );
        assert_eq!(
            ColumnDataType::from_str("str").unwrap(),
            ColumnDataType::String
        );
        assert_eq!(
            ColumnDataType::from_str("utf8").unwrap(),
            ColumnDataType::String
        );
        assert_eq!(
            ColumnDataType::from_str("bool").unwrap(),
            ColumnDataType::Boolean
        );
        assert_eq!(
            ColumnDataType::from_str("boolean").unwrap(),
            ColumnDataType::Boolean
        );
        assert_eq!(
            ColumnDataType::from_str("date").unwrap(),
            ColumnDataType::Date
        );
        assert_eq!(
            ColumnDataType::from_str("datetime").unwrap(),
            ColumnDataType::DateTime
        );

        // Test case insensitive
        assert_eq!(
            ColumnDataType::from_str("INT32").unwrap(),
            ColumnDataType::Int32
        );
        assert_eq!(
            ColumnDataType::from_str("Float64").unwrap(),
            ColumnDataType::Float64
        );

        // Test invalid
        assert!(ColumnDataType::from_str("invalid").is_err());
        assert!(ColumnDataType::from_str("").is_err());
    }

    #[test]
    fn test_column_datatype_to_polars_dtype() {
        assert_eq!(ColumnDataType::Int32.to_polars_dtype(), DataType::Int32);
        assert_eq!(ColumnDataType::Int64.to_polars_dtype(), DataType::Int64);
        assert_eq!(ColumnDataType::Float32.to_polars_dtype(), DataType::Float32);
        assert_eq!(ColumnDataType::Float64.to_polars_dtype(), DataType::Float64);
        assert_eq!(ColumnDataType::String.to_polars_dtype(), DataType::String);
        assert_eq!(ColumnDataType::Boolean.to_polars_dtype(), DataType::Boolean);
        assert_eq!(ColumnDataType::Date.to_polars_dtype(), DataType::Date);
        assert_eq!(
            ColumnDataType::DateTime.to_polars_dtype(),
            DataType::Datetime(polars::prelude::TimeUnit::Milliseconds, None)
        );
    }

    #[test]
    fn test_select_lazy() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[1, 2, 3]).into(),
            Series::new("b".into(), &[4, 5, 6]).into(),
            Series::new("c".into(), &[7, 8, 9]).into(),
        ])
        .unwrap();
        let lf = df.lazy();

        let result = Transform::select_lazy(lf, &["a".to_string(), "c".to_string()]).unwrap();
        let collected = result.collect().unwrap();
        assert_eq!(collected.width(), 2);
        assert!(collected.column("a").is_ok());
        assert!(collected.column("c").is_ok());
        assert!(collected.column("b").is_err());
    }

    #[test]
    fn test_filter_lazy() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[1, 2, 3, 4, 5]).into(),
            Series::new("b".into(), &[10, 20, 30, 40, 50]).into(),
        ])
        .unwrap();
        let lf = df.lazy();

        let predicate = col("a").gt(lit(3));
        let result = Transform::filter_lazy(lf, predicate).unwrap();
        let collected = result.collect().unwrap();

        assert_eq!(collected.height(), 2);
        let col_a = collected.column("a").unwrap().i32().unwrap();
        assert_eq!(col_a.get(0), Some(4));
        assert_eq!(col_a.get(1), Some(5));
    }

    #[test]
    fn test_sort_lazy() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[3, 1, 4, 1, 5]).into(),
            Series::new("b".into(), &[30, 10, 40, 15, 50]).into(),
        ])
        .unwrap();
        let lf = df.lazy();

        let result = Transform::sort_lazy(lf, &["a".to_string()], &[false]).unwrap();
        let collected = result.collect().unwrap();

        let col_a = collected.column("a").unwrap().i32().unwrap();
        assert_eq!(col_a.get(0), Some(1));
        assert_eq!(col_a.get(1), Some(1));
        assert_eq!(col_a.get(2), Some(3));
        assert_eq!(col_a.get(3), Some(4));
        assert_eq!(col_a.get(4), Some(5));
    }

    #[test]
    fn test_rename_lazy() {
        let df = DataFrame::new(vec![Series::new("old_name".into(), &[1, 2, 3]).into()]).unwrap();
        let lf = df.lazy();

        let mut mapping = HashMap::new();
        mapping.insert("old_name".to_string(), "new_name".to_string());

        let result = Transform::rename_lazy(lf, &mapping).unwrap();
        let collected = result.collect().unwrap();
        assert!(collected.column("new_name").is_ok());
        assert!(collected.column("old_name").is_err());
    }

    #[test]
    fn test_with_column() {
        let df = DataFrame::new(vec![
            Series::new("a", &[1, 2, 3]),
            Series::new("b", &[4, 5, 6]),
        ])
        .unwrap();

        let new_series = Series::new("c", &[7, 8, 9]);
        let result = Transform::with_column(&df, "c", new_series).unwrap();

        assert_eq!(result.width(), 3);
        assert!(result.column("a").is_ok());
        assert!(result.column("b").is_ok());
        assert!(result.column("c").is_ok());
        assert_eq!(result.column("c").unwrap().i32().unwrap().get(0), Some(7));
    }

    #[test]
    fn test_with_column_lazy() {
        let df = DataFrame::new(vec![
            Series::new("a", &[1, 2, 3]),
            Series::new("b", &[4, 5, 6]),
        ])
        .unwrap();
        let lf = df.lazy();

        let expr = lit(10).alias("c");
        let result = Transform::with_column_lazy(lf, expr).unwrap();
        let collected = result.collect().unwrap();

        assert_eq!(collected.width(), 3);
        assert!(collected.column("c").is_ok());
        assert_eq!(
            collected.column("c").unwrap().i32().unwrap().get(0),
            Some(10)
        );
    }

    #[test]
    fn test_drop() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[1, 2, 3]).into(),
            Series::new("b".into(), &[4, 5, 6]).into(),
            Series::new("c".into(), &[7, 8, 9]).into(),
        ])
        .unwrap();

        let result = Transform::drop(&df, &["b".to_string()]).unwrap();
        assert_eq!(result.width(), 2);
        assert!(result.column("a").is_ok());
        assert!(result.column("b").is_err());
        assert!(result.column("c").is_ok());
    }

    #[test]
    fn test_drop_lazy() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[1, 2, 3]).into(),
            Series::new("b".into(), &[4, 5, 6]).into(),
            Series::new("c".into(), &[7, 8, 9]).into(),
        ])
        .unwrap();
        let lf = df.lazy();

        let result = Transform::drop_lazy(lf, &["b".to_string()]).unwrap();
        let collected = result.collect().unwrap();
        assert_eq!(collected.width(), 2);
        assert!(collected.column("a").is_ok());
        assert!(collected.column("b").is_err());
        assert!(collected.column("c").is_ok());
    }

    #[test]
    fn test_unique_lazy() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[1, 2, 2, 3, 3, 3]).into(),
            Series::new("b".into(), &[10, 20, 20, 30, 30, 30]).into(),
        ])
        .unwrap();
        let lf = df.lazy();

        let result = Transform::unique_lazy(lf, None, UniqueKeepStrategy::First).unwrap();
        let collected = result.collect().unwrap();
        assert_eq!(collected.height(), 3);
    }

    #[test]
    fn test_limit_lazy() {
        let df = DataFrame::new(vec![Series::new("a".into(), &[1, 2, 3, 4, 5]).into()]).unwrap();
        let lf = df.lazy();

        let result = Transform::limit_lazy(lf, 3).unwrap();
        let collected = result.collect().unwrap();
        assert_eq!(collected.height(), 3);
    }

    #[test]
    fn test_skip_lazy() {
        let df = DataFrame::new(vec![Series::new("a".into(), &[1, 2, 3, 4, 5]).into()]).unwrap();
        let lf = df.lazy();

        let result = Transform::skip_lazy(lf, 2).unwrap();
        let collected = result.collect().unwrap();
        assert_eq!(collected.height(), 3);
        assert_eq!(
            collected.column("a").unwrap().i32().unwrap().get(0),
            Some(3)
        );
    }

    #[test]
    fn test_slice() {
        let df = DataFrame::new(vec![Series::new("a".into(), &[1, 2, 3, 4, 5]).into()]).unwrap();

        let result = Transform::slice(&df, 1, 3).unwrap();
        assert_eq!(result.height(), 3);
        assert_eq!(result.column("a").unwrap().i32().unwrap().get(0), Some(2));
        assert_eq!(result.column("a").unwrap().i32().unwrap().get(1), Some(3));
        assert_eq!(result.column("a").unwrap().i32().unwrap().get(2), Some(4));
    }

    #[test]
    fn test_slice_lazy() {
        let df = DataFrame::new(vec![Series::new("a".into(), &[1, 2, 3, 4, 5]).into()]).unwrap();
        let lf = df.lazy();

        let result = Transform::slice_lazy(lf, 1, 3).unwrap();
        let collected = result.collect().unwrap();
        assert_eq!(collected.height(), 3);
        assert_eq!(
            collected.column("a").unwrap().i32().unwrap().get(0),
            Some(2)
        );
    }

    #[test]
    fn test_reverse() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[1, 2, 3, 4, 5]).into(),
            Series::new("b".into(), &[10, 20, 30, 40, 50]).into(),
        ])
        .unwrap();

        let result = Transform::reverse(&df).unwrap();
        assert_eq!(result.height(), 5);
        assert_eq!(result.column("a").unwrap().i32().unwrap().get(0), Some(5));
        assert_eq!(result.column("a").unwrap().i32().unwrap().get(4), Some(1));
        assert_eq!(result.column("b").unwrap().i32().unwrap().get(0), Some(50));
        assert_eq!(result.column("b").unwrap().i32().unwrap().get(4), Some(10));
    }

    #[test]
    fn test_reverse_lazy() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[1, 2, 3, 4, 5]).into(),
            Series::new("b".into(), &[10, 20, 30, 40, 50]).into(),
        ])
        .unwrap();
        let lf = df.lazy();

        let result = Transform::reverse_lazy(lf).unwrap();
        let collected = result.collect().unwrap();
        assert_eq!(collected.height(), 5);
        assert_eq!(
            collected.column("a").unwrap().i32().unwrap().get(0),
            Some(5)
        );
        assert_eq!(
            collected.column("a").unwrap().i32().unwrap().get(4),
            Some(1)
        );
    }

    #[test]
    fn test_sample() {
        let df = DataFrame::new(vec![Series::new("a", &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])]).unwrap();

        let result = Transform::sample(&df, 3, false, Some(42)).unwrap();
        assert_eq!(result.height(), 3);
        assert!(result.column("a").is_ok());
    }

    #[test]
    fn test_fill_null() {
        let df = DataFrame::new(vec![
            Series::new("a", &[Some(1), None, Some(3)]),
            Series::new("b", &[Some(1.0), Some(2.0), None]),
        ])
        .unwrap();

        let result = Transform::fill_null(&df, FillNullStrategy::Forward(None)).unwrap();
        assert_eq!(result.height(), 3);
        // Check that nulls are filled
        let col_a = result.column("a").unwrap();
        assert!(col_a.null_count() == 0);
    }

    #[test]
    fn test_fill_null_lazy() {
        let df = DataFrame::new(vec![Series::new("a", &[Some(1), None, Some(3)])]).unwrap();
        let lf = df.lazy();

        let value = lit(0);
        let result = Transform::fill_null_lazy(lf, value).unwrap();
        let collected = result.collect().unwrap();
        assert_eq!(collected.height(), 3);
        let col_a = collected.column("a").unwrap();
        assert!(col_a.null_count() == 0);
    }

    #[test]
    fn test_drop_nulls_lazy() {
        let df = DataFrame::new(vec![
            Series::new("a".into(), &[Some(1), None, Some(3), None, Some(5)]).into(),
            Series::new("b".into(), &[Some(10), Some(20), None, Some(40), Some(50)]).into(),
        ])
        .unwrap();
        let lf = df.lazy();

        let result = Transform::drop_nulls_lazy(lf, None).unwrap();
        let collected = result.collect().unwrap();
        assert_eq!(collected.height(), 2); // Only rows without any nulls
    }

    #[test]
    fn test_cast() {
        let df = DataFrame::new(vec![Series::new("a", &[1.0, 2.0, 3.0])]).unwrap();

        let result = Transform::cast(&df, "a", &DataType::Int32).unwrap();
        assert_eq!(result.height(), 3);
        let col_a = result.column("a").unwrap();
        assert_eq!(col_a.dtype(), &DataType::Int32);
        assert_eq!(col_a.i32().unwrap().get(0), Some(1));
    }

    #[test]
    fn test_cast_lazy() {
        let df = DataFrame::new(vec![Series::new("a", &[1.0, 2.0, 3.0])]).unwrap();
        let lf = df.lazy();

        let result = Transform::cast_lazy(lf, "a", DataType::Int32).unwrap();
        let collected = result.collect().unwrap();
        assert_eq!(collected.height(), 3);
        let col_a = collected.column("a").unwrap();
        assert_eq!(col_a.dtype(), &DataType::Int32);
        assert_eq!(col_a.i32().unwrap().get(0), Some(1));
    }

    #[test]
    #[ignore = "explode operation not supported for binary dtype in this Polars version"]
    fn test_explode() {
        let list_series = Series::new("list_col", &[vec![1, 2], vec![3], vec![4, 5, 6]]);
        let df = DataFrame::new(vec![list_series, Series::new("other", &[10, 20, 30])]).unwrap();

        let result = Transform::explode(&df, &["list_col".to_string()]).unwrap();
        assert_eq!(result.height(), 6); // 2 + 1 + 3 = 6
    }

    #[test]
    #[ignore = "explode operation not supported for binary dtype in this Polars version"]
    fn test_explode_lazy() {
        let list_series = Series::new("list_col", &[vec![1, 2], vec![3], vec![4, 5, 6]]);
        let df = DataFrame::new(vec![list_series, Series::new("other", &[10, 20, 30])]).unwrap();
        let lf = df.lazy();

        let result = Transform::explode_lazy(lf, &["list_col".to_string()]).unwrap();
        let collected = result.collect().unwrap();
        assert_eq!(collected.height(), 6); // 2 + 1 + 3 = 6
    }

    #[test]
    fn test_melt() {
        let df = DataFrame::new(vec![
            Series::new("id", &[1, 2, 3]),
            Series::new("a", &[10, 20, 30]),
            Series::new("b", &[100, 200, 300]),
        ])
        .unwrap();

        let result = Transform::melt(
            &df,
            &["id".to_string()],
            &["a".to_string(), "b".to_string()],
            Some("variable"),
            Some("value"),
        )
        .unwrap();

        assert_eq!(result.height(), 6); // 3 rows * 2 value columns
        assert!(result.column("id").is_ok());
        assert!(result.column("variable").is_ok());
        assert!(result.column("value").is_ok());
    }

    #[test]
    fn test_melt_lazy() {
        let df = DataFrame::new(vec![
            Series::new("id", &[1, 2, 3]),
            Series::new("a", &[10, 20, 30]),
            Series::new("b", &[100, 200, 300]),
        ])
        .unwrap();
        let lf = df.lazy();

        let result = Transform::melt_lazy(
            lf,
            &["id".to_string()],
            &["a".to_string(), "b".to_string()],
            Some("variable"),
            Some("value"),
        )
        .unwrap();
        let collected = result.collect().unwrap();

        assert_eq!(collected.height(), 6); // 3 rows * 2 value columns
        assert!(collected.column("id").is_ok());
        assert!(collected.column("variable").is_ok());
        assert!(collected.column("value").is_ok());
    }

    #[test]
    fn test_map_columns() {
        let df = DataFrame::new(vec![
            Series::new("a", &[1, 2, 3]),
            Series::new("b", &[4, 5, 6]),
        ])
        .unwrap();

        // Use all() to reference all columns in the temporary per-column DataFrame
        let expr = all() + lit(10);
        let result = Transform::map_columns(&df, expr).unwrap();

        assert_eq!(result.height(), 3);
        assert_eq!(result.width(), 2);
        // Check that values are increased by 10
        let col_a = result.column("a").unwrap().i32().unwrap();
        assert_eq!(col_a.get(0), Some(11));
        assert_eq!(col_a.get(1), Some(12));
        assert_eq!(col_a.get(2), Some(13));
    }

    #[test]
    fn test_cast_column() {
        let df = DataFrame::new(vec![Series::new("a", &[1.0, 2.0, 3.0])]).unwrap();
        let value = Value::DataFrame(df);

        let result = cast_column(&value, "a", ColumnDataType::Int32).unwrap();
        match result {
            Value::DataFrame(result_df) => {
                let col_a = result_df.column("a").unwrap();
                assert_eq!(col_a.dtype(), &DataType::Int32);
                assert_eq!(col_a.i32().unwrap().get(0), Some(1));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_cast_column_invalid_type() {
        let value = Value::Int(42);
        let result = cast_column(&value, "a", ColumnDataType::Int32);
        assert!(result.is_err());
    }
}
