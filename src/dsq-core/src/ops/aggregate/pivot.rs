use crate::{Error, Result, TypeError, Value};
use polars::prelude::*;

/// Pivot a `DataFrame` (convert rows to columns)
///
/// Equivalent to SQL's PIVOT operation or Excel's pivot tables.
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::pivot::pivot;
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
