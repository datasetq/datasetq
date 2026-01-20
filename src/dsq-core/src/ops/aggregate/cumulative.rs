use super::rolling::WindowFunction;
use crate::{Error, Result, TypeError, Value};

/// Cumulative aggregations
///
/// Apply cumulative aggregation functions (running totals, etc.).
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::cumulative::cumulative_agg;
/// use dsq_core::ops::rolling::WindowFunction;
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
