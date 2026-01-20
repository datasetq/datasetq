use crate::{Error, Result, TypeError, Value};
use polars::prelude::*;

/// Unpivot a `DataFrame` (convert columns to rows)
///
/// Equivalent to SQL's UNPIVOT operation or pandas' melt function.
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::unpivot::unpivot;
/// use dsq_core::value::Value;
///
/// let result = unpivot(
///     &dataframe_value,
///     &["id".to_string()],           // columns to keep as identifiers
///     &["col1".to_string(), "col2".to_string()], // columns to unpivot
///     "variable",                    // name for the variable column
///     "value"                        // name for the value column
/// ).unwrap();
/// ```
pub fn unpivot(
    value: &Value,
    id_columns: &[String],
    value_columns: &[String],
    variable_name: &str,
    value_name: &str,
) -> Result<Value> {
    match value {
        Value::DataFrame(df) => {
            // Use unpivot method from UnpivotDF trait
            let mut unpivoted = if id_columns.is_empty() {
                df.clone()
                    .unpivot([] as [&str; 0], value_columns)
                    .map_err(Error::from)?
            } else {
                df.clone()
                    .unpivot(id_columns, value_columns)
                    .map_err(Error::from)?
            };
            unpivoted
                .rename("variable", variable_name.into())
                .map_err(Error::from)?;
            unpivoted
                .rename("value", value_name.into())
                .map_err(Error::from)?;

            Ok(Value::DataFrame(unpivoted))
        }
        Value::LazyFrame(lf) => {
            let df = lf.clone().collect().map_err(Error::from)?;
            unpivot(
                &Value::DataFrame(df),
                id_columns,
                value_columns,
                variable_name,
                value_name,
            )
        }
        _ => Err(TypeError::UnsupportedOperation {
            operation: "unpivot".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}
