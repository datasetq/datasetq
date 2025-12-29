use crate::{Error, Result, TypeError, Value};
use polars::prelude::*;

/// Window functions for rolling aggregations
#[derive(Debug, Clone)]
pub enum WindowFunction {
    /// Sum of values
    Sum,
    /// Mean (average) of values
    Mean,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Count of values
    Count,
    /// Standard deviation
    Std,
    /// Variance
    Var,
}

impl WindowFunction {
    /// Get the function name as a string
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            WindowFunction::Sum => "sum",
            WindowFunction::Mean => "mean",
            WindowFunction::Min => "min",
            WindowFunction::Max => "max",
            WindowFunction::Count => "count",
            WindowFunction::Std => "std",
            WindowFunction::Var => "var",
        }
    }
}

/// Rolling window aggregations
///
/// Apply aggregation functions over a rolling window of rows.
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::rolling::{rolling_agg, WindowFunction};
/// use dsq_core::value::Value;
///
/// let result = rolling_agg(
///     &dataframe_value,
///     "value",                       // column to aggregate
///     WindowFunction::Sum,           // aggregation function
///     3,                            // window size
///     None                          // min_periods (optional)
/// ).unwrap();
/// ```
pub fn rolling_agg(
    value: &Value,
    _column: &str,
    _function: WindowFunction,
    window_size: usize,
    min_periods: Option<usize>,
) -> Result<Value> {
    let _min_periods = min_periods.unwrap_or(window_size);

    match value {
        Value::DataFrame(_df) => {
            // Rolling functions are not available in Polars 0.35 Expr API
            // Use a simple implementation for now
            Err(Error::operation(
                "Rolling window functions not yet implemented",
            ))
        }
        Value::LazyFrame(_lf) => {
            // Rolling functions are not available in Polars 0.35 Expr API
            // Use a simple implementation for now
            Err(Error::operation(
                "Rolling window functions not yet implemented",
            ))
        }
        _ => Err(TypeError::UnsupportedOperation {
            operation: "rolling_agg".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}

/// Rolling standard deviation calculation
///
/// Apply rolling standard deviation over a window of rows.
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::rolling::rolling_std;
/// use dsq_core::value::Value;
///
/// let result = rolling_std(
///     &dataframe_value,
///     "value",                       // column to calculate std for
///     3,                            // window size
///     None                          // min_periods (optional)
/// ).unwrap();
/// ```
pub fn rolling_std(
    value: &Value,
    column: &str,
    window_size: usize,
    min_periods: Option<usize>,
) -> Result<Value> {
    let min_periods = min_periods.unwrap_or(window_size);

    match value {
        Value::DataFrame(df) => {
            // Get the column to operate on
            let series = df.column(column).map_err(Error::from)?;

            // Convert to Vec<f64> for manual rolling calculation
            let mut values: Vec<Option<f64>> = Vec::with_capacity(series.len());
            for i in 0..series.len() {
                let val = series.get(i).map_err(Error::from)?;
                let numeric_val = match val {
                    AnyValue::Int8(i) => Some(f64::from(i)),
                    AnyValue::Int16(i) => Some(f64::from(i)),
                    AnyValue::Int32(i) => Some(f64::from(i)),
                    AnyValue::Int64(i) =>
                    {
                        #[allow(clippy::cast_precision_loss)]
                        Some(i as f64)
                    }
                    AnyValue::UInt8(i) => Some(f64::from(i)),
                    AnyValue::UInt16(i) => Some(f64::from(i)),
                    AnyValue::UInt32(i) => Some(f64::from(i)),
                    AnyValue::UInt64(i) =>
                    {
                        #[allow(clippy::cast_precision_loss)]
                        Some(i as f64)
                    }
                    AnyValue::Float32(f) => Some(f64::from(f)),
                    AnyValue::Float64(f) => Some(f),
                    AnyValue::Null => None,
                    _ => {
                        return Err(TypeError::UnsupportedOperation {
                            operation: "rolling_std".to_string(),
                            typ: format!("{val:?}"),
                        }
                        .into());
                    }
                };
                values.push(numeric_val);
            }

            // Calculate rolling std
            let mut result_values: Vec<Option<f64>> = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                let window_start = if i + 1 >= window_size {
                    i + 1 - window_size
                } else {
                    0
                };
                let window = &values[window_start..=i];

                // Filter out None values
                let valid_values: Vec<f64> = window.iter().filter_map(|&v| v).collect();

                if valid_values.len() >= min_periods {
                    // Calculate std
                    #[allow(clippy::cast_precision_loss)]
                    let mean = valid_values.iter().sum::<f64>() / valid_values.len() as f64;
                    #[allow(clippy::cast_precision_loss)]
                    let variance = valid_values.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                        / (valid_values.len() - 1).max(1) as f64;
                    result_values.push(Some(variance.sqrt()));
                } else {
                    result_values.push(None);
                }
            }

            // Create a new series with the result
            let result_series = Series::new(format!("{column}_rolling_std").into(), result_values);

            // Clone the dataframe and add the new column
            let mut result_df = df.clone();
            result_df.with_column(result_series).map_err(Error::from)?;

            Ok(Value::DataFrame(result_df))
        }
        Value::LazyFrame(lf) => {
            // For LazyFrame, we need to collect first
            let df = lf.clone().collect().map_err(Error::from)?;
            rolling_std(
                &Value::DataFrame(df),
                column,
                window_size,
                Some(min_periods),
            )
        }
        Value::Array(arr) => {
            // For arrays, we can implement a similar logic
            // Extract values from array of objects
            let mut values: Vec<Option<f64>> = Vec::with_capacity(arr.len());

            for item in arr {
                if let Value::Object(obj) = item {
                    if let Some(val) = obj.get(column) {
                        let numeric_val = match val {
                            Value::Int(i) =>
                            {
                                #[allow(clippy::cast_precision_loss)]
                                Some(*i as f64)
                            }
                            Value::Float(f) => Some(*f),
                            Value::Null => None,
                            _ => {
                                return Err(TypeError::UnsupportedOperation {
                                    operation: "rolling_std".to_string(),
                                    typ: val.type_name().to_string(),
                                }
                                .into());
                            }
                        };
                        values.push(numeric_val);
                    } else {
                        values.push(None);
                    }
                } else {
                    return Err(TypeError::UnsupportedOperation {
                        operation: "rolling_std".to_string(),
                        typ: item.type_name().to_string(),
                    }
                    .into());
                }
            }

            // Calculate rolling std
            let mut result_arr = Vec::with_capacity(arr.len());
            for (i, item) in arr.iter().enumerate() {
                let window_start = if i + 1 >= window_size {
                    i + 1 - window_size
                } else {
                    0
                };
                let window = &values[window_start..=i];

                // Filter out None values
                let valid_values: Vec<f64> = window.iter().filter_map(|&v| v).collect();

                let rolling_std_val = if valid_values.len() >= min_periods {
                    // Calculate std
                    #[allow(clippy::cast_precision_loss)]
                    let mean = valid_values.iter().sum::<f64>() / valid_values.len() as f64;
                    #[allow(clippy::cast_precision_loss)]
                    let variance = valid_values.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                        / (valid_values.len() - 1).max(1) as f64;
                    Value::Float(variance.sqrt())
                } else {
                    Value::Null
                };

                // Clone the object and add the rolling_std field
                if let Value::Object(obj) = item {
                    let mut new_obj = obj.clone();
                    new_obj.insert(format!("{column}_rolling_std"), rolling_std_val);
                    result_arr.push(Value::Object(new_obj));
                }
            }

            Ok(Value::Array(result_arr))
        }
        _ => Err(TypeError::UnsupportedOperation {
            operation: "rolling_std".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}
