use crate::{Error, Result, TypeError, Value};
use polars::prelude::*;

/// Exponentially Weighted Moving Average (EWMA) calculation
///
/// Apply exponentially weighted moving average over a column.
/// The smoothing factor (alpha) controls how quickly older values decay.
/// alpha = 2 / (span + 1), where span is the number of periods for the EMA
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_core::ops::ewma::ewma;
/// use dsq_core::value::Value;
///
/// let result = ewma(
///     &dataframe_value,
///     "value",                       // column to calculate EWMA for
///     0.3,                           // smoothing factor (alpha)
///     None                           // min_periods (optional)
/// ).unwrap();
/// ```
pub fn ewma(value: &Value, column: &str, alpha: f64, min_periods: Option<usize>) -> Result<Value> {
    if !(0.0..=1.0).contains(&alpha) {
        return Err(Error::operation("Alpha must be between 0 and 1"));
    }

    let min_periods = min_periods.unwrap_or(1);

    match value {
        Value::DataFrame(df) => {
            // Get the column to operate on
            let series = df.column(column).map_err(Error::from)?;

            // Convert to Vec<f64> for EWMA calculation
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
                            operation: "ewma".to_string(),
                            typ: format!("{val:?}"),
                        }
                        .into());
                    }
                };
                values.push(numeric_val);
            }

            // Calculate EWMA
            let mut result_values: Vec<Option<f64>> = Vec::with_capacity(values.len());
            let mut ewma_val: Option<f64> = None;
            let mut count = 0;

            for val_opt in &values {
                if let Some(val) = val_opt {
                    count += 1;
                    ewma_val = match ewma_val {
                        None => Some(*val),
                        Some(prev_ewma) => Some(alpha * val + (1.0 - alpha) * prev_ewma),
                    };

                    if count >= min_periods {
                        result_values.push(ewma_val);
                    } else {
                        result_values.push(None);
                    }
                } else {
                    // Propagate the previous EWMA for null values
                    if count >= min_periods {
                        result_values.push(ewma_val);
                    } else {
                        result_values.push(None);
                    }
                }
            }

            // Create a new series with the result
            let result_series = Series::new(format!("{column}_ewma").into(), result_values);

            // Clone the dataframe and add the new column
            let mut result_df = df.clone();
            result_df.with_column(result_series).map_err(Error::from)?;

            Ok(Value::DataFrame(result_df))
        }
        Value::LazyFrame(lf) => {
            // For LazyFrame, we need to collect first
            let df = lf.clone().collect().map_err(Error::from)?;
            ewma(&Value::DataFrame(df), column, alpha, Some(min_periods))
        }
        Value::Array(arr) => {
            // For arrays, extract values from array of objects
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
                                    operation: "ewma".to_string(),
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
                        operation: "ewma".to_string(),
                        typ: item.type_name().to_string(),
                    }
                    .into());
                }
            }

            // Calculate EWMA
            let mut result_arr = Vec::with_capacity(arr.len());
            let mut ewma_val: Option<f64> = None;
            let mut count = 0;

            for (i, item) in arr.iter().enumerate() {
                if let Some(val) = values[i] {
                    count += 1;
                    ewma_val = match ewma_val {
                        None => Some(val),
                        Some(prev_ewma) => Some(alpha * val + (1.0 - alpha) * prev_ewma),
                    };
                }

                let ewma_result = if count >= min_periods {
                    ewma_val.map_or(Value::Null, Value::Float)
                } else {
                    Value::Null
                };

                // Clone the object and add the ewma field
                if let Value::Object(obj) = item {
                    let mut new_obj = obj.clone();
                    new_obj.insert(format!("{column}_ewma"), ewma_result);
                    result_arr.push(Value::Object(new_obj));
                }
            }

            Ok(Value::Array(result_arr))
        }
        _ => Err(TypeError::UnsupportedOperation {
            operation: "ewma".to_string(),
            typ: value.type_name().to_string(),
        }
        .into()),
    }
}
