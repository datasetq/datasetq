use crate::error::{Error, Result};
use crate::reader::options::ReadOptions;
use polars::prelude::*;

/// Helper function to convert JSON value to DataFrame
pub fn json_to_dataframe(value: &serde_json::Value, options: &ReadOptions) -> Result<DataFrame> {
    match value {
        serde_json::Value::Array(arr) => {
            // Array of objects
            if arr.is_empty() {
                return Ok(DataFrame::empty());
            }
            let start = options.skip_rows;
            let end = start + options.max_rows.unwrap_or(arr.len() - start);
            let arr_slice = &arr[start..end.min(arr.len())];
            if arr_slice.is_empty() {
                return Ok(DataFrame::empty());
            }

            // Pre-allocate series vector with capacity
            let row_count = arr_slice.len();
            let mut series_vec = Vec::new();

            if let Some(columns) = &options.columns {
                // Pre-allocate with known column count
                series_vec.reserve(columns.len());
                for col in columns {
                    // Pre-allocate values vector with exact capacity
                    let mut values = Vec::with_capacity(row_count);
                    for item in arr_slice {
                        if let Some(val) = item.get(col) {
                            values.push(json_value_to_anyvalue(val));
                        } else {
                            values.push(AnyValue::Null);
                        }
                    }
                    series_vec.push(Series::new(col, values));
                }
            } else {
                if let Some(serde_json::Value::Object(obj)) = arr_slice.first() {
                    // Pre-allocate with known column count
                    let column_count = obj.len();
                    series_vec.reserve(column_count);

                    for key in obj.keys() {
                        // Pre-allocate values vector with exact capacity
                        let mut values = Vec::with_capacity(row_count);
                        for item in arr_slice {
                            if let Some(val) = item.get(key) {
                                values.push(json_value_to_anyvalue(val));
                            } else {
                                values.push(AnyValue::Null);
                            }
                        }
                        series_vec.push(Series::new(key, values));
                    }
                }
            }
            DataFrame::new(series_vec).map_err(Error::from)
        }
        serde_json::Value::Object(obj) => {
            // Single object
            if options.skip_rows > 0 || options.max_rows == Some(0) {
                return Ok(DataFrame::empty());
            }
            let mut series_vec = Vec::new();
            let keys: Vec<&String> = if let Some(columns) = &options.columns {
                columns.iter().filter(|c| obj.contains_key(*c)).collect()
            } else {
                obj.keys().collect()
            };
            for key in keys {
                let val = obj
                    .get(key)
                    .ok_or_else(|| Error::operation("Key not found in object"))?;
                series_vec.push(Series::new(key, vec![json_value_to_anyvalue(val)]));
            }
            DataFrame::new(series_vec).map_err(Error::from)
        }
        _ => Err(Error::Format(
            crate::error::FormatError::UnsupportedFeature(
                "JSON must be an object or array of objects".to_string(),
            ),
        )),
    }
}

/// Convert JSON value to Polars AnyValue
fn json_value_to_anyvalue(value: &serde_json::Value) -> AnyValue<'_> {
    match value {
        serde_json::Value::Null => AnyValue::Null,
        serde_json::Value::Bool(b) => AnyValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                AnyValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                AnyValue::Float64(f)
            } else {
                AnyValue::Null
            }
        }
        serde_json::Value::String(s) => AnyValue::Utf8(s),
        serde_json::Value::Array(_) => AnyValue::Null, // TODO: Handle nested arrays
        serde_json::Value::Object(_) => AnyValue::Null, // TODO: Handle nested objects
    }
}
