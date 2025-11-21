use crate::{Error, Result};
use polars::prelude::*;

/// Helper to convert a single Series value to JSON
pub fn series_value_to_json(series: &Series, index: usize) -> Result<serde_json::Value> {
    use polars::datatypes::*;

    if series.is_null().get(index).unwrap_or(false) {
        return Ok(serde_json::Value::Null);
    }

    match series.dtype() {
        DataType::Boolean => {
            let val = series.bool().map_err(Error::from)?.get(index);
            Ok(serde_json::Value::Bool(val.unwrap_or(false)))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let val = series.i64().map_err(Error::from)?.get(index);
            Ok(serde_json::Value::Number(serde_json::Number::from(
                val.unwrap_or(0),
            )))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let val = series.u64().map_err(Error::from)?.get(index);
            Ok(serde_json::Value::Number(serde_json::Number::from(
                val.unwrap_or(0),
            )))
        }
        DataType::Float32 | DataType::Float64 => {
            let val = series.f64().map_err(Error::from)?.get(index);
            serde_json::Number::from_f64(val.unwrap_or(0.0))
                .map(serde_json::Value::Number)
                .ok_or_else(|| Error::Other("Invalid float value".to_string()))
        }
        DataType::Utf8 => {
            let val = series.utf8().map_err(Error::from)?.get(index);
            Ok(serde_json::Value::String(val.unwrap_or("").to_string()))
        }
        DataType::Date => {
            let val = series.date().map_err(Error::from)?.get(index);
            if let Some(date) = val {
                Ok(serde_json::Value::String(date.to_string()))
            } else {
                Ok(serde_json::Value::Null)
            }
        }
        DataType::Datetime(_, _) => {
            let val = series.datetime().map_err(Error::from)?.get(index);
            if let Some(dt) = val {
                Ok(serde_json::Value::String(dt.to_string()))
            } else {
                Ok(serde_json::Value::Null)
            }
        }
        _ => Err(Error::Other(format!(
            "Unsupported data type for JSON conversion: {:?}",
            series.dtype()
        ))),
    }
}
