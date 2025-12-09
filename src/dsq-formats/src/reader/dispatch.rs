use crate::error::{Error, Result};
use crate::format::DataFormat;
use crate::reader::options::{FormatReadOptions, ReadOptions};
use dsq_shared::value::Value;
use std::io::Read;

/// Deserialize CSV data from a reader
pub fn deserialize_csv<R: Read + polars::io::mmap::MmapBytesReader>(
    reader: R,
    options: &ReadOptions,
    format_options: &FormatReadOptions,
) -> Result<Value> {
    crate::csv::deserialize_csv(reader, options, format_options)
}

/// Deserialize JSON data from a reader
pub fn deserialize_json<R: Read>(
    reader: R,
    options: &ReadOptions,
    format_options: &FormatReadOptions,
) -> Result<Value> {
    crate::json::deserialize_json(reader, options, format_options)
}

/// Deserialize JSON5 data from a reader
pub fn deserialize_json5<R: Read>(
    mut reader: R,
    options: &ReadOptions,
    _format_options: &FormatReadOptions,
) -> Result<Value> {
    use polars::prelude::*;
    use std::io::Cursor;

    let mut json5_str = String::new();
    reader.read_to_string(&mut json5_str).map_err(Error::from)?;

    // Handle empty input
    if json5_str.trim().is_empty() {
        return Ok(Value::DataFrame(DataFrame::empty()));
    }

    // Parse JSON5 to standard JSON
    let value: serde_json::Value = json5::from_str(&json5_str)
        .map_err(|e| Error::Format(crate::error::FormatError::SerializationError(e.to_string())))?;

    // Convert serde_json::Value to JSON string and use Polars' native reader
    let json_str = serde_json::to_string(&value)
        .map_err(|e| Error::Format(crate::error::FormatError::SerializationError(e.to_string())))?;

    let cursor = Cursor::new(json_str.as_bytes());
    let mut df = polars::io::json::JsonReader::new(cursor)
        .finish()
        .map_err(Error::from)?;

    // Apply max_rows by slicing the DataFrame
    if let Some(max_rows) = options.max_rows {
        if df.height() > max_rows {
            df = df.slice(0, max_rows);
        }
    }

    Ok(Value::DataFrame(df))
}

/// Deserialize Parquet data from a reader
#[cfg(feature = "parquet")]
pub fn deserialize_parquet<R: Read + polars::io::mmap::MmapBytesReader + std::io::Seek>(
    reader: R,
    options: &ReadOptions,
    format_options: &FormatReadOptions,
) -> Result<Value> {
    use polars::prelude::*;

    let parquet_opts = match format_options {
        FormatReadOptions::Parquet {
            parallel: _,
            use_statistics: _,
            columns,
        } => columns.clone(),
        _ => None,
    };

    use polars::prelude::SerReader;

    let mut parquet_reader = ParquetReader::new(reader);

    if let Some(columns) = parquet_opts {
        parquet_reader = parquet_reader.with_columns(Some(columns));
    }

    if let Some(max_rows) = options.max_rows {
        parquet_reader = parquet_reader.with_slice(Some((0, max_rows)));
    }

    let df = parquet_reader.finish().map_err(Error::from)?;
    Ok(Value::DataFrame(df))
}

/// Deserialize ADT (ASCII Delimited Text) data from a reader
pub fn deserialize_adt<R: Read>(
    reader: R,
    options: &ReadOptions,
    format_options: &FormatReadOptions,
) -> Result<Value> {
    crate::adt::deserialize_adt(reader, options, format_options)
}

/// Deserialize data from a reader based on format
pub fn deserialize<R: Read + polars::io::mmap::MmapBytesReader>(
    reader: R,
    format: DataFormat,
    options: &ReadOptions,
    format_options: &FormatReadOptions,
) -> Result<Value> {
    match format {
        DataFormat::Csv => deserialize_csv(reader, options, format_options),
        DataFormat::Json => deserialize_json(reader, options, format_options),
        DataFormat::Json5 => deserialize_json5(reader, options, format_options),
        #[cfg(feature = "parquet")]
        DataFormat::Parquet => deserialize_parquet(reader, options, format_options),
        #[cfg(not(feature = "parquet"))]
        DataFormat::Parquet => Err(Error::Format(
            crate::error::FormatError::UnsupportedFeature(
                "Parquet not supported in this build".to_string(),
            ),
        )),
        _ => Err(Error::Format(crate::error::FormatError::Unknown(
            format.to_string(),
        ))),
    }
}

/// Parse JSON string into a Value
pub fn from_json(json: &str) -> Result<Value> {
    let json_val: serde_json::Value = serde_json::from_str(json).map_err(|e| {
        Error::Format(crate::error::FormatError::SerializationError(format!(
            "Invalid JSON: {}",
            e
        )))
    })?;
    let value = Value::from_json(json_val);
    // For arrays, try to convert to DataFrame for better compatibility
    if let Value::Array(_) = &value {
        match value.to_dataframe() {
            Ok(df) => return Ok(Value::DataFrame(df)),
            Err(_) => {} // Fall back to keeping as array
        }
    }
    // Keep as JSON value for compatibility with jq-like operations
    Ok(value)
}

/// Parse CSV string into a Value
pub fn from_csv(csv: &str) -> Result<Value> {
    let reader = std::io::Cursor::new(csv.as_bytes());
    let options = ReadOptions::default();
    let format_options = FormatReadOptions::default();
    deserialize_csv(reader, &options, &format_options)
}
