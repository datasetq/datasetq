//! ADT (ASCII Delimited Text) format support
//!
//! ADT uses ASCII control characters for field and record separation:
//! - Field separator: 0x1F (Unit Separator)
//! - Record separator: 0x1E (Record Separator)

use crate::error::{Error, Result};
use crate::reader::{FormatReadOptions, ReadOptions};
use crate::writer::{FormatWriteOptions, WriteOptions};
use dsq_shared::value::Value;
use polars::prelude::*;
use std::collections::HashMap;
use std::io::{Read, Write};

const FIELD_SEPARATOR: u8 = dsq_shared::constants::FIELD_SEPARATOR;
const RECORD_SEPARATOR: u8 = dsq_shared::constants::RECORD_SEPARATOR;

/// ADT reader options
#[derive(Debug, Clone, Default)]
pub struct AdtReadOptions {
    /// Custom field separator (default: 0x1F)
    pub field_separator: Option<u8>,
    /// Custom record separator (default: 0x1E)
    pub record_separator: Option<u8>,
}

/// ADT writer options
#[derive(Debug, Clone, Default)]
pub struct AdtWriteOptions {
    /// Custom field separator (default: 0x1F)
    pub field_separator: Option<u8>,
    /// Custom record separator (default: 0x1E)
    pub record_separator: Option<u8>,
}

/// Deserialize ADT (ASCII Delimited Text) data from a reader
pub fn deserialize_adt<R: Read>(
    mut reader: R,
    options: &ReadOptions,
    _format_options: &FormatReadOptions,
) -> Result<Value> {
    // Read all content
    let mut content = Vec::new();
    reader.read_to_end(&mut content).map_err(Error::from)?;

    if content.is_empty() {
        return Err(Error::operation("ADT data is empty"));
    }

    // Split content by record separator
    let records: Vec<&[u8]> = content
        .split(|&b| b == RECORD_SEPARATOR)
        .filter(|record| !record.is_empty())
        .collect();

    if records.is_empty() {
        return Err(Error::operation("No records found in ADT data"));
    }

    // Parse first record to get headers
    let header_fields: Vec<String> = records[0]
        .split(|&b| b == FIELD_SEPARATOR)
        .map(|field| String::from_utf8_lossy(field).to_string())
        .collect();

    if header_fields.is_empty() {
        return Err(Error::operation("No fields found in ADT header"));
    }

    // Initialize columns
    let mut columns: HashMap<String, Vec<String>> = HashMap::new();
    for header in &header_fields {
        columns.insert(header.clone(), Vec::new());
    }

    // Parse data records (skip header)
    let data_records = if records.len() > 1 {
        &records[1..]
    } else {
        &[]
    };

    let mut rows_to_process = data_records.len();
    if let Some(max_rows) = options.max_rows {
        rows_to_process = rows_to_process.min(max_rows);
    }

    let skip_rows = if options.skip_rows > 0 && options.skip_rows < data_records.len() {
        options.skip_rows
    } else {
        0
    };

    for (row_idx, record) in data_records.iter().enumerate() {
        if row_idx < skip_rows {
            continue;
        }
        if row_idx - skip_rows >= rows_to_process {
            break;
        }

        let fields: Vec<String> = record
            .split(|&b| b == FIELD_SEPARATOR)
            .map(|field| String::from_utf8_lossy(field).to_string())
            .collect();

        // Pad fields to match header count
        for (i, header) in header_fields.iter().enumerate() {
            let value = fields.get(i).cloned().unwrap_or_default();
            columns.get_mut(header).unwrap().push(value);
        }
    }

    // Convert to DataFrame
    let mut df_columns = Vec::new();

    for header in &header_fields {
        let values = columns.get(header).unwrap();
        let series = Series::new(header.as_str().into(), values);
        df_columns.push(series.into());
    }

    let df = DataFrame::new(df_columns).map_err(Error::from)?;

    Ok(Value::DataFrame(df))
}

/// Serialize ADT (ASCII Delimited Text) data to a writer
pub fn serialize_adt<W: Write>(
    mut writer: W,
    value: &Value,
    options: &WriteOptions,
    _format_options: &FormatWriteOptions,
) -> Result<()> {
    let df = match value {
        Value::DataFrame(df) => df.clone(),
        Value::LazyFrame(lf) => (*lf).clone().collect().map_err(Error::from)?,
        _ => return Err(Error::operation("Expected DataFrame for ADT serialization")),
    };

    // Write header if requested
    if options.include_header {
        let headers: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        for (i, header) in headers.iter().enumerate() {
            if i > 0 {
                writer.write_all(&[FIELD_SEPARATOR]).map_err(Error::from)?;
            }
            writer.write_all(header.as_bytes()).map_err(Error::from)?;
        }
        writer.write_all(&[RECORD_SEPARATOR]).map_err(Error::from)?;
    }

    // Write data rows
    let height = df.height();
    for row_idx in 0..height {
        for (col_idx, column) in df.get_columns().iter().enumerate() {
            if col_idx > 0 {
                writer.write_all(&[FIELD_SEPARATOR]).map_err(Error::from)?;
            }

            // Get value and convert to string
            let value_str = match column.get(row_idx).map_err(Error::from)? {
                AnyValue::String(s) => s.to_string(),
                AnyValue::Int64(i) => i.to_string(),
                AnyValue::Float64(f) => f.to_string(),
                AnyValue::Boolean(b) => b.to_string(),
                AnyValue::Null => String::new(),
                AnyValue::Int32(i) => i.to_string(),
                AnyValue::Float32(f) => f.to_string(),
                AnyValue::Date(d) => d.to_string(),
                AnyValue::Datetime(dt, _, _) => dt.to_string(),
                other => format!("{}", other),
            };

            writer
                .write_all(value_str.as_bytes())
                .map_err(Error::from)?;
        }
        writer.write_all(&[RECORD_SEPARATOR]).map_err(Error::from)?;
    }

    Ok(())
}

/// Detect if content is ADT format
pub fn detect_adt_format(content: &[u8]) -> bool {
    // Check if content contains ADT separators
    content.contains(&FIELD_SEPARATOR) || content.contains(&RECORD_SEPARATOR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_adt_round_trip() {
        // Create a simple DataFrame
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob"]),
            Series::new("age".into(), vec![30i64, 25i64]),
        ])
        .unwrap();
        let value = Value::DataFrame(df);

        // Serialize
        let mut buffer = Vec::new();
        let write_options = WriteOptions {
            include_header: true,
            ..Default::default()
        };
        let format_options = FormatWriteOptions::default();
        serialize_adt(&mut buffer, &value, &write_options, &format_options).unwrap();

        // Deserialize
        let cursor = Cursor::new(buffer);
        let read_options = ReadOptions::default();
        let read_format_options = FormatReadOptions::default();
        let result = deserialize_adt(cursor, &read_options, &read_format_options).unwrap();

        // Verify
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                assert_eq!(df.width(), 2);
                assert!(df.get_column_names().contains(&"name".into()));
                assert!(df.get_column_names().contains(&"age".into()));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_detect_adt_format() {
        let adt_data = b"name\x1Fage\x1EAlice\x1F30\x1E";
        assert!(detect_adt_format(adt_data));

        let csv_data = b"name,age\nAlice,30\n";
        assert!(!detect_adt_format(csv_data));
    }
}
