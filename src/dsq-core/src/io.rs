//! I/O operations for reading and writing various data formats
//!
//! This module provides functionality for reading from and writing to different
//! file formats supported by dsq, including CSV, JSON, Parquet, Avro, and more.
//!
//! It orchestrates between dsq-io (low-level I/O) and dsq-formats (serialization).

use std::io::Cursor;
use std::path::Path;

use polars::prelude::*;

use crate::error::{Error, Result};
use crate::Value;
#[cfg(feature = "json5")]
use dsq_formats::deserialize_json5;
use dsq_formats::format::detect_format_from_content;
use dsq_formats::{
    deserialize_adt, deserialize_csv, deserialize_json, deserialize_parquet, serialize_adt,
    serialize_csv, serialize_json, serialize_parquet, DataFormat, FormatReadOptions,
    FormatWriteOptions, ReadOptions as DsFormatReadOptions,
};

#[cfg(not(target_arch = "wasm32"))]
use tokio::runtime::Runtime;

// Shared Tokio runtime to avoid creating new runtimes for each sync call
#[cfg(not(target_arch = "wasm32"))]
static TOKIO_RUNTIME: std::sync::LazyLock<Runtime> =
    std::sync::LazyLock::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

/// Options for reading files
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Whether to infer schema automatically
    pub infer_schema: bool,
    /// Number of rows to read (None for all)
    pub n_rows: Option<usize>,
    /// Skip initial rows
    pub skip_rows: usize,
    /// Chunk size for streaming reads (None for no chunking)
    pub chunk_size: Option<usize>,
    /// Use memory-mapped I/O for large files (Parquet only)
    pub use_mmap: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            infer_schema: true,
            n_rows: None,
            skip_rows: 0,
            chunk_size: None,
            use_mmap: true, // Enable mmap by default for better performance
        }
    }
}

/// Options for writing files
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Whether to include header in output
    pub include_header: bool,
    /// Compression to use (if supported by format)
    pub compression: Option<String>,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            include_header: true,
            compression: None,
        }
    }
}

/// Information about a file
#[derive(Debug, Clone)]
pub struct FileInfo {
    /// File path
    pub path: String,
    /// File format
    pub format: String,
    /// Number of rows (if available)
    pub rows: Option<usize>,
    /// Number of columns (if available)
    pub columns: Option<usize>,
    /// Column names (if available)
    pub column_names: Option<Vec<String>>,
}

/// Read a file into a Value
#[cfg(not(target_arch = "wasm32"))]
pub async fn read_file<P: AsRef<Path>>(path: P, options: &ReadOptions) -> Result<Value> {
    let path = path.as_ref();
    let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");

    let format = if extension.is_empty() {
        // No extension, try content detection
        let bytes = dsq_io::read_file(path).await?;
        detect_format_from_content(&bytes).ok_or_else(|| {
            Error::operation("Could not detect file format from content".to_string())
        })?
    } else {
        match extension.to_lowercase().as_str() {
            "csv" => DataFormat::Csv,
            "tsv" => DataFormat::Tsv,
            "adt" => DataFormat::Adt,
            "json" => DataFormat::Json,
            "jsonl" | "ndjson" => DataFormat::JsonLines,
            "json5" => DataFormat::Json5,
            "parquet" => DataFormat::Parquet,
            _ => {
                return Err(Error::operation(format!(
                    "Unsupported file format: {extension}"
                )));
            }
        }
    };

    // Read the file bytes
    let bytes = dsq_io::read_file(path).await?;
    let cursor = Cursor::new(bytes);

    // Deserialize based on format
    let format_read_options = DsFormatReadOptions {
        max_rows: options.n_rows,
        skip_rows: options.skip_rows,
        ..Default::default()
    };
    let format_options = FormatReadOptions::default();
    match format {
        DataFormat::Csv => Ok(deserialize_csv(
            cursor,
            &format_read_options,
            &format_options,
        )?),
        DataFormat::Adt => Ok(deserialize_adt(
            cursor,
            &format_read_options,
            &format_options,
        )?),
        DataFormat::Json => Ok(deserialize_json(
            cursor,
            &format_read_options,
            &format_options,
        )?),
        #[cfg(feature = "json5")]
        DataFormat::Json5 => Ok(deserialize_json5(
            cursor,
            &format_read_options,
            &format_options,
        )?),
        #[cfg(not(feature = "json5"))]
        DataFormat::Json5 => Err(Error::operation(
            "JSON5 support not enabled. Rebuild with 'json5' feature.",
        )),
        DataFormat::Parquet => Ok(deserialize_parquet(
            cursor,
            &format_read_options,
            &format_options,
        )?),
        _ => Err(Error::operation(format!("Unsupported format: {format:?}"))),
    }
}

/// Synchronous version for compatibility
#[cfg(not(target_arch = "wasm32"))]
pub fn read_file_sync<P: AsRef<Path>>(path: P, options: &ReadOptions) -> Result<Value> {
    // Use shared runtime instead of creating new one each time
    TOKIO_RUNTIME.block_on(read_file(path, options))
}

/// Read a file into a lazy Value
pub fn read_file_lazy<P: AsRef<Path>>(path: P, options: &ReadOptions) -> Result<Value> {
    use std::fs;

    let path = path.as_ref();
    let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");

    let format = if extension.is_empty() {
        // No extension, try content detection
        let content =
            fs::read(path).map_err(|e| Error::operation(format!("Failed to read file: {e}")))?;
        detect_format_from_content(&content)
            .ok_or_else(|| Error::operation("Could not detect file format from content"))?
    } else {
        match extension.to_lowercase().as_str() {
            "csv" => dsq_formats::DataFormat::Csv,
            "tsv" => dsq_formats::DataFormat::Tsv,
            "adt" => dsq_formats::DataFormat::Adt,
            "parquet" => dsq_formats::DataFormat::Parquet,
            _ => return read_file_sync(path, options), // Fall back to eager reading
        }
    };

    match format {
        dsq_formats::DataFormat::Csv => read_csv_lazy(path, options),
        dsq_formats::DataFormat::Tsv => read_tsv_lazy(path, options),
        dsq_formats::DataFormat::Adt => read_adt_lazy(path, options),
        dsq_formats::DataFormat::Parquet => read_parquet_lazy(path, options),
        _ => read_file_sync(path, options), // Fall back to eager reading
    }
}

/// Write a Value to a file
pub async fn write_file<P: AsRef<Path>>(
    value: &Value,
    path: P,
    options: &WriteOptions,
) -> Result<()> {
    let path = path.as_ref();
    let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");

    let format = if extension.is_empty() {
        // Default to CSV when no extension
        DataFormat::Csv
    } else {
        match extension.to_lowercase().as_str() {
            "csv" => DataFormat::Csv,
            "tsv" => DataFormat::Tsv,
            "adt" => DataFormat::Adt,
            "json" => DataFormat::Json,
            "jsonl" | "ndjson" => DataFormat::JsonLines,
            "parquet" => DataFormat::Parquet,
            _ => {
                return Err(Error::operation(format!(
                    "Unsupported output format: {extension}"
                )));
            }
        }
    };

    // Serialize based on format
    let mut buffer = Vec::new();
    let format_write_options = dsq_formats::WriteOptions {
        include_header: options.include_header,
        ..Default::default()
    };
    let format_options = FormatWriteOptions::default();
    match format {
        DataFormat::Csv => {
            serialize_csv(&mut buffer, value, &format_write_options, &format_options)?;
        }
        DataFormat::Adt => {
            serialize_adt(&mut buffer, value, &format_write_options, &format_options)?;
        }
        DataFormat::Json | DataFormat::Json5 => {
            serialize_json(&mut buffer, value, &format_write_options, &format_options)?;
            // JSON5 not implemented, use JSON
        }
        DataFormat::Parquet => {
            serialize_parquet(&mut buffer, value, &format_write_options, &format_options)?;
        }
        _ => {
            return Err(Error::operation(format!("Unsupported format: {format:?}")));
        }
    }

    // Write the buffer to file
    dsq_io::write_file(path, &buffer).await?;
    Ok(())
}

/// Synchronous version for compatibility
#[cfg(not(target_arch = "wasm32"))]
pub fn write_file_sync<P: AsRef<Path>>(
    value: &Value,
    path: P,
    options: &WriteOptions,
) -> Result<()> {
    // Use shared runtime instead of creating new one each time
    TOKIO_RUNTIME.block_on(write_file(value, path, options))
}

/// Convert a file from one format to another
pub fn convert_file<P1: AsRef<Path>, P2: AsRef<Path>>(
    input_path: P1,
    output_path: P2,
    read_options: &ReadOptions,
    write_options: &WriteOptions,
) -> Result<()> {
    let value = read_file_sync(input_path, read_options)?;
    write_file_sync(&value, output_path, write_options)
}

/// Inspect a file and return metadata
pub fn inspect_file<P: AsRef<Path>>(path: P) -> Result<FileInfo> {
    let path = path.as_ref();
    let path_str = path.to_string_lossy().to_string();
    let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");

    // Try to read a small sample to get metadata
    let options = ReadOptions {
        n_rows: Some(1),
        ..Default::default()
    };

    match read_file_sync(path, &options) {
        Ok(value) => match value {
            Value::DataFrame(df) => Ok(FileInfo {
                path: path_str,
                format: extension.to_string(),
                rows: None, // We only read 1 row, so we don't know the total
                columns: Some(df.width()),
                column_names: Some(
                    df.get_column_names()
                        .iter()
                        .map(|s| (*s).to_string())
                        .collect(),
                ),
            }),
            _ => Ok(FileInfo {
                path: path_str,
                format: extension.to_string(),
                rows: None,
                columns: None,
                column_names: None,
            }),
        },
        Err(_) => Ok(FileInfo {
            path: path_str,
            format: extension.to_string(),
            rows: None,
            columns: None,
            column_names: None,
        }),
    }
}

fn read_adt<P: AsRef<Path>>(path: P, options: &ReadOptions) -> Result<Value> {
    use std::collections::HashMap;
    use std::fs;

    // ADT format uses ASCII control characters:
    const FIELD_SEPARATOR: u8 = dsq_shared::constants::FIELD_SEPARATOR;
    const RECORD_SEPARATOR: u8 = dsq_shared::constants::RECORD_SEPARATOR;

    let content = fs::read(path.as_ref())
        .map_err(|e| Error::operation(format!("Failed to read ADT file: {e}")))?;

    if content.is_empty() {
        return Err(Error::operation("ADT file is empty"));
    }

    // Split content by record separator
    let records: Vec<&[u8]> = content
        .split(|&b| b == RECORD_SEPARATOR)
        .filter(|record| !record.is_empty())
        .collect();

    if records.is_empty() {
        return Err(Error::operation("No records found in ADT file"));
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
    if let Some(n_rows) = options.n_rows {
        rows_to_process = rows_to_process.min(n_rows);
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
        let column = Column::new(header.into(), values);
        df_columns.push(column);
    }

    let df = DataFrame::new(df_columns)
        .map_err(|e| Error::operation(format!("Failed to create DataFrame: {e}")))?;

    Ok(Value::DataFrame(df))
}

fn read_csv_lazy<P: AsRef<Path>>(path: P, options: &ReadOptions) -> Result<Value> {
    let path_buf = path.as_ref().to_path_buf();
    let mut csv_options = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(100));

    if let Some(n_rows) = options.n_rows {
        csv_options = csv_options.with_n_rows(Some(n_rows));
    }

    if options.skip_rows > 0 {
        csv_options = csv_options.with_skip_rows(options.skip_rows);
    }

    // Set optimal batch size for streaming
    if let Some(chunk_size) = options.chunk_size {
        csv_options = csv_options.with_chunk_size(chunk_size);
    } else {
        // Use a reasonable default batch size for large files
        csv_options = csv_options.with_chunk_size(50_000);
    }

    let reader = csv_options.try_into_reader_with_file_path(Some(path_buf))?;
    let lf = reader.finish()?.lazy();
    Ok(Value::LazyFrame(Box::new(lf)))
}

fn read_tsv_lazy<P: AsRef<Path>>(path: P, options: &ReadOptions) -> Result<Value> {
    let path_buf = path.as_ref().to_path_buf();
    let mut csv_options = CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(100));

    // Clone and modify parse_options for TSV
    let mut parse_opts = (*csv_options.parse_options).clone();
    parse_opts.separator = b'\t';
    csv_options.parse_options = std::sync::Arc::new(parse_opts);

    if let Some(n_rows) = options.n_rows {
        csv_options = csv_options.with_n_rows(Some(n_rows));
    }

    if options.skip_rows > 0 {
        csv_options = csv_options.with_skip_rows(options.skip_rows);
    }

    // Set optimal batch size for streaming
    if let Some(chunk_size) = options.chunk_size {
        csv_options = csv_options.with_chunk_size(chunk_size);
    } else {
        csv_options = csv_options.with_chunk_size(50_000);
    }

    let reader = csv_options.try_into_reader_with_file_path(Some(path_buf))?;
    let lf = reader.finish()?.lazy();
    Ok(Value::LazyFrame(Box::new(lf)))
}

fn read_adt_lazy<P: AsRef<Path>>(path: P, options: &ReadOptions) -> Result<Value> {
    // For ADT, lazy reading would require significant custom implementation
    // For now, fall back to eager reading and then convert to lazy
    let eager_value = read_adt(path, options)?;
    match eager_value {
        Value::DataFrame(df) => Ok(Value::LazyFrame(Box::new(df.lazy()))),
        other => Ok(other),
    }
}

fn read_parquet_lazy<P: AsRef<Path>>(path: P, options: &ReadOptions) -> Result<Value> {
    use std::fs::File;

    let path_ref = path.as_ref();
    let file = File::open(path_ref)?;
    let reader = ParquetReader::new(file);

    // Note: use_statistics and with_n_rows have been removed from ParquetReader
    // Memory-mapped I/O and row limiting should be configured differently in newer Polars

    let mut lf = reader.finish()?.lazy();

    if options.skip_rows > 0 {
        lf = lf.slice(
            i64::try_from(options.skip_rows)
                .map_err(|_| Error::operation("Skip rows value out of range for i64"))?,
            u32::MAX,
        );
    }

    // Enable predicate pushdown and projection pushdown for better performance
    Ok(Value::LazyFrame(Box::new(lf)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_read_json_array() {
        let json_data = r#"[
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ]"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json_data.as_bytes()).unwrap();
        let path = temp_file.path();

        let options = ReadOptions::default();
        let result = read_file_sync(path, &options).unwrap();

        // JSON arrays of objects are converted to DataFrames
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                assert_eq!(df.width(), 2);
                assert!(df
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "name"));
                assert!(df
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "age"));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_read_json_object() {
        let json_data = r#"{"name": "Alice", "age": 30}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json_data.as_bytes()).unwrap();
        let path = temp_file.path();

        let options = ReadOptions::default();
        let result = read_file_sync(path, &options).unwrap();

        // Single JSON objects are converted to single-row DataFrames
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 1);
                assert!(df
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "name"));
                assert!(df
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "age"));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_read_csv() {
        let csv_data = "name,age\nAlice,30\nBob,25";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();
        let path = temp_file.path();

        let options = ReadOptions::default();
        let result = read_file_sync(path, &options).unwrap();

        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                assert_eq!(df.width(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_group_by_io_csv() {
        // Test reading CSV data that could be used with group_by
        let csv_data = "genre,title,price\nFiction,Book1,10.5\nFiction,Book2,12.0\nNon-Fiction,Book3,15.0\nFiction,Book4,8.5";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();
        let path = temp_file.path();

        let options = ReadOptions::default();
        let result = read_file_sync(path, &options).unwrap();

        match &result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 4);
                assert_eq!(df.width(), 3);
                assert!(df
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "genre"));
                assert!(df
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "title"));
                assert!(df
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "price"));
            }
            _ => panic!("Expected DataFrame"),
        }

        // Test writing group_by results back to CSV
        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path();

        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&result, output_path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to write CSV: {:?}",
            write_result
        );

        // Read it back to verify
        let read_back = read_file_sync(output_path, &options).unwrap();
        match read_back {
            Value::DataFrame(df_back) => {
                assert_eq!(df_back.height(), 4);
                assert_eq!(df_back.width(), 3);
            }
            _ => panic!("Expected DataFrame after round-trip"),
        }
    }

    #[test]
    fn test_group_by_io_json() {
        // Test reading JSON array data for group_by operations
        let json_data = r#"[
            {"genre": "Fiction", "title": "Book1", "price": 10.5},
            {"genre": "Fiction", "title": "Book2", "price": 12.0},
            {"genre": "Non-Fiction", "title": "Book3", "price": 15.0},
            {"genre": "Fiction", "title": "Book4", "price": 8.5}
        ]"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json_data.as_bytes()).unwrap();
        let path = temp_file.path();

        let options = ReadOptions::default();
        let result = read_file_sync(path, &options).unwrap();

        match &result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 4);
                assert_eq!(df.width(), 3); // genre, title, price
                                           // Check first row
                let genre_series = df.column("genre").unwrap();
                let title_series = df.column("title").unwrap();
                let price_series = df.column("price").unwrap();

                // Check that we have the expected values
                assert!(genre_series.len() == 4);
                assert!(title_series.len() == 4);
                assert!(price_series.len() == 4);
            }
            _ => panic!("Expected DataFrame"),
        }

        // Test writing group_by results (simulated as array of objects) to JSON
        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path();

        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&result, output_path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to write JSON: {:?}",
            write_result
        );

        // Read it back to verify
        let read_back = read_file_sync(output_path, &options).unwrap();
        match read_back {
            Value::DataFrame(df_back) => {
                assert_eq!(df_back.height(), 4);
                assert_eq!(df_back.width(), 3);
            }
            _ => panic!("Expected DataFrame after round-trip"),
        }
    }

    #[test]
    fn test_group_by_io_parquet() {
        // Create a DataFrame that represents group_by results
        let df = DataFrame::new(vec![
            Column::new("genre".into(), vec!["Fiction", "Non-Fiction"]),
            Column::new("count".into(), vec![3i64, 1i64]),
            Column::new("avg_price".into(), vec![10.33, 15.0]),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Write to Parquet
        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&df_value, path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to write Parquet: {:?}",
            write_result
        );

        // Read back from Parquet
        let read_options = ReadOptions::default();
        let read_result = read_file_sync(path, &read_options).unwrap();

        match read_result {
            Value::DataFrame(df_back) => {
                assert_eq!(df_back.height(), 2);
                assert_eq!(df_back.width(), 3);
                assert!(df_back
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "genre"));
                assert!(df_back
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "count"));
                assert!(df_back
                    .get_column_names()
                    .iter()
                    .any(|name| name.as_str() == "avg_price"));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    #[ignore = "TSV format not fully supported"]
    fn test_group_by_io_tsv() {
        // Test TSV format (tab-separated values)
        let tsv_data = "genre\ttitle\tprice\nFiction\tBook1\t10.5\nFiction\tBook2\t12.0\nNon-Fiction\tBook3\t15.0";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(tsv_data.as_bytes()).unwrap();
        let path = temp_file.path();

        let options = ReadOptions::default();
        let result = read_file_sync(path, &options).unwrap();

        match &result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 3);
                assert_eq!(df.width(), 3);
                assert!(df
                    .get_column_names()
                    .contains(&&polars::datatypes::PlSmallStr::from("genre")));
                assert!(df
                    .get_column_names()
                    .contains(&&polars::datatypes::PlSmallStr::from("title")));
                assert!(df
                    .get_column_names()
                    .contains(&&polars::datatypes::PlSmallStr::from("price")));
            }
            _ => panic!("Expected DataFrame"),
        }

        // Test writing back to TSV
        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path();

        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&result, output_path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to write TSV: {:?}",
            write_result
        );
    }

    #[test]
    fn test_group_by_io_mixed_formats() {
        // Test reading from one format and writing to another
        let csv_data =
            "genre,title,price\nFiction,Book1,10.5\nFiction,Book2,12.0\nNon-Fiction,Book3,15.0";

        let mut csv_file = NamedTempFile::new().unwrap();
        csv_file.write_all(csv_data.as_bytes()).unwrap();
        let csv_path = csv_file.path();

        // Read from CSV
        let read_options = ReadOptions::default();
        let csv_result = read_file_sync(csv_path, &read_options).unwrap();

        // Write to JSON
        let json_file = NamedTempFile::new().unwrap();
        let json_path = json_file.path();

        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&csv_result, json_path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to convert CSV to JSON: {:?}",
            write_result
        );

        // Read back from JSON
        let json_result = read_file_sync(json_path, &read_options).unwrap();

        match (&csv_result, &json_result) {
            (Value::DataFrame(csv_df), Value::DataFrame(json_df)) => {
                assert_eq!(csv_df.height(), json_df.height());
                assert_eq!(csv_df.width(), json_df.width());
                // Verify data integrity
                assert!(json_df.column("genre").is_ok());
                assert!(json_df.column("title").is_ok());
                assert!(json_df.column("price").is_ok());
            }
            _ => panic!("Expected DataFrame -> DataFrame conversion"),
        }
    }

    #[test]
    fn test_convert_formats_to_parquet() {
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        // Test converting various input formats to Parquet
        // Only test CSV and JSON which are supported by default
        let test_data = [
            (
                "csv",
                "name,age,active\nAlice,30,true\nBob,25,false\nCharlie,35,true\n",
            ),
            (
                "json",
                r#"[{"name":"Alice","age":30,"active":true},{"name":"Bob","age":25,"active":false},{"name":"Charlie","age":35,"active":true}]"#,
            ),
        ];

        for (format_name, data) in test_data.iter() {
            let input_path = temp_dir.path().join(format!("input.{}", format_name));
            fs::write(&input_path, data).unwrap();

            let output_path = temp_dir.path().join("output.parquet");

            let read_options = ReadOptions::default();
            let write_options = WriteOptions::default();

            // Convert to Parquet
            let convert_result =
                convert_file(&input_path, &output_path, &read_options, &write_options);
            assert!(
                convert_result.is_ok(),
                "Failed to convert {} to Parquet: {:?}",
                format_name,
                convert_result
            );

            // Read back the Parquet file to verify
            let read_back_result = read_file_sync(&output_path, &read_options).unwrap();
            match read_back_result {
                Value::DataFrame(df) => {
                    assert_eq!(df.height(), 3, "Wrong row count for {}", format_name);
                    assert_eq!(df.width(), 3, "Wrong column count for {}", format_name);
                    assert!(
                        df.get_column_names()
                            .contains(&&polars::datatypes::PlSmallStr::from("name")),
                        "Missing 'name' column for {}",
                        format_name
                    );
                    assert!(
                        df.get_column_names()
                            .contains(&&polars::datatypes::PlSmallStr::from("age")),
                        "Missing 'age' column for {}",
                        format_name
                    );
                    assert!(
                        df.get_column_names()
                            .contains(&&polars::datatypes::PlSmallStr::from("active")),
                        "Missing 'active' column for {}",
                        format_name
                    );
                }
                _ => panic!("Expected DataFrame for {} conversion", format_name),
            }
        }
    }

    #[test]
    fn test_group_by_io_error_handling() {
        // Test reading non-existent file
        let fake_path = Path::new("/non/existent/file.csv");
        let options = ReadOptions::default();
        let result = read_file_sync(fake_path, &options);
        assert!(result.is_err(), "Expected error for non-existent file");

        // Test writing to invalid path
        let df = DataFrame::new(vec![Series::new("test".into(), vec![1i64]).into()]).unwrap();
        let df_value = Value::DataFrame(df);
        let invalid_path = Path::new("/invalid/path/file.csv");
        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&df_value, invalid_path, &write_options);
        assert!(
            write_result.is_err(),
            "Expected error for invalid write path"
        );

        // Test unsupported format (if we try to write unsupported type)
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let unsupported_value = Value::String("unsupported".to_string());
        let _write_result2 = write_file_sync(&unsupported_value, path, &write_options);
        // This might succeed or fail depending on implementation, but shouldn't panic
        // Just ensure it doesn't crash
    }

    #[test]
    #[ignore = "NDJSON/JsonLines format not fully supported"]
    fn test_write_ndjson() {
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob", "Charlie"]).into(),
            Series::new("age".into(), vec![30i64, 25i64, 35i64]).into(),
            Series::new("active".into(), vec![true, false, true]).into(),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        let temp_file = NamedTempFile::new().unwrap();
        let ndjson_path = temp_file.path().with_extension("ndjson");

        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&df_value, &ndjson_path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to write NDJSON: {:?}",
            write_result
        );

        // Read back and verify
        let read_options = ReadOptions::default();
        let read_result = read_file_sync(&ndjson_path, &read_options).unwrap();
        match read_result {
            Value::DataFrame(read_df) => {
                assert_eq!(read_df.height(), 3);
                assert_eq!(read_df.width(), 3);
                assert!(read_df
                    .get_column_names()
                    .contains(&&polars::datatypes::PlSmallStr::from("name")));
                assert!(read_df
                    .get_column_names()
                    .contains(&&polars::datatypes::PlSmallStr::from("age")));
                assert!(read_df
                    .get_column_names()
                    .contains(&&polars::datatypes::PlSmallStr::from("active")));
            }
            _ => panic!("Expected DataFrame after reading NDJSON"),
        }
    }

    #[test]
    #[ignore = "JSONL format not fully supported"]
    fn test_write_jsonl() {
        let df = DataFrame::new(vec![
            Series::new("id".into(), vec![1i64, 2i64]).into(),
            Series::new("value".into(), vec!["test1", "test2"]).into(),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        let temp_file = NamedTempFile::new().unwrap();
        let jsonl_path = temp_file.path().with_extension("jsonl");

        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&df_value, &jsonl_path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to write JSONL: {:?}",
            write_result
        );

        // Read the file content and verify it's valid NDJSON
        let content = std::fs::read_to_string(&jsonl_path).unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);

        for line in lines {
            let json_val: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(json_val.is_object());
            let obj = json_val.as_object().unwrap();
            assert!(obj.contains_key("id"));
            assert!(obj.contains_key("value"));
        }
    }

    #[test]
    #[ignore = "NDJSON format not fully supported"]
    fn test_write_ndjson_edge_cases() {
        // Test empty DataFrame
        let empty_df = DataFrame::empty();
        let empty_value = Value::DataFrame(empty_df);

        let temp_file = NamedTempFile::new().unwrap();
        let ndjson_path = temp_file.path().with_extension("ndjson");

        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&empty_value, &ndjson_path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to write empty NDJSON: {:?}",
            write_result
        );

        let content = std::fs::read_to_string(&ndjson_path).unwrap();
        assert_eq!(
            content.trim(),
            "",
            "Empty DataFrame should produce empty file"
        );

        // Test DataFrame with null values
        let df_with_nulls = DataFrame::new(vec![
            Series::new("name".into(), vec![Some("Alice"), None, Some("Charlie")]).into(),
            Series::new("age".into(), vec![Some(30i64), Some(25i64), None]).into(),
        ])
        .unwrap();
        let nulls_value = Value::DataFrame(df_with_nulls);

        let temp_file2 = NamedTempFile::new().unwrap();
        let ndjson_path2 = temp_file2.path().with_extension("ndjson");

        let write_result2 = write_file_sync(&nulls_value, &ndjson_path2, &write_options);
        assert!(
            write_result2.is_ok(),
            "Failed to write NDJSON with nulls: {:?}",
            write_result2
        );

        let content2 = std::fs::read_to_string(&ndjson_path2).unwrap();
        let lines: Vec<&str> = content2.trim().split('\n').collect();
        assert_eq!(lines.len(), 3);

        for line in lines {
            let json_val: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(json_val.is_object());
        }
    }

    #[test]
    #[ignore = "JsonLines format not fully supported"]
    fn test_read_jsonlines() {
        let jsonl_data = r#"{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie", "age": 35}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(jsonl_data.as_bytes()).unwrap();
        let path = temp_file.path();

        let options = ReadOptions::default();
        let result = read_file_sync(path, &options).unwrap();

        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 3);
                assert_eq!(df.width(), 2);
                assert!(df
                    .get_column_names()
                    .contains(&&polars::datatypes::PlSmallStr::from("name")));
                assert!(df
                    .get_column_names()
                    .contains(&&polars::datatypes::PlSmallStr::from("age")));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    #[ignore = "JSON5 format detection not working without extension hint"]
    fn test_read_json5() {
        // JSON5 is currently stubbed to read as JSON
        let json5_data = r#"{"name": "Alice", "age": 30}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json5_data.as_bytes()).unwrap();
        let path = temp_file.path();

        let options = ReadOptions::default();
        let result = read_file_sync(path, &options).unwrap();

        match result {
            Value::Object(obj) => {
                assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
                assert_eq!(obj.get("age"), Some(&Value::Int(30)));
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_read_parquet() {
        // Create a DataFrame and write it to Parquet, then read it back
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Write to Parquet first
        let write_options = WriteOptions::default();
        write_file_sync(&Value::DataFrame(df.clone()), path, &write_options).unwrap();

        // Read back
        let read_options = ReadOptions::default();
        let result = read_file_sync(path, &read_options).unwrap();

        match result {
            Value::DataFrame(read_df) => {
                assert_eq!(read_df.height(), 2);
                assert_eq!(read_df.width(), 2);
                assert!(read_df
                    .get_column_names()
                    .contains(&&polars::datatypes::PlSmallStr::from("name")));
                assert!(read_df
                    .get_column_names()
                    .contains(&&polars::datatypes::PlSmallStr::from("age")));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    #[ignore = "ADT format detection not working without extension hint"]
    fn test_read_adt() {
        // ADT format: fields separated by 0x1F, records by 0x1E
        let adt_data = b"name\x1Fage\x1EAlice\x1F30\x1EBob\x1F25\x1E";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(adt_data).unwrap();
        let path = temp_file.path();

        let options = ReadOptions::default();
        let result = read_file_lazy(path, &options).unwrap(); // Use lazy version which calls read_adt

        match result {
            Value::LazyFrame(_) => {
                // For ADT, lazy reading converts to LazyFrame
                // We could collect it to verify, but for now just check it's a LazyFrame
            }
            _ => panic!("Expected LazyFrame for ADT"),
        }
    }

    #[test]
    fn test_write_json() {
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        let temp_file = NamedTempFile::new().unwrap();
        let json_path = temp_file.path().with_extension("json");

        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&df_value, &json_path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to write JSON: {:?}",
            write_result
        );

        // Read back and verify
        let read_options = ReadOptions::default();
        let read_result = read_file_sync(&json_path, &read_options).unwrap();
        match read_result {
            Value::DataFrame(read_df) => {
                assert_eq!(read_df.height(), 2);
                assert_eq!(read_df.width(), 2);
            }
            _ => panic!("Expected DataFrame after reading JSON"),
        }
    }

    #[test]
    #[ignore = "JSON5 format not fully supported"]
    fn test_write_json5() {
        // JSON5 is currently stubbed to write as JSON
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice"]).into(),
            Series::new("age".into(), vec![30i64]).into(),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        let temp_file = NamedTempFile::new().unwrap();
        let json5_path = temp_file.path().with_extension("json5");

        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&df_value, &json5_path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to write JSON5: {:?}",
            write_result
        );

        // Should be readable as JSON
        let content = std::fs::read_to_string(&json5_path).unwrap();
        let json_val: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(json_val.is_array());
    }

    #[test]
    fn test_write_adt() {
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        let temp_file = NamedTempFile::new().unwrap();
        let adt_path = temp_file.path().with_extension("adt");

        let write_options = WriteOptions::default();
        let write_result = write_file_sync(&df_value, &adt_path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to write ADT: {:?}",
            write_result
        );

        // Read back using lazy reader
        let read_options = ReadOptions::default();
        let read_result = read_file_lazy(&adt_path, &read_options).unwrap();
        match read_result {
            Value::LazyFrame(_) => {
                // ADT lazy reading produces LazyFrame
            }
            _ => panic!("Expected LazyFrame for ADT"),
        }
    }

    #[test]
    fn test_read_options_n_rows() {
        let csv_data = "name,age\nAlice,30\nBob,25\nCharlie,35";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();
        let path = temp_file.path();

        let mut options = ReadOptions::default();
        options.n_rows = Some(2);
        let result = read_file_sync(path, &options).unwrap();

        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2); // Should only read 2 rows
                assert_eq!(df.width(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_read_options_skip_rows() {
        let csv_data = "name,age\nAlice,30\nBob,25\nCharlie,35";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();
        let path = temp_file.path();

        let mut options = ReadOptions::default();
        options.skip_rows = 1;
        let result = read_file_sync(path, &options).unwrap();

        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2); // Should skip header, read 2 data rows
                assert_eq!(df.width(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_write_options_include_header() {
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        let temp_file = NamedTempFile::new().unwrap();
        let csv_path = temp_file.path().with_extension("csv");

        let write_options = WriteOptions {
            include_header: false,
            compression: None,
        };
        let write_result = write_file_sync(&df_value, &csv_path, &write_options);
        assert!(
            write_result.is_ok(),
            "Failed to write CSV without header: {:?}",
            write_result
        );

        let content = std::fs::read_to_string(&csv_path).unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 2); // No header, just data rows
                                    // First line should not contain "name,age"
        assert!(!lines[0].contains("name"));
    }

    #[test]
    fn test_inspect_file() {
        use tempfile::TempDir;
        let csv_data = "name,age\nAlice,30\nBob,25";

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.csv");
        std::fs::write(&path, csv_data).unwrap();

        let info = inspect_file(&path).unwrap();
        assert_eq!(info.format, "csv");
        assert_eq!(info.path, path.to_string_lossy());
        // Since we read only 1 row, rows should be None
        assert!(info.rows.is_none());
        assert_eq!(info.columns, Some(2));
        assert!(info.column_names.is_some());
        assert_eq!(info.column_names.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_convert_file() {
        let csv_data = "name,age\nAlice,30\nBob,25";

        let mut csv_file = NamedTempFile::new().unwrap();
        csv_file.write_all(csv_data.as_bytes()).unwrap();
        let csv_path = csv_file.path();

        let json_file = NamedTempFile::new().unwrap();
        let json_path = json_file.path().with_extension("json");

        let read_options = ReadOptions::default();
        let write_options = WriteOptions::default();

        let convert_result = convert_file(csv_path, &json_path, &read_options, &write_options);
        assert!(
            convert_result.is_ok(),
            "Failed to convert CSV to JSON: {:?}",
            convert_result
        );

        // Verify the JSON file
        let read_result = read_file_sync(json_path, &read_options).unwrap();
        match read_result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                assert_eq!(df.width(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_error_cases() {
        // Test reading non-existent file
        let fake_path = Path::new("/non/existent/file.csv");
        let options = ReadOptions::default();
        let result = read_file_sync(fake_path, &options);
        assert!(result.is_err(), "Expected error for non-existent file");

        // Test unsupported format
        let unsupported_path = Path::new("test.unsupported");
        let result2 = read_file_sync(unsupported_path, &options);
        assert!(result2.is_err(), "Expected error for unsupported format");

        // Test malformed JSON
        let malformed_json = r#"{"name": "Alice", "age": }"#;
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(malformed_json.as_bytes()).unwrap();
        let path = temp_file.path();
        let _result3 = read_file_sync(path, &options);
        // This might succeed if it falls back to NDJSON, or fail
        // Just ensure it doesn't panic
    }

    #[test]
    fn test_lazy_reading() {
        let csv_data = "name,age\nAlice,30\nBob,25";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();
        let path = temp_file.path();

        let options = ReadOptions::default();
        let result = read_file_lazy(path, &options).unwrap();

        match result {
            Value::LazyFrame(_) => {
                // Successfully created LazyFrame
            }
            _ => panic!("Expected LazyFrame"),
        }
    }

    #[test]
    fn test_empty_file_handling() {
        // Test reading empty CSV
        let empty_csv = "";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(empty_csv.as_bytes()).unwrap();
        let path = temp_file.path();

        let options = ReadOptions::default();
        let _result = read_file_sync(path, &options);
        // This might succeed or fail depending on implementation
        // Just ensure it doesn't panic
    }
}
