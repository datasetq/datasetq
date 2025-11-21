use crate::error::{Error, FormatError, Result};
use dsq_shared::value::Value;
#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
use polars::prelude::*;

use std::io::Write;

/// Options for writing data
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Whether to include header row (for CSV/TSV)
    pub include_header: bool,
    /// Whether to overwrite existing files
    pub overwrite: bool,
    /// Compression level (if supported by format)
    pub compression: Option<CompressionLevel>,
    /// Custom schema to enforce
    #[cfg(any(
        feature = "csv",
        feature = "json",
        feature = "json5",
        feature = "parquet",
        feature = "avro"
    ))]
    pub schema: Option<Schema>,
    /// Batch size for streaming writes
    pub batch_size: Option<usize>,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            include_header: true,
            overwrite: false,
            compression: None,
            #[cfg(any(
                feature = "csv",
                feature = "json",
                feature = "json5",
                feature = "parquet",
                feature = "avro"
            ))]
            schema: None,
            batch_size: None,
        }
    }
}

/// Compression levels for output formats
#[derive(Debug, Clone, Copy)]
pub enum CompressionLevel {
    /// No compression
    None,
    /// Fast compression
    Fast,
    /// Balanced compression
    Balanced,
    /// High compression
    High,
}

/// Format-specific write options
#[derive(Debug, Clone)]
pub enum FormatWriteOptions {
    /// CSV format options
    Csv {
        /// Separator character
        separator: u8,
        /// Quote character
        quote_char: Option<u8>,
        /// Line terminator
        line_terminator: Option<String>,
        /// Quote style
        quote_style: Option<String>,
        /// Null value string
        null_value: Option<String>,
        /// DateTime format
        datetime_format: Option<String>,
        /// Date format
        date_format: Option<String>,
        /// Time format
        time_format: Option<String>,
        /// Float precision
        float_precision: Option<usize>,
        /// Null values
        null_values: Option<Vec<String>>,
        /// Encoding
        encoding: CsvEncoding,
    },
    /// Parquet format options
    #[cfg(feature = "parquet")]
    Parquet {
        /// Compression type
        compression: ParquetCompression,
    },
    /// JSON format options
    Json {
        /// Whether to write lines
        lines: bool,
        /// Whether to pretty print
        pretty: bool,
    },
    /// JSON5 format options
    Json5 {
        /// Whether to write lines
        lines: bool,
        /// Whether to pretty print
        pretty: bool,
    },
    /// Avro format options
    Avro {
        /// Compression type
        compression: AvroCompression,
    },
    /// Arrow format
    Arrow,
    /// Excel format options
    Excel {
        /// Worksheet name
        worksheet_name: String,
        /// Include header
        include_header: bool,
        /// Autofit columns
        autofit: bool,
        /// Float precision
        float_precision: Option<usize>,
    },
    /// ORC format options
    Orc {
        /// Compression type
        compression: OrcCompression,
    },
}

/// ORC compression options
#[derive(Debug, Clone)]
pub enum OrcCompression {
    /// No compression
    Uncompressed,
    /// Zlib compression
    Zlib,
    /// Snappy compression
    Snappy,
    /// LZO compression
    Lzo,
    /// LZ4 compression
    Lz4,
    /// Zstandard compression
    Zstd,
}

/// CSV encoding options
#[derive(Debug, Clone)]
pub enum CsvEncoding {
    /// UTF-8 encoding
    Utf8,
    /// UTF-8 with lossy conversion
    Utf8Lossy,
}

/// Parquet compression options
#[cfg(feature = "parquet")]
#[derive(Debug, Clone)]
pub enum ParquetCompression {
    /// No compression
    Uncompressed,
    /// Snappy compression
    Snappy,
    /// Gzip compression
    Gzip,
    /// LZO compression
    Lzo,
    /// Brotli compression
    Brotli,
    /// LZ4 compression
    Lz4,
    /// Zstandard compression
    Zstd,
}

/// Avro compression options
#[derive(Debug, Clone)]
pub enum AvroCompression {
    /// No compression
    Null,
    /// Deflate compression
    Deflate,
    /// Snappy compression
    Snappy,
    /// Bzip2 compression
    Bzip2,
    /// XZ compression
    Xz,
    /// Zstandard compression
    Zstandard,
}

impl Default for FormatWriteOptions {
    fn default() -> Self {
        FormatWriteOptions::Csv {
            separator: b',',
            quote_char: Some(b'"'),
            line_terminator: None,
            quote_style: None,
            null_value: None,
            datetime_format: None,
            date_format: None,
            time_format: None,
            float_precision: None,
            null_values: None,
            encoding: CsvEncoding::Utf8,
        }
    }
}

/// Serialize CSV data to a writer
#[cfg(feature = "csv")]
pub fn serialize_csv<W: Write>(
    writer: W,
    value: &Value,
    options: &WriteOptions,
    format_options: &FormatWriteOptions,
) -> Result<()> {
    crate::csv::serialize_csv(writer, value, options, format_options)
}

/// Serialize JSON data to a writer
#[cfg(feature = "json")]
pub fn serialize_json<W: Write>(
    writer: W,
    value: &Value,
    options: &WriteOptions,
    format_options: &FormatWriteOptions,
) -> Result<()> {
    crate::json::serialize_json(writer, value, options, format_options)
}

/// Serialize JSON5 data to a writer
#[cfg(all(feature = "json5", feature = "json"))]
pub fn serialize_json5<W: Write>(
    mut writer: W,
    value: &Value,
    _options: &WriteOptions,
    format_options: &FormatWriteOptions,
) -> Result<()> {
    use crate::json::row_to_json_value;

    let df = match value {
        Value::DataFrame(df) => df.clone(),
        Value::LazyFrame(lf) => (*lf).clone().collect().map_err(Error::from)?,
        _ => {
            return Err(Error::operation(
                "Expected DataFrame for JSON5 serialization",
            ));
        }
    };

    let json5_opts = match format_options {
        FormatWriteOptions::Json5 { lines, pretty } => (*lines, *pretty),
        _ => (false, false),
    };

    let column_names = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    if json5_opts.0 {
        // JSON5 Lines
        for i in 0..df.height() {
            let row = df.get_row(i).map_err(Error::from)?;
            let json_value = row_to_json_value(&row.0, &column_names);
            let json5_str = json5::to_string(&json_value)
                .map_err(|e| Error::Format(FormatError::SerializationError(e.to_string())))?;
            writer
                .write_all(json5_str.as_bytes())
                .map_err(Error::from)?;
            writer.write_all(b"\n").map_err(Error::from)?;
        }
    } else {
        // Regular JSON5 array
        let mut rows = Vec::new();
        for i in 0..df.height() {
            let row = df.get_row(i).map_err(Error::from)?;
            rows.push(row_to_json_value(&row.0, &column_names));
        }
        let json5_str = json5::to_string(&rows)
            .map_err(|e| Error::Format(FormatError::SerializationError(e.to_string())))?;
        writer
            .write_all(json5_str.as_bytes())
            .map_err(Error::from)?;
    }

    Ok(())
}

/// Serialize Parquet data to a writer
#[cfg(feature = "parquet")]
pub fn serialize_parquet<W: Write>(
    writer: W,
    value: &Value,
    _options: &WriteOptions,
    format_options: &FormatWriteOptions,
) -> Result<()> {
    let df = match value {
        Value::DataFrame(df) => df.clone(),
        Value::LazyFrame(lf) => (*lf).clone().collect().map_err(Error::from)?,
        _ => {
            return Err(Error::operation(
                "Expected DataFrame for Parquet serialization",
            ));
        }
    };

    let parquet_opts = match format_options {
        FormatWriteOptions::Parquet { compression } => compression,
        _ => &ParquetCompression::Snappy,
    };

    let compression = match parquet_opts {
        ParquetCompression::Uncompressed => polars::prelude::ParquetCompression::Uncompressed,
        ParquetCompression::Snappy => polars::prelude::ParquetCompression::Snappy,
        ParquetCompression::Gzip => polars::prelude::ParquetCompression::Gzip(None),
        ParquetCompression::Lzo => polars::prelude::ParquetCompression::Lzo,
        ParquetCompression::Brotli => polars::prelude::ParquetCompression::Brotli(None),
        ParquetCompression::Lz4 => polars::prelude::ParquetCompression::Lz4Raw,
        ParquetCompression::Zstd => polars::prelude::ParquetCompression::Zstd(None),
    };

    let parquet_writer = ParquetWriter::new(writer).with_compression(compression);

    parquet_writer
        .finish(&mut df.clone())
        .map_err(Error::from)?;
    Ok(())
}

/// Serialize ADT (ASCII Delimited Text) data to a writer
#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
pub fn serialize_adt<W: Write>(
    writer: W,
    value: &Value,
    options: &WriteOptions,
    format_options: &FormatWriteOptions,
) -> Result<()> {
    crate::adt::serialize_adt(writer, value, options, format_options)
}

/// Serialize data to a writer based on format
#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
pub fn serialize<W: Write>(
    writer: W,
    value: &Value,
    format: DataFormat,
    options: &WriteOptions,
    format_options: &FormatWriteOptions,
) -> Result<()> {
    match format {
        #[cfg(feature = "csv")]
        DataFormat::Csv => serialize_csv(writer, value, options, format_options),
        #[cfg(not(feature = "csv"))]
        DataFormat::Csv => Err(Error::Format(FormatError::UnsupportedFeature(
            "CSV not supported in this build".to_string(),
        ))),
        #[cfg(feature = "csv")]
        DataFormat::Tsv => {
            // For TSV, override the separator in format_options
            let tsv_options = match format_options {
                FormatWriteOptions::Csv {
                    quote_char,
                    line_terminator,
                    quote_style,
                    null_value,
                    datetime_format,
                    date_format,
                    time_format,
                    float_precision,
                    null_values,
                    encoding,
                    ..
                } => FormatWriteOptions::Csv {
                    separator: b'\t',
                    quote_char: *quote_char,
                    line_terminator: line_terminator.clone(),
                    quote_style: quote_style.clone(),
                    null_value: null_value.clone(),
                    datetime_format: datetime_format.clone(),
                    date_format: date_format.clone(),
                    time_format: time_format.clone(),
                    float_precision: *float_precision,
                    null_values: null_values.clone(),
                    encoding: encoding.clone(),
                },
                _ => FormatWriteOptions::Csv {
                    separator: b'\t',
                    quote_char: Some(b'"'),
                    line_terminator: None,
                    quote_style: None,
                    null_value: None,
                    datetime_format: None,
                    date_format: None,
                    time_format: None,
                    float_precision: None,
                    null_values: None,
                    encoding: CsvEncoding::Utf8,
                },
            };
            serialize_csv(writer, value, options, &tsv_options)
        }
        #[cfg(not(feature = "csv"))]
        DataFormat::Tsv => Err(Error::Format(FormatError::UnsupportedFeature(
            "TSV not supported in this build".to_string(),
        ))),
        #[cfg(feature = "json")]
        DataFormat::Json | DataFormat::JsonLines => {
            serialize_json(writer, value, options, format_options)
        }
        #[cfg(not(feature = "json"))]
        DataFormat::Json | DataFormat::JsonLines => Err(Error::Format(
            FormatError::UnsupportedFeature("JSON not supported in this build".to_string()),
        )),
        #[cfg(all(feature = "json5", feature = "json"))]
        DataFormat::Json5 => serialize_json5(writer, value, options, format_options),
        #[cfg(not(all(feature = "json5", feature = "json")))]
        DataFormat::Json5 => Err(Error::Format(FormatError::UnsupportedFeature(
            "JSON5 not supported in this build".to_string(),
        ))),
        #[cfg(feature = "parquet")]
        DataFormat::Parquet => serialize_parquet(writer, value, options, format_options),
        #[cfg(not(feature = "parquet"))]
        DataFormat::Parquet => Err(Error::Format(FormatError::UnsupportedFeature(
            "Parquet not supported in this build".to_string(),
        ))),
        DataFormat::Arrow => Err(Error::Format(FormatError::UnsupportedFeature(
            "Arrow serialization not yet implemented".to_string(),
        ))),
        DataFormat::Avro => Err(Error::Format(FormatError::UnsupportedFeature(
            "Avro serialization not yet implemented".to_string(),
        ))),
        DataFormat::Excel => Err(Error::Format(FormatError::UnsupportedFeature(
            "Excel serialization not yet implemented".to_string(),
        ))),
        DataFormat::Orc => Err(Error::Format(FormatError::UnsupportedFeature(
            "ORC serialization not yet implemented".to_string(),
        ))),
        _ => Err(Error::Format(FormatError::Unknown(format.to_string()))),
    }
}

/// Legacy compatibility - these will be removed in future versions
pub use crate::format::DataFormat;

/// Trait for writing data to various formats
#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
pub trait DataWriter {
    /// Write a value with options
    fn write(&mut self, value: &Value, options: &WriteOptions) -> Result<()>;
    /// Get the data format
    fn format(&self) -> DataFormat;
}

/// File-based data writer
#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
pub struct FileWriter {
    path: String,
    format: DataFormat,
    format_options: FormatWriteOptions,
}

#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
impl FileWriter {
    /// Create a new file writer with automatic format detection
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let format = crate::format::DataFormat::from_path(path_ref)?;

        Ok(Self {
            path: path_ref.to_string_lossy().to_string(),
            format,
            format_options: Self::default_format_options_for_format(format),
        })
    }

    /// Create a new file writer with explicit format
    pub fn with_format<P: AsRef<std::path::Path>>(path: P, format: DataFormat) -> Self {
        Self {
            path: path.as_ref().to_string_lossy().to_string(),
            format,
            format_options: Self::default_format_options_for_format(format),
        }
    }

    /// Get default format options for a given format
    fn default_format_options_for_format(format: DataFormat) -> FormatWriteOptions {
        match format {
            DataFormat::Csv => FormatWriteOptions::Csv {
                separator: b',',
                quote_char: Some(b'"'),
                line_terminator: None,
                quote_style: None,
                null_value: None,
                datetime_format: None,
                date_format: None,
                time_format: None,
                float_precision: None,
                null_values: None,
                encoding: CsvEncoding::Utf8,
            },
            DataFormat::Tsv => FormatWriteOptions::Csv {
                separator: b'\t',
                quote_char: Some(b'"'),
                line_terminator: None,
                quote_style: None,
                null_value: None,
                datetime_format: None,
                date_format: None,
                time_format: None,
                float_precision: None,
                null_values: None,
                encoding: CsvEncoding::Utf8,
            },
            DataFormat::Json => FormatWriteOptions::Json {
                lines: false,
                pretty: false,
            },
            DataFormat::JsonLines => FormatWriteOptions::Json {
                lines: true,
                pretty: false,
            },
            DataFormat::Json5 => FormatWriteOptions::Json5 {
                lines: false,
                pretty: false,
            },
            #[cfg(feature = "parquet")]
            DataFormat::Parquet => FormatWriteOptions::Parquet {
                compression: ParquetCompression::Snappy,
            },
            #[cfg(not(feature = "parquet"))]
            DataFormat::Parquet => FormatWriteOptions::Json {
                lines: false,
                pretty: false,
            },
            DataFormat::Arrow => FormatWriteOptions::Arrow,
            DataFormat::Avro => FormatWriteOptions::Avro {
                compression: AvroCompression::Null,
            },
            _ => FormatWriteOptions::default(),
        }
    }

    /// Set format-specific options
    pub fn with_format_options(mut self, options: FormatWriteOptions) -> Self {
        self.format_options = options;
        self
    }

    /// Check if file exists and handle overwrite logic
    fn check_overwrite(&self, options: &WriteOptions) -> Result<()> {
        use std::path::Path;
        let path = Path::new(&self.path);
        if path.exists() && !options.overwrite {
            return Err(Error::operation(format!(
                "File {} already exists. Use --overwrite to replace it.",
                self.path
            )));
        }
        Ok(())
    }
}

#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
impl DataWriter for FileWriter {
    fn write(&mut self, value: &Value, options: &WriteOptions) -> Result<()> {
        use std::fs::File;
        use std::io::BufWriter;

        self.check_overwrite(options)?;
        let file = File::create(&self.path)?;
        let writer = BufWriter::new(file);
        serialize(writer, value, self.format, options, &self.format_options)
    }

    fn format(&self) -> DataFormat {
        self.format
    }
}

/// Writer for in-memory output
#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
pub struct MemoryWriter {
    buffer: Vec<u8>,
    format: DataFormat,
    format_options: FormatWriteOptions,
}

#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
impl MemoryWriter {
    /// Create a new memory writer
    pub fn new(format: DataFormat) -> Self {
        Self {
            buffer: Vec::new(),
            format,
            format_options: FormatWriteOptions::default(),
        }
    }

    /// Set format-specific options
    pub fn with_format_options(mut self, options: FormatWriteOptions) -> Self {
        self.format_options = options;
        self
    }

    /// Get the written data
    pub fn into_inner(self) -> Vec<u8> {
        self.buffer
    }

    /// Get a reference to the written data
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }
}

#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
impl DataWriter for MemoryWriter {
    fn write(&mut self, value: &Value, options: &WriteOptions) -> Result<()> {
        use std::io::Cursor;
        let cursor = Cursor::new(&mut self.buffer);
        serialize(cursor, value, self.format, options, &self.format_options)
    }

    fn format(&self) -> DataFormat {
        self.format
    }
}

/// Create a data writer to a file path
#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
pub fn to_path<P: AsRef<std::path::Path>>(path: P) -> Result<FileWriter> {
    FileWriter::new(path)
}

/// Create a data writer to a file path with format
#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
pub fn to_path_with_format<P: AsRef<std::path::Path>>(path: P, format: DataFormat) -> FileWriter {
    FileWriter::with_format(path, format)
}

/// Create a data writer to memory
#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
pub fn to_memory(format: DataFormat) -> MemoryWriter {
    MemoryWriter::new(format)
}

#[cfg(test)]
#[cfg(any(
    feature = "csv",
    feature = "json",
    feature = "json5",
    feature = "parquet",
    feature = "avro"
))]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;
    use std::io::Cursor;

    fn create_test_dataframe() -> DataFrame {
        let s1 = Series::new("name", &["Alice", "Bob", "Charlie"]);
        let s2 = Series::new("age", &[25i64, 30, 35]);
        let s3 = Series::new("active", &[true, false, true]);
        DataFrame::new(vec![s1, s2, s3]).unwrap()
    }

    #[test]
    fn test_serialize_csv() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);
        let options = WriteOptions::default();
        let format_options = FormatWriteOptions::default();

        let mut buffer = Vec::new();
        let result = serialize_csv(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_ok());

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("name,age,active"));
        assert!(output.contains("Alice,25,true"));
        assert!(output.contains("Bob,30,false"));
        assert!(output.contains("Charlie,35,true"));
    }

    #[test]
    fn test_serialize_csv_with_header_false() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);
        let options = WriteOptions {
            include_header: false,
            ..Default::default()
        };
        let format_options = FormatWriteOptions::default();

        let mut buffer = Vec::new();
        let result = serialize_csv(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_ok());

        let output = String::from_utf8(buffer).unwrap();
        assert!(!output.contains("name,age,active"));
        assert!(output.contains("Alice,25,true"));
    }

    #[test]
    fn test_serialize_csv_custom_separator() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);
        let options = WriteOptions::default();
        let format_options = FormatWriteOptions::Csv {
            separator: b';',
            quote_char: Some(b'"'),
            line_terminator: None,
            quote_style: None,
            null_value: None,
            datetime_format: None,
            date_format: None,
            time_format: None,
            float_precision: None,
            null_values: None,
            encoding: CsvEncoding::Utf8,
        };

        let mut buffer = Vec::new();
        let result = serialize_csv(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_ok());

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("name;age;active"));
        assert!(output.contains("Alice;25;true"));
    }

    #[test]
    fn test_serialize_csv_lazy_frame() {
        let df = create_test_dataframe();
        let lf = df.clone().lazy();
        let value = Value::LazyFrame(Box::new(lf));
        let options = WriteOptions::default();
        let format_options = FormatWriteOptions::default();

        let mut buffer = Vec::new();
        let result = serialize_csv(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_ok());

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("Alice,25,true"));
    }

    #[test]
    fn test_serialize_csv_wrong_value_type() {
        let value = Value::String("not a dataframe".to_string());
        let options = WriteOptions::default();
        let format_options = FormatWriteOptions::default();

        let mut buffer = Vec::new();
        let result = serialize_csv(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected DataFrame"));
    }

    #[test]
    fn test_serialize_json() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);
        let options = WriteOptions::default();
        let format_options = FormatWriteOptions::Json {
            lines: false,
            pretty: false,
        };

        let mut buffer = Vec::new();
        let result = serialize_json(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_ok());

        let output = String::from_utf8(buffer).unwrap();
        // Check that all expected key-value pairs are present (order may vary)
        assert!(output.contains(r#""name":"Alice""#));
        assert!(output.contains(r#""age":25"#));
        assert!(output.contains(r#""active":true"#));
        assert!(output.contains(r#""name":"Bob""#));
        assert!(output.contains(r#""age":30"#));
        assert!(output.contains(r#""active":false"#));
    }

    #[test]
    fn test_serialize_json_lines() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);
        let options = WriteOptions::default();
        let format_options = FormatWriteOptions::Json {
            lines: true,
            pretty: false,
        };

        let mut buffer = Vec::new();
        let result = serialize_json(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_ok());

        let output = String::from_utf8(buffer).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 3);
        // Check that each line contains the expected key-value pairs (order may vary)
        assert!(lines[0].contains(r#""name":"Alice""#));
        assert!(lines[0].contains(r#""age":25"#));
        assert!(lines[0].contains(r#""active":true"#));
        assert!(lines[1].contains(r#""name":"Bob""#));
        assert!(lines[1].contains(r#""age":30"#));
        assert!(lines[1].contains(r#""active":false"#));
        assert!(lines[2].contains(r#""name":"Charlie""#));
        assert!(lines[2].contains(r#""age":35"#));
        assert!(lines[2].contains(r#""active":true"#));
    }

    #[test]
    fn test_serialize_json_pretty() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);
        let options = WriteOptions::default();
        let format_options = FormatWriteOptions::Json {
            lines: false,
            pretty: true,
        };

        let mut buffer = Vec::new();
        let result = serialize_json(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_ok());

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("  \"name\": \"Alice\""));
        assert!(output.contains("  \"age\": 25"));
    }

    #[test]
    fn test_serialize_json5() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);
        let options = WriteOptions::default();
        let format_options = FormatWriteOptions::Json5 {
            lines: false,
            pretty: false,
        };

        let mut buffer = Vec::new();
        let result = serialize_json5(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_ok());

        let output = String::from_utf8(buffer).unwrap();
        // Check that all expected key-value pairs are present (order may vary)
        assert!(output.contains(r#""name":"Alice""#));
        assert!(output.contains(r#""age":25"#));
        assert!(output.contains(r#""active":true"#));
    }

    #[test]
    fn test_serialize_json5_lines() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);
        let options = WriteOptions::default();
        let format_options = FormatWriteOptions::Json5 {
            lines: true,
            pretty: false,
        };

        let mut buffer = Vec::new();
        let result = serialize_json5(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_ok());

        let output = String::from_utf8(buffer).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 3);
        // Check that each line contains the expected key-value pairs (order may vary)
        assert!(lines[0].contains(r#""name":"Alice""#));
        assert!(lines[0].contains(r#""age":25"#));
        assert!(lines[0].contains(r#""active":true"#));
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn test_serialize_parquet() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);
        let options = WriteOptions::default();
        let format_options = FormatWriteOptions::Parquet {
            compression: ParquetCompression::Uncompressed,
        };

        let mut buffer = Vec::new();
        let result = serialize_parquet(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_ok());
        assert!(!buffer.is_empty());
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn test_serialize_parquet_compression() {
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);
        let options = WriteOptions::default();
        let format_options = FormatWriteOptions::Parquet {
            compression: ParquetCompression::Snappy,
        };

        let mut buffer = Vec::new();
        let result = serialize_parquet(Cursor::new(&mut buffer), &value, &options, &format_options);
        assert!(result.is_ok());
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_row_to_json_value() {
        use crate::json::row_to_json_value;

        let df = create_test_dataframe();
        let row = df.get_row(0).unwrap();
        let column_names = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let json_value = row_to_json_value(&row.0, &column_names);

        if let serde_json::Value::Object(map) = json_value {
            assert_eq!(
                map.get("name"),
                Some(&serde_json::Value::String("Alice".to_string()))
            );
            assert_eq!(map.get("age"), Some(&serde_json::Value::Number(25.into())));
            assert_eq!(map.get("active"), Some(&serde_json::Value::Bool(true)));
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_row_to_json_value_with_nulls() {
        use crate::json::row_to_json_value;

        let s1 = Series::new("name", &["Alice", "Bob"]);
        let s2 = Series::new("age", &[Some(25i64), None]);
        let df = DataFrame::new(vec![s1, s2]).unwrap();
        let row = df.get_row(1).unwrap();
        let column_names = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let json_value = row_to_json_value(&row.0, &column_names);

        if let serde_json::Value::Object(map) = json_value {
            assert_eq!(
                map.get("name"),
                Some(&serde_json::Value::String("Bob".to_string()))
            );
            assert_eq!(map.get("age"), Some(&serde_json::Value::Null));
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_write_options_default() {
        let opts = WriteOptions::default();
        assert!(opts.include_header);
        assert!(!opts.overwrite);
        assert!(opts.compression.is_none());
        assert!(opts.schema.is_none());
        assert!(opts.batch_size.is_none());
    }

    #[test]
    fn test_format_write_options_default() {
        let opts = FormatWriteOptions::default();
        match opts {
            FormatWriteOptions::Csv {
                separator,
                quote_char,
                line_terminator,
                quote_style,
                null_value,
                datetime_format,
                date_format,
                time_format,
                float_precision,
                null_values,
                encoding,
            } => {
                assert_eq!(separator, b',');
                assert_eq!(quote_char, Some(b'"'));
                assert!(line_terminator.is_none());
                assert!(quote_style.is_none());
                assert!(null_value.is_none());
                assert!(datetime_format.is_none());
                assert!(date_format.is_none());
                assert!(time_format.is_none());
                assert!(float_precision.is_none());
                assert!(null_values.is_none());
                assert!(matches!(encoding, CsvEncoding::Utf8));
            }
            _ => panic!("Expected Csv"),
        }
    }
}
