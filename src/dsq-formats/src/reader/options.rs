use crate::writer::CsvEncoding;

/// Options for reading data
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Maximum number of rows to read (None for all)
    pub max_rows: Option<usize>,
    /// Whether to infer schema from data
    pub infer_schema: bool,
    /// Number of rows to use for schema inference
    pub infer_schema_length: Option<usize>,
    /// Whether to use lazy evaluation
    pub lazy: bool,
    /// Custom schema to apply
    #[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
    pub schema: Option<polars::prelude::Schema>,
    /// Skip first N rows
    pub skip_rows: usize,
    /// Column names to select (None for all)
    pub columns: Option<Vec<String>>,
    /// Whether to parse dates
    pub parse_dates: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            max_rows: None,
            infer_schema: true,
            infer_schema_length: Some(1000),
            lazy: false,
            #[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
            schema: None,
            skip_rows: 0,
            columns: None,
            parse_dates: true,
        }
    }
}

/// Format-specific read options
#[derive(Debug, Clone)]
pub enum FormatReadOptions {
    /// CSV format options
    Csv {
        /// Separator character
        separator: u8,
        /// Whether the file has a header row
        has_header: bool,
        /// Quote character
        quote_char: Option<u8>,
        /// Comment character
        comment_char: Option<u8>,
        /// Null values
        null_values: Option<Vec<String>>,
        /// Encoding
        encoding: CsvEncoding,
    },
    /// Parquet format options
    Parquet {
        /// Whether to read in parallel
        parallel: bool,
        /// Whether to use statistics
        use_statistics: bool,
        /// Columns to read
        columns: Option<Vec<String>>,
    },
    /// JSON format options
    Json {
        /// Whether to read lines
        lines: bool,
        /// Whether to ignore errors
        ignore_errors: bool,
    },
    /// JSON5 format options
    Json5 {
        /// Whether to read lines
        lines: bool,
        /// Whether to ignore errors
        ignore_errors: bool,
    },
    /// Avro format options
    Avro {
        /// Columns to read
        columns: Option<Vec<String>>,
    },
    /// Arrow format options
    Arrow {
        /// Columns to read
        columns: Option<Vec<String>>,
    },
}

impl Default for FormatReadOptions {
    fn default() -> Self {
        FormatReadOptions::Csv {
            separator: b',',
            has_header: true,
            quote_char: Some(b'"'),
            comment_char: None,
            null_values: None,
            encoding: CsvEncoding::Utf8,
        }
    }
}
