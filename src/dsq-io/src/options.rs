/// Options for writing data files
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Whether to include header row (for CSV/TSV)
    pub include_header: bool,
    /// Whether to overwrite existing files
    pub overwrite: bool,
    /// Compression level (if supported by format)
    pub compression: Option<CompressionLevel>,
    /// Custom schema to enforce
    pub schema: Option<polars::prelude::Schema>,
    /// Batch size for streaming writes
    pub batch_size: Option<usize>,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            include_header: true,
            overwrite: false,
            compression: None,
            schema: None,
            batch_size: None,
        }
    }
}

/// Compression levels for output formats
#[derive(Debug, Clone, Copy)]
pub enum CompressionLevel {
    None,
    Fast,
    Balanced,
    High,
}

/// Format-specific write options
#[derive(Debug, Clone)]
pub enum FormatWriteOptions {
    Csv {
        separator: u8,
        quote_char: u8,
        line_terminator: String,
        quote_style: DsQuoteStyle,
        null_value: String,
        datetime_format: Option<String>,
        date_format: Option<String>,
        time_format: Option<String>,
        float_precision: Option<usize>,
    },
    Parquet {
        compression: DsParquetCompression,
        statistics: bool,
        row_group_size: Option<usize>,
        data_pagesize_limit: Option<usize>,
    },
    Json {
        lines: bool,
        pretty: bool,
        maintain_order: bool,
    },
    Json5 {
        lines: bool,
        pretty: bool,
        maintain_order: bool,
    },
    Excel {
        worksheet_name: String,
        include_header: bool,
        autofit: bool,
        float_precision: Option<usize>,
    },
    Arrow {
        compression: Option<DsIpcCompression>,
    },
    Avro {
        compression: DsAvroCompression,
    },
    Orc {
        compression: DsOrcCompression,
    },
}

#[derive(Debug, Clone)]
pub enum DsQuoteStyle {
    Always,
    Necessary,
    NonNumeric,
    Never,
}

#[derive(Debug, Clone)]
pub enum DsParquetCompression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone)]
pub enum DsIpcCompression {
    Uncompressed,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone)]
pub enum DsAvroCompression {
    Uncompressed,
    Deflate,
    Snappy,
}

#[derive(Debug, Clone)]
pub enum DsOrcCompression {
    Uncompressed,
    Zlib,
    Snappy,
    Lzo,
    Lz4,
    Zstd,
}

impl Default for FormatWriteOptions {
    fn default() -> Self {
        FormatWriteOptions::Csv {
            separator: b',',
            quote_char: b'"',
            line_terminator: "\n".to_string(),
            quote_style: DsQuoteStyle::Necessary,
            null_value: "".to_string(),
            datetime_format: None,
            date_format: None,
            time_format: None,
            float_precision: None,
        }
    }
}
