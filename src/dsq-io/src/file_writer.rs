use crate::{Error, Result};
use dsq_formats::{format::DataFormat, serialize, FormatWriteOptions, WriteOptions};
use dsq_shared::value::Value;
use polars::prelude::*;

use std::path::Path;

use super::traits::DataWriter;

/// File-based data writer
pub struct FileWriter {
    path: String,
    format: DataFormat,
    format_options: FormatWriteOptions,
}

impl FileWriter {
    /// Create a new file writer with automatic format detection
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let format = DataFormat::from_path(path_ref)?;

        Ok(Self {
            path: path_ref.to_string_lossy().to_string(),
            format,
            format_options: Self::default_format_options_for_format(format),
        })
    }

    /// Create a new file writer with explicit format
    pub fn with_format<P: AsRef<Path>>(path: P, format: DataFormat) -> Self {
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
                line_terminator: Some("\n".to_string()),
                quote_style: Some("necessary".to_string()),
                null_value: Some("".to_string()),
                datetime_format: None,
                date_format: None,
                time_format: None,
                float_precision: None,
                null_values: None,
                encoding: dsq_formats::CsvEncoding::Utf8,
            },
            DataFormat::Tsv => FormatWriteOptions::Csv {
                separator: b'\t',
                quote_char: Some(b'"'),
                line_terminator: Some("\n".to_string()),
                quote_style: Some("necessary".to_string()),
                null_value: Some("".to_string()),
                datetime_format: None,
                date_format: None,
                time_format: None,
                float_precision: None,
                null_values: None,
                encoding: dsq_formats::CsvEncoding::Utf8,
            },
            DataFormat::Adt => FormatWriteOptions::Csv {
                separator: 28u8, // ASCII FS (File Separator)
                quote_char: Some(b'"'),
                line_terminator: Some("\n".to_string()),
                quote_style: Some("necessary".to_string()),
                null_value: Some("".to_string()),
                datetime_format: None,
                date_format: None,
                time_format: None,
                float_precision: None,
                null_values: None,
                encoding: dsq_formats::CsvEncoding::Utf8,
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
            DataFormat::Parquet => FormatWriteOptions::Parquet {
                compression: dsq_formats::ParquetCompression::Snappy,
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
        let path = Path::new(&self.path);
        if path.exists() && !options.overwrite {
            return Err(Error::Other(format!(
                "File {} already exists. Use --overwrite to replace it.",
                self.path
            )));
        }
        Ok(())
    }
}

impl DataWriter for FileWriter {
    fn write(&mut self, value: &Value, options: &WriteOptions) -> Result<()> {
        use std::fs::File;
        use std::io::BufWriter;

        self.check_overwrite(options)?;
        let file = File::create(&self.path)?;
        let writer = BufWriter::new(file);
        serialize(writer, value, self.format, options, &self.format_options)?;
        Ok(())
    }

    fn write_lazy(&mut self, lf: &LazyFrame, options: &WriteOptions) -> Result<()> {
        // For now, collect lazy frames and use regular write
        let df = lf.clone().collect().map_err(Error::from)?;
        self.write(&Value::DataFrame(df), options)
    }

    fn supports_streaming(&self) -> bool {
        matches!(
            self.format,
            DataFormat::Csv | DataFormat::Tsv | DataFormat::JsonLines
        )
    }

    fn format(&self) -> DataFormat {
        self.format
    }
}

/// Create a writer for a file path with automatic format detection
pub fn to_path<P: AsRef<Path>>(path: P) -> Result<FileWriter> {
    FileWriter::new(path)
}

/// Create a writer for a file path with explicit format
pub fn to_path_with_format<P: AsRef<Path>>(path: P, format: DataFormat) -> FileWriter {
    FileWriter::with_format(path, format)
}
