use crate::error::{Error, Result};
use crate::format::DataFormat;
use crate::reader::options::{FormatReadOptions, ReadOptions};
use dsq_shared::value::Value;
use polars::prelude::*;

/// Reader for in-memory data
pub struct MemoryReader {
    data: Vec<u8>,
    format: DataFormat,
    format_options: FormatReadOptions,
}

impl MemoryReader {
    /// Create a new memory reader
    pub fn new(data: Vec<u8>, format: DataFormat) -> Self {
        Self {
            data,
            format,
            format_options: FormatReadOptions::default(),
        }
    }

    /// Set format-specific options
    pub fn with_format_options(mut self, options: FormatReadOptions) -> Self {
        self.format_options = options;
        self
    }
}

impl crate::reader::data_reader::DataReader for MemoryReader {
    fn read(&mut self, options: &ReadOptions) -> Result<Value> {
        use std::io::Cursor;

        match self.format {
            DataFormat::Csv | DataFormat::Tsv => {
                let cursor = Cursor::new(&self.data);
                let separator = if self.format == DataFormat::Tsv {
                    b'\t'
                } else {
                    b','
                };

                let parse_options = CsvParseOptions::default().with_separator(separator);

                let mut read_options = polars::prelude::CsvReadOptions::default()
                    .with_parse_options(parse_options)
                    .with_has_header(true);

                if let Some(max_rows) = options.max_rows {
                    read_options = read_options.with_n_rows(Some(max_rows));
                }

                let reader = CsvReader::new(cursor).with_options(read_options);

                let df = reader.finish().map_err(Error::from)?;

                if options.lazy {
                    Ok(Value::LazyFrame(Box::new(df.lazy())))
                } else {
                    Ok(Value::DataFrame(df))
                }
            }
            DataFormat::Json | DataFormat::JsonLines => {
                let json_val: serde_json::Value =
                    serde_json::from_slice(&self.data).map_err(|e| {
                        Error::Format(crate::error::FormatError::SerializationError(format!(
                            "Invalid JSON: {}",
                            e
                        )))
                    })?;

                let value = Value::from_json(json_val);
                let df = value.to_dataframe()?;

                if options.lazy {
                    Ok(Value::LazyFrame(Box::new(df.lazy())))
                } else {
                    Ok(Value::DataFrame(df))
                }
            }
            _ => Err(Error::Format(
                crate::error::FormatError::UnsupportedFeature(format!(
                    "{} format not supported for memory reading",
                    self.format
                )),
            )),
        }
    }

    fn format(&self) -> DataFormat {
        self.format
    }
}
