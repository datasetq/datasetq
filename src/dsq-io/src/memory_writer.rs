use crate::Result;
use dsq_formats::{format::DataFormat, serialize, FormatWriteOptions, WriteOptions};
use dsq_shared::value::Value;
use std::io::Cursor;

use super::traits::DataWriter;

/// Writer for in-memory output
pub struct MemoryWriter {
    buffer: Vec<u8>,
    format: DataFormat,
    format_options: FormatWriteOptions,
}

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

impl DataWriter for MemoryWriter {
    fn write(&mut self, value: &Value, options: &WriteOptions) -> Result<()> {
        let cursor = Cursor::new(&mut self.buffer);
        serialize(cursor, value, self.format, options, &self.format_options)?;
        Ok(())
    }

    fn format(&self) -> DataFormat {
        self.format
    }
}

/// Create a writer for in-memory output
pub fn to_memory(format: DataFormat) -> MemoryWriter {
    MemoryWriter::new(format)
}
