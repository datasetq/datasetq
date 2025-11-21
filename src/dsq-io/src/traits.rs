use crate::{Error, Result};
use dsq_formats::{format::DataFormat, WriteOptions};
use dsq_shared::value::Value;
use polars::prelude::*;

/// Trait for writing data to various destinations
pub trait DataWriter {
    /// Write a DataFrame to the destination
    fn write(&mut self, value: &Value, options: &WriteOptions) -> Result<()>;

    /// Write a LazyFrame to the destination (if supported)
    fn write_lazy(&mut self, lf: &LazyFrame, options: &WriteOptions) -> Result<()> {
        // Default implementation: collect then write
        let df = lf.clone().collect().map_err(Error::from)?;
        self.write(&Value::DataFrame(df), options)
    }

    /// Write streaming data (if supported)
    fn write_streaming<I>(&mut self, _iter: I, _options: &WriteOptions) -> Result<()>
    where
        I: Iterator<Item = Result<Value>>,
    {
        Err(Error::Other("Streaming write not supported".to_string()))
    }

    /// Check if the writer supports streaming
    fn supports_streaming(&self) -> bool {
        false
    }

    /// Get the output format
    fn format(&self) -> DataFormat;

    /// Finalize the write operation (flush buffers, etc.)
    fn finalize(&mut self) -> Result<()> {
        Ok(())
    }
}
