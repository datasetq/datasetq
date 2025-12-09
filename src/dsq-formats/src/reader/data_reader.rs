use crate::error::Result;
use crate::format::DataFormat;
use crate::reader::options::ReadOptions;
use dsq_shared::value::Value;
use polars::prelude::*;

/// Trait for reading data from various sources
pub trait DataReader {
    /// Read data into a DataFrame
    fn read(&mut self, options: &ReadOptions) -> Result<Value>;

    /// Read data into a LazyFrame (if supported)
    fn read_lazy(&mut self, options: &ReadOptions) -> Result<LazyFrame> {
        // Default implementation: read eagerly then convert to lazy
        match self.read(options)? {
            Value::DataFrame(df) => Ok(df.lazy()),
            Value::LazyFrame(lf) => Ok(*lf),
            _ => Err(crate::error::Error::operation(
                "Expected DataFrame or LazyFrame from reader",
            )),
        }
    }

    /// Check if the reader supports lazy evaluation
    fn supports_lazy(&self) -> bool {
        false
    }

    /// Get the detected or specified format
    fn format(&self) -> DataFormat;

    /// Peek at the first few rows without consuming the reader
    fn peek(&mut self, rows: usize) -> Result<DataFrame> {
        let options = ReadOptions {
            max_rows: Some(rows),
            ..Default::default()
        };
        match self.read(&options)? {
            Value::DataFrame(df) => Ok(df),
            Value::LazyFrame(lf) => lf.collect().map_err(crate::error::Error::from),
            _ => Err(crate::error::Error::operation(
                "Expected DataFrame from peek",
            )),
        }
    }
}
