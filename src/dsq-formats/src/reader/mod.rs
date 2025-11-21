// Include all submodules
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
mod data_reader;
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
mod dispatch;
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
mod file_reader;
/// JSON parsing utilities
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub mod json_utils;
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
mod memory_reader;
/// Reader options and configuration
pub mod options;

// Re-export all components
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub use data_reader::DataReader;
#[cfg(feature = "parquet")]
pub use dispatch::deserialize_parquet;
#[cfg(feature = "csv")]
pub use dispatch::deserialize_csv;
#[cfg(feature = "json")]
pub use dispatch::deserialize_json;
#[cfg(feature = "json5")]
pub use dispatch::deserialize_json5;
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub use dispatch::{deserialize, deserialize_adt, from_csv, from_json};
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub use file_reader::FileReader;
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub use memory_reader::MemoryReader;
pub use options::{FormatReadOptions, ReadOptions};

// Re-export CsvEncoding for convenience
pub use crate::writer::CsvEncoding;

/// Create a reader from a file path with automatic format detection
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub fn from_path<P: AsRef<std::path::Path>>(path: P) -> crate::error::Result<FileReader> {
    FileReader::new(path)
}

/// Create a reader from a file path with explicit format
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub fn from_path_with_format<P: AsRef<std::path::Path>>(
    path: P,
    format: crate::format::DataFormat,
) -> FileReader {
    FileReader::with_format(path, format)
}

/// Create a reader from in-memory data
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub fn from_memory(data: Vec<u8>, format: crate::format::DataFormat) -> MemoryReader {
    MemoryReader::new(data, format)
}
