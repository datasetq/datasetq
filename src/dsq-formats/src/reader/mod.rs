// Include all submodules
mod data_reader;
mod dispatch;
mod file_reader;
pub mod json_utils;
mod memory_reader;
pub mod options;

// Re-export all components
pub use data_reader::DataReader;
pub use dispatch::{
    deserialize, deserialize_adt, deserialize_csv, deserialize_json, deserialize_json5,
    deserialize_parquet, from_csv, from_json,
};
pub use file_reader::FileReader;
pub use memory_reader::MemoryReader;
pub use options::{FormatReadOptions, ReadOptions};

// Re-export CsvEncoding for convenience
pub use crate::csv::CsvEncoding;

/// Create a reader from a file path with automatic format detection
pub fn from_path<P: AsRef<std::path::Path>>(path: P) -> crate::error::Result<FileReader> {
    FileReader::new(path)
}

/// Create a reader from a file path with explicit format
pub fn from_path_with_format<P: AsRef<std::path::Path>>(
    path: P,
    format: crate::format::DataFormat,
) -> FileReader {
    FileReader::with_format(path, format)
}

/// Create a reader from in-memory data
pub fn from_memory(data: Vec<u8>, format: crate::format::DataFormat) -> MemoryReader {
    MemoryReader::new(data, format)
}
