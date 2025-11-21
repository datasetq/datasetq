//! dsq-formats: File format support for dsq
//!
//! This crate provides comprehensive support for reading and writing various
//! structured data formats including CSV, Parquet, JSON, and more.
//!
//! # Features
//!
//! - **Format Detection**: Automatic format detection from file extensions and content
//! - **Unified Interface**: Consistent reader/writer traits across all formats
//! - **Performance**: Optimized implementations using Polars DataFrames
//! - **Extensibility**: Easy to add new formats with macro-based boilerplate reduction
//!
//! # Supported Formats
//!
//! ## Input Formats
//! - **CSV** (`.csv`) - Comma-separated values with customizable options
//! - **TSV** (`.tsv`) - Tab-separated values
//! - **Parquet** (`.parquet`) - Columnar storage with compression
//! - **JSON** (`.json`) - Standard JSON arrays and objects
//! - **JSON Lines** (`.jsonl`, `.ndjson`) - Newline-delimited JSON
//! - **Arrow** (`.arrow`) - Apache Arrow IPC format
//! - **Avro** (`.avro`) - Apache Avro serialization
//!
//! ## Output Formats
//! All input formats plus:
//! - **Excel** (`.xlsx`) - Microsoft Excel format
//! - **ORC** (`.orc`) - Optimized Row Columnar format
//!
//! # Architecture
//!
//! The format system is built around:
//! - [`DataFormat`] - Enum representing all supported formats
//! - [`DataReader`] / [`DataWriter`] - Traits for reading/writing data
//! - Format-specific implementations with consistent option structs
//! - Macros to reduce boilerplate for new format implementations

#![warn(missing_docs)]
#![warn(clippy::all)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::doc_markdown,
    clippy::uninlined_format_args,
    clippy::must_use_candidate,
    clippy::should_implement_trait,
    clippy::needless_borrow,
    clippy::len_zero,
    clippy::iter_cloned_collect,
    clippy::manual_string_new,
    clippy::return_self_not_must_use,
    clippy::naive_bytecount,
    clippy::map_unwrap_or,
    clippy::unnecessary_wraps,
    clippy::needless_pass_by_value,
    clippy::wildcard_imports,
    clippy::cast_lossless,
    clippy::needless_question_mark,
    clippy::manual_is_multiple_of,
    clippy::redundant_closure,
    clippy::clone_on_copy,
    clippy::struct_excessive_bools,
    clippy::unused_self,
    clippy::if_same_then_else,
    clippy::match_same_arms,
    clippy::to_string_trait_impl,
    clippy::ptr_as_ptr,
    clippy::ref_as_ptr,
    clippy::field_reassign_with_default,
    clippy::single_match_else,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::collapsible_else_if,
    clippy::to_string_in_format_args,
    clippy::ptr_as_ptr,
    clippy::single_match
)]

// Re-export shared types
pub use dsq_shared::{BuildInfo, VERSION};

// Core modules
/// Error types and result handling
pub mod error;
/// File format detection and metadata
pub mod format;

// Format implementations
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
/// ADT (ASCII Delimited Text) format reading and writing
pub mod adt;
#[cfg(feature = "csv")]
/// CSV format reading and writing
pub mod csv;
#[cfg(feature = "json")]
/// JSON format reading and writing
pub mod json;
#[cfg(feature = "json5")]
/// JSON5 format reading and writing
pub mod json5;
#[cfg(feature = "parquet")]
/// Parquet format reading and writing
pub mod parquet;

// Generic reader/writer interfaces
/// Generic data reader interface
pub mod reader;
/// Generic data writer interface
pub mod writer;
/// Old writer implementation (for testing)
// Re-export main types for convenience
pub use error::{Error, FormatError, Result};
pub use format::{detect_format_from_content, DataFormat, FormatOptions};
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub use reader::{
    from_memory, from_path, from_path_with_format, DataReader, FileReader, MemoryReader,
};
pub use reader::{FormatReadOptions, ReadOptions};
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub use writer::{
    to_memory, to_path, to_path_with_format, DataWriter, FileWriter, MemoryWriter,
};
pub use writer::{
    AvroCompression, CompressionLevel, CsvEncoding, FormatWriteOptions, OrcCompression,
    WriteOptions,
};

#[cfg(feature = "parquet")]
pub use writer::ParquetCompression;

// Deserialize/serialize functions
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub use reader::{deserialize, deserialize_adt, from_csv, from_json};
#[cfg(feature = "csv")]
pub use reader::deserialize_csv;
#[cfg(feature = "json")]
pub use reader::deserialize_json;
#[cfg(feature = "json5")]
pub use reader::deserialize_json5;

#[cfg(feature = "parquet")]
pub use reader::deserialize_parquet;

#[cfg(feature = "csv")]
pub use writer::serialize_csv;
#[cfg(feature = "json")]
pub use writer::serialize_json;
#[cfg(all(feature = "json5", feature = "json"))]
pub use writer::serialize_json5;
#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub use writer::{serialize, serialize_adt};

#[cfg(feature = "parquet")]
pub use writer::serialize_parquet;

// Format-specific re-exports
#[cfg(feature = "csv")]
pub use csv::{
    detect_csv_format, read_csv_file, read_csv_file_with_options, write_csv_file,
    write_csv_file_with_options, CsvReadOptions, CsvReader, CsvWriteOptions, CsvWriter,
};

#[cfg(feature = "json")]
pub use json::{
    detect_json_format, read_json_file, read_json_file_with_options, read_jsonl_file,
    write_json_file, write_json_file_with_options, write_jsonl_file, JsonReadOptions, JsonReader,
    JsonWriteOptions, JsonWriter,
};

#[cfg(feature = "json5")]
pub use json5::{
    detect_json5_format, read_json5_file, read_json5_file_with_options, read_json5l_file,
    write_json5_file, write_json5_file_with_options, write_json5l_file, Json5ReadOptions,
    Json5Reader, Json5WriteOptions, Json5Writer,
};

#[cfg(feature = "parquet")]
pub use parquet::{
    detect_parquet_format, read_parquet_file, read_parquet_file_lazy,
    read_parquet_file_lazy_with_options, read_parquet_file_with_options, write_parquet_file,
    write_parquet_file_with_options, ParquetReadOptions, ParquetReader, ParquetWriteOptions,
    ParquetWriter,
};

#[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
pub use adt::{detect_adt_format, AdtReadOptions, AdtWriteOptions};

/// Build information for dsq-formats
pub const BUILD_INFO: BuildInfo = BuildInfo {
    version: VERSION,
    git_hash: option_env!("VERGEN_GIT_SHA"),
    build_date: option_env!("VERGEN_BUILD_TIMESTAMP"),
    rust_version: option_env!("VERGEN_RUSTC_SEMVER"),
    features: &[
        #[cfg(feature = "csv")]
        "csv",
        #[cfg(feature = "json")]
        "json",
        #[cfg(feature = "parquet")]
        "parquet",
        #[cfg(feature = "avro")]
        "avro",
    ],
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_info() {
        assert_eq!(BUILD_INFO.version, VERSION);
        // Features array should contain enabled features
        let features = BUILD_INFO.features;
        #[cfg(feature = "csv")]
        assert!(features.contains(&"csv"));
        #[cfg(feature = "json")]
        assert!(features.contains(&"json"));
        #[cfg(feature = "parquet")]
        assert!(features.contains(&"parquet"));
        #[cfg(feature = "avro")]
        assert!(features.contains(&"avro"));
    }

    #[test]
    fn test_re_exports() {
        // Test that main types are re-exported correctly
        let _error: Error = Error::operation("test");
        let _format: DataFormat = DataFormat::Csv;
        let _options: ReadOptions = ReadOptions::default();
        let _write_options: WriteOptions = WriteOptions::default();

        // Test format-specific re-exports if features are enabled
        #[cfg(feature = "csv")]
        {
            let _csv_options: CsvReadOptions = CsvReadOptions::default();
            let _csv_write_options: CsvWriteOptions = CsvWriteOptions::default();
        }

        #[cfg(feature = "json")]
        {
            let _json_options: JsonReadOptions = JsonReadOptions::default();
            let _json_write_options: JsonWriteOptions = JsonWriteOptions::default();
        }

        #[cfg(feature = "json5")]
        {
            let _json5_options: Json5ReadOptions = Json5ReadOptions::default();
            let _json5_write_options: Json5WriteOptions = Json5WriteOptions::default();
        }

        #[cfg(feature = "parquet")]
        {
            let _parquet_options: ParquetReadOptions = ParquetReadOptions::default();
            let _parquet_write_options: ParquetWriteOptions = ParquetWriteOptions::default();
        }
    }

    #[test]
    fn test_format_detection_re_export() {
        use std::io::Cursor;
        // Test that detect_format_from_content is re-exported
        let json_data = b"{\"test\": \"data\"}";
        let result = detect_format_from_content(json_data);
        assert_eq!(result, Some(DataFormat::Json));
    }

    #[test]
    #[cfg(any(feature = "csv", feature = "json", feature = "json5", feature = "parquet", feature = "avro"))]
    fn test_reader_writer_functions_re_export() {
        // Test that reader/writer functions are re-exported
        let reader = from_path("nonexistent.csv");
        assert!(reader.is_ok());
        // Reading should fail for nonexistent file
        let mut reader = reader.unwrap();
        let result = reader.read(&ReadOptions::default());
        assert!(result.is_err());

        let mut reader = from_path_with_format("nonexistent.csv", DataFormat::Csv);
        // from_path_with_format returns FileReader directly
        let result = reader.read(&ReadOptions::default());
        assert!(result.is_err());

        let data = vec![];
        let reader = from_memory(data, DataFormat::Csv);
        // from_memory returns MemoryReader directly
        let _reader = reader;

        let result = to_path("nonexistent.csv");
        assert!(result.is_err());

        let result = to_path_with_format("nonexistent.csv", DataFormat::Csv);
        // to_path_with_format returns FileWriter directly, file creation happens on write
        let _writer = result;

        let result = to_memory(DataFormat::Csv);
        // to_memory now returns a MemoryWriter, not a Result
        let _writer = result;
    }

    #[test]
    fn test_format_options_re_export() {
        let _format_options: FormatOptions = FormatOptions::default();
    }
}
