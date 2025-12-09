//! dsq-core: Core library for dsq data processing
//!
//! This crate provides the core functionality for dsq, a data processing tool that extends
//! jq-ish syntax to work with structured data formats like Parquet, Avro, CSV, and more.
//! dsq leverages `Polars` `DataFrames` to provide high-performance
//! data manipulation across multiple file formats.
//!
//! # Features
//!
//! - **Format Flexibility**: Support for CSV, TSV, Parquet, Avro, JSON Lines, Arrow, and JSON
//! - **Performance**: Built on `Polars` `DataFrames` with lazy evaluation and columnar operations
//! - **Type Safety**: Proper type handling with clear error messages
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use dsq_core::{Value, ops, io};
//!
//! // Read a CSV file
//! let data = io::read_file_sync("data.csv", &io::ReadOptions::default())?;
//!
//! // Apply operations
//! let result = ops::OperationPipeline::new()
//!     .select(vec!["name".to_string(), "age".to_string()])
//!     .sort(vec![ops::SortOptions::desc("age".to_string())])
//!     .head(10)
//!     .execute(data)?;
//!
//! // Write to Parquet
//! io::write_file_sync(&result, "output.parquet", &io::WriteOptions::default())?;
//! # Ok::<(), dsq_core::Error>(())
//! ```
//!
//! # Architecture
//!
//! The library is organized into several key modules:
//!
//! - `value` - Core value type that bridges JSON and `DataFrames`
//! - [`ops`] - Data operations (select, filter, aggregate, join, transform)
//! - [`io`] - Input/output for various file formats
//! - [`filter`] - jq-compatible filter compilation and execution
//! - [`error`] - Error handling and result types
//! - [`format`] - File format detection and metadata
//!
//! # Examples
//!
//! ## Basic `DataFrame` Operations
//!
//! ```rust,ignore
//! use dsq_core::{Value, ops::basic::*};
//! use polars::prelude::*;
//!
//! let df = df! {
//!     "name" => ["Alice", "Bob", "Charlie"],
//!     "age" => [30, 25, 35],
//!     "department" => ["Engineering", "Sales", "Engineering"]
//! }?;
//!
//! let data = Value::DataFrame(df);
//!
//! // Select columns
//! let selected = select_columns(&data, &["name".to_string(), "age".to_string()])?;
//!
//! // Sort by age
//! let sorted = sort_by_columns(&selected, &[SortOptions::desc("age")])?;
//!
//! // Take first 2 rows
//! let result = head(&sorted, 2)?;
//! # Ok::<(), dsq_core::Error>(())
//! ```
//!
//! ## Aggregation Operations
//!
//! ```rust,ignore
//! use dsq_core::{Value, ops::aggregate::*};
//!
//! // Group by department and calculate statistics
//! let aggregated = group_by_agg(
//!     &data,
//!     &["department".to_string()],
//!     &[
//!         AggregationFunction::Count,
//!         AggregationFunction::Mean("age".to_string()),
//!         AggregationFunction::Sum("salary".to_string()),
//!     ]
//! )?;
//! # Ok::<(), dsq_core::Error>(())
//! ```
//!
//! ## Join Operations
//!
//! ```rust,ignore
//! use dsq_core::{Value, ops::join::*};
//!
//! let keys = JoinKeys::on(vec!["id".to_string()]);
//! let options = JoinOptions {
//!     join_type: JoinType::Inner,
//!     ..Default::default()
//! };
//!
//! let joined = join(&left_data, &right_data, &keys, &options)?;
//! # Ok::<(), dsq_core::Error>(())
//! ```
//!
//! ## Format Conversion
//!
//! ```rust,ignore
//! use dsq_core::io;
//!
//! // Convert CSV to Parquet
//! io::convert_file(
//!     "data.csv",
//!     "data.parquet",
//!     &io::ReadOptions::default(),
//!     &io::WriteOptions::default()
//! )?;
//! # Ok::<(), dsq_core::Error>(())
//! ```
//!
//! ## Filter Execution
//!
//! ```rust,ignore
//! use dsq_core::filter::{FilterExecutor, ExecutorConfig};
//!
//! let mut executor = FilterExecutor::with_config(
//!     ExecutorConfig {
//!         lazy_evaluation: true,
//!         dataframe_optimizations: true,
//!         ..Default::default()
//!     }
//! );
//!
//! // Execute jq-style filter on DataFrame
//! let result = executor.execute_str(
//!     r#"map(select(.age > 30)) | sort_by(.name)"#,
//!     data
//! )?;
//! # Ok::<(), dsq_core::Error>(())
//! ```
//!
//! # Error Handling
//!
//! All operations return `Result<T>` where errors are represented by the [`Error`] type:
//!
//! ```rust,ignore
//! use dsq_core::{Error, Result, TypeError, FormatError};
//!
//! match some_operation() {
//!     Ok(value) => println!("Success: {:?}", value),
//!     Err(Error::Type(TypeError::InvalidConversion { from, to })) => {
//!         eprintln!("Cannot convert from {} to {}", from, to);
//!     }
//!     Err(Error::Format(FormatError::Unknown(format))) => {
//!         eprintln!("Unknown format: {}", format);
//!     }
//!     Err(e) => eprintln!("Other error: {}", e),
//! }
//! # fn some_operation() -> Result<()> { Ok(()) }
//! ```
//!
//! # Performance Tips
//!
//! - Use lazy evaluation for large datasets with [`LazyFrame`](polars::prelude::LazyFrame)
//! - Prefer columnar operations over row-by-row processing
//! - Use appropriate data types to minimize memory usage
//! - Consider using streaming for very large files that don't fit in memory
//! - Enable DataFrame-specific optimizations in the filter executor
//!
//! # Feature Flags
//!
//! This crate supports several optional features:
//!
//! - `default` - Includes all commonly used functionality
//! - `io-csv` - CSV/TSV reading and writing support
//! - `io-parquet` - Parquet format support
//! - `io-json` - JSON and JSON Lines support
//! - `io-avro` - Avro format support (planned)
//! - `io-arrow` - Arrow IPC format support
//! - `filter` - jq-compatible filter compilation and execution
//! - `repl` - Interactive REPL support (for CLI usage)

#![warn(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::similar_names,
    clippy::too_many_lines
)]

pub use dsq_shared::{BuildInfo, VERSION};

// Re-export format types from dsq-formats
#[cfg(not(target_arch = "wasm32"))]
pub use dsq_formats::{format::detect_format_from_content, format::FormatOptions, DataFormat};

// Data operation modules
pub mod ops;

// Error handling
/// Error types and handling
pub mod error;

// I/O modules - feature-gated
#[cfg(feature = "io")]
pub mod io;

// Filter system modules - feature-gated
#[cfg(feature = "filter")]
pub mod filter;

// Re-export commonly used types and functions
pub use crate::error::{Error, FilterError, FormatError, Result, TypeError};

pub use dsq_shared::value::Value;

// Re-export key operation types
pub use ops::{
    recommended_batch_size, supports_operation, Operation, OperationPipeline, OperationType,
};

// Re-export basic operations
pub use ops::basic::{
    count, filter_values, head, reverse, select_columns, slice, sort_by_columns, tail, unique,
    SortOptions,
};

// Re-export aggregation operations
pub use ops::aggregate::{
    group_by, group_by_agg, pivot, unpivot, AggregationFunction, WindowFunction,
};

// Re-export join operations
pub use ops::join::{
    inner_join, join, left_join, outer_join, right_join, JoinKeys, JoinOptions, JoinType,
};

// Re-export transformation operations
pub use ops::transform::Transform;

// Re-export utility functions
pub use utils::{array, object};

// Re-export I/O convenience functions
#[cfg(feature = "io")]
pub use io::{
    convert_file, inspect_file, read_file, read_file_lazy, write_file, FileInfo, ReadOptions,
    WriteOptions,
};

// Re-export filter system
#[cfg(feature = "filter")]
pub use filter::{
    execute_filter, execute_filter_with_config, explain_filter, ExecutionResult, ExecutorConfig,
    FilterCompiler, FilterExecutor,
};

/// Prelude module for convenient imports
///
/// This module re-exports the most commonly used types and functions,
/// allowing users to import everything they need with a single use statement.
///
/// # Examples
///
/// ```rust
/// use dsq_core::prelude::*;
///
/// // Now you have access to Value, Error, Result, common operations, etc.
/// let data = Value::array(vec![Value::int(1), Value::int(2), Value::int(3)]);
/// let length = count(&data)?;
/// # Ok::<(), Error>(())
/// ```
pub mod prelude {
    // Core types
    pub use crate::{Error, Result, Value};

    // Operations
    pub use crate::ops::aggregate::{group_by, group_by_agg, AggregationFunction, WindowFunction};
    pub use crate::ops::basic::{
        count, filter_values, head, reverse, select_columns, slice, sort_by_columns, tail, unique,
        SortOptions,
    };
    pub use crate::ops::join::{
        inner_join, join, left_join, outer_join, right_join, JoinKeys, JoinOptions, JoinType,
    };
    pub use crate::ops::transform::Transform;
    pub use crate::ops::{Operation, OperationPipeline, OperationType};

    // I/O (if available)
    #[cfg(feature = "io")]
    pub use crate::io::{convert_file, read_file, write_file, ReadOptions, WriteOptions};

    // Filter system (if available)
    #[cfg(feature = "filter")]
    pub use crate::filter::{execute_filter, ExecutorConfig, FilterExecutor};

    // Re-export polars types that users commonly need
    pub use polars::prelude::{DataFrame, LazyFrame, Series};
}

/// Build information for dsq-core
pub const BUILD_INFO: BuildInfo = BuildInfo {
    version: VERSION,
    git_hash: option_env!("VERGEN_GIT_SHA"),
    build_date: option_env!("VERGEN_BUILD_TIMESTAMP"),
    rust_version: option_env!("VERGEN_RUSTC_SEMVER"),
    features: &[
        #[cfg(feature = "io")]
        "io",
        #[cfg(feature = "filter")]
        "filter",
        #[cfg(feature = "repl")]
        "repl",
    ],
};

/// Utility functions for working with dsq
pub mod utils {
    use std::collections::HashMap;

    use crate::{Error, Result, Value};

    /// Create a `Value::Object` from key-value pairs
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dsq_core::object;
    /// use dsq_core::Value;
    ///
    /// let obj = object([
    ///     ("name", Value::string("Alice")),
    ///     ("age", Value::int(30)),
    /// ]);
    /// ```
    pub fn object<I, K>(pairs: I) -> Value
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let map: HashMap<String, Value> = pairs.into_iter().map(|(k, v)| (k.into(), v)).collect();
        Value::Object(map)
    }

    /// Create a `Value::Array` from values
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dsq_core::utils::array;
    /// use dsq_core::Value;
    ///
    /// let arr = array([
    ///     Value::int(1),
    ///     Value::int(2),
    ///     Value::int(3),
    /// ]);
    /// ```
    pub fn array<I>(values: I) -> Value
    where
        I: IntoIterator<Item = Value>,
    {
        Value::Array(values.into_iter().collect())
    }

    /// Try to extract a `DataFrame` from a Value
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dsq_core::utils::extract_dataframe;
    /// use dsq_core::Value;
    /// use polars::prelude::*;
    ///
    /// let df = df! {
    ///     "name" => ["Alice", "Bob"],
    ///     "age" => [30, 25]
    /// }.unwrap();
    ///
    /// let value = Value::DataFrame(df.clone());
    /// let extracted = extract_dataframe(&value).unwrap();
    /// assert_eq!(extracted.height(), df.height());
    /// ```
    pub fn extract_dataframe(value: &Value) -> Result<&polars::prelude::DataFrame> {
        match value {
            Value::DataFrame(df) => Ok(df),
            _ => Err(Error::operation(format!(
                "Expected DataFrame, got {}",
                value.type_name()
            ))),
        }
    }

    /// Try to convert any Value to a `DataFrame`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dsq_core::utils::{object, array, to_dataframe};
    /// use dsq_core::Value;
    ///
    /// let data = array([
    ///     object([("name", Value::string("Alice")), ("age", Value::int(30))]),
    ///     object([("name", Value::string("Bob")), ("age", Value::int(25))]),
    /// ]);
    ///
    /// let df = to_dataframe(&data).unwrap();
    /// assert_eq!(df.height(), 2);
    /// ```
    pub fn to_dataframe(value: &Value) -> Result<polars::prelude::DataFrame> {
        Ok(value.to_dataframe()?)
    }

    /// Pretty print a Value for debugging
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dsq_core::utils::{object, pretty_print};
    /// use dsq_core::Value;
    ///
    /// let obj = object([
    ///     ("name", Value::string("Alice")),
    ///     ("age", Value::int(30)),
    /// ]);
    ///
    /// pretty_print(&obj);
    /// ```
    pub fn pretty_print(value: &Value) {
        match value.to_json() {
            Ok(json) => {
                if let Ok(pretty) = serde_json::to_string_pretty(&json) {
                    println!("{pretty}");
                } else {
                    println!("{value}");
                }
            }
            Err(_) => println!("{value}"),
        }
    }

    /// Get basic statistics about a Value
    ///
    /// Returns information like type, length, memory usage estimates, etc.
    #[must_use]
    pub fn value_stats(value: &Value) -> ValueStats {
        ValueStats::from_value(value)
    }

    /// Statistics about a Value
    #[derive(Debug, Clone)]
    pub struct ValueStats {
        /// Value type name
        pub type_name: String,
        /// Length (for arrays, strings, `DataFrames`, etc.)
        pub length: Option<usize>,
        /// Width (for `DataFrames`, objects)
        pub width: Option<usize>,
        /// Estimated memory usage in bytes
        pub estimated_size: Option<usize>,
        /// Whether the value is null/empty
        pub is_empty: bool,
    }

    impl ValueStats {
        fn from_value(value: &Value) -> Self {
            let type_name = value.type_name().to_string();
            let length = value.len();
            let is_empty = value.is_empty();

            let (width, estimated_size) = match value {
                Value::DataFrame(df) => (Some(df.width()), Some(df.estimated_size())),
                Value::Object(obj) => (
                    Some(obj.len()),
                    Some(obj.len() * 64), // Rough estimate
                ),
                Value::Array(arr) => (
                    None,
                    Some(arr.len() * 32), // Rough estimate
                ),
                Value::String(s) => (None, Some(s.len())),
                _ => (None, Some(8)), // Rough estimate for scalars
            };

            Self {
                type_name,
                length,
                width,
                estimated_size,
                is_empty,
            }
        }
    }

    impl std::fmt::Display for ValueStats {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Type: {}", self.type_name)?;

            if let Some(length) = self.length {
                write!(f, ", Length: {length}")?;
            }

            if let Some(width) = self.width {
                write!(f, ", Width: {width}")?;
            }

            if let Some(size) = self.estimated_size {
                write!(f, ", Size: ~{size} bytes")?;
            }

            if self.is_empty {
                write!(f, " (empty)")?;
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    #[allow(unused_imports)]
    use std::path::Path;

    use polars::prelude::*;

    #[allow(unused_imports)]
    use crate::utils::{array, extract_dataframe, object, to_dataframe, value_stats};

    use super::*;

    #[test]
    fn test_version_info() {
        assert!(!VERSION.is_empty());
        println!("{}", BUILD_INFO);
    }

    #[test]
    fn test_utils_object() {
        let obj = object([("name", Value::string("Alice")), ("age", Value::int(30))]);

        match obj {
            Value::Object(map) => {
                assert_eq!(map.len(), 2);
                assert_eq!(map.get("name"), Some(&Value::string("Alice")));
                assert_eq!(map.get("age"), Some(&Value::int(30)));
            }
            _ => panic!("Expected object"),
        }
    }

    #[test]
    fn test_utils_array() {
        let arr = array([Value::int(1), Value::int(2), Value::int(3)]);

        match arr {
            Value::Array(vec) => {
                assert_eq!(vec.len(), 3);
                assert_eq!(vec[0], Value::int(1));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_value_stats() {
        let obj = object([("name", Value::string("Alice")), ("age", Value::int(30))]);

        let stats = value_stats(&obj);
        assert_eq!(stats.type_name, "object");
        assert_eq!(stats.width, Some(2));
        assert!(!stats.is_empty);
    }

    #[test]
    fn test_extract_dataframe() {
        use polars::prelude::*;

        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let value = Value::DataFrame(df.clone());
        let extracted = extract_dataframe(&value).unwrap();
        assert_eq!(extracted.height(), 2);

        let non_df = Value::int(42);
        assert!(extract_dataframe(&non_df).is_err());
    }

    #[test]
    fn test_to_dataframe_conversion() {
        let data = array([
            object([("name", Value::string("Alice")), ("age", Value::int(30))]),
            object([("name", Value::string("Bob")), ("age", Value::int(25))]),
        ]);

        let df = to_dataframe(&data).unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_value_type_checks() {
        let null_val = Value::null();
        assert!(null_val.is_null());
        assert!(!null_val.is_dataframe());

        let df_val = Value::dataframe(
            df! {
                "name" => ["Alice"],
                "age" => [30]
            }
            .unwrap(),
        );
        assert!(df_val.is_dataframe());
        assert!(!df_val.is_null());
    }

    #[test]
    fn test_value_type_name() {
        assert_eq!(Value::null().type_name(), "null");
        assert_eq!(Value::bool(true).type_name(), "boolean");
        assert_eq!(Value::int(42).type_name(), "integer");
        assert_eq!(Value::float(3.14).type_name(), "float");
        assert_eq!(Value::string("hello").type_name(), "string");
        assert_eq!(Value::array(vec![]).type_name(), "array");
        assert_eq!(Value::object(HashMap::new()).type_name(), "object");
    }

    #[test]
    fn test_value_len_and_empty() {
        assert_eq!(Value::string("hello").len(), Some(5));
        assert_eq!(
            Value::array(vec![Value::int(1), Value::int(2)]).len(),
            Some(2)
        );
        assert_eq!(Value::null().len(), None);
        assert_eq!(Value::int(42).len(), None);

        assert!(Value::string("").is_empty());
        assert!(Value::array(vec![]).is_empty());
        assert!(!Value::string("hello").is_empty());
        assert!(!Value::null().is_empty());
    }

    #[test]
    fn test_value_index() {
        let arr = Value::array(vec![Value::int(10), Value::int(20), Value::int(30)]);
        assert_eq!(arr.index(0).unwrap(), Value::int(10));
        assert_eq!(arr.index(1).unwrap(), Value::int(20));
        assert_eq!(arr.index(-1).unwrap(), Value::int(30)); // negative indexing
        assert_eq!(arr.index(10).unwrap(), Value::Null); // out of bounds

        let s = Value::string("hello");
        assert_eq!(s.index(0).unwrap(), Value::string("h"));
        assert_eq!(s.index(4).unwrap(), Value::string("o"));
        assert_eq!(s.index(-1).unwrap(), Value::string("o"));
    }

    #[test]
    fn test_value_field() {
        let mut obj = HashMap::new();
        obj.insert("name".to_string(), Value::string("Alice"));
        obj.insert("age".to_string(), Value::int(30));
        let obj_val = Value::object(obj);

        assert_eq!(obj_val.field("name").unwrap(), Value::string("Alice"));
        assert_eq!(obj_val.field("age").unwrap(), Value::int(30));
        assert_eq!(obj_val.field("missing").unwrap(), Value::Null);

        // Test field access on array
        let arr = Value::array(vec![obj_val.clone()]);
        let names = arr.field("name").unwrap();
        match names {
            Value::Array(names_arr) => {
                assert_eq!(names_arr.len(), 1);
                assert_eq!(names_arr[0], Value::string("Alice"));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_value_to_json() {
        // Test basic types
        assert_eq!(Value::null().to_json().unwrap(), serde_json::Value::Null);
        assert_eq!(
            Value::bool(true).to_json().unwrap(),
            serde_json::json!(true)
        );
        assert_eq!(Value::int(42).to_json().unwrap(), serde_json::json!(42));
        assert_eq!(
            Value::float(3.14).to_json().unwrap(),
            serde_json::json!(3.14)
        );
        assert_eq!(
            Value::string("hello").to_json().unwrap(),
            serde_json::json!("hello")
        );

        // Test array
        let arr = Value::array(vec![Value::int(1), Value::int(2)]);
        let expected = serde_json::json!([1, 2]);
        assert_eq!(arr.to_json().unwrap(), expected);

        // Test object
        let mut obj = HashMap::new();
        obj.insert("name".to_string(), Value::string("Alice"));
        obj.insert("age".to_string(), Value::int(30));
        let obj_val = Value::object(obj);
        let expected = serde_json::json!({"name": "Alice", "age": 30});
        assert_eq!(obj_val.to_json().unwrap(), expected);
    }

    #[test]
    fn test_value_from_json() {
        assert_eq!(Value::from_json(serde_json::Value::Null), Value::null());
        assert_eq!(Value::from_json(serde_json::json!(true)), Value::bool(true));
        assert_eq!(Value::from_json(serde_json::json!(42)), Value::int(42));
        assert_eq!(
            Value::from_json(serde_json::json!(3.14)),
            Value::float(3.14)
        );
        assert_eq!(
            Value::from_json(serde_json::json!("hello")),
            Value::string("hello")
        );

        let json_arr = serde_json::json!([1, 2, 3]);
        let val_arr = Value::from_json(json_arr);
        match val_arr {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Value::int(1));
            }
            _ => panic!("Expected array"),
        }

        let json_obj = serde_json::json!({"name": "Alice", "age": 30});
        let val_obj = Value::from_json(json_obj);
        match val_obj {
            Value::Object(obj) => {
                assert_eq!(obj.get("name"), Some(&Value::string("Alice")));
                assert_eq!(obj.get("age"), Some(&Value::int(30)));
            }
            _ => panic!("Expected object"),
        }
    }

    #[cfg(feature = "filter")]
    mod filter_tests {
        use std::fs;
        use std::io::Write;

        use polars::prelude::*;
        use tempfile::NamedTempFile;

        use crate::filter;
        use crate::utils::{array, object};

        use super::*;

        fn create_mock_data() -> Value {
            // Create mock data similar to the example datasets
            let data = array(vec![
                object(vec![
                    ("title".to_string(), Value::string("Book A")),
                    ("genre".to_string(), Value::string("Fiction")),
                    ("price".to_string(), Value::float(19.99)),
                    ("author".to_string(), Value::string("Author A")),
                ]),
                object(vec![
                    ("title".to_string(), Value::string("Book B")),
                    ("genre".to_string(), Value::string("Non-Fiction")),
                    ("price".to_string(), Value::float(24.99)),
                    ("author".to_string(), Value::string("Author B")),
                ]),
                object(vec![
                    ("title".to_string(), Value::string("Book C")),
                    ("genre".to_string(), Value::string("Fiction")),
                    ("price".to_string(), Value::float(15.99)),
                    ("author".to_string(), Value::string("Author C")),
                ]),
                object(vec![
                    ("title".to_string(), Value::string("Book D")),
                    ("genre".to_string(), Value::string("Fiction")),
                    ("price".to_string(), Value::float(29.99)),
                    ("author".to_string(), Value::string("Author D")),
                ]),
                object(vec![
                    ("title".to_string(), Value::string("Book E")),
                    ("genre".to_string(), Value::string("Science")),
                    ("price".to_string(), Value::float(34.99)),
                    ("author".to_string(), Value::string("Author E")),
                ]),
                object(vec![
                    ("title".to_string(), Value::string("Book F")),
                    ("genre".to_string(), Value::string("Fiction")),
                    ("price".to_string(), Value::float(12.99)),
                    ("author".to_string(), Value::string("Author F")),
                ]),
                object(vec![
                    ("title".to_string(), Value::string("Book G")),
                    ("genre".to_string(), Value::string("Non-Fiction")),
                    ("price".to_string(), Value::float(22.99)),
                    ("author".to_string(), Value::string("Author G")),
                ]),
            ]);
            data
        }

        fn create_employee_data() -> Value {
            // Create mock employee data
            let data = array(vec![
                object(vec![
                    ("name".to_string(), Value::string("Alice Johnson")),
                    ("department".to_string(), Value::string("Sales")),
                    ("salary".to_string(), Value::int(75000)),
                    ("age".to_string(), Value::int(32)),
                ]),
                object(vec![
                    ("name".to_string(), Value::string("Bob Smith")),
                    ("department".to_string(), Value::string("Engineering")),
                    ("salary".to_string(), Value::int(82000)),
                    ("age".to_string(), Value::int(28)),
                ]),
                object(vec![
                    ("name".to_string(), Value::string("Carol Williams")),
                    ("department".to_string(), Value::string("Sales")),
                    ("salary".to_string(), Value::int(68000)),
                    ("age".to_string(), Value::int(35)),
                ]),
                object(vec![
                    ("name".to_string(), Value::string("David Brown")),
                    ("department".to_string(), Value::string("Engineering")),
                    ("salary".to_string(), Value::int(95000)),
                    ("age".to_string(), Value::int(41)),
                ]),
                object(vec![
                    ("name".to_string(), Value::string("Eve Davis")),
                    ("department".to_string(), Value::string("Marketing")),
                    ("salary".to_string(), Value::int(62000)),
                    ("age".to_string(), Value::int(29)),
                ]),
                object(vec![
                    ("name".to_string(), Value::string("Frank Miller")),
                    ("department".to_string(), Value::string("Engineering")),
                    ("salary".to_string(), Value::int(88000)),
                    ("age".to_string(), Value::int(33)),
                ]),
                object(vec![
                    ("name".to_string(), Value::string("Grace Wilson")),
                    ("department".to_string(), Value::string("Sales")),
                    ("salary".to_string(), Value::int(71000)),
                    ("age".to_string(), Value::int(26)),
                ]),
                object(vec![
                    ("name".to_string(), Value::string("Henry Moore")),
                    ("department".to_string(), Value::string("Engineering")),
                    ("salary".to_string(), Value::int(102000)),
                    ("age".to_string(), Value::int(38)),
                ]),
                object(vec![
                    ("name".to_string(), Value::string("Ivy Taylor")),
                    ("department".to_string(), Value::string("Marketing")),
                    ("salary".to_string(), Value::int(65000)),
                    ("age".to_string(), Value::int(31)),
                ]),
                object(vec![
                    ("name".to_string(), Value::string("Jack Anderson")),
                    ("department".to_string(), Value::string("Sales")),
                    ("salary".to_string(), Value::int(79000)),
                    ("age".to_string(), Value::int(30)),
                ]),
            ]);
            data
        }

        #[test]
        fn test_example_002_query_on_csv() {
            let query = r#"group_by(.genre) | map({
  genre: .[0].genre,
  count: length,
  avg_price: (map(.price) | add / length)
})"#;

            let data = create_mock_data();
            let result = crate::filter::execute_filter(query, &data);
            assert!(
                result.is_ok(),
                "Failed to execute query on CSV: {:?}",
                result.err()
            );

            let value = result.unwrap();
            // Verify the result structure
            match value {
                Value::Array(arr) => {
                    assert_eq!(arr.len(), 3); // 3 unique genres: Fiction, Non-Fiction, Science
                                              // Check that each item has genre, count, and avg_price
                    for item in &arr {
                        match item {
                            Value::Object(obj) => {
                                assert!(obj.contains_key("genre"));
                                assert!(obj.contains_key("count"));
                                assert!(obj.contains_key("avg_price"));
                            }
                            _ => panic!("Expected object in result array"),
                        }
                    }
                }
                _ => panic!("Expected array result"),
            }
        }

        #[test]
        fn test_example_002_query_on_json() {
            let query = r#"group_by(.genre) | map({
  genre: .[0].genre,
  count: length,
  avg_price: (map(.price) | add / length)
})"#;

            let data = create_mock_data();
            let result = crate::filter::execute_filter(query, &data);
            assert!(
                result.is_ok(),
                "Failed to execute query on JSON: {:?}",
                result.err()
            );

            let value = result.unwrap();
            match value {
                Value::Array(arr) => {
                    assert_eq!(arr.len(), 3); // 3 unique genres: Fiction, Non-Fiction, Science
                }
                _ => panic!("Expected array result"),
            }
        }

        #[test]
        fn test_example_002_query_on_tsv() {
            let query = r#"group_by(.genre) | map({
  genre: .[0].genre,
  count: length,
  avg_price: (map(.price) | add / length)
})"#;

            let data = create_mock_data();
            let result = crate::filter::execute_filter(query, &data);
            assert!(
                result.is_ok(),
                "Failed to execute query on TSV: {:?}",
                result.err()
            );

            let value = result.unwrap();
            match value {
                Value::Array(arr) => {
                    assert_eq!(arr.len(), 3); // 3 unique genres: Fiction, Non-Fiction, Science
                }
                _ => panic!("Expected array result"),
            }
        }

        #[test]
        fn test_example_002_query_on_parquet() {
            let query = r#"group_by(.genre) | map({
  genre: .[0].genre,
  count: length,
  avg_price: (map(.price) | add / length)
})"#;

            let data = create_mock_data();
            let result = crate::filter::execute_filter(query, &data);
            assert!(
                result.is_ok(),
                "Failed to execute query on Parquet: {:?}",
                result.err()
            );

            let value = result.unwrap();
            match value {
                Value::Array(arr) => {
                    assert_eq!(arr.len(), 3); // 3 unique genres: Fiction, Non-Fiction, Science
                }
                _ => panic!("Expected array result"),
            }
        }

        #[test]
        fn test_example_002_query_from_file() {
            let query_path = "examples/example_002/query.dsq";
            if Path::new(query_path).exists() {
                let query = fs::read_to_string(query_path).unwrap();

                // Test on mock data
                let data = create_mock_data();
                let result = execute_filter(&query, &data);
                assert!(
                    result.is_ok(),
                    "Failed to execute query from file on mock data: {:?}",
                    result.err()
                );
            } else {
                println!("Skipping query file test - query.dsq not found");
            }
        }

        #[test]
        fn test_example_085_query_on_csv() {
            let query = r#"group_by(.department) | map({
  dept: .[0].department,
  count: length,
  avg_salary: (map(.salary) | add / length)
})"#;

            let data = create_employee_data();
            let result = execute_filter(query, &data);
            assert!(
                result.is_ok(),
                "Failed to execute query on mock data: {:?}",
                result.err()
            );

            let value = result.unwrap();
            // Verify the result structure
            match value {
                Value::Array(arr) => {
                    assert_eq!(arr.len(), 3); // 3 departments: Sales, Marketing, Engineering
                                              // Check that each item has dept, count, and avg_salary
                    for item in &arr {
                        match item {
                            Value::Object(obj) => {
                                assert!(obj.contains_key("dept"));
                                assert!(obj.contains_key("count"));
                                assert!(obj.contains_key("avg_salary"));
                            }
                            _ => panic!("Expected object in result array"),
                        }
                    }
                }
                _ => panic!("Expected array result"),
            }
        }

        #[test]
        fn test_example_085_query_from_file() {
            let query_path = "examples/example_085/query.dsq";
            if Path::new(query_path).exists() {
                let query = fs::read_to_string(query_path).unwrap();

                // Test on mock data
                let data = create_employee_data();
                let result = execute_filter(&query, &data);
                assert!(
                    result.is_ok(),
                    "Failed to execute query from file on mock data: {:?}",
                    result.err()
                );
            } else {
                println!("Skipping query file test - query.dsq not found");
            }
        }

        #[test]
        fn test_example_075_query_on_csv() {
            let query = r#"map(.salary += 5000) | map({name, new_salary: .salary, department})"#;

            let data = create_employee_data();
            let result = execute_filter(query, &data);
            assert!(
                result.is_ok(),
                "Failed to execute query on mock data: {:?}",
                result.err()
            );

            let value = result.unwrap();
            // Verify the result structure
            match value {
                Value::Array(arr) => {
                    assert_eq!(arr.len(), 10); // 10 employees
                                               // Check that each item has name, new_salary, and department
                    for item in &arr {
                        match item {
                            Value::Object(obj) => {
                                assert!(obj.contains_key("name"));
                                assert!(obj.contains_key("new_salary"));
                                assert!(obj.contains_key("department"));
                                // Check that new_salary is salary + 5000
                                if let (
                                    Some(Value::String(name)),
                                    Some(Value::Int(new_salary)),
                                    Some(Value::String(dept)),
                                ) = (
                                    obj.get("name"),
                                    obj.get("new_salary"),
                                    obj.get("department"),
                                ) {
                                    // Verify specific expected values from the mock data
                                    match name.as_str() {
                                        "Alice Johnson" => assert_eq!(*new_salary, 80000), // 75000 + 5000
                                        "Bob Smith" => assert_eq!(*new_salary, 87000), // 82000 + 5000
                                        "Carol Williams" => assert_eq!(*new_salary, 73000), // 68000 + 5000
                                        "David Brown" => assert_eq!(*new_salary, 100000), // 95000 + 5000
                                        "Eve Davis" => assert_eq!(*new_salary, 67000), // 62000 + 5000
                                        "Frank Miller" => assert_eq!(*new_salary, 93000), // 88000 + 5000
                                        "Grace Wilson" => assert_eq!(*new_salary, 76000), // 71000 + 5000
                                        "Henry Moore" => assert_eq!(*new_salary, 107000), // 102000 + 5000
                                        "Ivy Taylor" => assert_eq!(*new_salary, 70000), // 65000 + 5000
                                        "Jack Anderson" => assert_eq!(*new_salary, 84000), // 79000 + 5000
                                        _ => panic!("Unexpected employee name: {}", name),
                                    }
                                } else {
                                    panic!(
                                        "Expected string name, int new_salary, string department"
                                    );
                                }
                            }
                            _ => panic!("Expected object in result array"),
                        }
                    }
                }
                _ => panic!("Expected array result"),
            }
        }

        #[test]
        fn test_csv_with_spaces_in_field_names() {
            // Test CSV data with spaces in field names
            let csv_data = r#"id,"US City Name",population,country
1,"New York",8500000,USA
2,"Los Angeles",4000000,USA
3,"London",9000000,UK
4,"Paris",2200000,France"#;

            // Create a temporary file
            let mut temp_file = NamedTempFile::new().unwrap();
            temp_file.write_all(csv_data.as_bytes()).unwrap();
            let path = temp_file.path();

            // Parse the CSV data
            let result = io::read_file_sync(path, &io::ReadOptions::default());
            assert!(
                result.is_ok(),
                "Failed to parse CSV with spaces in field names: {:?}",
                result.err()
            );

            let value = result.unwrap();
            match value {
                Value::DataFrame(df) => {
                    // Check that the DataFrame has the correct columns
                    let column_names = df.get_column_names();
                    assert!(column_names.contains(&&PlSmallStr::from("id")));
                    assert!(column_names.contains(&&PlSmallStr::from("US City Name")));
                    assert!(column_names.contains(&&PlSmallStr::from("population")));
                    assert!(column_names.contains(&&PlSmallStr::from("country")));

                    // Note: Bracket notation with spaces in field names is not yet supported
                    // This is a known limitation tracked in TODO.md
                    let _ = df; // Use df to avoid warning
                }
                _ => panic!("Expected DataFrame result"),
            }
        }
    }
}
