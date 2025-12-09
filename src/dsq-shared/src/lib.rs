//! dsq-shared: Shared types and utilities for DSQ crates
//!
//! This crate contains common types, traits, and utilities used across
//! multiple DSQ crates to avoid code duplication and ensure consistency.
//!
//! # Features
//!
//! - **Common Result Type**: Standardized Result type alias
//! - **Error Utilities**: Common error handling patterns and traits
//! - **Version Information**: Build and version metadata
//! - **Common Types**: Shared data structures and enums

#![warn(missing_docs)]
#![warn(clippy::all)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::must_use_candidate,
    clippy::doc_markdown,
    clippy::type_complexity,
    clippy::uninlined_format_args,
    clippy::manual_let_else
)]

/// Result type alias for DSQ operations
pub type Result<T> = anyhow::Result<T>;

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Build information structure
#[derive(Debug, Clone)]
pub struct BuildInfo {
    /// Package version
    pub version: &'static str,
    /// Git commit hash (if available)
    pub git_hash: Option<&'static str>,
    /// Build timestamp (if available)
    pub build_date: Option<&'static str>,
    /// Rust compiler version (if available)
    pub rust_version: Option<&'static str>,
    /// Enabled features
    pub features: &'static [&'static str],
}

impl std::fmt::Display for BuildInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "dsq-shared {}", self.version)?;

        if let Some(hash) = self.git_hash {
            writeln!(f, "Git hash: {hash}")?;
        }

        if let Some(date) = self.build_date {
            writeln!(f, "Built: {date}")?;
        }

        if let Some(rust_ver) = self.rust_version {
            writeln!(f, "Rust: {rust_ver}")?;
        }

        if !self.features.is_empty() {
            writeln!(f, "Features: {}", self.features.join(", "))?;
        }

        Ok(())
    }
}

/// Common error handling utilities
pub mod error {
    /// Create a generic operation error
    pub fn operation_error(msg: impl Into<String>) -> anyhow::Error {
        anyhow::anyhow!("Operation error: {}", msg.into())
    }

    /// Create a configuration error
    pub fn config_error(msg: impl Into<String>) -> anyhow::Error {
        anyhow::anyhow!("Configuration error: {}", msg.into())
    }
}

/// Core value types for data processing
pub mod value;

/// Core operations for data processing
pub mod ops;

// Re-export commonly used functions
pub use value::is_truthy;

/// Common utility functions
pub mod utils {
    use std::collections::HashMap;

    /// Create a `HashMap` from key-value pairs
    pub fn hashmap<K, V, I>(pairs: I) -> HashMap<K, V>
    where
        I: IntoIterator<Item = (K, V)>,
        K: std::hash::Hash + Eq,
    {
        pairs.into_iter().collect()
    }

    /// Check if a string is empty or whitespace-only
    #[must_use]
    pub fn is_blank(s: &str) -> bool {
        s.trim().is_empty()
    }

    /// Capitalize the first character of a string
    #[must_use]
    pub fn capitalize_first(s: &str) -> String {
        let mut chars = s.chars();
        match chars.next() {
            None => String::new(),
            Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        }
    }
}

/// Common constants
pub mod constants {
    /// Default batch size for operations
    pub const DEFAULT_BATCH_SIZE: usize = 1000;

    /// Maximum allowed batch size
    pub const MAX_BATCH_SIZE: usize = 100_000;

    /// Default buffer size for I/O operations
    pub const DEFAULT_BUFFER_SIZE: usize = 8192;

    /// Small buffer size for specialized operations
    pub const SMALL_BUFFER_SIZE: usize = 1024;

    /// Large buffer size for file operations
    pub const LARGE_BUFFER_SIZE: usize = 128 * 1024; // 128KB

    /// Default schema inference length for data format detection
    pub const DEFAULT_SCHEMA_INFERENCE_LENGTH: usize = 1000;

    /// Maximum recursion depth for filter execution
    pub const MAX_RECURSION_DEPTH: usize = 1000;

    /// Default batch size for high-throughput operations
    pub const HIGH_THROUGHPUT_BATCH_SIZE: usize = 10000;

    /// Field separator for ADT format (ASCII 31 - Unit Separator)
    pub const FIELD_SEPARATOR: u8 = 31;

    /// Record separator for ADT format (ASCII 30 - Record Separator)
    pub const RECORD_SEPARATOR: u8 = 30;

    /// Sample size for content detection
    pub const CONTENT_SAMPLE_SIZE: usize = 4096;

    /// Default memory limit for operations (1GB)
    pub const DEFAULT_MEMORY_LIMIT: usize = 1024 * 1024 * 1024;

    /// Maximum memory file size (100MB)
    pub const MAX_MEMORY_FILE_SIZE: usize = 100 * 1024 * 1024;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_info() {
        assert!(!VERSION.is_empty());
        // Version should be a valid semver-like string
        assert!(VERSION.contains('.'));
    }

    #[test]
    fn test_build_info_display_full() {
        let build_info = BuildInfo {
            version: "1.0.0",
            git_hash: Some("abc123"),
            build_date: Some("2023-01-01"),
            rust_version: Some("1.70.0"),
            features: &["default", "serde"],
        };

        let display = format!("{}", build_info);
        assert!(display.contains("dsq-shared 1.0.0"));
        assert!(display.contains("Git hash: abc123"));
        assert!(display.contains("Built: 2023-01-01"));
        assert!(display.contains("Rust: 1.70.0"));
        assert!(display.contains("Features: default, serde"));
    }

    #[test]
    fn test_build_info_display_minimal() {
        let build_info = BuildInfo {
            version: "2.0.0",
            git_hash: None,
            build_date: None,
            rust_version: None,
            features: &[],
        };

        let display = format!("{}", build_info);
        assert!(display.contains("dsq-shared 2.0.0"));
        assert!(!display.contains("Git hash:"));
        assert!(!display.contains("Built:"));
        assert!(!display.contains("Rust:"));
        assert!(!display.contains("Features:"));
    }

    #[test]
    fn test_build_info_display_partial() {
        let build_info = BuildInfo {
            version: "1.5.0",
            git_hash: Some("def456"),
            build_date: None,
            rust_version: Some("1.75.0"),
            features: &[],
        };

        let display = format!("{}", build_info);
        assert!(display.contains("dsq-shared 1.5.0"));
        assert!(display.contains("Git hash: def456"));
        assert!(display.contains("Rust: 1.75.0"));
        assert!(!display.contains("Built:"));
        assert!(!display.contains("Features:"));
    }

    #[test]
    fn test_error_functions() {
        let err = error::operation_error("test message");
        assert!(err.to_string().contains("Operation error: test message"));

        let err = error::config_error("config issue");
        assert!(err
            .to_string()
            .contains("Configuration error: config issue"));
    }

    #[test]
    fn test_utils_hashmap() {
        let map = utils::hashmap([("key1", 1), ("key2", 2)]);
        assert_eq!(map.get("key1"), Some(&1));
        assert_eq!(map.get("key2"), Some(&2));
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("nonexistent"), None);
    }

    #[test]
    fn test_utils_hashmap_empty() {
        let map: std::collections::HashMap<&str, i32> = utils::hashmap([]);
        assert!(map.is_empty());
    }

    #[test]
    fn test_utils_is_blank() {
        assert!(utils::is_blank(""));
        assert!(utils::is_blank("   "));
        assert!(utils::is_blank("\t\n"));
        assert!(utils::is_blank(" \t \n "));
        assert!(!utils::is_blank("hello"));
        assert!(!utils::is_blank(" hello "));
        assert!(!utils::is_blank("a"));
        assert!(!utils::is_blank("0"));
    }

    #[test]
    fn test_utils_capitalize_first() {
        assert_eq!(utils::capitalize_first("hello"), "Hello");
        assert_eq!(utils::capitalize_first("HELLO"), "HELLO");
        assert_eq!(utils::capitalize_first(""), "");
        assert_eq!(utils::capitalize_first("a"), "A");
        assert_eq!(utils::capitalize_first("123"), "123");
        assert_eq!(utils::capitalize_first(" hello"), " hello");
        assert_eq!(utils::capitalize_first("ñandu"), "Ñandu"); // Unicode test
    }

    #[test]
    fn test_constants() {
        assert_eq!(constants::DEFAULT_BATCH_SIZE, 1000);
        assert_eq!(constants::MAX_BATCH_SIZE, 100_000);
        assert_eq!(constants::DEFAULT_BUFFER_SIZE, 8192);
        assert_eq!(constants::SMALL_BUFFER_SIZE, 1024);
        assert_eq!(constants::LARGE_BUFFER_SIZE, 128 * 1024);
        assert_eq!(constants::DEFAULT_SCHEMA_INFERENCE_LENGTH, 1000);
        assert_eq!(constants::MAX_RECURSION_DEPTH, 1000);
        assert_eq!(constants::HIGH_THROUGHPUT_BATCH_SIZE, 10000);
        assert_eq!(constants::FIELD_SEPARATOR, 31u8);
        assert_eq!(constants::RECORD_SEPARATOR, 30u8);
        assert_eq!(constants::CONTENT_SAMPLE_SIZE, 4096);
        assert_eq!(constants::DEFAULT_MEMORY_LIMIT, 1024 * 1024 * 1024);
        assert_eq!(constants::MAX_MEMORY_FILE_SIZE, 100 * 1024 * 1024);

        // Sanity checks
        assert!(constants::DEFAULT_BATCH_SIZE > 0);
        assert!(constants::MAX_BATCH_SIZE > constants::DEFAULT_BATCH_SIZE);
        assert!(constants::DEFAULT_BUFFER_SIZE > 0);
        assert!(constants::SMALL_BUFFER_SIZE > 0);
        assert!(constants::LARGE_BUFFER_SIZE > constants::DEFAULT_BUFFER_SIZE);
        assert!(constants::DEFAULT_SCHEMA_INFERENCE_LENGTH > 0);
        assert!(constants::MAX_RECURSION_DEPTH > 0);
        assert!(constants::HIGH_THROUGHPUT_BATCH_SIZE > constants::DEFAULT_BATCH_SIZE);
        assert!(constants::CONTENT_SAMPLE_SIZE > 0);
        assert!(constants::DEFAULT_MEMORY_LIMIT > 0);
    }
}
