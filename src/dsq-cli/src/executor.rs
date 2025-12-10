//! Filter execution engine for dsq CLI
//!
//! This module provides the main execution logic for running jq-compatible
//! filters on data files through the command-line interface.

use std::borrow::Cow;
use std::fmt::Write;
use std::io::{self, Read};
use std::path::Path;

use polars::prelude::SerWriter;

use crate::config::Config;
use dsq_core::error::{Error, Result};
use dsq_core::filter::{FilterCompiler, FilterExecutor as CoreFilterExecutor};
use dsq_core::io::{read_file, write_file};
use dsq_core::Value;

/// Main executor for dsq operations
pub struct Executor {
    config: Config,
    pub filter_executor: CoreFilterExecutor,
}

impl Executor {
    /// Create a new executor with the given configuration
    pub fn new(config: Config) -> Self {
        let executor_config = config.to_executor_config();
        let filter_executor = CoreFilterExecutor::with_config(executor_config);
        Self {
            config,
            filter_executor,
        }
    }

    /// Execute a filter on input data
    pub async fn execute_filter(
        &mut self,
        filter: &str,
        input_path: Option<&Path>,
        output_path: Option<&Path>,
    ) -> Result<()> {
        // Read input data
        let input_value = if let Some(path) = input_path {
            self.read_input(path).await?
        } else {
            // Read from stdin
            self.read_from_stdin().await?
        };

        self.execute_filter_on_value(filter, input_value, output_path)
            .await
    }

    /// Execute a filter on a value directly
    pub async fn execute_filter_on_value(
        &mut self,
        filter: &str,
        input_value: Value,
        output_path: Option<&Path>,
    ) -> Result<()> {
        // Execute the filter
        #[cfg(feature = "profiling")]
        coz::progress!("filter_execution");

        let result = self.filter_executor.execute_str(filter, input_value)?;
        let mut result_value = result.value;

        #[cfg(feature = "profiling")]
        coz::progress!("filter_complete");

        // Apply limit if specified
        if let Some(limit) = self.config.io.limit {
            result_value = self.apply_limit(result_value, limit)?;
        }

        // Write output
        if let Some(path) = output_path {
            self.write_output(&result_value, path).await?;
        } else {
            // Write to stdout
            self.write_to_stdout(&result_value)?;
        }

        #[cfg(feature = "profiling")]
        coz::progress!("output_complete");

        // Handle exit status
        if self.config.display.exit_status {
            let exit_code = match &result_value {
                Value::Null => 1,
                Value::Bool(false) => 1,
                Value::Array(arr) if arr.is_empty() => 1,
                Value::String(s) if s.is_empty() => 1,
                _ => 0,
            };
            std::process::exit(exit_code);
        }

        // Print execution stats if requested
        if self.config.debug.verbosity > 0 {
            eprintln!(
                "Execution time: {} ms",
                result
                    .stats
                    .as_ref()
                    .map(|s| s.execution_time.as_millis() as u64)
                    .unwrap_or(0)
            );
            eprintln!(
                "Operations: {}",
                result
                    .stats
                    .as_ref()
                    .map(|s| s.operations_executed)
                    .unwrap_or(0)
            );
        }

        Ok(())
    }

    /// Apply limit to a value
    fn apply_limit(&self, value: Value, limit: usize) -> Result<Value> {
        match value {
            Value::Array(arr) => {
                let limited = arr.into_iter().take(limit).collect();
                Ok(Value::Array(limited))
            }
            Value::DataFrame(df) => Ok(Value::DataFrame(df.head(Some(limit)))),
            // For other types, return as-is (limit doesn't apply)
            other => Ok(other),
        }
    }

    /// Validate that a filter is syntactically correct
    pub fn validate_filter(&self, filter: &str) -> Result<()> {
        // For now, just try to compile it
        let compiler = FilterCompiler::new();
        let _compiled = compiler.compile_str(filter)?;
        Ok(())
    }

    /// Explain what a filter does
    pub fn explain_filter(&self, filter: &str) -> Result<String> {
        Ok(dsq_core::filter::explain_filter(filter)?)
    }

    /// Read input from a file path
    pub async fn read_input(&self, path: &Path) -> Result<Value> {
        let read_options = self.config.to_read_options();
        let result = read_file(path, &read_options).await?;

        #[cfg(feature = "profiling")]
        coz::progress!("input_read");

        Ok(result)
    }

    /// Read input from stdin
    async fn read_from_stdin(&self) -> Result<Value> {
        use std::io::BufRead;
        let stdin = io::stdin();
        let mut reader = io::BufReader::new(stdin);

        // Read first line to detect format
        let mut first_line = String::new();
        reader.read_line(&mut first_line)?;

        // Try to detect format from first non-whitespace character
        let trimmed = first_line.trim();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            // Assume JSON - read entire input
            let mut buffer = first_line;
            reader.read_to_string(&mut buffer)?;
            let json_value: serde_json::Value = serde_json::from_str(&buffer)
                .map_err(|e| Error::operation(Cow::Owned(format!("Invalid JSON: {}", e))))?;
            Ok(Value::from_json(json_value))
        } else {
            // Assume CSV - write to temp file and read
            use std::io::Write;
            let mut temp_file = tempfile::NamedTempFile::new()?;
            temp_file.write_all(first_line.as_bytes())?;
            io::copy(&mut reader, &mut temp_file)?;
            let temp_path = temp_file.path().to_path_buf();
            let read_options = self.config.to_read_options();
            read_file(&temp_path, &read_options).await
        }
    }

    /// Write output to a file path
    async fn write_output(&self, value: &Value, path: &Path) -> Result<()> {
        let write_options = self.config.to_write_options();
        write_file(value, path, &write_options).await
    }

    /// Write output to stdout
    pub fn write_to_stdout(&self, value: &Value) -> Result<()> {
        use dsq_core::DataFormat;

        // Handle raw output
        if self.config.display.raw_output {
            match value {
                Value::String(s) => {
                    println!("{}", s);
                    return Ok(());
                }
                Value::Array(arr) => {
                    for item in arr {
                        if let Value::String(s) = item {
                            println!("{}", s);
                        } else {
                            let json = item.to_json()?;
                            println!("{}", json);
                        }
                    }
                    return Ok(());
                }
                _ => {
                    let json = value.to_json()?;
                    println!("{}", json);
                    return Ok(());
                }
            }
        }

        let output_format = self
            .config
            .io
            .default_output_format
            .unwrap_or(DataFormat::Json);

        match output_format {
            DataFormat::Json => {
                // Write as JSON to stdout
                let json_value = value.to_json()?;
                let json_str = if self.config.display.compact {
                    serde_json::to_string(&json_value)
                } else {
                    serde_json::to_string_pretty(&json_value)
                }
                .map_err(|e| {
                    Error::operation(Cow::Owned(format!("JSON serialization error: {}", e)))
                })?;
                println!("{}", json_str);
            }
            DataFormat::JsonCompact => {
                // Write as compact JSON to stdout
                let json_value = value.to_json()?;
                let json_str = serde_json::to_string(&json_value)
                    .map_err(|e| Error::operation(format!("JSON serialization error: {}", e)))?;
                println!("{}", json_str);
            }
            DataFormat::JsonLines => {
                // Write as NDJSON to stdout
                let json_value = value.to_json()?;
                match json_value {
                    serde_json::Value::Array(arr) => {
                        for item in arr {
                            let json_str = serde_json::to_string(&item).map_err(|e| {
                                Error::operation(format!("JSON serialization error: {}", e))
                            })?;
                            println!("{}", json_str);
                        }
                    }
                    _ => {
                        // For non-arrays, output as single line
                        let json_str = serde_json::to_string(&json_value).map_err(|e| {
                            Error::operation(format!("JSON serialization error: {}", e))
                        })?;
                        println!("{}", json_str);
                    }
                }
            }
            DataFormat::Csv => {
                match value {
                    Value::DataFrame(df) => {
                        // Write as CSV to stdout - avoid clone by using a mutable reference
                        use polars::prelude::CsvWriter;
                        use std::io::BufWriter;
                        let stdout = std::io::stdout();
                        let mut writer = BufWriter::with_capacity(65536, stdout.lock());
                        CsvWriter::new(&mut writer)
                            .include_header(true)
                            .finish(&mut df.clone())
                            .map_err(|e| Error::operation(format!("CSV write error: {}", e)))?;
                    }
                    Value::LazyFrame(lf) => {
                        let df = lf.clone().collect()?;
                        self.write_to_stdout(&Value::DataFrame(df))?;
                    }
                    _ => {
                        // For non-DataFrame values, fall back to JSON
                        let json_value = value.to_json()?;
                        let json_str = serde_json::to_string_pretty(&json_value).map_err(|e| {
                            Error::operation(format!("JSON serialization error: {}", e))
                        })?;
                        println!("{}", json_str);
                    }
                }
            }
            DataFormat::Adt => {
                match value {
                    Value::DataFrame(df) => {
                        // Write as ADT to stdout with buffering for better performance
                        use std::io::{self, BufWriter, Write};

                        const FIELD_SEPARATOR: u8 = 31;
                        const RECORD_SEPARATOR: u8 = 30;

                        let stdout_handle = io::stdout();
                        let mut stdout = BufWriter::with_capacity(65536, stdout_handle.lock());

                        // Write header
                        let headers: Vec<&str> =
                            df.get_column_names().iter().map(|s| s.as_str()).collect();
                        for (i, header) in headers.iter().enumerate() {
                            if i > 0 {
                                stdout.write_all(&[FIELD_SEPARATOR])?;
                            }
                            stdout.write_all(header.as_bytes())?;
                        }
                        stdout.write_all(&[RECORD_SEPARATOR])?;

                        // Write data rows
                        let height = df.height();
                        let mut value_buffer = String::new(); // Pre-allocated buffer for value formatting
                        for row_idx in 0..height {
                            for (col_idx, column) in df.get_columns().iter().enumerate() {
                                if col_idx > 0 {
                                    stdout.write_all(&[FIELD_SEPARATOR])?;
                                }

                                value_buffer.clear(); // Reuse the buffer
                                match column.get(row_idx).map_err(|e| {
                                    Error::operation(Cow::Owned(format!(
                                        "Failed to get column value: {}",
                                        e
                                    )))
                                })? {
                                    polars::prelude::AnyValue::String(s) => {
                                        value_buffer.push_str(s)
                                    }
                                    polars::prelude::AnyValue::Int64(i) => {
                                        write!(value_buffer, "{}", i).unwrap()
                                    }
                                    polars::prelude::AnyValue::Float64(f) => {
                                        write!(value_buffer, "{}", f).unwrap()
                                    }
                                    polars::prelude::AnyValue::Boolean(b) => {
                                        write!(value_buffer, "{}", b).unwrap()
                                    }
                                    polars::prelude::AnyValue::Null => {} // buffer remains empty
                                    other => write!(value_buffer, "{}", other).unwrap(),
                                };

                                stdout.write_all(value_buffer.as_bytes())?;
                            }
                            stdout.write_all(&[RECORD_SEPARATOR])?;
                        }
                        stdout.flush()?;
                    }
                    Value::LazyFrame(lf) => {
                        let df = lf.clone().collect()?;
                        self.write_to_stdout(&Value::DataFrame(df))?;
                    }
                    _ => {
                        // For non-DataFrame values, fall back to JSON
                        let json_value = value.to_json()?;
                        let json_str = serde_json::to_string_pretty(&json_value).map_err(|e| {
                            Error::operation(format!("JSON serialization error: {}", e))
                        })?;
                        println!("{}", json_str);
                    }
                }
            }
            _ => {
                // For other formats, fall back to string representation
                println!("{}", value);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_execute_filter_on_value_identity() {
        let config = Config::default();
        let mut executor = Executor::new(config);
        let input_value = dsq_core::utils::object([
            ("name", dsq_core::Value::string("Alice")),
            ("age", dsq_core::Value::int(30)),
        ]);

        // Test identity filter "."
        let result = executor
            .execute_filter_on_value(".", input_value.clone(), None)
            .await;
        assert!(result.is_ok());
        // For now, just check it doesn't error
    }

    #[test]
    fn test_validate_filter() {
        let config = Config::default();
        let executor = Executor::new(config);

        // Valid filter
        assert!(executor.validate_filter(".").is_ok());
        assert!(executor.validate_filter(".name").is_ok());

        // Invalid filter
        assert!(executor.validate_filter("invalid syntax +++").is_err());
    }

    #[test]
    fn test_explain_filter() {
        let config = Config::default();
        let executor = Executor::new(config);

        // Test explaining a filter
        let result = executor.explain_filter(".");
        assert!(result.is_ok());
        let explanation = result.unwrap();
        assert!(!explanation.is_empty());
    }

    #[tokio::test]
    async fn test_execute_filter_on_value_with_filter() {
        let config = Config::default();
        let mut executor = Executor::new(config);
        let input_value = dsq_core::utils::object([
            ("name", dsq_core::Value::string("Alice")),
            ("age", dsq_core::Value::int(30)),
        ]);

        // Test filter ".name"
        let result = executor
            .execute_filter_on_value(".name", input_value.clone(), None)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_filter_on_value_with_filter() {
        let config = Config::default();
        let mut executor = Executor::new(config);
        let input_value = dsq_core::utils::object([
            ("name", dsq_core::Value::string("Alice")),
            ("age", dsq_core::Value::int(30)),
        ]);

        // Test filter ".name"
        let result = executor
            .execute_filter_on_value(".name", input_value.clone(), None)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_filter_on_value_with_limit() {
        let mut config = Config::default();
        config.io.limit = Some(1);
        let mut executor = Executor::new(config);
        let input_value = Value::Array(vec![
            dsq_core::Value::int(1),
            dsq_core::Value::int(2),
            dsq_core::Value::int(3),
        ]);

        // Test with limit
        let result = executor
            .execute_filter_on_value(".", input_value, None)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_filter_on_value_invalid_filter() {
        let config = Config::default();
        let mut executor = Executor::new(config);
        let input_value = Value::Null;

        // Test invalid filter
        let result = executor
            .execute_filter_on_value("invalid +++", input_value, None)
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_limit_array() {
        let config = Config::default();
        let executor = Executor::new(config);
        let value = Value::Array(vec![
            Value::int(1),
            Value::int(2),
            Value::int(3),
            Value::int(4),
        ]);
        let limited = executor.apply_limit(value, 2).unwrap();
        match limited {
            Value::Array(arr) => assert_eq!(arr.len(), 2),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_apply_limit_other() {
        let config = Config::default();
        let executor = Executor::new(config);
        let value = Value::string("test");
        let limited = executor.apply_limit(value.clone(), 1).unwrap();
        assert_eq!(limited, value);
    }

    #[test]
    fn test_validate_filter_invalid() {
        let config = Config::default();
        let executor = Executor::new(config);

        // More invalid filters
        assert!(executor.validate_filter(".").is_ok());
        assert!(executor.validate_filter("invalid syntax +++").is_err());
        assert!(executor.validate_filter(".name.").is_err());
        assert!(executor.validate_filter("(.name").is_err());
    }

    #[tokio::test]
    async fn test_read_input() {
        let config = Config::default();
        let executor = Executor::new(config);
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(&temp_file, r#"{"name": "test"}"#).unwrap();
        let path = temp_file.path();

        let result = executor.read_input(path).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_write_output() {
        let config = Config::default();
        let executor = Executor::new(config);
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let path = temp_file.path();
        let value = Value::string("test");

        let result = executor.write_output(&value, path).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_filter_with_file() {
        let config = Config::default();
        let mut executor = Executor::new(config);
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(&temp_file, r#"{"name": "Alice"}"#).unwrap();
        let input_path = temp_file.path();

        let result = executor
            .execute_filter(".name", Some(input_path), None)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_filter_on_value_with_stats() {
        let mut config = Config::default();
        config.debug.verbosity = 1;
        let mut executor = Executor::new(config);
        let input_value = Value::Null;

        // Capture stderr for stats, but since it's eprintln, hard to test
        let result = executor
            .execute_filter_on_value(".", input_value, None)
            .await;
        assert!(result.is_ok());
    }
}
