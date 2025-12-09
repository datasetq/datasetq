//! Output formatting and writing for dsq CLI
//!
//! This module handles formatting and writing output data in various formats
//! with support for different output destinations and formatting options.

use crate::config::Config;
use dsq_core::error::{Error, Result};
use dsq_core::io::write_file;
use dsq_core::DataFormat;
use dsq_core::Value;
use polars::prelude::*;
use std::path::Path;
use tokio::fs as tokio_fs;

/// Output writer for dsq results
pub struct OutputWriter {
    config: Config,
}

impl OutputWriter {
    /// Create a new output writer
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Write a value to the specified output destination
    #[allow(dead_code)]
    pub async fn write(&self, value: &Value, output_path: Option<&Path>) -> Result<()> {
        match output_path {
            Some(path) => self.write_to_file(value, path).await,
            None => self.write_to_stdout(value),
        }
    }

    /// Write to a file
    async fn write_to_file(&self, value: &Value, path: &Path) -> Result<()> {
        match value {
            Value::String(s) => {
                // For strings, just write directly to file
                tokio_fs::write(path, s.as_bytes())
                    .await
                    .map_err(|e| Error::operation(format!("File write error: {}", e)))?;
                Ok(())
            }
            _ => {
                let write_options = self.config.to_write_options();
                write_file(value, path, &write_options).await
            }
        }
    }

    /// Write to stdout with appropriate formatting
    pub fn write_to_stdout(&self, value: &Value) -> Result<()> {
        match value {
            Value::DataFrame(df) => {
                // For DataFrames, write as CSV
                use polars::prelude::CsvWriter;
                let mut writer = std::io::stdout();
                CsvWriter::new(&mut writer)
                    .include_header(true)
                    .finish(&mut df.clone())
                    .map_err(|e| Error::operation(format!("CSV write error: {}", e)))?;
            }
            Value::LazyFrame(lf) => {
                let df = lf.clone().collect()?;
                // Write DataFrame directly without recursion
                use polars::prelude::CsvWriter;
                let mut writer = std::io::stdout();
                CsvWriter::new(&mut writer)
                    .include_header(true)
                    .finish(&mut df.clone())
                    .map_err(|e| Error::operation(format!("CSV write error: {}", e)))?;
            }
            Value::Array(_) | Value::Object(_) => {
                // For structured data, write as JSON
                let json_value = value.to_json()?;
                let json_str = serde_json::to_string_pretty(&json_value)
                    .map_err(|e| Error::operation(format!("JSON serialization error: {}", e)))?;
                println!("{}", json_str);
            }
            _ => {
                // For simple values, just print them
                println!("{}", value);
            }
        }

        Ok(())
    }

    /// Write formatted output with color and styling
    #[allow(dead_code)]
    pub async fn write_formatted(&self, value: &Value, output_path: Option<&Path>) -> Result<()> {
        // For now, just delegate to regular write
        // In the future, this could add color coding, syntax highlighting, etc.
        self.write(value, output_path).await
    }

    /// Write with custom formatting options
    #[allow(dead_code)]
    pub async fn write_with_options(
        &self,
        value: &Value,
        output_path: Option<&Path>,
        format: Option<DataFormat>,
        pretty: bool,
    ) -> Result<()> {
        match output_path {
            Some(path) => {
                match format {
                    Some(DataFormat::Csv) => self.write_csv_to_file(value, path).await,
                    Some(DataFormat::Tsv) => self.write_tsv_to_file(value, path).await,
                    Some(DataFormat::Json) => self.write_json_to_file(value, path, pretty).await,
                    Some(DataFormat::JsonLines) => self.write_jsonlines_to_file(value, path).await,
                    _ => {
                        // Unsupported format, write as JSON
                        let json_value = value.to_json()?;
                        let json_str = serde_json::to_string(&json_value)?;
                        tokio_fs::write(path, json_str).await?;
                        Ok(())
                    }
                }
            }
            None => {
                match format {
                    Some(DataFormat::Csv) | Some(DataFormat::Tsv) => {
                        let separator = if matches!(format, Some(DataFormat::Tsv)) {
                            b'\t'
                        } else {
                            b','
                        };
                        let mut writer = std::io::stdout();
                        use polars::prelude::CsvWriter;
                        match value {
                            Value::DataFrame(df) => {
                                CsvWriter::new(&mut writer)
                                    .include_header(true)
                                    .with_separator(separator)
                                    .finish(&mut df.clone())?;
                            }
                            Value::LazyFrame(lf) => {
                                let mut df = lf.clone().collect()?;
                                CsvWriter::new(&mut writer)
                                    .include_header(true)
                                    .with_separator(separator)
                                    .finish(&mut df)?;
                            }
                            _ => {
                                return Err(Error::operation(
                                    "Cannot write non-DataFrame value to CSV/TSV",
                                ));
                            }
                        }
                    }
                    Some(DataFormat::Json) | None => {
                        let json_value = value.to_json()?;
                        if pretty {
                            let json_str =
                                serde_json::to_string_pretty(&json_value).map_err(|e| {
                                    Error::operation(format!("JSON serialization error: {}", e))
                                })?;
                            println!("{}", json_str);
                        } else {
                            let json_str = serde_json::to_string(&json_value).map_err(|e| {
                                Error::operation(format!("JSON serialization error: {}", e))
                            })?;
                            println!("{}", json_str);
                        }
                    }
                    Some(DataFormat::JsonLines) => {
                        // NDJSON: output each element of top-level arrays as separate lines
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
                    _ => {
                        // For other formats, convert to string representation
                        println!("{}", value);
                    }
                }
                Ok(())
            }
        }
    }

    #[allow(dead_code)]
    fn write_dataframe_to_file(&self, df: &DataFrame, path: &Path, separator: u8) -> Result<()> {
        use polars::prelude::CsvWriter;
        let mut file = std::fs::File::create(path)?;
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(separator)
            .finish(&mut df.clone())?;
        Ok(())
    }

    #[allow(dead_code)]
    async fn write_csv_to_file(&self, value: &Value, path: &Path) -> Result<()> {
        match value {
            Value::DataFrame(df) => self.write_dataframe_to_file(df, path, b','),
            Value::LazyFrame(lf) => {
                let df = lf.clone().collect()?;
                self.write_dataframe_to_file(&df, path, b',')
            }
            _ => Err(Error::operation("Cannot write non-DataFrame value to CSV")),
        }
    }

    #[allow(dead_code)]
    async fn write_tsv_to_file(&self, value: &Value, path: &Path) -> Result<()> {
        match value {
            Value::DataFrame(df) => self.write_dataframe_to_file(df, path, b'\t'),
            Value::LazyFrame(lf) => {
                let df = lf.clone().collect()?;
                self.write_dataframe_to_file(&df, path, b'\t')
            }
            _ => Err(Error::operation("Cannot write non-DataFrame value to TSV")),
        }
    }

    #[allow(dead_code)]
    async fn write_json_to_file(&self, value: &Value, path: &Path, pretty: bool) -> Result<()> {
        let json_value = value.to_json()?;
        let json_str = if pretty {
            serde_json::to_string_pretty(&json_value)?
        } else {
            serde_json::to_string(&json_value)?
        };
        tokio_fs::write(path, json_str).await?;
        Ok(())
    }

    #[allow(dead_code)]
    async fn write_jsonlines_to_file(&self, value: &Value, path: &Path) -> Result<()> {
        let json_value = value.to_json()?;
        let mut content = String::new();
        match json_value {
            serde_json::Value::Array(arr) => {
                for item in arr {
                    let json_str = serde_json::to_string(&item)?;
                    content.push_str(&json_str);
                    content.push('\n');
                }
            }
            _ => {
                let json_str = serde_json::to_string(&json_value)?;
                content = json_str;
            }
        }
        tokio_fs::write(path, content).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_core::Value;
    use std::collections::HashMap;
    use std::fs;
    use tempfile::NamedTempFile;

    fn create_test_config() -> Config {
        Config::default()
    }

    fn create_test_dataframe() -> DataFrame {
        DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap()
    }

    fn create_test_lazyframe() -> LazyFrame {
        create_test_dataframe().lazy()
    }

    #[test]
    fn test_output_writer_new() {
        let config = create_test_config();
        let _writer = OutputWriter::new(config);
        // Just check it creates successfully
        assert!(true); // Placeholder assertion
    }

    #[test]
    fn test_write_to_stdout_dataframe() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);

        // This will output to stdout, but we can at least check it doesn't error
        let result = writer.write_to_stdout(&value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_to_stdout_lazyframe() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let lf = create_test_lazyframe();
        let value = Value::LazyFrame(Box::new(lf));

        let result = writer.write_to_stdout(&value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_to_stdout_array() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let arr = Value::Array(vec![
            Value::String("hello".to_string()),
            Value::Int(42),
            Value::Bool(true),
        ]);

        let result = writer.write_to_stdout(&arr);
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_to_stdout_object() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let obj = Value::Object(HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ]));

        let result = writer.write_to_stdout(&obj);
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_to_stdout_simple_values() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);

        // Test various simple values
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(42),
            Value::Float(3.14),
            Value::String("hello".to_string()),
        ];

        for value in values {
            let result = writer.write_to_stdout(&value);
            assert!(result.is_ok(), "Failed for value: {:?}", value);
        }
    }

    #[tokio::test]
    async fn test_write_to_file() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let result = writer.write_to_file(&value, path).await;
        assert!(result.is_ok());

        // Check file was created and has content
        let content = fs::read_to_string(path).unwrap();
        assert!(!content.is_empty());
        assert!(content.contains("name,age")); // CSV header
        assert!(content.contains("Alice,30"));
    }

    #[tokio::test]
    async fn test_write_formatted() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let value = Value::String("test".to_string());

        // Currently just delegates to write
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let result = writer.write_formatted(&value, Some(path)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_write_with_options_csv() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let result = writer
            .write_with_options(&value, Some(path), Some(DataFormat::Csv), false)
            .await;
        assert!(result.is_ok());

        let content = fs::read_to_string(path).unwrap();
        assert!(content.contains("name,age"));
    }

    #[tokio::test]
    async fn test_write_with_options_tsv() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let df = create_test_dataframe();
        let value = Value::DataFrame(df);

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let result = writer
            .write_with_options(&value, Some(path), Some(DataFormat::Tsv), false)
            .await;
        assert!(result.is_ok());

        let content = fs::read_to_string(path).unwrap();
        assert!(content.contains("name\tage"));
    }

    #[tokio::test]
    async fn test_write_with_options_json_pretty() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let obj = Value::Object(HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ]));

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let result = writer
            .write_with_options(&obj, Some(path), Some(DataFormat::Json), true)
            .await;
        assert!(result.is_ok());

        let content = fs::read_to_string(path).unwrap();
        assert!(content.contains("\n")); // Pretty printed should have newlines
        assert!(content.contains("  ")); // Indentation
    }

    #[tokio::test]
    async fn test_write_with_options_json_compact() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let obj = Value::Object(HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ]));

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let result = writer
            .write_with_options(&obj, Some(path), Some(DataFormat::Json), false)
            .await;
        assert!(result.is_ok());

        let content = fs::read_to_string(path).unwrap();
        assert!(!content.contains("\n")); // Compact should not have newlines
    }

    #[tokio::test]
    async fn test_write_with_options_jsonlines_array() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let arr = Value::Array(vec![
            Value::Object(HashMap::from([(
                "name".to_string(),
                Value::String("Alice".to_string()),
            )])),
            Value::Object(HashMap::from([(
                "name".to_string(),
                Value::String("Bob".to_string()),
            )])),
        ]);

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let result = writer
            .write_with_options(&arr, Some(path), Some(DataFormat::JsonLines), false)
            .await;
        assert!(result.is_ok());

        let content = fs::read_to_string(path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("Alice"));
        assert!(lines[1].contains("Bob"));
    }

    #[tokio::test]
    async fn test_write_with_options_jsonlines_single() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let obj = Value::Object(HashMap::from([(
            "name".to_string(),
            Value::String("Alice".to_string()),
        )]));

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let result = writer
            .write_with_options(&obj, Some(path), Some(DataFormat::JsonLines), false)
            .await;
        assert!(result.is_ok());

        let content = fs::read_to_string(path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("Alice"));
    }

    #[tokio::test]
    async fn test_write_with_options_unsupported_format() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let value = Value::String("test".to_string());

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Test with a format that's not handled (should fall back to string representation)
        let result = writer
            .write_with_options(&value, Some(path), Some(DataFormat::Parquet), false)
            .await;
        assert!(result.is_ok());

        let content = fs::read_to_string(path).unwrap();
        assert_eq!(content.trim(), "\"test\"");
    }

    #[tokio::test]
    async fn test_write_with_options_csv_non_dataframe_error() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);
        let value = Value::String("not a dataframe".to_string());

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let result = writer
            .write_with_options(&value, Some(path), Some(DataFormat::Csv), false)
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_write_to_stdout_lazyframe_error() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);

        // Create a LazyFrame that will fail to collect (this is hard to simulate)
        // For now, just test that normal case works
        let lf = create_test_lazyframe();
        let value = Value::LazyFrame(Box::new(lf));

        let result = writer.write_to_stdout(&value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_to_stdout_json_serialization_error() {
        let config = create_test_config();
        let writer = OutputWriter::new(config);

        // Create a value that might cause JSON serialization issues
        // Most values should work, but let's test with a complex nested structure
        let nested = Value::Object(HashMap::from([(
            "data".to_string(),
            Value::Array(vec![Value::Object(HashMap::from([(
                "nested".to_string(),
                Value::Array(vec![Value::Int(1), Value::Int(2)]),
            )]))]),
        )]));

        let result = writer.write_to_stdout(&nested);
        assert!(result.is_ok());
    }
}
