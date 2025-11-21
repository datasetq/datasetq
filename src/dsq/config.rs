//! Configuration management for dsq
//!
//! This module provides configuration management for dsq, handling configuration
//! from multiple sources including command-line arguments, environment variables,
//! and configuration files. It provides a unified configuration structure that
//! can be used throughout the application.

use crate::cli::CliConfig;
use dsq_core::{
    error::{Error, Result},
    filter::{ErrorMode, ExecutorConfig},
    io::{ReadOptions, WriteOptions},
    DataFormat,
};
use dsq_shared::value::Value;

use serde::{Deserialize, Serialize};

use std::fs;
use std::path::{Path, PathBuf};

/// Main configuration structure for dsq runtime
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Input/output configuration
    pub io: IoConfig,
    /// Filter execution configuration
    pub filter: FilterConfig,
    /// Format-specific configurations
    pub formats: FormatConfigs,
    /// Display and output configuration
    pub display: DisplayConfig,
    /// Performance and resource configuration
    pub performance: PerformanceConfig,
    /// Module and library configuration
    pub modules: ModuleConfig,
    /// Debug and diagnostic configuration
    pub debug: DebugConfig,
    /// Variables for filter execution
    pub variables: std::collections::HashMap<String, serde_json::Value>,
}

/// Input/output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoConfig {
    /// Default input format when not detected
    pub default_input_format: Option<DataFormat>,
    /// Default output format when not specified
    pub default_output_format: Option<DataFormat>,
    /// Whether to auto-detect formats
    pub auto_detect_format: bool,
    /// Buffer size for I/O operations
    pub buffer_size: usize,
    /// Whether to overwrite existing files by default
    pub overwrite_by_default: bool,
    /// Maximum file size for in-memory processing
    pub max_memory_file_size: usize,
    /// Maximum number of rows to output
    pub limit: Option<usize>,
}

/// Filter execution configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FilterConfig {
    /// Whether to use lazy evaluation by default
    pub lazy_evaluation: bool,
    /// Whether to enable DataFrame optimizations
    pub dataframe_optimizations: bool,
    /// Filter optimization level
    pub optimization_level: String,
    /// Maximum recursion depth
    pub max_recursion_depth: usize,
    /// Maximum execution time in seconds
    pub max_execution_time: Option<u64>,
    /// Whether to collect execution statistics
    pub collect_stats: bool,
    /// Error handling mode
    pub error_mode: String,
}

/// Format-specific configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FormatConfigs {
    /// CSV configuration
    pub csv: CsvConfig,
    /// JSON configuration
    pub json: JsonConfig,
    /// Parquet configuration
    pub parquet: ParquetConfig,
}

/// CSV format configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CsvConfig {
    /// Default field separator
    pub separator: String,
    /// Whether files have headers by default
    pub has_header: bool,
    /// Quote character
    pub quote_char: String,
    /// Comment character
    pub comment_char: Option<String>,
    /// Values to treat as null
    pub null_values: Vec<String>,
    /// Whether to trim whitespace
    pub trim_whitespace: bool,
    /// Number of rows for schema inference
    pub infer_schema_length: usize,
}

/// JSON format configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonConfig {
    /// Whether to pretty-print by default
    pub pretty_print: bool,
    /// Whether to maintain field order
    pub maintain_order: bool,
    /// Whether to escape Unicode characters
    pub escape_unicode: bool,
    /// Whether to flatten nested objects by default
    pub flatten: bool,
    /// Separator for flattened field names
    pub flatten_separator: String,
}

/// Parquet format configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    /// Default compression algorithm
    pub compression: String,
    /// Whether to write statistics
    pub write_statistics: bool,
    /// Row group size
    pub row_group_size: usize,
    /// Data page size limit
    pub data_page_size: usize,
}

/// Display and output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayConfig {
    /// Whether to use colored output
    pub color: ColorConfig,
    /// Whether to use compact output by default
    pub compact: bool,
    /// Whether to sort object keys
    pub sort_keys: bool,
    /// Whether to use raw output for strings
    pub raw_output: bool,
    /// Whether to set exit status based on filter result
    pub exit_status: bool,
    /// Number format configuration
    pub number_format: NumberFormatConfig,
    /// Date/time format configuration
    pub datetime_format: DateTimeFormatConfig,
}

/// Color configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColorConfig {
    /// Whether colors are enabled
    pub enabled: Option<bool>,
    /// Color scheme name
    pub scheme: String,
    /// Whether to detect terminal capabilities
    pub auto_detect: bool,
}

/// Number formatting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumberFormatConfig {
    /// Decimal precision for floats
    pub float_precision: Option<usize>,
    /// Whether to use scientific notation
    pub scientific_notation: bool,
    /// Threshold for scientific notation
    pub scientific_threshold: f64,
}

/// Date/time formatting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateTimeFormatConfig {
    /// Default date format
    pub date_format: String,
    /// Default datetime format
    pub datetime_format: String,
    /// Default time format
    pub time_format: String,
    /// Timezone handling
    pub timezone: String,
}

/// Performance and resource configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PerformanceConfig {
    /// Default batch size for processing
    pub batch_size: usize,
    /// Memory limit in bytes
    pub memory_limit: Option<usize>,
    /// Number of threads to use (0 = auto)
    pub threads: usize,
    /// Whether to enable parallel execution
    pub parallel: bool,
    /// Cache size for repeated operations
    pub cache_size: usize,
}

/// Module and library configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleConfig {
    /// Library search paths
    pub library_paths: Vec<PathBuf>,
    /// Auto-load modules
    pub auto_load: Vec<String>,
    /// Module cache directory
    pub cache_dir: Option<PathBuf>,
}

/// Debug and diagnostic configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DebugConfig {
    /// Verbosity level
    pub verbosity: u8,
    /// Whether to show execution plans
    pub show_plans: bool,
    /// Whether to show timing information
    pub show_timing: bool,
    /// Whether to enable debug mode
    pub debug_mode: bool,
    /// Log file path
    pub log_file: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            io: IoConfig::default(),
            filter: FilterConfig::default(),
            formats: FormatConfigs::default(),
            display: DisplayConfig::default(),
            performance: PerformanceConfig::default(),
            modules: ModuleConfig::default(),
            debug: DebugConfig::default(),
            variables: std::collections::HashMap::new(),
        }
    }
}

impl Default for IoConfig {
    fn default() -> Self {
        Self {
            default_input_format: None,
            default_output_format: None,
            auto_detect_format: true,
            buffer_size: 8192,
            overwrite_by_default: false,
            max_memory_file_size: 100 * 1024 * 1024, // 100MB
            limit: None,
        }
    }
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            lazy_evaluation: true,
            dataframe_optimizations: true,
            optimization_level: "basic".to_string(),
            max_recursion_depth: 1000,
            max_execution_time: Some(300), // 5 minutes
            collect_stats: false,
            error_mode: "strict".to_string(),
        }
    }
}

impl Default for FormatConfigs {
    fn default() -> Self {
        Self {
            csv: CsvConfig::default(),
            json: JsonConfig::default(),
            parquet: ParquetConfig::default(),
        }
    }
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            separator: ",".to_string(),
            has_header: true,
            quote_char: "\"".to_string(),
            comment_char: None,
            null_values: vec!["".to_string(), "NA".to_string(), "NULL".to_string()],
            trim_whitespace: false,
            infer_schema_length: 1000,
        }
    }
}

impl Default for JsonConfig {
    fn default() -> Self {
        Self {
            pretty_print: true,
            maintain_order: false,
            escape_unicode: false,
            flatten: false,
            flatten_separator: ".".to_string(),
        }
    }
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            compression: "snappy".to_string(),
            write_statistics: true,
            row_group_size: 1024 * 1024,
            data_page_size: 1024 * 1024,
        }
    }
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            color: ColorConfig::default(),
            compact: false,
            sort_keys: false,
            raw_output: false,
            exit_status: false,
            number_format: NumberFormatConfig::default(),
            datetime_format: DateTimeFormatConfig::default(),
        }
    }
}

impl Default for ColorConfig {
    fn default() -> Self {
        Self {
            enabled: None,
            scheme: "default".to_string(),
            auto_detect: true,
        }
    }
}

impl Default for NumberFormatConfig {
    fn default() -> Self {
        Self {
            float_precision: None,
            scientific_notation: false,
            scientific_threshold: 1e9,
        }
    }
}

impl Default for DateTimeFormatConfig {
    fn default() -> Self {
        Self {
            date_format: "%Y-%m-%d".to_string(),
            datetime_format: "%Y-%m-%d %H:%M:%S".to_string(),
            time_format: "%H:%M:%S".to_string(),
            timezone: "local".to_string(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            batch_size: 10000,
            memory_limit: None,
            threads: 0, // Auto-detect
            parallel: true,
            cache_size: 100,
        }
    }
}

impl Default for ModuleConfig {
    fn default() -> Self {
        Self {
            library_paths: vec![],
            auto_load: vec![],
            cache_dir: None,
        }
    }
}

impl Default for DebugConfig {
    fn default() -> Self {
        Self {
            verbosity: 0,
            show_plans: false,
            show_timing: false,
            debug_mode: false,
            log_file: None,
        }
    }
}

impl Config {
    /// Create a new configuration with defaults
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Load configuration from a specific file
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let mut config = Self::default();
        config.merge_file(path)?;
        Ok(config)
    }

    /// Load configuration from multiple sources
    pub fn load() -> Result<Self> {
        let mut config = Self::default();

        // 1. Load from config file if it exists
        if let Some(config_path) = Self::find_config_file(None) {
            config.merge_file(&config_path)?;
        }

        // 2. Apply environment variables
        config.merge_env()?;

        Ok(config)
    }

    /// Find configuration file in standard locations
    pub(crate) fn find_config_file(current_dir: Option<&Path>) -> Option<PathBuf> {
        let current_dir_buf = if let Some(dir) = current_dir {
            dir.to_path_buf()
        } else {
            std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf())
        };
        let current_dir = current_dir_buf.as_path();
        let config_names = ["dsq.toml", ".dsq.toml", "dsq.yaml", ".dsq.yaml"];

        // Check current directory
        for &name in &config_names {
            let path = current_dir.join(name);
            if path.exists() {
                return Some(path.canonicalize().unwrap_or(path));
            }
        }

        // Check home directory
        if let Ok(home) = std::env::var("HOME") {
            for name in &config_names {
                let path = Path::new(&home).join(".config").join("dsq").join(name);
                if path.exists() {
                    return Some(path.canonicalize().unwrap_or(path));
                }

                let path = Path::new(&home).join(name);
                if path.exists() {
                    return Some(path.canonicalize().unwrap_or(path));
                }
            }
        }

        // Check system config
        for name in &config_names {
            let path = Path::new("/etc/dsq").join(name);
            if path.exists() {
                return Some(path.canonicalize().unwrap_or(path));
            }
        }

        None
    }

    /// Merge configuration from file
    pub fn merge_file(&mut self, path: &Path) -> Result<()> {
        let content = fs::read_to_string(path)
            .map_err(|e| Error::config(format!("Failed to read config file: {}", e)))?;

        let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");

        match extension {
            "toml" => {
                let file_config: Config = toml::from_str(&content)
                    .map_err(|e| Error::config(format!("Invalid TOML config: {}", e)))?;
                self.merge(file_config);
            }
            "yaml" | "yml" => {
                let file_config: Config = serde_yaml::from_str(&content)
                    .map_err(|e| Error::config(format!("Invalid YAML config: {}", e)))?;
                self.merge(file_config);
            }
            _ => return Err(Error::config("Unsupported config file format")),
        }

        Ok(())
    }

    /// Merge configuration from environment variables
    fn merge_env(&mut self) -> Result<()> {
        self.merge_env_with_reader(|key| std::env::var(key).ok())
    }

    /// Merge configuration from environment variables with custom reader
    fn merge_env_with_reader<F>(&mut self, env_reader: F) -> Result<()>
    where
        F: Fn(&str) -> Option<String>,
    {
        // DSQ_LAZY
        if let Some(val) = env_reader("DSQ_LAZY") {
            self.filter.lazy_evaluation = val != "0" && val.to_lowercase() != "false";
        }

        // DSQ_COLORS
        if let Some(val) = env_reader("DSQ_COLORS") {
            self.display.color.enabled = Some(val != "0" && val.to_lowercase() != "false");
        }

        // DSQ_LIBRARY_PATH
        if let Some(val) = env_reader("DSQ_LIBRARY_PATH") {
            self.modules.library_paths = std::env::split_paths(&val).collect();
        }

        // DSQ_BATCH_SIZE
        if let Some(val) = env_reader("DSQ_BATCH_SIZE") {
            if let Ok(size) = val.parse() {
                self.performance.batch_size = size;
            } else {
                self.performance.batch_size = PerformanceConfig::default().batch_size;
            }
        }

        // DSQ_MEMORY_LIMIT
        if let Some(val) = env_reader("DSQ_MEMORY_LIMIT") {
            if let Ok(limit) = parse_memory_limit(&val) {
                self.performance.memory_limit = Some(limit);
            } else {
                self.performance.memory_limit = PerformanceConfig::default().memory_limit;
            }
        }

        // DSQ_THREADS
        if let Some(val) = env_reader("DSQ_THREADS") {
            if let Ok(threads) = val.parse() {
                self.performance.threads = threads;
            } else {
                self.performance.threads = PerformanceConfig::default().threads;
            }
        }

        // DSQ_DEBUG
        if let Some(val) = env_reader("DSQ_DEBUG") {
            self.debug.debug_mode = val != "0" && val.to_lowercase() != "false";
        }

        // DSQ_VERBOSITY
        if let Some(val) = env_reader("DSQ_VERBOSITY") {
            if let Ok(level) = val.parse() {
                self.debug.verbosity = level;
            } else {
                self.debug.verbosity = DebugConfig::default().verbosity;
            }
        }

        Ok(())
    }

    /// Merge another config into this one
    fn merge(&mut self, other: Config) {
        // Merge I/O config
        if other.io.default_input_format.is_some() {
            self.io.default_input_format = other.io.default_input_format;
        }
        if other.io.default_output_format.is_some() {
            self.io.default_output_format = other.io.default_output_format;
        }
        if !other.io.auto_detect_format {
            self.io.auto_detect_format = other.io.auto_detect_format;
        }
        if other.io.buffer_size != IoConfig::default().buffer_size {
            self.io.buffer_size = other.io.buffer_size;
        }
        if other.io.overwrite_by_default {
            self.io.overwrite_by_default = other.io.overwrite_by_default;
        }
        if other.io.max_memory_file_size != IoConfig::default().max_memory_file_size {
            self.io.max_memory_file_size = other.io.max_memory_file_size;
        }

        // Merge filter config
        if !other.filter.lazy_evaluation {
            self.filter.lazy_evaluation = other.filter.lazy_evaluation;
        }
        if !other.filter.dataframe_optimizations {
            self.filter.dataframe_optimizations = other.filter.dataframe_optimizations;
        }
        if other.filter.optimization_level != FilterConfig::default().optimization_level {
            self.filter.optimization_level = other.filter.optimization_level;
        }
        if other.filter.max_recursion_depth != FilterConfig::default().max_recursion_depth {
            self.filter.max_recursion_depth = other.filter.max_recursion_depth;
        }
        if other.filter.max_execution_time.is_some() {
            self.filter.max_execution_time = other.filter.max_execution_time;
        }
        if other.filter.collect_stats {
            self.filter.collect_stats = other.filter.collect_stats;
        }
        if other.filter.error_mode != FilterConfig::default().error_mode {
            self.filter.error_mode = other.filter.error_mode;
        }

        // Merge format configs
        self.merge_csv_config(other.formats.csv);
        self.merge_json_config(other.formats.json);
        self.merge_parquet_config(other.formats.parquet);

        // Merge display config
        self.merge_display_config(other.display);

        // Merge performance config
        if other.performance.batch_size != PerformanceConfig::default().batch_size {
            self.performance.batch_size = other.performance.batch_size;
        }
        if other.performance.memory_limit.is_some() {
            self.performance.memory_limit = other.performance.memory_limit;
        }
        if other.performance.threads != PerformanceConfig::default().threads {
            self.performance.threads = other.performance.threads;
        }
        if !other.performance.parallel {
            self.performance.parallel = other.performance.parallel;
        }
        if other.performance.cache_size != PerformanceConfig::default().cache_size {
            self.performance.cache_size = other.performance.cache_size;
        }

        // Merge module config
        if !other.modules.library_paths.is_empty() {
            self.modules
                .library_paths
                .extend(other.modules.library_paths);
        }
        if !other.modules.auto_load.is_empty() {
            self.modules.auto_load.extend(other.modules.auto_load);
        }
        if other.modules.cache_dir.is_some() {
            self.modules.cache_dir = other.modules.cache_dir;
        }

        // Merge debug config
        if other.debug.verbosity != DebugConfig::default().verbosity {
            self.debug.verbosity = other.debug.verbosity;
        }
        if other.debug.show_plans {
            self.debug.show_plans = other.debug.show_plans;
        }
        if other.debug.show_timing {
            self.debug.show_timing = other.debug.show_timing;
        }
        if other.debug.debug_mode {
            self.debug.debug_mode = other.debug.debug_mode;
        }
        if other.debug.log_file.is_some() {
            self.debug.log_file = other.debug.log_file;
        }

        // Merge variables (extend, don't replace)
        for (key, value) in other.variables {
            self.variables.insert(key, value);
        }
    }

    /// Merge CSV format config
    fn merge_csv_config(&mut self, other: CsvConfig) {
        if other.separator != CsvConfig::default().separator {
            self.formats.csv.separator = other.separator;
        }
        if !other.has_header {
            self.formats.csv.has_header = other.has_header;
        }
        if other.quote_char != CsvConfig::default().quote_char {
            self.formats.csv.quote_char = other.quote_char;
        }
        if other.comment_char.is_some() {
            self.formats.csv.comment_char = other.comment_char;
        }
        if !other.null_values.is_empty() {
            self.formats.csv.null_values = other.null_values;
        }
        if other.trim_whitespace {
            self.formats.csv.trim_whitespace = other.trim_whitespace;
        }
        if other.infer_schema_length != CsvConfig::default().infer_schema_length {
            self.formats.csv.infer_schema_length = other.infer_schema_length;
        }
    }

    /// Merge JSON format config
    fn merge_json_config(&mut self, other: JsonConfig) {
        if !other.pretty_print {
            self.formats.json.pretty_print = other.pretty_print;
        }
        if other.maintain_order {
            self.formats.json.maintain_order = other.maintain_order;
        }
        if other.escape_unicode {
            self.formats.json.escape_unicode = other.escape_unicode;
        }
        if other.flatten {
            self.formats.json.flatten = other.flatten;
        }
        if other.flatten_separator != JsonConfig::default().flatten_separator {
            self.formats.json.flatten_separator = other.flatten_separator;
        }
    }

    /// Merge Parquet format config
    fn merge_parquet_config(&mut self, other: ParquetConfig) {
        if other.compression != ParquetConfig::default().compression {
            self.formats.parquet.compression = other.compression;
        }
        if !other.write_statistics {
            self.formats.parquet.write_statistics = other.write_statistics;
        }
        if other.row_group_size != ParquetConfig::default().row_group_size {
            self.formats.parquet.row_group_size = other.row_group_size;
        }
        if other.data_page_size != ParquetConfig::default().data_page_size {
            self.formats.parquet.data_page_size = other.data_page_size;
        }
    }

    /// Merge display config
    fn merge_display_config(&mut self, other: DisplayConfig) {
        // Merge color config
        if other.color.enabled.is_some() {
            self.display.color.enabled = other.color.enabled;
        }
        if other.color.scheme != ColorConfig::default().scheme {
            self.display.color.scheme = other.color.scheme;
        }
        if !other.color.auto_detect {
            self.display.color.auto_detect = other.color.auto_detect;
        }

        if other.compact {
            self.display.compact = other.compact;
        }
        if other.sort_keys {
            self.display.sort_keys = other.sort_keys;
        }
        if other.raw_output {
            self.display.raw_output = other.raw_output;
        }

        // Merge number format
        if other.number_format.float_precision.is_some() {
            self.display.number_format.float_precision = other.number_format.float_precision;
        }
        if other.number_format.scientific_notation {
            self.display.number_format.scientific_notation =
                other.number_format.scientific_notation;
        }
        if (other.number_format.scientific_threshold
            - NumberFormatConfig::default().scientific_threshold)
            .abs()
            > f64::EPSILON
        {
            self.display.number_format.scientific_threshold =
                other.number_format.scientific_threshold;
        }

        // Merge datetime format
        if other.datetime_format.date_format != DateTimeFormatConfig::default().date_format {
            self.display.datetime_format.date_format = other.datetime_format.date_format;
        }
        if other.datetime_format.datetime_format != DateTimeFormatConfig::default().datetime_format
        {
            self.display.datetime_format.datetime_format = other.datetime_format.datetime_format;
        }
        if other.datetime_format.time_format != DateTimeFormatConfig::default().time_format {
            self.display.datetime_format.time_format = other.datetime_format.time_format;
        }
        if other.datetime_format.timezone != DateTimeFormatConfig::default().timezone {
            self.display.datetime_format.timezone = other.datetime_format.timezone;
        }
    }

    /// Apply CLI configuration overrides
    pub fn apply_cli(&mut self, cli_config: &CliConfig) -> Result<()> {
        // I/O settings
        if let Some(format) = &cli_config.input_format {
            self.io.default_input_format = Some(*format);
        }
        if let Some(format) = &cli_config.output_format {
            self.io.default_output_format = Some(*format);
        }
        self.io.limit = cli_config.limit;

        // Filter settings
        self.filter.lazy_evaluation = cli_config.lazy;
        self.filter.dataframe_optimizations = cli_config.dataframe_optimizations;

        // Display settings
        self.display.compact = cli_config.compact_output;
        self.display.raw_output = cli_config.raw_output;
        self.display.sort_keys = cli_config.sort_keys;
        self.display.exit_status = cli_config.exit_status;
        if let Some(color) = cli_config.color_output {
            self.display.color.enabled = Some(color);
        }

        // CSV settings
        if let Some(sep) = &cli_config.csv_separator {
            self.formats.csv.separator = sep.clone();
        }
        if let Some(has_header) = cli_config.csv_headers {
            self.formats.csv.has_header = has_header;
        }

        // Performance settings
        if let Some(batch_size) = cli_config.batch_size {
            self.performance.batch_size = batch_size;
        }
        if let Some(limit) = &cli_config.memory_limit {
            self.performance.memory_limit = Some(parse_memory_limit(limit)?);
        }

        // Module settings
        if !cli_config.library_path.is_empty() {
            self.modules.library_paths = cli_config.library_path.clone();
        }

        // Debug settings
        self.debug.verbosity = cli_config.verbose;
        self.debug.show_plans = cli_config.explain;

        // Variables
        self.variables = cli_config.variables.clone();

        Ok(())
    }

    /// Convert to ReadOptions for dsq-core
    pub fn to_read_options(&self) -> ReadOptions {
        ReadOptions {
            infer_schema: true,
            n_rows: None,
            skip_rows: 0,
        }
    }

    /// Convert to WriteOptions for dsq-core
    pub fn to_write_options(&self) -> WriteOptions {
        WriteOptions {
            include_header: true,
            compression: None,
        }
    }

    /// Convert to ExecutorConfig for dsq-filter
    pub fn to_executor_config(&self) -> ExecutorConfig {
        let variables = self
            .variables
            .iter()
            .map(|(k, v)| (k.clone(), dsq_shared::value::Value::from_json(v.clone())))
            .collect();

        let error_mode = match self.filter.error_mode.as_str() {
            "strict" => ErrorMode::Strict,
            "collect" => ErrorMode::Collect,
            "ignore" => ErrorMode::Ignore,
            _ => ErrorMode::Strict,
        };

        ExecutorConfig {
            timeout_ms: self.filter.max_execution_time.map(|s| s as u64 * 1000), // convert seconds to ms
            error_mode,
            collect_stats: self.filter.collect_stats,
            max_recursion_depth: self.filter.max_recursion_depth,
            debug_mode: self.debug.debug_mode,
            batch_size: self.performance.batch_size,
            variables,
        }
    }

    /// Get format-specific read options
    pub fn get_format_read_options(&self, format: DataFormat) -> ReadOptions {
        let mut options = ReadOptions {
            infer_schema: true,
            n_rows: None,
            skip_rows: 0,
        };

        match format {
            DataFormat::Csv => {
                options.infer_schema = true;
                // Could add CSV-specific options like encoding, etc.
            }
            DataFormat::Json | DataFormat::JsonLines | DataFormat::Json5 => {
                options.infer_schema = true;
                // Could add JSON-specific options
            }
            DataFormat::Parquet => {
                // Parquet typically doesn't need schema inference
                options.infer_schema = false;
            }
            _ => {
                // Use defaults for other formats
            }
        }

        options
    }

    /// Get format-specific write options
    pub fn get_format_write_options(&self, format: DataFormat) -> WriteOptions {
        let mut options = WriteOptions {
            include_header: true,
            compression: None,
        };

        match format {
            DataFormat::Csv => {
                options.include_header = self.formats.csv.has_header;
                // Could add CSV-specific options like separator, quote_char, etc.
            }
            DataFormat::Json | DataFormat::JsonLines | DataFormat::Json5 => {
                // JSON-specific write options could include pretty_print, etc.
                options.include_header = false; // JSON doesn't have headers
            }
            DataFormat::Parquet => {
                options.compression = Some(self.formats.parquet.compression.clone());
                options.include_header = false; // Parquet doesn't have headers in the same way
            }
            _ => {
                // Use defaults for other formats
            }
        }

        options
    }

    /// Check if color output should be enabled
    pub fn should_use_color(&self) -> bool {
        match self.display.color.enabled {
            Some(enabled) => enabled,
            None if self.display.color.auto_detect => {
                // Auto-detect based on terminal capabilities
                atty::is(atty::Stream::Stdout)
                    && std::env::var("TERM").map(|t| t != "dumb").unwrap_or(true)
            }
            None => false,
        }
    }

    /// Get the number of threads to use
    pub fn get_thread_count(&self) -> usize {
        if self.performance.threads == 0 {
            num_cpus::get()
        } else {
            self.performance.threads
        }
    }

    /// Get variables as dsq_core::Value map for filter execution
    pub fn get_variables_as_value(&self) -> std::collections::HashMap<String, dsq_core::Value> {
        self.variables
            .iter()
            .map(|(k, v)| {
                let value = dsq_core::Value::from_json(v.clone());
                (k.clone(), value)
            })
            .collect()
    }

    /// Save configuration to file
    pub fn save(&self, path: &Path) -> Result<()> {
        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("toml");

        let content = match extension {
            "toml" => toml::to_string_pretty(self)
                .map_err(|e| Error::config(format!("Failed to serialize config: {}", e)))?,
            "yaml" | "yml" => serde_yaml::to_string(self)
                .map_err(|e| Error::config(format!("Failed to serialize config: {}", e)))?,
            _ => return Err(Error::config("Unsupported config file format")),
        };

        fs::write(path, content)
            .map_err(|e| Error::config(format!("Failed to write config file: {}", e)))?;

        Ok(())
    }
}

/// Parse memory limit string into bytes
fn parse_memory_limit(limit: &str) -> Result<usize> {
    let limit = limit.to_uppercase();

    if let Some(num_str) = limit.strip_suffix("GB") {
        let num: usize = num_str
            .parse()
            .map_err(|_| Error::config(format!("Invalid memory limit: {}", limit)))?;
        Ok(num * 1024 * 1024 * 1024)
    } else if let Some(num_str) = limit.strip_suffix("MB") {
        let num: usize = num_str
            .parse()
            .map_err(|_| Error::config(format!("Invalid memory limit: {}", limit)))?;
        Ok(num * 1024 * 1024)
    } else if let Some(num_str) = limit.strip_suffix("KB") {
        let num: usize = num_str
            .parse()
            .map_err(|_| Error::config(format!("Invalid memory limit: {}", limit)))?;
        Ok(num * 1024)
    } else if let Some(num_str) = limit.strip_suffix("B") {
        num_str
            .parse()
            .map_err(|_| Error::config(format!("Invalid memory limit: {}", limit)))
    } else {
        // Try parsing as plain number (bytes)
        limit.parse().map_err(|_| {
            Error::config(format!(
                "Invalid memory limit: {} (use format like '1GB', '500MB')",
                limit
            ))
        })
    }
}

/// Create a default config file template
pub fn create_default_config_file(path: &Path) -> Result<()> {
    let config = Config::default();
    config.save(path)?;
    Ok(())
}

/// Validate configuration
pub fn validate_config(config: &Config) -> Result<()> {
    // Validate performance settings
    if config.performance.batch_size == 0 {
        return Err(Error::config("Batch size must be greater than 0"));
    }

    if config.performance.threads > 1024 {
        return Err(Error::config("Thread count seems unreasonably high"));
    }

    // Validate filter settings
    if config.filter.max_recursion_depth == 0 {
        return Err(Error::config("Max recursion depth must be greater than 0"));
    }

    // Validate format settings
    if config.formats.csv.separator.len() != 1 {
        return Err(Error::config("CSV separator must be a single character"));
    }

    if config.formats.csv.quote_char.len() != 1 {
        return Err(Error::config(
            "CSV quote character must be a single character",
        ));
    }

    // Validate paths
    for path in &config.modules.library_paths {
        if !path.exists() {
            eprintln!("Warning: Library path does not exist: {}", path.display());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.filter.lazy_evaluation);
        assert!(config.filter.dataframe_optimizations);
        assert_eq!(config.performance.batch_size, 10000);
        assert_eq!(config.formats.csv.separator, ",");
    }

    #[test]
    fn test_all_default_implementations() {
        // Test Config default
        let config = Config::default();
        assert!(config.filter.lazy_evaluation);
        assert_eq!(config.filter.optimization_level, "basic");
        assert_eq!(config.performance.batch_size, 10000);
        assert_eq!(config.debug.verbosity, 0);
        assert!(config.variables.is_empty());

        // Test IoConfig default
        let io = IoConfig::default();
        assert!(io.auto_detect_format);
        assert_eq!(io.buffer_size, 8192);
        assert!(!io.overwrite_by_default);
        assert_eq!(io.max_memory_file_size, 100 * 1024 * 1024);

        // Test FilterConfig default
        let filter = FilterConfig::default();
        assert!(filter.lazy_evaluation);
        assert!(filter.dataframe_optimizations);
        assert_eq!(filter.optimization_level, "basic");
        assert_eq!(filter.max_recursion_depth, 1000);
        assert_eq!(filter.max_execution_time, Some(300));
        assert!(!filter.collect_stats);
        assert_eq!(filter.error_mode, "strict");

        // Test FormatConfigs default
        let formats = FormatConfigs::default();
        assert_eq!(formats.csv.separator, ",");
        assert!(formats.csv.has_header);
        assert_eq!(formats.json.pretty_print, true);
        assert_eq!(formats.parquet.compression, "snappy");

        // Test CsvConfig default
        let csv = CsvConfig::default();
        assert_eq!(csv.separator, ",");
        assert!(csv.has_header);
        assert_eq!(csv.quote_char, "\"");
        assert_eq!(
            csv.null_values,
            vec!["".to_string(), "NA".to_string(), "NULL".to_string()]
        );
        assert!(!csv.trim_whitespace);
        assert_eq!(csv.infer_schema_length, 1000);

        // Test JsonConfig default
        let json = JsonConfig::default();
        assert!(json.pretty_print);
        assert!(!json.maintain_order);
        assert!(!json.escape_unicode);
        assert!(!json.flatten);
        assert_eq!(json.flatten_separator, ".");

        // Test ParquetConfig default
        let parquet = ParquetConfig::default();
        assert_eq!(parquet.compression, "snappy");
        assert!(parquet.write_statistics);
        assert_eq!(parquet.row_group_size, 1024 * 1024);
        assert_eq!(parquet.data_page_size, 1024 * 1024);

        // Test DisplayConfig default
        let display = DisplayConfig::default();
        assert_eq!(display.color.scheme, "default");
        assert!(display.color.auto_detect);
        assert!(!display.compact);
        assert!(!display.sort_keys);
        assert!(!display.raw_output);

        // Test ColorConfig default
        let color = ColorConfig::default();
        assert!(color.enabled.is_none());
        assert_eq!(color.scheme, "default");
        assert!(color.auto_detect);

        // Test NumberFormatConfig default
        let number = NumberFormatConfig::default();
        assert!(number.float_precision.is_none());
        assert!(!number.scientific_notation);
        assert_eq!(number.scientific_threshold, 1e9);

        // Test DateTimeFormatConfig default
        let datetime = DateTimeFormatConfig::default();
        assert_eq!(datetime.date_format, "%Y-%m-%d");
        assert_eq!(datetime.datetime_format, "%Y-%m-%d %H:%M:%S");
        assert_eq!(datetime.time_format, "%H:%M:%S");
        assert_eq!(datetime.timezone, "local");

        // Test PerformanceConfig default
        let perf = PerformanceConfig::default();
        assert_eq!(perf.batch_size, 10000);
        assert!(perf.memory_limit.is_none());
        assert_eq!(perf.threads, 0);
        assert!(perf.parallel);
        assert_eq!(perf.cache_size, 100);

        // Test ModuleConfig default
        let module = ModuleConfig::default();
        assert!(module.library_paths.is_empty());
        assert!(module.auto_load.is_empty());
        assert!(module.cache_dir.is_none());

        // Test DebugConfig default
        let debug = DebugConfig::default();
        assert_eq!(debug.verbosity, 0);
        assert!(!debug.show_plans);
        assert!(!debug.show_timing);
        assert!(!debug.debug_mode);
        assert!(debug.log_file.is_none());
    }

    #[test]
    fn test_config_new() {
        let config = Config::new();
        // Should be same as default
        assert_eq!(
            config.filter.lazy_evaluation,
            Config::default().filter.lazy_evaluation
        );
        assert_eq!(
            config.performance.batch_size,
            Config::default().performance.batch_size
        );
    }

    #[test]
    fn test_parse_memory_limit() {
        assert_eq!(parse_memory_limit("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_limit("500MB").unwrap(), 500 * 1024 * 1024);
        assert_eq!(parse_memory_limit("1024KB").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_limit("2048B").unwrap(), 2048);
        assert_eq!(parse_memory_limit("2048").unwrap(), 2048);

        // Test case insensitivity
        assert_eq!(parse_memory_limit("1gb").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_limit("500mb").unwrap(), 500 * 1024 * 1024);

        // Test invalid formats
        assert!(parse_memory_limit("invalid").is_err());
        assert!(parse_memory_limit("1XB").is_err());
        assert!(parse_memory_limit("").is_err());
        assert!(parse_memory_limit("GB").is_err());
        assert!(parse_memory_limit("1.5GB").is_err()); // floats not supported
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        assert!(validate_config(&config).is_ok());

        // Test batch size validation
        config.performance.batch_size = 0;
        assert!(validate_config(&config).is_err());

        // Test recursion depth validation
        config = Config::default();
        config.filter.max_recursion_depth = 0;
        assert!(validate_config(&config).is_err());

        // Test CSV separator validation
        config = Config::default();
        config.formats.csv.separator = ",,".to_string();
        assert!(validate_config(&config).is_err());

        config.formats.csv.separator = "".to_string();
        assert!(validate_config(&config).is_err());

        // Test CSV quote validation
        config = Config::default();
        config.formats.csv.quote_char = "quote".to_string();
        assert!(validate_config(&config).is_err());

        // Test thread count validation (should be ok up to high numbers)
        config = Config::default();
        config.performance.threads = 1024;
        assert!(validate_config(&config).is_ok());

        config.performance.threads = 1025;
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_find_config_file() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        // Ensure HOME is not set
        // SAFETY: This is test-only code that manipulates environment variables.
        // It's safe because tests run in isolated processes and we're controlling
        // the test environment to verify configuration loading behavior.
        unsafe {
            env::remove_var("HOME");
        }

        // Test no config files exist
        assert!(Config::find_config_file(Some(temp_path)).is_none());

        // Create a config file in current directory
        fs::write(temp_path.join("dsq.toml"), "test").unwrap();
        assert_eq!(
            Config::find_config_file(Some(temp_path)).unwrap(),
            temp_path.join("dsq.toml")
        );

        // Test priority: current dir first
        fs::write(temp_path.join("dsq.yaml"), "test").unwrap();
        assert_eq!(
            Config::find_config_file(Some(temp_path)).unwrap(),
            temp_path.join("dsq.toml")
        );

        // Remove toml, should find yaml
        fs::remove_file(temp_path.join("dsq.toml")).unwrap();
        assert_eq!(
            Config::find_config_file(Some(temp_path)).unwrap(),
            temp_path.join("dsq.yaml")
        );

        // Test hidden files
        fs::write(temp_path.join(".dsq.toml"), "test").unwrap();
        assert_eq!(
            Config::find_config_file(Some(temp_path)).unwrap(),
            temp_path.join(".dsq.toml")
        ); // hidden toml comes before yaml

        // Remove yaml, should find hidden toml
        fs::remove_file(temp_path.join("dsq.yaml")).unwrap();
        assert_eq!(
            Config::find_config_file(Some(temp_path)).unwrap(),
            temp_path.join(".dsq.toml")
        );
    }

    #[test]
    fn test_merge_file_toml() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test.toml");

        let toml_content = r#"
[filter]
lazy_evaluation = false
dataframe_optimizations = true
optimization_level = "advanced"

[formats.csv]
separator = "|"

[performance]
batch_size = 5000
"#;

        fs::write(&config_path, toml_content).unwrap();

        let mut config = Config::default();
        config.merge_file(&config_path).unwrap();

        assert!(!config.filter.lazy_evaluation);
        assert_eq!(config.filter.optimization_level, "advanced");
        assert_eq!(config.formats.csv.separator, "|");
        assert_eq!(config.performance.batch_size, 5000);
    }

    #[test]
    fn test_merge_file_yaml() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test.yaml");

        let yaml_content = r#"
filter:
  lazy_evaluation: false
  dataframe_optimizations: true
  optimization_level: advanced
formats:
  csv:
    separator: "|"
performance:
  batch_size: 5000
"#;

        fs::write(&config_path, yaml_content).unwrap();

        let mut config = Config::default();
        config.merge_file(&config_path).unwrap();

        assert!(!config.filter.lazy_evaluation);
        assert_eq!(config.filter.optimization_level, "advanced");
        assert_eq!(config.formats.csv.separator, "|");
        assert_eq!(config.performance.batch_size, 5000);
    }

    #[test]
    fn test_merge_file_errors() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("invalid.toml");

        // Test invalid TOML
        fs::write(&config_path, "invalid toml content [").unwrap();
        let mut config = Config::default();
        assert!(config.merge_file(&config_path).is_err());

        // Test unsupported extension
        let config_path = temp_dir.path().join("config.json");
        fs::write(&config_path, "{}").unwrap();
        assert!(config.merge_file(&config_path).is_err());

        // Test non-existent file
        let config_path = temp_dir.path().join("nonexistent.toml");
        assert!(config.merge_file(&config_path).is_err());
    }

    #[test]
    fn test_merge_env() {
        let mut config = Config::default();

        // Use a mock environment reader to avoid interference with other tests
        let env_reader = |key: &str| match key {
            "DSQ_LAZY" => Some("false".to_string()),
            "DSQ_COLORS" => Some("true".to_string()),
            "DSQ_LIBRARY_PATH" => Some("/lib1:/lib2".to_string()),
            "DSQ_BATCH_SIZE" => Some("2500".to_string()),
            "DSQ_MEMORY_LIMIT" => Some("1GB".to_string()),
            "DSQ_THREADS" => Some("8".to_string()),
            "DSQ_DEBUG" => Some("true".to_string()),
            "DSQ_VERBOSITY" => Some("2".to_string()),
            _ => None,
        };

        config.merge_env_with_reader(env_reader).unwrap();

        assert!(!config.filter.lazy_evaluation);
        assert_eq!(config.display.color.enabled, Some(true));
        assert_eq!(
            config.modules.library_paths,
            vec![
                std::path::PathBuf::from("/lib1"),
                std::path::PathBuf::from("/lib2")
            ]
        );
        assert_eq!(config.performance.batch_size, 2500);
        assert_eq!(config.performance.memory_limit, Some(1024 * 1024 * 1024));
        assert_eq!(config.performance.threads, 8);
        assert!(config.debug.debug_mode);
        assert_eq!(config.debug.verbosity, 2);
    }

    #[test]
    fn test_merge_env_invalid_values() {
        let mut config = Config::default();

        // Use a mock environment reader with invalid values
        let env_reader = |key: &str| match key {
            "DSQ_MEMORY_LIMIT" => Some("invalid".to_string()),
            "DSQ_THREADS" => Some("invalid".to_string()),
            "DSQ_VERBOSITY" => Some("invalid".to_string()),
            _ => None,
        };

        // Should not panic, should keep defaults
        config.merge_env_with_reader(env_reader).unwrap();
        assert_eq!(config.performance.batch_size, 10000); // default
        assert!(config.performance.memory_limit.is_none()); // default
        assert_eq!(config.performance.threads, 0); // default
        assert_eq!(config.debug.verbosity, 0); // default
    }

    #[test]
    fn test_cli_override_comprehensive() {
        let mut config = Config::default();
        let mut cli_config = CliConfig::default();

        // Set various CLI options
        cli_config.lazy = false;
        cli_config.dataframe_optimizations = false;
        cli_config.compact_output = true;
        cli_config.raw_output = true;
        cli_config.sort_keys = true;
        cli_config.color_output = Some(true);
        cli_config.csv_separator = Some("|".to_string());
        cli_config.csv_headers = Some(false);
        cli_config.batch_size = Some(5000);
        cli_config.memory_limit = Some("2GB".to_string());
        cli_config.library_path = vec![std::path::PathBuf::from("/lib")];
        cli_config.verbose = 2;
        cli_config.explain = true;
        cli_config
            .variables
            .insert("test".to_string(), serde_json::json!("value"));

        config.apply_cli(&cli_config).unwrap();

        assert!(!config.filter.lazy_evaluation);
        assert!(!config.filter.dataframe_optimizations);
        assert!(config.display.compact);
        assert!(config.display.raw_output);
        assert!(config.display.sort_keys);
        assert_eq!(config.display.color.enabled, Some(true));
        assert_eq!(config.formats.csv.separator, "|");
        assert!(!config.formats.csv.has_header);
        assert_eq!(config.performance.batch_size, 5000);
        assert_eq!(
            config.performance.memory_limit,
            Some(2 * 1024 * 1024 * 1024)
        );
        assert_eq!(
            config.modules.library_paths,
            vec![std::path::PathBuf::from("/lib")]
        );
        assert_eq!(config.debug.verbosity, 2);
        assert!(config.debug.show_plans);
        assert_eq!(config.variables["test"], serde_json::json!("value"));
    }

    #[test]
    fn test_format_options_expanded() {
        let mut config = Config::default();

        // Test CSV write options
        let csv_write = config.get_format_write_options(DataFormat::Csv);
        assert!(csv_write.include_header); // default

        config.formats.csv.has_header = false;
        let csv_write = config.get_format_write_options(DataFormat::Csv);
        assert!(!csv_write.include_header);

        // Test Parquet write options
        let parquet_write = config.get_format_write_options(DataFormat::Parquet);
        assert_eq!(parquet_write.compression, Some("snappy".to_string()));

        config.formats.parquet.compression = "gzip".to_string();
        let parquet_write = config.get_format_write_options(DataFormat::Parquet);
        assert_eq!(parquet_write.compression, Some("gzip".to_string()));

        // Test read options (currently defaults)
        let _csv_read = config.get_format_read_options(DataFormat::Csv);
        let _json_read = config.get_format_read_options(DataFormat::Json);
        let _parquet_read = config.get_format_read_options(DataFormat::Parquet);
    }

    #[test]
    fn test_thread_count() {
        let mut config = Config::default();

        // Auto-detect
        config.performance.threads = 0;
        assert!(config.get_thread_count() > 0);

        // Explicit count
        config.performance.threads = 4;
        assert_eq!(config.get_thread_count(), 4);
    }

    #[test]
    fn test_should_use_color() {
        let mut config = Config::default();

        // Explicit true
        config.display.color.enabled = Some(true);
        assert!(config.should_use_color());

        // Explicit false
        config.display.color.enabled = Some(false);
        assert!(!config.should_use_color());

        // Auto-detect (None with auto_detect=true)
        config.display.color.enabled = None;
        config.display.color.auto_detect = true;
        // This depends on atty, but we can't easily test it in unit tests
        // The logic is there, so we'll assume it's working
    }

    #[test]
    fn test_variables_conversion() {
        let mut config = Config::default();
        config
            .variables
            .insert("string".to_string(), serde_json::json!("hello"));
        config
            .variables
            .insert("number".to_string(), serde_json::json!(42));
        config
            .variables
            .insert("array".to_string(), serde_json::json!([1, 2, 3]));

        let converted = config.get_variables_as_value();
        assert_eq!(converted.len(), 3);
        assert_eq!(
            converted["string"],
            dsq_core::Value::String("hello".to_string())
        );
        assert_eq!(converted["number"], dsq_core::Value::Int(42));
        // Array conversion would depend on dsq_core::Value implementation
    }

    #[test]
    fn test_save_toml() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("saved.toml");

        let mut config = Config::default();
        config.filter.lazy_evaluation = false;
        config.performance.batch_size = 1234;

        config.save(&config_path).unwrap();

        // Read back and verify
        let content = fs::read_to_string(&config_path).unwrap();
        assert!(content.contains("lazy_evaluation = false"));
        assert!(content.contains("batch_size = 1234"));
    }

    #[test]
    fn test_save_yaml() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("saved.yaml");

        let mut config = Config::default();
        config.filter.lazy_evaluation = false;
        config.performance.batch_size = 1234;

        config.save(&config_path).unwrap();

        // Read back and verify
        let content = fs::read_to_string(&config_path).unwrap();
        assert!(content.contains("lazy_evaluation: false"));
        assert!(content.contains("batch_size: 1234"));
    }

    #[test]
    fn test_save_errors() {
        let config = Config::default();

        // Invalid extension
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.json");
        assert!(config.save(&config_path).is_err());

        // Non-existent directory
        let config_path = std::path::PathBuf::from("/nonexistent/dir/config.toml");
        assert!(config.save(&config_path).is_err());
    }

    #[test]
    fn test_create_default_config_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("default.toml");

        create_default_config_file(&config_path).unwrap();

        // Verify file was created and contains default config
        assert!(config_path.exists());
        let content = fs::read_to_string(&config_path).unwrap();
        assert!(content.contains("[filter]"));
        assert!(content.contains("lazy_evaluation = true"));
    }

    #[test]
    fn test_config_load() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        let toml_content = r#"
[filter]
lazy_evaluation = false
dataframe_optimizations = true
[performance]
batch_size = 7777
"#;

        // Clean up
        // SAFETY: This is test-only code that manipulates environment variables.
        // It's safe in a test context because tests run in isolated processes
        // and we're controlling the environment to verify configuration loading.
        unsafe {
            env::remove_var("DSQ_BATCH_SIZE");
        }
        unsafe {
            env::remove_var("HOME");
        }

        // Set HOME to temp_dir
        unsafe {
            env::set_var("HOME", temp_path);
        }
        fs::create_dir_all(temp_path.join(".config").join("dsq")).unwrap();
        fs::write(
            temp_path.join(".config").join("dsq").join("dsq.toml"),
            toml_content,
        )
        .unwrap();

        let config = Config::load().unwrap();

        assert!(!config.filter.lazy_evaluation);
        assert_eq!(config.performance.batch_size, 7777);
        assert!(!config.debug.debug_mode); // default

        // Clean up
        // SAFETY: Test cleanup - restoring environment to original state
        unsafe {
            env::remove_var("HOME");
        }
    }

    #[test]
    fn test_config_load_from_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test.toml");

        let toml_content = r#"
[formats.csv]
separator = ";"
has_header = true
[debug]
verbosity = 3
"#;
        fs::write(&config_path, toml_content).unwrap();

        let config = Config::load_from_file(&config_path).unwrap();

        assert_eq!(config.formats.csv.separator, ";");
        assert_eq!(config.debug.verbosity, 3);
        // Other fields should be defaults
        assert!(config.filter.lazy_evaluation);
    }

    #[test]
    fn test_to_read_options() {
        let config = Config::default();
        let opts = config.to_read_options();

        // Currently returns defaults
        assert!(opts.infer_schema);
        assert!(opts.n_rows.is_none());
        assert_eq!(opts.skip_rows, 0);
    }

    #[test]
    fn test_to_write_options() {
        let config = Config::default();
        let opts = config.to_write_options();

        assert!(opts.include_header);
        assert!(opts.compression.is_none());
    }

    #[test]
    fn test_to_executor_config() {
        let mut config = Config::default();
        config
            .variables
            .insert("test".to_string(), serde_json::json!("value"));
        config.filter.max_recursion_depth = 500;

        let exec_config = config.to_executor_config();

        assert_eq!(exec_config.timeout_ms, Some(300000)); // 300 seconds * 1000
        assert_eq!(exec_config.error_mode, dsq_filter::ErrorMode::Strict);
        assert!(!exec_config.collect_stats);
        assert_eq!(exec_config.max_recursion_depth, 500);
        assert!(!exec_config.debug_mode);
        assert_eq!(exec_config.batch_size, 10000);
        assert!(exec_config.variables.contains_key("test"));
    }
}
