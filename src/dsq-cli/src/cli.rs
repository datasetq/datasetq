//! Command-line interface for dsq
//!
//! This module provides the command-line argument parsing and CLI structure
//! for dsq. It uses clap to define the command-line interface and provides
//! a configuration structure that can be used throughout the application.

use atty;
use clap::{Parser, Subcommand, ValueEnum};
use dsq_core::DataFormat;
use std::collections::HashMap;
use std::path::PathBuf;

/// dsq - A data processing tool for structured data formats with jq-like syntax
///
/// dsq processes structured data formats like CSV, Parquet, JSON, and more using
/// a jq-inspired filter language. Built on Polars DataFrames for high-performance
/// data manipulation.
#[derive(Parser, Debug)]
#[command(name = "dsq")]
#[command(author, version, about)]
#[command(
    long_about = "dsq - A data processing tool for structured data formats\n\n\
    dsq processes CSV, Parquet, JSON, and other structured data formats using a filter\n\
    language inspired by jq. Built on Polars DataFrames for high-performance operations."
)]
#[command(after_help = "EXAMPLES:\n  \
    # Filter rows from a CSV file\n  \
    dsq 'map(select(.age > 30))' people.csv\n\n  \
    # Read from stdin\n  \
    cat data.json | dsq '.[] | select(.active)'\n  \
    dsq '.' < input.csv\n\n  \
    # Convert between formats\n  \
    dsq '.' data.csv -o data.parquet\n\n  \
    # Aggregate data\n  \
    dsq 'group_by(.dept) | map({dept: .[0].dept, count: length})' employees.csv\n\n  \
    # Interactive mode\n  \
    dsq --interactive\n\n  \
    # Use example directory with query.dsq file\n  \
    dsq examples/001_filtering/\n\n\
For more information, visit: https://github.com/durableprogramming/dsq")]
#[command(propagate_version = true)]
pub struct Cli {
    /// Input file(s) to process (stdin if not specified)
    #[arg(index = 2, value_name = "FILE", num_args = 0..)]
    pub input_files: Vec<PathBuf>,

    /// The jq-style filter expression to apply
    #[arg(index = 1, value_name = "FILTER")]
    pub filter: Option<String>,

    /// Filter file containing the jq-style filter expression
    #[arg(short = 'f', long, value_name = "FILE")]
    pub filter_file: Option<PathBuf>,

    /// Output file (stdout if not specified)
    #[arg(short, long, value_name = "FILE")]
    pub output: Option<PathBuf>,

    /// Input format (auto-detected if not specified)
    #[arg(short = 'i', long, value_enum)]
    pub input_format: Option<DataFormat>,

    /// Output format (defaults to input format)
    #[arg(long, value_enum)]
    pub output_format: Option<DataFormat>,

    /// Use compact output (no pretty-printing)
    #[arg(short, long)]
    pub compact_output: bool,

    /// Output raw strings, not JSON texts
    #[arg(short, long)]
    pub raw_output: bool,

    /// Sort object keys in output
    #[arg(short = 'S', long)]
    pub sort_keys: bool,

    /// Use tab character for indentation
    #[arg(long)]
    pub tab: bool,

    /// Number of spaces for indentation (default: 2)
    #[arg(long, value_name = "N", conflicts_with = "tab")]
    pub indent: Option<usize>,

    /// Colorize output (auto/always/never)
    #[arg(short = 'C', long, value_enum)]
    pub color: Option<ColorMode>,

    /// Read entire input stream into memory before processing
    #[arg(short = 's', long)]
    pub slurp: bool,

    /// Exit with status code 0 if the filter returns false/null/empty
    #[arg(short = 'e', long)]
    pub exit_status: bool,

    /// Use null as single input value
    #[arg(short = 'n', long)]
    pub null_input: bool,

    /// Join output values with newlines
    #[arg(short = 'j', long)]
    pub join_output: bool,

    /// Define a variable (can be used multiple times)
    #[arg(long = "arg", value_name = "NAME VALUE", num_args = 2, action = clap::ArgAction::Append)]
    pub args: Vec<String>,

    /// Define a variable from JSON string
    #[arg(long = "argjson", value_name = "NAME JSON", num_args = 2, action = clap::ArgAction::Append)]
    pub argjson: Vec<String>,

    /// Define a variable from file contents
    #[arg(long = "argfile", value_name = "NAME FILE", num_args = 2, action = clap::ArgAction::Append)]
    pub argfile: Vec<String>,

    /// Include a library of jq/dsq definitions
    #[arg(short = 'L', long, value_name = "DIR", action = clap::ArgAction::Append)]
    pub library_path: Vec<PathBuf>,

    /// Import a module
    #[arg(long, value_name = "MODULE", action = clap::ArgAction::Append)]
    pub import: Vec<String>,

    /// Include a file
    #[arg(long, value_name = "FILE", action = clap::ArgAction::Append)]
    pub include: Vec<PathBuf>,

    /// CSV field separator (default: ,)
    #[arg(long = "csv-separator", value_name = "SEP")]
    pub csv_separator: Option<String>,

    /// CSV files have headers
    #[arg(long = "csv-headers")]
    pub csv_headers: Option<bool>,

    /// CSV quote character
    #[arg(long = "csv-quote", value_name = "CHAR")]
    pub csv_quote: Option<String>,

    /// Values to treat as null in CSV
    #[arg(long = "csv-null", value_name = "VALUE", action = clap::ArgAction::Append)]
    pub csv_null_values: Vec<String>,

    /// Skip first N rows when reading
    #[arg(long, value_name = "N")]
    pub skip_rows: Option<usize>,

    /// Limit output to N rows
    #[arg(long, value_name = "N")]
    pub limit: Option<usize>,

    /// Select specific columns (can be used multiple times)
    #[arg(long, value_name = "COL", action = clap::ArgAction::Append)]
    pub select: Vec<String>,

    /// Enable lazy evaluation (default: true)
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    pub lazy: bool,

    /// Enable DataFrame-specific optimizations (default: true)
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    pub dataframe_optimizations: bool,

    /// Batch size for processing
    #[arg(long, value_name = "SIZE")]
    pub batch_size: Option<usize>,

    /// Memory limit (e.g., 1GB, 500MB)
    #[arg(long, value_name = "LIMIT")]
    pub memory_limit: Option<String>,

    /// Number of threads to use (0 = auto)
    #[arg(long, value_name = "N")]
    pub threads: Option<usize>,

    /// Enable parallel processing
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    pub parallel: bool,

    /// Show execution plan without running
    #[arg(long)]
    pub explain: bool,

    /// Show execution statistics
    #[arg(long)]
    pub stats: bool,

    /// Show timing information
    #[arg(long)]
    pub time: bool,

    /// Increase verbosity (can be used multiple times)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Suppress all non-error output
    #[arg(short, long)]
    pub quiet: bool,

    /// Configuration file to use
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Overwrite existing output files
    #[arg(long)]
    pub overwrite: bool,

    /// Test filter validity without executing
    #[arg(long)]
    pub test: bool,

    /// Interactive mode (REPL)
    #[arg(short = 'I', long)]
    pub interactive: bool,

    /// Subcommands
    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Available subcommands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Convert between data formats
    #[command(after_help = "EXAMPLES:\n  \
        dsq convert input.csv output.parquet\n  \
        dsq convert data.json data.csv --overwrite")]
    Convert {
        /// Input file
        input: PathBuf,

        /// Output file
        output: PathBuf,

        /// Input format (auto-detected if not specified)
        #[arg(short = 'i', long, value_enum)]
        input_format: Option<DataFormat>,

        /// Output format (required if not detectable from output file)
        #[arg(short = 'o', long, value_enum)]
        output_format: Option<DataFormat>,

        /// Overwrite existing output file
        #[arg(long)]
        overwrite: bool,
    },

    /// Inspect data file structure and schema
    #[command(after_help = "EXAMPLES:\n  \
        dsq inspect data.csv --schema\n  \
        dsq inspect large.parquet --sample 20 --stats")]
    Inspect {
        /// File to inspect
        file: PathBuf,

        /// Show detailed schema information
        #[arg(long)]
        schema: bool,

        /// Show sample rows
        #[arg(long, value_name = "N")]
        sample: Option<usize>,

        /// Show statistics for numeric columns
        #[arg(long)]
        stats: bool,
    },

    /// Validate data files
    #[command(after_help = "EXAMPLES:\n  \
        dsq validate data.csv --check-duplicates --check-nulls\n  \
        dsq validate *.csv --schema expected_schema.json")]
    Validate {
        /// Files to validate
        files: Vec<PathBuf>,

        /// Expected schema file
        #[arg(long, value_name = "FILE")]
        schema: Option<PathBuf>,

        /// Check for duplicate rows
        #[arg(long)]
        check_duplicates: bool,

        /// Check for null values
        #[arg(long)]
        check_nulls: bool,
    },

    /// Merge multiple data files
    #[command(after_help = "EXAMPLES:\n  \
        # Concatenate files\n  \
        dsq merge q1.csv q2.csv q3.csv -o yearly.csv --method concat\n\n  \
        # Join files on a key\n  \
        dsq merge users.csv orders.csv -o combined.csv --method join --on user_id")]
    Merge {
        /// Input files to merge
        inputs: Vec<PathBuf>,

        /// Output file
        #[arg(short, long)]
        output: PathBuf,

        /// How to merge (concat/join)
        #[arg(long, value_enum, default_value = "concat")]
        method: MergeMethod,

        /// Columns to join on (for join method)
        #[arg(long, value_name = "COL", action = clap::ArgAction::Append)]
        on: Vec<String>,

        /// Join type (for join method)
        #[arg(long, value_enum, default_value = "inner")]
        join_type: JoinType,
    },

    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },

    /// Manage configuration
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
}

/// Configuration management subcommands
#[derive(Subcommand, Debug)]
pub enum ConfigCommands {
    /// Show current configuration
    Show,

    /// List all available configuration keys with descriptions
    List,

    /// Edit configuration file in $EDITOR
    Edit {
        /// Configuration file to edit (defaults to user config)
        path: Option<PathBuf>,
    },

    /// Create default configuration file
    Init {
        /// Path to create config file
        #[arg(default_value = "dsq.toml")]
        path: PathBuf,

        /// Force overwrite if file exists
        #[arg(short, long)]
        force: bool,
    },

    /// Validate configuration file
    Check {
        /// Configuration file to check
        path: PathBuf,
    },

    /// Get a configuration value
    Get {
        /// Configuration key (e.g., filter.lazy_evaluation)
        key: String,
    },

    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,

        /// Value to set
        value: String,

        /// Configuration file to update
        #[arg(long)]
        config: Option<PathBuf>,
    },

    /// Reset a configuration value to default
    Reset {
        /// Configuration key to reset
        key: String,

        /// Configuration file to update
        #[arg(long)]
        config: Option<PathBuf>,
    },
}

/// Color output mode
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ColorMode {
    /// Automatically detect if terminal supports colors
    Auto,
    /// Always use colors
    Always,
    /// Never use colors
    Never,
}

/// Merge method for combining files
#[derive(Debug, Clone, Copy, PartialEq, ValueEnum)]
pub enum MergeMethod {
    /// Concatenate files vertically
    Concat,
    /// Join files on common columns
    Join,
}

/// Join type for merge operations
#[derive(Debug, Clone, Copy, PartialEq, ValueEnum)]
pub enum JoinType {
    /// Inner join
    Inner,
    /// Left outer join
    Left,
    /// Right outer join
    Right,
    /// Full outer join
    Outer,
}

/// CLI configuration derived from parsed arguments
#[derive(Debug, Clone, Default)]
pub struct CliConfig {
    // Core options
    pub filter: Option<String>,
    pub filter_file: Option<PathBuf>,
    pub input_files: Vec<PathBuf>,
    pub output: Option<PathBuf>,
    pub input_format: Option<DataFormat>,
    pub output_format: Option<DataFormat>,

    // Output options
    pub compact_output: bool,
    pub raw_output: bool,
    pub sort_keys: bool,
    pub indent_size: usize,
    pub use_tabs: bool,
    pub color_output: Option<bool>,
    pub join_output: bool,

    // Input options
    pub slurp: bool,
    pub null_input: bool,
    pub exit_status: bool,

    // Variables and imports
    pub variables: HashMap<String, serde_json::Value>,
    pub library_path: Vec<PathBuf>,
    pub imports: Vec<String>,
    pub includes: Vec<PathBuf>,

    // Format-specific options
    pub csv_separator: Option<String>,
    pub csv_headers: Option<bool>,
    pub csv_quote: Option<String>,
    pub csv_null_values: Vec<String>,

    // Processing options
    pub skip_rows: Option<usize>,
    pub limit: Option<usize>,
    pub select_columns: Vec<String>,
    pub lazy: bool,
    pub dataframe_optimizations: bool,

    // Performance options
    pub batch_size: Option<usize>,
    pub memory_limit: Option<String>,
    pub threads: Option<usize>,
    pub parallel: bool,

    // Debug options
    pub explain: bool,
    pub stats: bool,
    pub time: bool,
    pub verbose: u8,
    pub quiet: bool,

    // Other options
    pub config_file: Option<PathBuf>,
    pub overwrite: bool,
    pub test: bool,
    pub interactive: bool,
}

impl From<&Cli> for CliConfig {
    fn from(cli: &Cli) -> Self {
        let mut filter_str = None;
        let mut input_files = cli.input_files.clone();
        if cli.filter_file.is_none() {
            if let Some(f) = &cli.filter {
                filter_str = Some(f.clone());
            } else if !input_files.is_empty() {
                let first = input_files.remove(0);
                filter_str = Some(first.to_string_lossy().to_string());
            }
        } else {
            // When filter_file is present, treat positional filter as input file
            if let Some(f) = &cli.filter {
                input_files.insert(0, PathBuf::from(f));
            }
        }
        let mut config = CliConfig {
            filter: filter_str,
            filter_file: cli.filter_file.clone(),
            input_files,
            output: cli.output.clone(),
            input_format: cli.input_format,
            output_format: cli.output_format,
            compact_output: cli.compact_output,
            raw_output: cli.raw_output,
            sort_keys: cli.sort_keys,
            indent_size: cli.indent.unwrap_or(2),
            use_tabs: cli.tab,
            color_output: cli.color.map(|mode| match mode {
                ColorMode::Always => true,
                ColorMode::Never => false,
                ColorMode::Auto => atty::is(atty::Stream::Stdout),
            }),
            join_output: cli.join_output,
            slurp: cli.slurp,
            null_input: cli.null_input,
            exit_status: cli.exit_status,
            variables: HashMap::new(),
            library_path: cli.library_path.clone(),
            imports: cli.import.clone(),
            includes: cli.include.clone(),
            csv_separator: cli.csv_separator.clone(),
            csv_headers: cli.csv_headers,
            csv_quote: cli.csv_quote.clone(),
            csv_null_values: cli.csv_null_values.clone(),
            skip_rows: cli.skip_rows,
            limit: cli.limit,
            select_columns: cli.select.clone(),
            lazy: cli.lazy,
            dataframe_optimizations: cli.dataframe_optimizations,
            batch_size: cli.batch_size,
            memory_limit: cli.memory_limit.clone(),
            threads: cli.threads,
            parallel: cli.parallel,
            explain: cli.explain,
            stats: cli.stats,
            time: cli.time,
            verbose: cli.verbose,
            quiet: cli.quiet,
            config_file: cli.config.clone(),
            overwrite: cli.overwrite,
            test: cli.test,
            interactive: cli.interactive,
        };

        // Process variables from --arg, --argjson, --argfile
        config.process_variables(cli);

        // Validate and warn about potentially conflicting or ineffective options
        config.validate_and_warn();

        config
    }
}

impl CliConfig {
    /// Validate configuration and warn about potential issues
    fn validate_and_warn(&self) {
        // Warn about --lazy with --null-input
        if self.lazy && self.null_input {
            eprintln!("Warning: --lazy has no effect with --null-input");
        }

        // Warn about --slurp with --limit
        if self.slurp && self.limit.is_some() {
            eprintln!(
                "Warning: --limit is applied after --slurp, which loads all data into memory first"
            );
        }

        // Warn about conflicting verbosity
        if self.quiet && self.verbose > 0 {
            eprintln!("Warning: --quiet and --verbose are contradictory");
        }

        // Warn about --explain or --test with output file
        if (self.explain || self.test) && self.output.is_some() {
            eprintln!("Warning: output file is ignored with --explain or --test");
        }

        // Warn about format-specific options without matching format
        if !self.csv_separator.is_none() && !self.is_likely_csv() {
            eprintln!("Warning: --csv-separator specified but input may not be CSV");
        }
    }

    /// Check if input is likely CSV based on format or file extension
    fn is_likely_csv(&self) -> bool {
        if matches!(self.input_format, Some(DataFormat::Csv)) {
            return true;
        }

        self.input_files.iter().any(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("csv") || ext.eq_ignore_ascii_case("tsv"))
                .unwrap_or(false)
        })
    }
}

impl CliConfig {
    /// Process variable definitions from CLI arguments
    fn process_variables(&mut self, cli: &Cli) {
        // Process --arg (string variables)
        for chunk in cli.args.chunks(2) {
            if let [name, value] = chunk {
                self.variables
                    .insert(name.clone(), serde_json::Value::String(value.clone()));
            }
        }

        // Process --argjson (JSON variables)
        for chunk in cli.argjson.chunks(2) {
            if let [name, json_str] = chunk {
                match serde_json::from_str(json_str) {
                    Ok(value) => {
                        self.variables.insert(name.clone(), value);
                    }
                    Err(e) => {
                        eprintln!("Warning: Invalid JSON for variable '{}': {}", name, e);
                    }
                }
            }
        }

        // Process --argfile (variables from files)
        for chunk in cli.argfile.chunks(2) {
            if let [name, file_path] = chunk {
                match std::fs::read_to_string(file_path) {
                    Ok(content) => match serde_json::from_str(&content) {
                        Ok(value) => {
                            self.variables.insert(name.clone(), value);
                        }
                        Err(e) => {
                            eprintln!("Warning: Invalid JSON in file '{}': {}", file_path, e);
                        }
                    },
                    Err(e) => {
                        eprintln!("Warning: Failed to read file '{}': {}", file_path, e);
                    }
                }
            }
        }
    }

    /// Check if output should be colored
    #[allow(dead_code)]
    pub fn should_use_color(&self) -> bool {
        // Respect NO_COLOR environment variable (https://no-color.org/)
        if std::env::var("NO_COLOR").is_ok() {
            return false;
        }

        self.color_output
            .unwrap_or_else(|| !self.quiet && atty::is(atty::Stream::Stdout))
    }

    /// Check if we should show progress
    pub fn should_show_progress(&self) -> bool {
        !self.quiet && self.verbose > 0 && atty::is(atty::Stream::Stderr)
    }

    /// Get the effective output format
    pub fn get_output_format(&self) -> Option<DataFormat> {
        self.output_format
            .or_else(|| {
                // Try to detect from output file extension
                self.output
                    .as_ref()
                    .and_then(|path| DataFormat::from_path(path).ok())
            })
            .or(self.input_format)
    }

    /// Check if this is a simple conversion operation
    pub fn is_conversion(&self) -> bool {
        self.filter.is_none() && self.input_format.is_some() && self.output_format.is_some()
    }

    /// Check if we're reading from stdin
    pub fn is_stdin(&self) -> bool {
        self.input_files.is_empty()
    }

    /// Check if we're writing to stdout
    pub fn is_stdout(&self) -> bool {
        self.output.is_none()
    }
}

/// Parse command-line arguments
pub fn parse_args() -> Cli {
    Cli::parse()
}

/// Parse command-line arguments from a vector (for testing)
#[allow(dead_code)]
pub fn parse_args_from<I, T>(args: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    Cli::try_parse_from(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_parsing() {
        let args = vec!["dsq", ".", "input.csv"];
        let cli = parse_args_from(args).unwrap();
        assert_eq!(cli.filter, Some(".".to_string()));
        assert_eq!(cli.input_files, vec![PathBuf::from("input.csv")]);
    }

    #[test]
    fn test_format_options() {
        let args = vec![
            "dsq",
            "-i",
            "csv",
            "--output-format",
            "parquet",
            ".",
            "input.csv",
            "-o",
            "output.parquet",
        ];
        let cli = parse_args_from(args).unwrap();
        assert_eq!(cli.input_format, Some(DataFormat::Csv));
        assert_eq!(cli.output_format, Some(DataFormat::Parquet));
    }

    #[test]
    fn test_csv_options() {
        let args = vec![
            "dsq",
            "--csv-separator",
            "\t",
            "--csv-headers",
            "true",
            "--csv-quote",
            "'",
            ".",
            "input.tsv",
        ];
        let cli = parse_args_from(args).unwrap();
        assert_eq!(cli.csv_separator, Some("\t".to_string()));
        assert_eq!(cli.csv_headers, Some(true));
        assert_eq!(cli.csv_quote, Some("'".to_string()));
    }

    #[test]
    fn test_variables() {
        let args = vec![
            "dsq",
            "--arg",
            "name",
            "value",
            "--argjson",
            "data",
            r#"{"key": "value"}"#,
            ".",
        ];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);

        assert_eq!(
            config.variables.get("name"),
            Some(&serde_json::Value::String("value".to_string()))
        );
        let data_value = config.variables.get("data").unwrap();
        assert!(data_value.is_object());
        assert_eq!(
            data_value["key"],
            serde_json::Value::String("value".to_string())
        );
    }

    #[test]
    fn test_variables_invalid_json() {
        let args = vec!["dsq", "--argjson", "invalid", r#"{"invalid": json"#, "."];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);

        // Invalid JSON should not be added to variables
        assert!(!config.variables.contains_key("invalid"));
    }

    #[test]
    fn test_performance_options() {
        let args = vec![
            "dsq",
            "--batch-size",
            "5000",
            "--memory-limit",
            "2GB",
            "--threads",
            "4",
            ".",
        ];
        let cli = parse_args_from(args).unwrap();
        assert_eq!(cli.batch_size, Some(5000));
        assert_eq!(cli.memory_limit, Some("2GB".to_string()));
        assert_eq!(cli.threads, Some(4));
    }

    #[test]
    fn test_convert_subcommand() {
        let args = vec![
            "dsq",
            "convert",
            "input.csv",
            "output.parquet",
            "--overwrite",
        ];
        let cli = parse_args_from(args).unwrap();

        match cli.command {
            Some(Commands::Convert {
                input,
                output,
                overwrite,
                ..
            }) => {
                assert_eq!(input, PathBuf::from("input.csv"));
                assert_eq!(output, PathBuf::from("output.parquet"));
                assert!(overwrite);
            }
            _ => panic!("Expected Convert command"),
        }
    }

    #[test]
    fn test_cli_config_conversion() {
        let args = vec![
            "dsq",
            "-c",
            "-r",
            "-S",
            "--lazy",
            "false",
            ".",
            "input.json",
        ];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);

        assert!(config.compact_output);
        assert!(config.raw_output);
        assert!(config.sort_keys);
        assert!(!config.lazy);
    }

    #[test]
    fn test_output_formatting_options() {
        let args = vec![
            "dsq",
            "--compact-output",
            "--raw-output",
            "--sort-keys",
            "--indent",
            "4",
            "--color",
            "always",
            "--join-output",
            ".",
            "input.json",
        ];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);

        assert!(config.compact_output);
        assert!(config.raw_output);
        assert!(config.sort_keys);
        assert!(!config.use_tabs); // Removed --tab to avoid conflict
        assert_eq!(config.indent_size, 4);
        assert_eq!(config.color_output, Some(true));
        assert!(config.join_output);
    }

    #[test]
    fn test_input_options() {
        let args = vec!["dsq", "--slurp", "--null-input", "--exit-status", "."];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);

        assert!(config.slurp);
        assert!(config.null_input);
        assert!(config.exit_status);
    }

    #[test]
    fn test_processing_options() {
        let args = vec![
            "dsq",
            "--skip-rows",
            "5",
            "--limit",
            "100",
            "--select",
            "col1,col2",
            "--lazy",
            "false",
            "--dataframe-optimizations",
            "false",
            ".",
            "input.csv",
        ];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);

        assert_eq!(config.skip_rows, Some(5));
        assert_eq!(config.limit, Some(100));
        assert_eq!(config.select_columns, vec!["col1,col2"]);
        assert!(!config.lazy);
        assert!(!config.dataframe_optimizations);
        assert_eq!(config.input_files, vec![PathBuf::from("input.csv")]);
    }

    #[test]
    fn test_performance_options_comprehensive() {
        let args = vec![
            "dsq",
            "--batch-size",
            "2000",
            "--memory-limit",
            "512MB",
            "--threads",
            "8",
            "--parallel",
            "false",
            ".",
            "input.json",
        ];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);

        assert_eq!(config.batch_size, Some(2000));
        assert_eq!(config.memory_limit, Some("512MB".to_string()));
        assert_eq!(config.threads, Some(8));
        assert!(!config.parallel);
    }

    #[test]
    fn test_debug_options() {
        let args = vec![
            "dsq",
            "--explain",
            "--stats",
            "--time",
            "-vv",
            "--quiet",
            ".",
            "input.json",
        ];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);

        assert!(config.explain);
        assert!(config.stats);
        assert!(config.time);
        assert_eq!(config.verbose, 2);
        assert!(config.quiet);
    }

    #[test]
    fn test_other_options() {
        let args = vec![
            "dsq",
            "--config",
            "config.toml",
            "--overwrite",
            "--test",
            ".",
            "input.json",
            "-o",
            "output.json",
        ];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);

        assert_eq!(config.config_file, Some(PathBuf::from("config.toml")));
        assert!(config.overwrite);
        assert!(config.test);
        assert_eq!(config.output, Some(PathBuf::from("output.json")));
    }

    #[test]
    fn test_variable_options() {
        let args = vec![
            "dsq",
            "--arg",
            "var1",
            "value1",
            "--argjson",
            "var2",
            r#"{"key": "value"}"#,
            "--library-path",
            "/lib/path1",
            "--library-path",
            "/lib/path2",
            "--import",
            "module1",
            "--include",
            "file1.dsq",
            ".",
            "input.json",
        ];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);

        assert_eq!(
            config.variables.get("var1"),
            Some(&serde_json::Value::String("value1".to_string()))
        );
        assert!(config.variables.contains_key("var2"));
        assert_eq!(
            config.library_path,
            vec![PathBuf::from("/lib/path1"), PathBuf::from("/lib/path2")]
        );
        assert_eq!(config.imports, vec!["module1"]);
        assert_eq!(config.includes, vec![PathBuf::from("file1.dsq")]);
    }

    #[test]
    fn test_csv_options_comprehensive() {
        let args = vec![
            "dsq",
            "--csv-separator",
            "|",
            "--csv-headers",
            "false",
            "--csv-quote",
            "\"",
            "--csv-null",
            "NA",
            "--csv-null",
            "NULL",
            ".",
            "input.csv",
        ];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);

        assert_eq!(config.csv_separator, Some("|".to_string()));
        assert_eq!(config.csv_headers, Some(false));
        assert_eq!(config.csv_quote, Some("\"".to_string()));
        assert_eq!(config.csv_null_values, vec!["NA", "NULL"]);
    }

    #[test]
    fn test_inspect_subcommand() {
        let args = vec![
            "dsq",
            "inspect",
            "file.json",
            "--schema",
            "--sample",
            "10",
            "--stats",
        ];
        let cli = parse_args_from(args).unwrap();

        match cli.command {
            Some(Commands::Inspect {
                file,
                schema,
                sample,
                stats,
            }) => {
                assert_eq!(file, PathBuf::from("file.json"));
                assert!(schema);
                assert_eq!(sample, Some(10));
                assert!(stats);
            }
            _ => panic!("Expected Inspect command"),
        }
    }

    #[test]
    fn test_validate_subcommand() {
        let args = vec![
            "dsq",
            "validate",
            "file1.csv",
            "file2.json",
            "--schema",
            "schema.json",
            "--check-duplicates",
            "--check-nulls",
        ];
        let cli = parse_args_from(args).unwrap();

        match cli.command {
            Some(Commands::Validate {
                files,
                schema,
                check_duplicates,
                check_nulls,
            }) => {
                assert_eq!(
                    files,
                    vec![PathBuf::from("file1.csv"), PathBuf::from("file2.json")]
                );
                assert_eq!(schema, Some(PathBuf::from("schema.json")));
                assert!(check_duplicates);
                assert!(check_nulls);
            }
            _ => panic!("Expected Validate command"),
        }
    }

    #[test]
    fn test_merge_subcommand() {
        let args = vec![
            "dsq",
            "merge",
            "file1.csv",
            "file2.csv",
            "--output",
            "merged.csv",
            "--method",
            "join",
            "--on",
            "id",
            "--join-type",
            "left",
        ];
        let cli = parse_args_from(args).unwrap();

        match cli.command {
            Some(Commands::Merge {
                inputs,
                output,
                method,
                on,
                join_type,
            }) => {
                assert_eq!(
                    inputs,
                    vec![PathBuf::from("file1.csv"), PathBuf::from("file2.csv")]
                );
                assert_eq!(output, PathBuf::from("merged.csv"));
                assert_eq!(method, MergeMethod::Join);
                assert_eq!(on, vec!["id"]);
                assert_eq!(join_type, JoinType::Left);
            }
            _ => panic!("Expected Merge command"),
        }
    }

    #[test]
    fn test_config_subcommands() {
        // Test config show
        let args = vec!["dsq", "config", "show"];
        let cli = parse_args_from(args).unwrap();
        match cli.command {
            Some(Commands::Config {
                command: ConfigCommands::Show,
            }) => {}
            _ => panic!("Expected Config Show command"),
        }

        // Test config init
        let args = vec!["dsq", "config", "init", "test.toml", "--force"];
        let cli = parse_args_from(args).unwrap();
        match cli.command {
            Some(Commands::Config {
                command: ConfigCommands::Init { path, force },
            }) => {
                assert_eq!(path, PathBuf::from("test.toml"));
                assert!(force);
            }
            _ => panic!("Expected Config Init command"),
        }

        // Test config check
        let args = vec!["dsq", "config", "check", "config.toml"];
        let cli = parse_args_from(args).unwrap();
        match cli.command {
            Some(Commands::Config {
                command: ConfigCommands::Check { path },
            }) => {
                assert_eq!(path, PathBuf::from("config.toml"));
            }
            _ => panic!("Expected Config Check command"),
        }

        // Test config get
        let args = vec!["dsq", "config", "get", "filter.lazy_evaluation"];
        let cli = parse_args_from(args).unwrap();
        match cli.command {
            Some(Commands::Config {
                command: ConfigCommands::Get { key },
            }) => {
                assert_eq!(key, "filter.lazy_evaluation");
            }
            _ => panic!("Expected Config Get command"),
        }

        // Test config set
        let args = vec![
            "dsq",
            "config",
            "set",
            "filter.lazy_evaluation",
            "false",
            "--config",
            "custom.toml",
        ];
        let cli = parse_args_from(args).unwrap();
        match cli.command {
            Some(Commands::Config {
                command: ConfigCommands::Set { key, value, config },
            }) => {
                assert_eq!(key, "filter.lazy_evaluation");
                assert_eq!(value, "false");
                assert_eq!(config, Some(PathBuf::from("custom.toml")));
            }
            _ => panic!("Expected Config Set command"),
        }
    }

    #[test]
    fn test_filter_file_option() {
        let args = vec!["dsq", "-f", "filter.dsq", "input.json"];
        let cli = parse_args_from(args).unwrap();
        assert_eq!(cli.filter_file, Some(PathBuf::from("filter.dsq")));
        assert_eq!(cli.filter, Some("input.json".to_string()));
        assert_eq!(cli.input_files, Vec::<PathBuf>::new());
    }

    #[test]
    fn test_interactive_mode() {
        let args = vec!["dsq", "-I"];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);
        assert!(config.interactive);
    }

    #[test]
    fn test_should_use_color() {
        let mut config = CliConfig::default();
        config.color_output = Some(true);
        assert!(config.should_use_color());

        config.color_output = Some(false);
        assert!(!config.should_use_color());

        // For None, it depends on quiet and atty, but since atty is external,
        // we'll test the logic indirectly through Cli parsing
        let args = vec!["dsq", "--color", "always", "."];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);
        assert!(config.should_use_color());

        let args = vec!["dsq", "--color", "never", "."];
        let cli = parse_args_from(args).unwrap();
        let config = CliConfig::from(&cli);
        assert!(!config.should_use_color());
    }

    #[test]
    fn test_should_show_progress() {
        let mut config = CliConfig::default();
        // Test cases where should_show_progress should return false
        config.verbose = 0;
        assert!(!config.should_show_progress());

        config.quiet = true;
        config.verbose = 1;
        assert!(!config.should_show_progress());
    }

    #[test]
    fn test_get_output_format() {
        let mut config = CliConfig::default();
        config.output_format = Some(DataFormat::Json);
        assert_eq!(config.get_output_format(), Some(DataFormat::Json));

        config.output_format = None;
        config.output = Some(PathBuf::from("test.json"));
        // Assuming DataFormat::from_path works
        assert_eq!(config.get_output_format(), Some(DataFormat::Json));

        config.output = None;
        config.input_format = Some(DataFormat::Csv);
        assert_eq!(config.get_output_format(), Some(DataFormat::Csv));
    }

    #[test]
    fn test_is_conversion() {
        let mut config = CliConfig::default();
        config.filter = None;
        config.input_format = Some(DataFormat::Csv);
        config.output_format = Some(DataFormat::Json);
        assert!(config.is_conversion());

        config.filter = Some(".".to_string());
        assert!(!config.is_conversion());

        config.filter = None;
        config.input_format = None;
        assert!(!config.is_conversion());
    }

    #[test]
    fn test_is_stdin_stdout() {
        let mut config = CliConfig::default();
        config.input_files = vec![];
        assert!(config.is_stdin());

        config.input_files = vec![PathBuf::from("file.json")];
        assert!(!config.is_stdin());

        config.output = None;
        assert!(config.is_stdout());

        config.output = Some(PathBuf::from("out.json"));
        assert!(!config.is_stdout());
    }

    #[test]
    fn test_edge_cases() {
        // Empty args should fail or have defaults
        let args = vec!["dsq"];
        let cli = parse_args_from(args).unwrap();
        assert_eq!(cli.filter, None);
        assert!(cli.input_files.is_empty());

        // Invalid color option
        let args = vec!["dsq", "--color", "invalid", "."];
        assert!(parse_args_from(args).is_err());

        // Conflicting indent and tab
        let args = vec!["dsq", "--indent", "4", "--tab", "."];
        assert!(parse_args_from(args).is_err());

        // Multiple filters (should be ok, but filter takes one)
        let args = vec!["dsq", "filter1", "filter2", "file.json"];
        let cli = parse_args_from(args).unwrap();
        assert_eq!(cli.filter, Some("filter1".to_string()));
        assert_eq!(
            cli.input_files,
            vec![PathBuf::from("filter2"), PathBuf::from("file.json")]
        );
    }

    #[test]
    fn test_subcommand_edge_cases() {
        // Convert without required output
        let args = vec!["dsq", "convert", "input.csv"];
        assert!(parse_args_from(args).is_err());

        // Merge without inputs
        let args = vec!["dsq", "merge", "--output", "out.csv"];
        let cli = parse_args_from(args).unwrap();
        match cli.command {
            Some(Commands::Merge { inputs, .. }) => assert!(inputs.is_empty()),
            _ => panic!("Expected Merge command"),
        }

        // Config set without value
        let args = vec!["dsq", "config", "set", "key"];
        assert!(parse_args_from(args).is_err());

        // Completions without shell
        let args = vec!["dsq", "completions"];
        assert!(parse_args_from(args).is_err());
    }
}
