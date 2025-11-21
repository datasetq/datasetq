#[cfg(not(target_arch = "wasm32"))]
mod cli;
#[cfg(feature = "cli")]
mod config;
mod executor;
mod output;
mod repl;

#[cfg(not(target_arch = "wasm32"))]
use clap::CommandFactory;
#[cfg(not(target_arch = "wasm32"))]
use clap_complete::generate;
#[cfg(not(target_arch = "wasm32"))]
use crate::cli::{parse_args, CliConfig, Commands, ConfigCommands};
#[cfg(not(target_arch = "wasm32"))]
use crate::config::{Config, create_default_config_file, validate_config};
#[cfg(not(target_arch = "wasm32"))]
use crate::executor::Executor;
#[cfg(not(target_arch = "wasm32"))]
use crate::repl::Repl;
#[cfg(not(target_arch = "wasm32"))]
use dsq_core::error::{Error, Result};
#[cfg(not(target_arch = "wasm32"))]
use dsq_core::DataFormat;
#[cfg(not(target_arch = "wasm32"))]
use dsq_core::Value;
#[cfg(not(target_arch = "wasm32"))]
use dsq_core::io::{read_file, write_file};
#[cfg(not(target_arch = "wasm32"))]
use std::fs;
#[cfg(not(target_arch = "wasm32"))]
use std::io;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(target_arch = "wasm32"))]
use std::process;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn run() -> Result<()> {
    let args = parse_args();
    let mut cli_config = CliConfig::from(&args);

    // When filter_file is provided, treat the positional filter as the first input file
    if cli_config.filter_file.is_some() {
        if let Some(filter_str) = &cli_config.filter {
            cli_config.input_files.insert(0, PathBuf::from(filter_str));
            cli_config.filter = None;
        }
    }

    // Load configuration
    let mut config = Config::load()?;
    if let Some(config_path) = &cli_config.config_file {
        config.merge_file(config_path)?;
    }
    config.apply_cli(&cli_config)?;

    // Set default output format based on input format when outputting to stdout
    // ONLY if no explicit output format was specified via CLI
    if cli_config.output.is_none() && !cli_config.input_files.is_empty() && config.io.default_output_format.is_none() && cli_config.output_format.is_none() {
        if let Ok(input_format) = DataFormat::from_path(&cli_config.input_files[0]) {
            config.io.default_output_format = Some(input_format);
        }
    }

    // Set up logging
    setup_logging(&config);
    
    // Handle subcommands
    if let Some(command) = args.command {
        return handle_command(command, &config).await;
    }
    
    // Handle interactive mode
    if cli_config.interactive {
        return run_interactive(&config).await;
    }
    
    // Handle test mode
    if cli_config.test {
        return test_filter(&cli_config, &config);
    }
    
    // Get the filter
    let filter = if let Some(filter_file) = &cli_config.filter_file {
        fs::read_to_string(filter_file)
            .map_err(|e| Error::operation(format!("Failed to read filter file {}: {}", filter_file.display(), e)))?
            .trim()
            .to_string()
    } else {
        cli_config.filter.clone().unwrap_or_else(|| ".".to_string())
    };

    // Main execution path
    let (filter, input_paths) = if let Some(input_path) = cli_config.input_files.first() {
        if input_path.is_dir() {
            // Handle example directory
            let (filter, paths) = handle_example_directory(input_path)?;
            (filter, paths)
        } else {
            // Normal case
            let input_paths = cli_config.input_files.clone();
            (filter, input_paths)
        }
    } else if let Some(filter_str) = &cli_config.filter {
        // Check if filter looks like a directory path (but not when null_input is true)
        let filter_path = Path::new(filter_str);
        if !cli_config.null_input && filter_path.is_dir() {
            // Handle example directory passed as filter
            let (filter, paths) = handle_example_directory(filter_path)?;
            (filter, paths)
        } else {
            // Normal case with filter
            (filter_str.clone(), vec![])
        }
    } else {
        // No input files, use stdin
        (filter, vec![])
    };

    let output_path = cli_config.output.as_deref();

    let mut executor = Executor::new(config);
        if input_paths.is_empty() {
            if cli_config.null_input {
                executor.execute_filter_on_value(&filter, Value::Null, output_path).await
            } else {
                executor.execute_filter(&filter, None, output_path).await
            }
        } else {
            // For now, just use the first input path
            // TODO: handle multiple input files
            executor.execute_filter(&filter, Some(&input_paths[0]), output_path).await
        }
}

async fn handle_command(command: Commands, config: &Config) -> Result<()> {
    match command {
        Commands::Convert { input, output, input_format, output_format, overwrite } => {
            convert_file(&input, &output, input_format, output_format, overwrite, config).await
        }
        Commands::Inspect { file, schema, sample, stats } => {
            inspect_file(&file, schema, sample, stats, config).await
        }
        Commands::Validate { files, schema, check_duplicates, check_nulls } => {
            validate_files(&files, schema.as_deref(), check_duplicates, check_nulls, config).await
        }
        Commands::Merge { inputs, output, method, on, join_type } => {
            merge_files(&inputs, &output, method, &on, join_type, config).await
        }
        Commands::Completions { shell } => {
            generate_completions(shell)
        }
        Commands::Config { command } => {
            handle_config_command(command, config)
        }
    }
}

fn handle_config_command(command: ConfigCommands, config: &Config) -> Result<()> {
    match command {
        ConfigCommands::Show => {
            let yaml = serde_yaml::to_string(config)
                .map_err(|e| Error::Config(format!("Failed to serialize config: {}", e)))?;
            println!("{}", yaml);
            Ok(())
        }
        ConfigCommands::Init { path, force } => {
            if path.exists() && !force {
                return Err(Error::operation(format!("Config file already exists: {}", path.display())));
            }
            create_default_config_file(&path)?;
            println!("Created config file: {}", path.display());
            Ok(())
        }
        ConfigCommands::Check { path } => {
            let check_config = Config::load_from_file(&path)?;
            validate_config(&check_config)?;
            println!("Config file is valid");
            Ok(())
        }
        ConfigCommands::Get { key } => {
            let value = get_config_value(config, &key)?;
            println!("{}", value);
            Ok(())
        }
        ConfigCommands::Set { key, value, config: config_path } => {
            let mut update_config = if let Some(ref path) = config_path {
                Config::load_from_file(&path)?
            } else {
                config.clone()
            };
            set_config_value(&mut update_config, &key, &value)?;
            if let Some(path) = config_path {
                update_config.save(&path)?;
            }
            Ok(())
        }
    }
}


async fn convert_file(
    input: &Path,
    output: &Path,
    input_format: Option<DataFormat>,
    output_format: Option<DataFormat>,
    overwrite: bool,
    config: &Config,
) -> Result<()> {
    let _in_format = input_format
        .or_else(|| DataFormat::from_path(input).ok())
        .ok_or_else(|| Error::operation("Cannot determine input format"))?;

    let _out_format = output_format
        .or_else(|| DataFormat::from_path(output).ok())
        .ok_or_else(|| Error::operation("Cannot determine output format"))?;

    if output.exists() && !overwrite {
        return Err(Error::operation(format!("Output file already exists: {}", output.display())));
    }
    
    let read_options = config.to_read_options();
    let data = read_file(input, &read_options).await?;
    let write_options = config.to_write_options();
    write_file(&data, output, &write_options).await?;
    
    println!("Converted {} to {}", input.display(), output.display());
    Ok(())
}

async fn inspect_file(
    file: &Path,
    show_schema: bool,
    sample: Option<usize>,
    show_stats: bool,
    config: &Config,
) -> Result<()> {
    let format = DataFormat::from_path(file)
        .map_err(|_| Error::operation("Cannot determine file format"))?;
    
    let mut read_options = config.to_read_options();
    read_options.n_rows = sample.map(|n| n.max(100));
    let data = read_file(file, &read_options).await?;

    println!("File: {}", file.display());
    println!("Format: {:?}", format);

    match &data {
        Value::DataFrame(df) => {
            println!("Rows: {}", df.height());
            println!("Columns: {}", df.width());

            if show_schema {
                println!("\nSchema:");
                for (name, dtype) in df.schema().iter() {
                    println!("  {}: {:?}", name, dtype);
                }
            }

            if let Some(n) = sample {
                println!("\nSample ({} rows):", n);
                println!("{}", df.head(Some(n)));
            }

            if show_stats {
                println!("\nStatistics:");
                println!("{}", df.describe(None)?);
            }
        }
        _ => {
            println!("Data type: {:?}", data);
            if let Some(_n) = sample {
                println!("\nSample:");
                println!("{:?}", data);
            }
        }
    }
    
    Ok(())
}

async fn validate_files(
    files: &[std::path::PathBuf],
    schema_path: Option<&Path>,
    check_duplicates: bool,
    check_nulls: bool,
    config: &Config,
) -> Result<()> {
    let expected_schema = if let Some(path) = schema_path {
        Some(load_schema(path)?)
    } else {
        None
    };
    
    for file in files {
        println!("Validating: {}", file.display());
        
        let _format = DataFormat::from_path(file)
            .map_err(|_| Error::operation("Cannot determine file format"))?;
        
        let read_options = config.to_read_options();
        let data = read_file(file, &read_options).await?;

        match &data {
            Value::DataFrame(df) => {
                // Check schema
                if let Some(ref expected) = expected_schema {
                    if !schemas_match(&df.schema(), expected) {
                        eprintln!("  ❌ Schema mismatch");
                        continue;
                    }
                }

                // Check duplicates
                if check_duplicates {
                    // Check for duplicate rows
                    let duplicate_mask = df.is_duplicated().map_err(|e| Error::operation(format!("Failed to check for duplicates: {}", e)))?;
                    let duplicate_count = duplicate_mask.sum().unwrap_or(0);
                    if duplicate_count > 0 {
                        eprintln!("  ⚠️  Found {} duplicate rows", duplicate_count);
                    }
                }

                // Check nulls
                if check_nulls {
                    for col in df.get_columns() {
                        let null_count = col.null_count();
                        if null_count > 0 {
                            eprintln!("  ⚠️  Column '{}' has {} null values", col.name(), null_count);
                        }
                    }
                }
            }
            _ => {
                eprintln!("  ⚠️  Not a DataFrame, skipping advanced validation");
            }
        }

        println!("  ✓ Valid");
    }

    Ok(())
}





fn generate_completions(shell: clap_complete::Shell) -> Result<()> {
    let mut cmd = crate::cli::Cli::command();
    let name = cmd.get_name().to_string();
    generate(shell, &mut cmd, name, &mut io::stdout());
    Ok(())
}

async fn run_interactive(config: &Config) -> Result<()> {
    let mut repl = Repl::new(config.clone())?;
    repl.run().await
}

fn test_filter(cli_config: &CliConfig, config: &Config) -> Result<()> {
    // Get the filter
    let filter = if let Some(filter_file) = &cli_config.filter_file {
        fs::read_to_string(filter_file)
            .map_err(|e| Error::operation(format!("Failed to read filter file {}: {}", filter_file.display(), e)))?
            .trim()
            .to_string()
    } else if let Some(input_path) = cli_config.input_files.first() {
        if input_path.is_dir() {
            // Handle example directory
            let (filter, _paths) = handle_example_directory(input_path)?;
            filter
        } else {
            // Normal case
            cli_config.filter.clone().unwrap_or_else(|| ".".to_string())
        }
    } else if let Some(filter_str) = &cli_config.filter {
        // Check if filter looks like a directory path
        let filter_path = Path::new(filter_str);
        if filter_path.is_dir() {
            // Handle example directory passed as filter
            let (filter, _paths) = handle_example_directory(filter_path)?;
            filter
        } else {
            // Normal case with filter
            filter_str.clone()
        }
    } else {
        // No input files, use stdin
        ".".to_string()
    };

    let executor = Executor::new(config.clone());
    
    match executor.validate_filter(&filter) {
        Ok(_) => {
            println!("Filter is valid: {}", filter);
            Ok(())
        }
        Err(e) => {
            eprintln!("Filter is invalid: {}", e);
            process::exit(1);
        }
    }
}



fn setup_logging(config: &Config) {
    let log_level = match config.debug.verbosity {
        0 => log::LevelFilter::Warn,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };
    
    env_logger::Builder::new()
        .filter_level(log_level)
        .init();
}

fn get_config_value(config: &Config, key: &str) -> Result<String> {
    let value = match key {
        "filter.lazy_evaluation" => config.filter.lazy_evaluation.to_string(),
        "filter.dataframe_optimizations" => config.filter.dataframe_optimizations.to_string(),
        "performance.batch_size" => config.performance.batch_size.to_string(),
        "performance.threads" => config.performance.threads.to_string(),
        "formats.csv.separator" => config.formats.csv.separator.clone(),
        "formats.csv.has_header" => config.formats.csv.has_header.to_string(),
        _ => return Err(Error::Config(format!("Unknown config key: {}", key))),
    };
    Ok(value)
}

fn set_config_value(config: &mut Config, key: &str, value: &str) -> Result<()> {
    match key {
        "filter.lazy_evaluation" => {
            config.filter.lazy_evaluation = value.parse()
                .map_err(|_| Error::Config("Invalid boolean value".to_string()))?;
        }
        "filter.dataframe_optimizations" => {
            config.filter.dataframe_optimizations = value.parse()
                .map_err(|_| Error::Config("Invalid boolean value".to_string()))?;
        }
        "performance.batch_size" => {
            config.performance.batch_size = value.parse()
                .map_err(|_| Error::Config("Invalid batch size".to_string()))?;
        }
        "performance.threads" => {
            config.performance.threads = value.parse()
                .map_err(|_| Error::Config("Invalid thread count".to_string()))?;
        }
        "formats.csv.separator" => {
            if value.len() != 1 {
                return Err(Error::Config("CSV separator must be a single character".to_string()));
            }
            config.formats.csv.separator = value.to_string();
        }
        "formats.csv.has_header" => {
            config.formats.csv.has_header = value.parse()
                .map_err(|_| Error::Config("Invalid boolean value".to_string()))?;
        }
        _ => return Err(Error::Config(format!("Unknown config key: {}", key))),
    }

    Ok(())
}
fn load_schema(path: &Path) -> Result<polars::prelude::Schema> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| Error::operation(format!("Failed to read schema file: {}", e)))?;

    let schema_map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&content)
        .map_err(|e| Error::operation(format!("Invalid schema JSON: {}", e)))?;
    
    let mut schema = polars::prelude::Schema::new();
    for (name, value) in schema_map {
        let dtype_str = value.as_str().ok_or_else(|| Error::operation(format!("dtype for {} must be string", name)))?;
        let dtype = parse_dtype(dtype_str)?;
        schema.with_column(name.into(), dtype);
    }
    
    Ok(schema)
}

fn parse_dtype(dtype_str: &str) -> Result<polars::prelude::DataType> {
    use polars::prelude::DataType;
    
    Ok(match dtype_str.to_lowercase().as_str() {
        "bool" | "boolean" => DataType::Boolean,
        "i8" | "int8" => DataType::Int8,
        "i16" | "int16" => DataType::Int16,
        "i32" | "int32" => DataType::Int32,
        "i64" | "int64" => DataType::Int64,
        "u8" | "uint8" => DataType::UInt8,
        "u16" | "uint16" => DataType::UInt16,
        "u32" | "uint32" => DataType::UInt32,
        "u64" | "uint64" => DataType::UInt64,
        "f32" | "float32" => DataType::Float32,
        "f64" | "float64" => DataType::Float64,
        "str" | "string" | "utf8" => DataType::Utf8,
        "date" => DataType::Date,
        "datetime" => DataType::Datetime(polars::prelude::TimeUnit::Microseconds, None),
        "time" => DataType::Time,
        _ => return Err(Error::operation(format!("Unknown data type: {}", dtype_str))),
    })
}

fn handle_example_directory(dir_path: &Path) -> Result<(String, Vec<PathBuf>)> {
    use std::fs;

    // Check if query.dsq exists
    let query_path = dir_path.join("query.dsq");
    if !query_path.exists() {
        return Err(Error::operation(format!("query.dsq not found in {}", dir_path.display())));
    }

    // Read the filter from query.dsq
    let filter = fs::read_to_string(&query_path)
        .map_err(|e| Error::operation(format!("Failed to read query.dsq: {}", e)))?
        .trim()
        .to_string();

    // Find data files (data.json, data.csv, etc.)
    let mut data_files = Vec::new();
    for entry in fs::read_dir(dir_path)
        .map_err(|e| Error::operation(format!("Failed to read directory {}: {}", dir_path.display(), e)))?
    {
        let entry = entry.map_err(|e| Error::operation(format!("Failed to read directory entry: {}", e)))?;
        let path = entry.path();
        if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.starts_with("data.") && (file_name.ends_with(".json") || file_name.ends_with(".csv") || file_name.ends_with(".tsv") || file_name.ends_with(".parquet") || file_name.ends_with(".jsonl") || file_name.ends_with(".ndjson")) {
                    data_files.push(path);
                }
            }
        }
    }

    if data_files.is_empty() {
        return Err(Error::operation(format!("No data files found in {}", dir_path.display())));
    }

    // Sort data files to ensure consistent order
    data_files.sort();

    Ok((filter, data_files))
}

fn schemas_match(actual: &polars::prelude::Schema, expected: &polars::prelude::Schema) -> bool {
    if actual.len() != expected.len() {
        return false;
    }

    for (name, dtype) in expected.iter() {
        match actual.get(name) {
            Some(actual_dtype) if actual_dtype == dtype => continue,
            _ => return false,
        }
    }

    true
}

async fn merge_files(
    inputs: &[std::path::PathBuf],
    output: &std::path::PathBuf,
    method: cli::MergeMethod,
    on: &[String],
    join_type: cli::JoinType,
    config: &Config,
) -> Result<()> {
    use polars::prelude::*;

    if inputs.is_empty() {
        return Err(Error::operation("No input files provided"));
    }

    if inputs.len() == 1 {
        return Err(Error::operation("At least two input files required for merge"));
    }

    // Read all input files
    let read_options = config.to_read_options();
    let mut dataframes = Vec::new();

    for input in inputs {
        let value = dsq_core::io::read_file(input, &read_options).await?;
        let df = match value {
            Value::DataFrame(df) => df,
            Value::LazyFrame(lf) => lf.collect().map_err(|e| Error::operation(format!("Failed to collect lazy frame: {}", e)))?,
            _ => return Err(Error::operation(format!("Input file {} does not contain tabular data", input.display()))),
        };
        dataframes.push(df);
    }

    let result_df = match method {
        cli::MergeMethod::Concat => {
            // Concatenate all dataframes vertically
            let lazy_frames: Vec<_> = dataframes.iter().map(|df| df.clone().lazy()).collect();
            concat(
                &lazy_frames,
                UnionArgs::default()
            ).map_err(|e| Error::operation(format!("Failed to concatenate dataframes: {}", e)))?
            .collect().map_err(|e| Error::operation(format!("Failed to collect concatenated result: {}", e)))?
        }
        cli::MergeMethod::Join => {
            if on.is_empty() {
                return Err(Error::operation("Join method requires 'on' parameter specifying join columns"));
            }

            // Start with the first dataframe as Value
            let mut result = Value::DataFrame(dataframes[0].clone());

            // Join each subsequent dataframe
            for df in dataframes.iter().skip(1) {
                use dsq_core::ops::join::{join, JoinKeys, JoinOptions, JoinType as CoreJoinType};

                let right = Value::DataFrame(df.clone());
                let keys = JoinKeys::on(on.to_vec());
                let join_type_core = match join_type {
                    cli::JoinType::Inner => CoreJoinType::Inner,
                    cli::JoinType::Left => CoreJoinType::Left,
                    cli::JoinType::Right => CoreJoinType::Right,
                    cli::JoinType::Outer => CoreJoinType::Outer,
                };
                let options = JoinOptions {
                    join_type: join_type_core,
                    ..Default::default()
                };

                result = join(&result, &right, &keys, &options)
                    .map_err(|e| Error::operation(format!("Failed to join dataframes: {}", e)))?;
            }

            match result {
                Value::DataFrame(df) => df,
                _ => return Err(Error::operation("Join result is not a DataFrame")),
            }
        }
    };

    // Write the result
    let write_options = config.to_write_options();
    dsq_core::io::write_file(&Value::DataFrame(result_df), output, &write_options).await?;

    println!("Successfully merged {} files into {}", inputs.len(), output.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_handle_example_directory_success() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        // Create query.dsq
        let query_content = "group_by(.department) | map({dept: .[0].department, count: length})";
        fs::write(dir_path.join("query.dsq"), query_content).unwrap();

        // Create data.csv
        let csv_content = "id,name,department\n1,Alice,Engineering\n2,Bob,Sales";
        fs::write(dir_path.join("data.csv"), csv_content).unwrap();

        // Call the function
        let result = handle_example_directory(dir_path);

        assert!(result.is_ok());
        let (filter, files) = result.unwrap();
        assert_eq!(filter, query_content);
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("data.csv"));
    }

    #[test]
    fn test_handle_example_directory_no_query() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        // Create data.csv but no query.dsq
        let csv_content = "id,name,department\n1,Alice,Engineering";
        fs::write(dir_path.join("data.csv"), csv_content).unwrap();

        let result = handle_example_directory(dir_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("query.dsq not found"));
    }

    #[test]
    fn test_handle_example_directory_no_data_files() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        // Create query.dsq but no data files
        let query_content = ".[]";
        fs::write(dir_path.join("query.dsq"), query_content).unwrap();

        let result = handle_example_directory(dir_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No data files found"));
    }

    #[test]
    fn test_handle_example_directory_multiple_data_files() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        // Create query.dsq
        let query_content = ".[]";
        fs::write(dir_path.join("query.dsq"), query_content).unwrap();

        // Create multiple data files
        fs::write(dir_path.join("data.csv"), "id,name\n1,Alice").unwrap();
        fs::write(dir_path.join("data.json"), "{\"id\":1,\"name\":\"Bob\"}").unwrap();
        fs::write(dir_path.join("data.jsonl"), "{\"id\":2,\"name\":\"Charlie\"}").unwrap();
        fs::write(dir_path.join("data.ndjson"), "{\"id\":3,\"name\":\"David\"}").unwrap();
        fs::write(dir_path.join("other.csv"), "id,name\n2,Charlie").unwrap(); // Should be ignored

        let result = handle_example_directory(dir_path);
        assert!(result.is_ok());
        let (filter, files) = result.unwrap();
        assert_eq!(filter, query_content);
        assert_eq!(files.len(), 4);
        // Files should be sorted
        assert!(files[0].ends_with("data.csv"));
        assert!(files[1].ends_with("data.json"));
        assert!(files[2].ends_with("data.jsonl"));
        assert!(files[3].ends_with("data.ndjson"));
    }
}
