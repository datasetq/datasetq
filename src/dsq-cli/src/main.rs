#[cfg(not(target_arch = "wasm32"))]
mod cli;
mod config;
mod executor;
#[cfg(feature = "cli")]
mod output;
#[cfg(feature = "cli")]
mod repl;

#[cfg(not(target_arch = "wasm32"))]
use std::fs;
#[cfg(not(target_arch = "wasm32"))]
use std::io;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
#[cfg(not(target_arch = "wasm32"))]
use std::process;

#[cfg(not(target_arch = "wasm32"))]
use clap::CommandFactory;
#[cfg(not(target_arch = "wasm32"))]
use clap_complete::generate;
#[cfg(not(target_arch = "wasm32"))]
use dsq_formats::DataFormat;
#[cfg(not(target_arch = "wasm32"))]
use dsq_shared::value::Value;
use dsq_shared::Result;

#[cfg(not(target_arch = "wasm32"))]
use crate::cli::{parse_args, CliConfig, Commands, ConfigCommands};
#[cfg(not(target_arch = "wasm32"))]
use crate::config::{create_default_config_file, validate_config, Config};
#[cfg(not(target_arch = "wasm32"))]
use crate::executor::Executor;
#[cfg(all(not(target_arch = "wasm32"), feature = "cli"))]
use crate::repl::Repl;
#[cfg(not(target_arch = "wasm32"))]
use dsq_core::io::{read_file, write_file};

#[cfg(all(not(target_arch = "wasm32"), feature = "cli"))]
#[tokio::main]
async fn main() {
    // Initialize coz profiling if enabled
    #[cfg(feature = "profiling")]
    coz::thread_init();

    // Install panic hook to handle broken pipe errors gracefully
    // This prevents panics when piping to commands like `head` that close the pipe early
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        if let Some(payload) = panic_info.payload().downcast_ref::<String>() {
            if payload.contains("Broken pipe") {
                process::exit(0);
            }
        } else if let Some(payload) = panic_info.payload().downcast_ref::<&str>() {
            if payload.contains("Broken pipe") {
                process::exit(0);
            }
        }
        default_panic(panic_info);
    }));

    // Check for --version flag and show enhanced version info
    if std::env::args().any(|arg| arg == "--version" || arg == "-V") {
        print_version();
        return;
    }

    if let Err(e) = run().await {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}

fn print_version() {
    let version = env!("CARGO_PKG_VERSION");
    let git_hash = option_env!("GIT_HASH").unwrap_or("unknown");
    let build_date = option_env!("BUILD_DATE").unwrap_or("unknown");
    let rustc_version = option_env!("RUSTC_VERSION").unwrap_or("unknown");

    println!("dsq {}", version);
    println!("Commit: {}", git_hash);
    println!("Built: {}", build_date);
    println!("Rustc: {}", rustc_version);

    // Show enabled features
    #[allow(unused_mut)]
    let mut features: Vec<&str> = vec![];
    #[cfg(feature = "csv")]
    features.push("csv");
    #[cfg(feature = "json")]
    features.push("json");
    #[cfg(feature = "parquet")]
    features.push("parquet");
    #[cfg(feature = "avro")]
    features.push("avro");
    #[cfg(feature = "io-arrow")]
    features.push("arrow");
    #[cfg(feature = "profiling")]
    features.push("profiling");

    if !features.is_empty() {
        println!("Features: {}", features.join(", "));
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

    // Default output format is JSON unless explicitly specified
    // This comment left for reference - we no longer set output format based on input format

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
            .map_err(|e| {
                let suggestion = if e.kind() == std::io::ErrorKind::NotFound {
                    format!(
                        "\n\nFilter file '{}' not found.\n\n\
                        Try one of:\n  \
                        - Check the file path is correct\n  \
                        - Use a filter string directly: dsq '.' data.csv\n  \
                        - Create the file: echo '.' > {}",
                        filter_file.display(),
                        filter_file.display()
                    )
                } else {
                    String::new()
                };
                anyhow::anyhow!(format!(
                    "Failed to read filter file {}: {}{}",
                    filter_file.display(),
                    e,
                    suggestion
                ))
            })?
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
        // Don't treat "." as a directory - it's a common jq identity filter
        let filter_path = Path::new(filter_str);
        if !cli_config.null_input && filter_str != "." && filter_path.is_dir() {
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

    if input_paths.is_empty() {
        let mut executor = Executor::new(config);
        if cli_config.null_input {
            executor
                .execute_filter_on_value(&filter, Value::Null, output_path)
                .await
                .map_err(|e| e.into())
        } else {
            executor
                .execute_filter(&filter, None, output_path)
                .await
                .map_err(|e| e.into())
        }
    } else if input_paths.len() == 1 {
        // Single input file
        let mut executor = Executor::new(config);
        executor
            .execute_filter(&filter, Some(&input_paths[0]), output_path)
            .await
            .map_err(|e| e.into())
    } else {
        // Multiple input files - process each and combine results
        use dsq_core::io::read_file;
        use polars::prelude::*;

        let read_options = config.to_read_options();
        let mut dataframes = Vec::new();

        for input_path in &input_paths {
            let value = read_file(input_path, &read_options).await?;
            match value {
                Value::DataFrame(df) => dataframes.push(df),
                Value::LazyFrame(lf) => {
                    let df = lf.collect().map_err(|e| {
                        anyhow::anyhow!(format!("Failed to collect lazy frame: {}", e))
                    })?;
                    dataframes.push(df);
                }
                _ => {
                    return Err(anyhow::anyhow!(format!(
                        "Input file {} does not contain tabular data",
                        input_path.display()
                    )));
                }
            }
        }

        // Concatenate all dataframes
        let lazy_frames: Vec<_> = dataframes.iter().map(|df| df.clone().lazy()).collect();
        let combined_df = concat(&lazy_frames, UnionArgs::default())
            .map_err(|e| anyhow::anyhow!(format!("Failed to concatenate dataframes: {}", e)))?
            .collect()
            .map_err(|e| {
                anyhow::anyhow!(format!("Failed to collect concatenated result: {}", e))
            })?;

        let mut executor = Executor::new(config);
        executor
            .execute_filter_on_value(&filter, Value::DataFrame(combined_df), output_path)
            .await
            .map_err(|e| e.into())
    }
}

async fn handle_command(command: Commands, config: &Config) -> Result<()> {
    match command {
        Commands::Convert {
            input,
            output,
            input_format,
            output_format,
            overwrite,
        } => {
            convert_file(
                &input,
                &output,
                input_format,
                output_format,
                overwrite,
                config,
            )
            .await
        }
        Commands::Inspect {
            file,
            schema,
            sample,
            stats,
        } => inspect_file(&file, schema, sample, stats, config).await,
        Commands::Validate {
            files,
            schema,
            check_duplicates,
            check_nulls,
        } => {
            validate_files(
                &files,
                schema.as_deref(),
                check_duplicates,
                check_nulls,
                config,
            )
            .await
        }
        Commands::Merge {
            inputs,
            output,
            method,
            on,
            join_type,
        } => merge_files(&inputs, &output, method, &on, join_type, config).await,
        Commands::Completions { shell } => generate_completions(shell),
        Commands::Config { command } => handle_config_command(command, config),
    }
}

fn handle_config_command(command: ConfigCommands, config: &Config) -> Result<()> {
    match command {
        ConfigCommands::Show => {
            let yaml = serde_yaml::to_string(config)
                .map_err(|e| anyhow::anyhow!(format!("Failed to serialize config: {}", e)))?;
            println!("{}", yaml);
            Ok(())
        }
        ConfigCommands::List => {
            println!("Available configuration keys:\n");
            println!("Filter options:");
            println!("  filter.lazy_evaluation          - Enable lazy evaluation (true/false)");
            println!(
                "  filter.dataframe_optimizations  - Enable DataFrame optimizations (true/false)"
            );
            println!("\nPerformance options:");
            println!("  performance.batch_size           - Batch size for processing (number)");
            println!("  performance.threads              - Number of threads (number, 0=auto)");
            println!("\nFormat options:");
            println!("  formats.csv.separator            - CSV field separator (single character)");
            println!("  formats.csv.has_header           - CSV has header row (true/false)");
            println!("\nExample usage:");
            println!("  dsq config get filter.lazy_evaluation");
            println!("  dsq config set filter.lazy_evaluation false");
            Ok(())
        }
        ConfigCommands::Edit { path } => {
            let config_path = if let Some(p) = path {
                p
            } else {
                // Use default config path
                let home = std::env::var("HOME")
                    .or_else(|_| std::env::var("USERPROFILE"))
                    .map_err(|_| anyhow::anyhow!("Cannot determine home directory"))?;
                PathBuf::from(home).join(".config/dsq/dsq.toml")
            };

            // Create config file if it doesn't exist
            if !config_path.exists() {
                if let Some(parent) = config_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                create_default_config_file(&config_path)?;
                println!("Created config file: {}", config_path.display());
            }

            // Get editor from environment
            let editor = std::env::var("EDITOR")
                .or_else(|_| std::env::var("VISUAL"))
                .unwrap_or_else(|_| {
                    if cfg!(windows) {
                        "notepad".to_string()
                    } else {
                        "vi".to_string()
                    }
                });

            // Launch editor
            let status = process::Command::new(&editor)
                .arg(&config_path)
                .status()
                .map_err(|e| {
                    anyhow::anyhow!(format!("Failed to launch editor '{}': {}", editor, e))
                })?;

            if !status.success() {
                return Err(anyhow::anyhow!("Editor exited with error"));
            }

            // Validate the edited config
            match Config::load_from_file(&config_path) {
                Ok(edited_config) => {
                    validate_config(&edited_config)?;
                    println!("Configuration updated and validated successfully");
                    Ok(())
                }
                Err(e) => {
                    eprintln!("Warning: Configuration file has errors: {}", e);
                    eprintln!("Please fix the errors or restore from backup");
                    Err(e.into())
                }
            }
        }
        ConfigCommands::Init { path, force } => {
            if path.exists() && !force {
                return Err(anyhow::anyhow!(format!(
                    "Config file already exists: {}\n\n\
                    Use --force to overwrite:\n  \
                    dsq config init {} --force",
                    path.display(),
                    path.display()
                )));
            }
            create_default_config_file(&path)?;
            println!("Created config file: {}", path.display());
            Ok(())
        }
        ConfigCommands::Check { path } => {
            let check_config = Config::load_from_file(&path)?;
            validate_config(&check_config)?;
            println!("✓ Config file is valid: {}", path.display());
            Ok(())
        }
        ConfigCommands::Get { key } => {
            let value = get_config_value(config, &key)?;
            println!("{}", value);
            Ok(())
        }
        ConfigCommands::Set {
            key,
            value,
            config: config_path,
        } => {
            let path = if let Some(ref p) = config_path {
                p.clone()
            } else {
                // Use default config path
                let home = std::env::var("HOME")
                    .or_else(|_| std::env::var("USERPROFILE"))
                    .map_err(|_| anyhow::anyhow!("Cannot determine home directory"))?;
                PathBuf::from(home).join(".config/dsq/dsq.toml")
            };

            let mut update_config = if path.exists() {
                Config::load_from_file(&path)?
            } else {
                Config::default()
            };

            set_config_value(&mut update_config, &key, &value)?;

            // Ensure parent directory exists
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }

            update_config.save(&path)?;
            println!("Set {} = {} in {}", key, value, path.display());
            Ok(())
        }
        ConfigCommands::Reset {
            key,
            config: config_path,
        } => {
            let path = if let Some(ref p) = config_path {
                p.clone()
            } else {
                // Use default config path
                let home = std::env::var("HOME")
                    .or_else(|_| std::env::var("USERPROFILE"))
                    .map_err(|_| anyhow::anyhow!("Cannot determine home directory"))?;
                PathBuf::from(home).join(".config/dsq/dsq.toml")
            };

            if !path.exists() {
                return Err(anyhow::anyhow!(format!(
                    "Config file not found: {}\n\n\
                    Create one with:\n  \
                    dsq config init",
                    path.display()
                )));
            }

            let mut update_config = Config::load_from_file(&path)?;
            let default_config = Config::default();

            // Reset to default value
            let default_value = get_config_value(&default_config, &key)?;
            set_config_value(&mut update_config, &key, &default_value)?;

            update_config.save(&path)?;
            println!("Reset {} to default value: {}", key, default_value);
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
        .ok_or_else(|| {
            anyhow::anyhow!(format!(
                "Cannot determine input format for '{}'.\n\n\
                Try specifying the format explicitly:\n  \
                dsq convert {} {} --input-format csv",
                input.display(),
                input.display(),
                output.display()
            ))
        })?;

    let _out_format = output_format
        .or_else(|| DataFormat::from_path(output).ok())
        .ok_or_else(|| {
            anyhow::anyhow!(format!(
                "Cannot determine output format for '{}'.\n\n\
                Try specifying the format explicitly:\n  \
                dsq convert {} {} --output-format parquet",
                output.display(),
                input.display(),
                output.display()
            ))
        })?;

    if output.exists() && !overwrite {
        return Err(anyhow::anyhow!(format!(
            "Output file already exists: {}\n\n\
            Use --overwrite to replace it:\n  \
            dsq convert {} {} --overwrite",
            output.display(),
            input.display(),
            output.display()
        )));
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
    let format =
        DataFormat::from_path(file).map_err(|_| anyhow::anyhow!("Cannot determine file format"))?;

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
                // Note: describe() method is not available in newer polars versions
                // Consider implementing custom statistics if needed
                println!("Statistics display is currently unavailable");
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
            .map_err(|_| anyhow::anyhow!("Cannot determine file format"))?;

        let read_options = config.to_read_options();
        let data = read_file(file, &read_options).await?;

        match &data {
            Value::DataFrame(df) => {
                // Check schema
                if let Some(ref expected) = expected_schema {
                    if !schemas_match(df.schema(), expected) {
                        eprintln!("  ❌ Schema mismatch");
                        continue;
                    }
                }

                // Check duplicates
                if check_duplicates {
                    // Check for duplicate rows
                    let duplicate_mask = df.is_duplicated().map_err(|e| {
                        anyhow::anyhow!(format!("Failed to check for duplicates: {}", e))
                    })?;
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
                            eprintln!(
                                "  ⚠️  Column '{}' has {} null values",
                                col.name(),
                                null_count
                            );
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
    repl.run().await.map_err(|e| e.into())
}

fn test_filter(cli_config: &CliConfig, config: &Config) -> Result<()> {
    // Get the filter
    let filter = if let Some(filter_file) = &cli_config.filter_file {
        fs::read_to_string(filter_file)
            .map_err(|e| {
                anyhow::anyhow!(format!(
                    "Failed to read filter file {}: {}",
                    filter_file.display(),
                    e
                ))
            })?
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

    env_logger::Builder::new().filter_level(log_level).init();
}

fn get_config_value(config: &Config, key: &str) -> Result<String> {
    let value = match key {
        "filter.lazy_evaluation" => config.filter.lazy_evaluation.to_string(),
        "filter.dataframe_optimizations" => config.filter.dataframe_optimizations.to_string(),
        "performance.batch_size" => config.performance.batch_size.to_string(),
        "performance.threads" => config.performance.threads.to_string(),
        "formats.csv.separator" => config.formats.csv.separator.clone(),
        "formats.csv.has_header" => config.formats.csv.has_header.to_string(),
        _ => return Err(anyhow::anyhow!(format!("Unknown config key: {}", key))),
    };
    Ok(value)
}

fn set_config_value(config: &mut Config, key: &str, value: &str) -> Result<()> {
    match key {
        "filter.lazy_evaluation" => {
            config.filter.lazy_evaluation = value
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid boolean value".to_string()))?;
        }
        "filter.dataframe_optimizations" => {
            config.filter.dataframe_optimizations = value
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid boolean value".to_string()))?;
        }
        "performance.batch_size" => {
            config.performance.batch_size = value
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid batch size".to_string()))?;
        }
        "performance.threads" => {
            config.performance.threads = value
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid thread count".to_string()))?;
        }
        "formats.csv.separator" => {
            if value.len() != 1 {
                return Err(anyhow::anyhow!(
                    "CSV separator must be a single character".to_string()
                ));
            }
            config.formats.csv.separator = value.to_string();
        }
        "formats.csv.has_header" => {
            config.formats.csv.has_header = value
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid boolean value".to_string()))?;
        }
        _ => return Err(anyhow::anyhow!(format!("Unknown config key: {}", key))),
    }

    Ok(())
}
fn load_schema(path: &Path) -> Result<polars::prelude::Schema> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!(format!("Failed to read schema file: {}", e)))?;

    let schema_map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&content)
        .map_err(|e| anyhow::anyhow!(format!("Invalid schema JSON: {}", e)))?;

    let mut fields = Vec::new();
    for (name, value) in schema_map {
        let dtype_str = value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!(format!("dtype for {} must be string", name)))?;
        let dtype = parse_dtype(dtype_str)?;
        fields.push((name.into(), dtype));
    }

    Ok(polars::prelude::Schema::from_iter(fields))
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
        "str" | "string" | "utf8" => DataType::String,
        "date" => DataType::Date,
        "datetime" => DataType::Datetime(polars::prelude::TimeUnit::Microseconds, None),
        "time" => DataType::Time,
        _ => return Err(anyhow::anyhow!(format!("Unknown data type: {}", dtype_str))),
    })
}

fn handle_example_directory(dir_path: &Path) -> Result<(String, Vec<PathBuf>)> {
    use std::fs;

    // Check if query.dsq exists
    let query_path = dir_path.join("query.dsq");
    if !query_path.exists() {
        return Err(anyhow::anyhow!(format!(
            "query.dsq not found in {}",
            dir_path.display()
        )));
    }

    // Read the filter from query.dsq
    let filter = fs::read_to_string(&query_path)
        .map_err(|e| anyhow::anyhow!(format!("Failed to read query.dsq: {}", e)))?
        .trim()
        .to_string();

    // Find data files (data.json, data.csv, etc.)
    let mut data_files = Vec::new();
    for entry in fs::read_dir(dir_path).map_err(|e| {
        anyhow::anyhow!(format!(
            "Failed to read directory {}: {}",
            dir_path.display(),
            e
        ))
    })? {
        let entry =
            entry.map_err(|e| anyhow::anyhow!(format!("Failed to read directory entry: {}", e)))?;
        let path = entry.path();
        if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.starts_with("data.")
                    && (file_name.ends_with(".json")
                        || file_name.ends_with(".csv")
                        || file_name.ends_with(".tsv")
                        || file_name.ends_with(".parquet")
                        || file_name.ends_with(".jsonl")
                        || file_name.ends_with(".ndjson"))
                {
                    data_files.push(path);
                }
            }
        }
    }

    if data_files.is_empty() {
        return Err(anyhow::anyhow!(format!(
            "No data files found in {}",
            dir_path.display()
        )));
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
        return Err(anyhow::anyhow!("No input files provided"));
    }

    if inputs.len() == 1 {
        return Err(anyhow::anyhow!(
            "At least two input files required for merge"
        ));
    }

    // Read all input files
    let read_options = config.to_read_options();
    let mut dataframes = Vec::new();

    for input in inputs {
        let value = dsq_core::io::read_file(input, &read_options).await?;
        let df = match value {
            Value::DataFrame(df) => df,
            Value::LazyFrame(lf) => lf
                .collect()
                .map_err(|e| anyhow::anyhow!(format!("Failed to collect lazy frame: {}", e)))?,
            _ => {
                return Err(anyhow::anyhow!(format!(
                    "Input file {} does not contain tabular data",
                    input.display()
                )));
            }
        };
        dataframes.push(df);
    }

    let result_df = match method {
        cli::MergeMethod::Concat => {
            // Concatenate all dataframes vertically
            let lazy_frames: Vec<_> = dataframes.iter().map(|df| df.clone().lazy()).collect();
            concat(&lazy_frames, UnionArgs::default())
                .map_err(|e| anyhow::anyhow!(format!("Failed to concatenate dataframes: {}", e)))?
                .collect()
                .map_err(|e| {
                    anyhow::anyhow!(format!("Failed to collect concatenated result: {}", e))
                })?
        }
        cli::MergeMethod::Join => {
            if on.is_empty() {
                return Err(anyhow::anyhow!(
                    "Join method requires 'on' parameter specifying join columns"
                ));
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
                    .map_err(|e| anyhow::anyhow!(format!("Failed to join dataframes: {}", e)))?;
            }

            match result {
                Value::DataFrame(df) => df,
                _ => return Err(anyhow::anyhow!("Join result is not a DataFrame")),
            }
        }
    };

    // Write the result
    let write_options = config.to_write_options();
    dsq_core::io::write_file(&Value::DataFrame(result_df), output, &write_options).await?;

    println!(
        "Successfully merged {} files into {}",
        inputs.len(),
        output.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("query.dsq not found"));
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No data files found"));
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
        fs::write(
            dir_path.join("data.jsonl"),
            "{\"id\":2,\"name\":\"Charlie\"}",
        )
        .unwrap();
        fs::write(
            dir_path.join("data.ndjson"),
            "{\"id\":3,\"name\":\"David\"}",
        )
        .unwrap();
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

    #[test]
    fn test_parse_dtype_valid() {
        use polars::prelude::DataType;

        assert_eq!(parse_dtype("bool").unwrap(), DataType::Boolean);
        assert_eq!(parse_dtype("boolean").unwrap(), DataType::Boolean);
        assert_eq!(parse_dtype("i8").unwrap(), DataType::Int8);
        assert_eq!(parse_dtype("int8").unwrap(), DataType::Int8);
        assert_eq!(parse_dtype("i32").unwrap(), DataType::Int32);
        assert_eq!(parse_dtype("f64").unwrap(), DataType::Float64);
        assert_eq!(parse_dtype("str").unwrap(), DataType::String);
        assert_eq!(parse_dtype("string").unwrap(), DataType::String);
        assert_eq!(parse_dtype("date").unwrap(), DataType::Date);
    }

    #[test]
    fn test_parse_dtype_invalid() {
        let result = parse_dtype("invalid_type");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown data type"));
    }

    #[test]
    fn test_parse_dtype_case_insensitive() {
        use polars::prelude::DataType;

        assert_eq!(parse_dtype("BOOL").unwrap(), DataType::Boolean);
        assert_eq!(parse_dtype("String").unwrap(), DataType::String);
    }

    #[test]
    fn test_schemas_match_identical() {
        use polars::prelude::{DataType, Schema};

        let schema1 = Schema::from_iter(vec![
            ("col1".into(), DataType::Int32),
            ("col2".into(), DataType::String),
        ]);
        let schema2 = Schema::from_iter(vec![
            ("col1".into(), DataType::Int32),
            ("col2".into(), DataType::String),
        ]);

        assert!(schemas_match(&schema1, &schema2));
    }

    #[test]
    fn test_schemas_match_different_lengths() {
        use polars::prelude::{DataType, Schema};

        let schema1 = Schema::from_iter(vec![("col1".into(), DataType::Int32)]);
        let schema2 = Schema::from_iter(vec![
            ("col1".into(), DataType::Int32),
            ("col2".into(), DataType::String),
        ]);

        assert!(!schemas_match(&schema1, &schema2));
    }

    #[test]
    fn test_schemas_match_different_types() {
        use polars::prelude::{DataType, Schema};

        let schema1 = Schema::from_iter(vec![("col1".into(), DataType::Int32)]);
        let schema2 = Schema::from_iter(vec![("col1".into(), DataType::String)]);

        assert!(!schemas_match(&schema1, &schema2));
    }

    #[test]
    fn test_schemas_match_missing_column() {
        use polars::prelude::{DataType, Schema};

        let schema1 = Schema::from_iter(vec![
            ("col1".into(), DataType::Int32),
            ("col2".into(), DataType::String),
        ]);
        let schema2 = Schema::from_iter(vec![
            ("col1".into(), DataType::Int32),
            ("col3".into(), DataType::String),
        ]);

        assert!(!schemas_match(&schema1, &schema2));
    }

    #[test]
    fn test_get_config_value_valid_keys() {
        use crate::config::Config;

        let config = Config::default();

        assert!(get_config_value(&config, "filter.lazy_evaluation").is_ok());
        assert!(get_config_value(&config, "performance.batch_size").is_ok());
        assert!(get_config_value(&config, "formats.csv.separator").is_ok());
    }

    #[test]
    fn test_get_config_value_invalid_key() {
        use crate::config::Config;

        let config = Config::default();
        let result = get_config_value(&config, "invalid.key");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown config key"));
    }

    #[test]
    fn test_set_config_value_valid() {
        use crate::config::Config;

        let mut config = Config::default();

        assert!(set_config_value(&mut config, "filter.lazy_evaluation", "false").is_ok());
        assert!(!config.filter.lazy_evaluation);

        assert!(set_config_value(&mut config, "performance.batch_size", "100").is_ok());
        assert_eq!(config.performance.batch_size, 100);

        assert!(set_config_value(&mut config, "formats.csv.separator", ",").is_ok());
        assert_eq!(config.formats.csv.separator, ",");
    }

    #[test]
    fn test_set_config_value_invalid_key() {
        use crate::config::Config;

        let mut config = Config::default();
        let result = set_config_value(&mut config, "invalid.key", "value");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown config key"));
    }

    #[test]
    fn test_set_config_value_invalid_value() {
        use crate::config::Config;

        let mut config = Config::default();

        let result = set_config_value(&mut config, "filter.lazy_evaluation", "not_bool");
        assert!(result.is_err());

        let result = set_config_value(&mut config, "performance.batch_size", "not_number");
        assert!(result.is_err());

        let result = set_config_value(&mut config, "formats.csv.separator", "too_long");
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_example_directory_empty_query() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        // Create empty query.dsq
        fs::write(dir_path.join("query.dsq"), "").unwrap();
        fs::write(dir_path.join("data.csv"), "id,name\n1,Alice").unwrap();

        let result = handle_example_directory(dir_path);
        assert!(result.is_ok());
        let (filter, files) = result.unwrap();
        assert_eq!(filter, "");
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn test_handle_example_directory_unsupported_files() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        // Create query.dsq
        fs::write(dir_path.join("query.dsq"), ".[]").unwrap();

        // Create unsupported data files
        fs::write(dir_path.join("data.txt"), "some text").unwrap();
        fs::write(dir_path.join("data.xml"), "<xml></xml>").unwrap();

        let result = handle_example_directory(dir_path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No data files found"));
    }

    #[test]
    fn test_handle_example_directory_mixed_files() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        // Create query.dsq
        fs::write(dir_path.join("query.dsq"), ".[]").unwrap();

        // Create mix of supported and unsupported
        fs::write(dir_path.join("data.csv"), "id,name\n1,Alice").unwrap();
        fs::write(dir_path.join("data.txt"), "ignored").unwrap();
        fs::write(dir_path.join("data.json"), "{\"id\":1}").unwrap();

        let result = handle_example_directory(dir_path);
        assert!(result.is_ok());
        let (_filter, files) = result.unwrap();
        assert_eq!(files.len(), 2); // Only csv and json
        assert!(files.iter().any(|p| p.ends_with("data.csv")));
        assert!(files.iter().any(|p| p.ends_with("data.json")));
    }

    #[test]
    fn test_load_schema_valid() {
        let temp_dir = TempDir::new().unwrap();
        let schema_path = temp_dir.path().join("schema.json");

        let schema_content = r#"{
            "name": "string",
            "age": "i32",
            "active": "bool"
        }"#;
        fs::write(&schema_path, schema_content).unwrap();

        let result = load_schema(&schema_path);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema.get("name").unwrap().to_string(), "str");
        assert_eq!(schema.get("age").unwrap().to_string(), "i32");
        assert_eq!(schema.get("active").unwrap().to_string(), "bool");
    }

    #[test]
    fn test_load_schema_invalid_json() {
        let temp_dir = TempDir::new().unwrap();
        let schema_path = temp_dir.path().join("schema.json");

        let invalid_json = r#"{
            "name": "string",
            "age": "i32",
            invalid json here
        }"#;
        fs::write(&schema_path, invalid_json).unwrap();

        let result = load_schema(&schema_path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid schema JSON"));
    }

    #[test]
    fn test_load_schema_invalid_dtype() {
        let temp_dir = TempDir::new().unwrap();
        let schema_path = temp_dir.path().join("schema.json");

        let schema_content = r#"{
            "name": "invalid_type"
        }"#;
        fs::write(&schema_path, schema_content).unwrap();

        let result = load_schema(&schema_path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown data type"));
    }

    #[test]
    fn test_load_schema_file_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let schema_path = temp_dir.path().join("nonexistent.json");

        let result = load_schema(&schema_path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Failed to read schema file"));
    }

    #[test]
    fn test_load_schema_non_string_dtype() {
        let temp_dir = TempDir::new().unwrap();
        let schema_path = temp_dir.path().join("schema.json");

        let schema_content = r#"{
            "name": 123
        }"#;
        fs::write(&schema_path, schema_content).unwrap();

        let result = load_schema(&schema_path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("dtype for name must be string"));
    }

    #[test]
    fn test_handle_config_command_show() {
        use crate::cli::ConfigCommands;
        use crate::config::Config;

        let config = Config::default();
        let result = handle_config_command(ConfigCommands::Show, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_config_command_list() {
        use crate::cli::ConfigCommands;
        use crate::config::Config;

        let config = Config::default();
        let result = handle_config_command(ConfigCommands::List, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_config_command_get_valid_key() {
        use crate::cli::ConfigCommands;
        use crate::config::Config;

        let config = Config::default();
        let result = handle_config_command(
            ConfigCommands::Get {
                key: "filter.lazy_evaluation".to_string(),
            },
            &config,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_config_command_get_invalid_key() {
        use crate::cli::ConfigCommands;
        use crate::config::Config;

        let config = Config::default();
        let result = handle_config_command(
            ConfigCommands::Get {
                key: "invalid.key".to_string(),
            },
            &config,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_config_command_set_valid() {
        use crate::cli::ConfigCommands;
        use crate::config::Config;

        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test_config.toml");

        let config = Config::default();
        let result = handle_config_command(
            ConfigCommands::Set {
                key: "filter.lazy_evaluation".to_string(),
                value: "false".to_string(),
                config: Some(config_path.clone()),
            },
            &config,
        );
        assert!(result.is_ok());
        assert!(config_path.exists());
    }

    #[test]
    fn test_handle_config_command_set_invalid_key() {
        use crate::cli::ConfigCommands;
        use crate::config::Config;

        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test_config.toml");

        let config = Config::default();
        let result = handle_config_command(
            ConfigCommands::Set {
                key: "invalid.key".to_string(),
                value: "value".to_string(),
                config: Some(config_path),
            },
            &config,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_config_command_reset_valid() {
        use crate::cli::ConfigCommands;
        use crate::config::Config;

        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("test_config.toml");

        // First set a value
        let config = Config::default();
        handle_config_command(
            ConfigCommands::Set {
                key: "filter.lazy_evaluation".to_string(),
                value: "false".to_string(),
                config: Some(config_path.clone()),
            },
            &config,
        )
        .unwrap();

        // Then reset it
        let result = handle_config_command(
            ConfigCommands::Reset {
                key: "filter.lazy_evaluation".to_string(),
                config: Some(config_path),
            },
            &config,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_config_command_init() {
        use crate::cli::ConfigCommands;
        use crate::config::Config;

        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("new_config.toml");

        let config = Config::default();
        let result = handle_config_command(
            ConfigCommands::Init {
                path: config_path.clone(),
                force: false,
            },
            &config,
        );
        assert!(result.is_ok());
        assert!(config_path.exists());
    }

    #[test]
    fn test_handle_config_command_check_valid() {
        use crate::cli::ConfigCommands;
        use crate::config::Config;

        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("valid_config.toml");

        // Create a valid config
        handle_config_command(
            ConfigCommands::Init {
                path: config_path.clone(),
                force: false,
            },
            &Config::default(),
        )
        .unwrap();

        let config = Config::default();
        let result = handle_config_command(ConfigCommands::Check { path: config_path }, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_generate_completions_bash() {
        use clap_complete::Shell;

        let result = generate_completions(Shell::Bash);
        assert!(result.is_ok());
    }

    #[test]
    fn test_generate_completions_zsh() {
        use clap_complete::Shell;

        let result = generate_completions(Shell::Zsh);
        assert!(result.is_ok());
    }

    #[test]
    fn test_generate_completions_fish() {
        use clap_complete::Shell;

        let result = generate_completions(Shell::Fish);
        assert!(result.is_ok());
    }

    #[test]
    fn test_test_filter_valid() {
        use crate::cli::CliConfig;
        use crate::config::Config;

        let cli_config = CliConfig {
            filter: Some(".name".to_string()),
            ..Default::default()
        };
        let config = Config::default();

        let result = test_filter(&cli_config, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_test_filter_from_file() {
        use crate::cli::CliConfig;
        use crate::config::Config;

        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("filter.dsq");
        fs::write(&filter_path, ".[]").unwrap();

        let cli_config = CliConfig {
            filter_file: Some(filter_path),
            ..Default::default()
        };
        let config = Config::default();

        let result = test_filter(&cli_config, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_setup_logging() {
        use crate::config::Config;

        let mut config = Config::default();
        config.debug.verbosity = 1;

        // This should not panic
        setup_logging(&config);
    }

    #[test]
    fn test_print_version() {
        // This should not panic
        print_version();
    }
}
