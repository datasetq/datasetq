# API Usage

This guide shows how to use dsq as a Rust library in your applications.

## Getting Started

Add dsq-core to your `Cargo.toml`:

```toml
[dependencies]
dsq-core = "0.1"  # Check crates.io for latest version

# Optional: enable specific features
dsq-core = { version = "0.1", features = ["all-formats", "filter", "io"] }
```

## Basic Usage

### Reading and Writing Files

```rust
use dsq_core::io;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read from CSV
    let data = io::read_file("data.csv", &io::ReadOptions::default())?;

    // Write to Parquet
    io::write_file(&data, "output.parquet", &io::WriteOptions::default())?;

    Ok(())
}
```

### Using the Fluent API

```rust
use dsq_core::api::Dsq;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Chain operations
    let result = Dsq::from_file("data.csv")?
        .select(&["name", "age", "city"])
        .filter_expr("age > 25")?
        .sort_by(&["age"])?
        .head(10)?
        .to_json()?;

    println!("{}", result);
    Ok(())
}
```

## Data Operations

### Basic Operations

```rust
use dsq_core::{Value, ops};

// Filter data
let filtered = ops::basic::filter(&data, "age > 30")?;

// Select columns
let selected = ops::basic::select(&data, &["name", "email"])?;

// Sort data
let sorted = ops::basic::sort_by(
    &data,
    vec![ops::SortOptions::asc("name".to_string())]
)?;

// Get first N rows
let first_10 = ops::basic::head(&data, 10)?;

// Get unique values
let unique = ops::basic::unique(&data, &["category"])?;
```

### Aggregation Operations

```rust
use dsq_core::ops::aggregate::*;

// Group by and aggregate
let aggregated = group_by_agg(
    &data,
    &["department".to_string()],
    &[
        AggregationFunction::Count,
        AggregationFunction::Mean("salary".to_string()),
        AggregationFunction::Sum("bonus".to_string()),
        AggregationFunction::Max("age".to_string()),
    ]
)?;

// Pivot table
let pivoted = pivot(
    &data,
    "date",
    "category",
    "amount",
    AggregationFunction::Sum("amount".to_string())
)?;

// Unpivot (melt)
let melted = melt(
    &data,
    &["id".to_string(), "name".to_string()],
    &["score1".to_string(), "score2".to_string(), "score3".to_string()]
)?;
```

### Join Operations

```rust
use dsq_core::ops::join::*;

// Inner join
let joined = join(
    &left_data,
    &right_data,
    &JoinKeys::on(vec!["id".to_string()]),
    &JoinOptions {
        join_type: JoinType::Inner,
        ..Default::default()
    }
)?;

// Left join with suffix
let left_joined = join(
    &users,
    &orders,
    &JoinKeys::on(vec!["user_id".to_string()]),
    &JoinOptions {
        join_type: JoinType::Left,
        suffix: Some("_order"),
        ..Default::default()
    }
)?;

// Multiple key join
let multi_join = join(
    &table1,
    &table2,
    &JoinKeys::on(vec!["key1".to_string(), "key2".to_string()]),
    &JoinOptions::default()
)?;
```

### Transform Operations

```rust
use dsq_core::ops::transform::*;

// Transpose
let transposed = transpose(&data)?;

// Cast column types
let casted = cast(&data, "age", DataType::Int64)?;

// Reshape
let reshaped = reshape(&data, &ReshapeOptions {
    index_cols: vec!["id".to_string()],
    value_cols: vec!["value".to_string()],
    ..Default::default()
})?;
```

## Filter Execution

### Basic Filter Execution

```rust
use dsq_core::filter::{FilterExecutor, ExecutorConfig};

// Create executor with default config
let mut executor = FilterExecutor::new();

// Execute filter string
let result = executor.execute_str(
    r#"map(select(.age > 30)) | sort_by(.name)"#,
    data
)?;
```

### Advanced Filter Execution

```rust
use dsq_core::filter::{FilterExecutor, ExecutorConfig, OptimizationLevel};

// Configure executor
let config = ExecutorConfig {
    lazy_evaluation: true,
    dataframe_optimizations: true,
    optimization_level: OptimizationLevel::Advanced,
    collect_stats: true,
    max_recursion_depth: 100,
    ..Default::default()
};

let mut executor = FilterExecutor::with_config(config);

// Execute and get stats
let result = executor.execute_str(
    r#"group_by(.category) | map({cat: .[0].category, total: (map(.amount) | add)})"#,
    data
)?;

if let Some(stats) = result.stats {
    println!("Execution time: {:?}", stats.execution_time);
    println!("Rows processed: {}", stats.rows_processed);
}
```

### Compiling Filters

```rust
use dsq_core::filter::FilterCompiler;

// Compile once, execute many times
let compiler = FilterCompiler::new();
let compiled = compiler.compile_str(r#"map(select(.active))"#)?;

// Execute on multiple datasets
for dataset in datasets {
    let result = executor.execute_compiled(&compiled, dataset)?;
    // ... process result
}
```

## Format-Specific Operations

### CSV Options

```rust
use dsq_core::io::{ReadOptions, FormatReadOptions};

let read_opts = ReadOptions {
    format_options: Some(FormatReadOptions::Csv {
        separator: b',',
        has_header: true,
        quote_char: Some(b'"'),
        comment_char: Some(b'#'),
        null_values: Some(vec!["NULL".to_string(), "".to_string()]),
        encoding: CsvEncoding::Utf8,
        trim_whitespace: true,
        infer_schema_length: Some(1000),
        skip_rows: 0,
        skip_rows_after_header: 0,
    }),
    ..Default::default()
};

let data = io::read_file("data.csv", &read_opts)?;
```

### Parquet Options

```rust
use dsq_core::io::{WriteOptions, FormatWriteOptions, ParquetCompression};

let write_opts = WriteOptions {
    format_options: Some(FormatWriteOptions::Parquet {
        compression: ParquetCompression::Snappy,
        statistics: true,
        row_group_size: Some(50000),
        use_dictionary: true,
    }),
    overwrite: true,
    ..Default::default()
};

io::write_file(&data, "output.parquet", &write_opts)?;
```

### Lazy Evaluation

```rust
use dsq_core::io::ReadOptions;

// Enable lazy reading
let read_opts = ReadOptions {
    lazy: true,
    max_rows: Some(1000),
    columns: Some(vec!["id".to_string(), "name".to_string()]),
    ..Default::default()
};

let lazy_data = io::read_file("large.parquet", &read_opts)?;
```

## Working with Values

### Creating Values

```rust
use dsq_core::Value;

// From JSON
let value: Value = serde_json::from_str(r#"{"name": "Alice", "age": 30}"#)?;

// From DataFrame
let df_value = Value::DataFrame(dataframe);

// From array
let arr_value = Value::Array(vec![
    Value::String("a".to_string()),
    Value::Number(42.into()),
]);
```

### Converting Values

```rust
// To JSON
let json_string = serde_json::to_string(&value)?;

// To DataFrame
if let Value::DataFrame(df) = value {
    // work with DataFrame
}

// Extract array
if let Value::Array(arr) = value {
    for item in arr {
        // process items
    }
}
```

## Pipeline Operations

### Building Pipelines

```rust
use dsq_core::ops::OperationPipeline;

let result = OperationPipeline::new()
    .select(vec!["name".to_string(), "age".to_string(), "city".to_string()])
    .filter("age > 25")?
    .sort_by(vec![ops::SortOptions::desc("age".to_string())])
    .head(10)
    .execute(data)?;
```

### Conditional Pipelines

```rust
let mut pipeline = OperationPipeline::new();

if filter_active {
    pipeline = pipeline.filter("status == 'active'")?;
}

if sort_by_date {
    pipeline = pipeline.sort_by(vec![ops::SortOptions::desc("date".to_string())]);
}

let result = pipeline.execute(data)?;
```

## Error Handling

```rust
use dsq_core::error::{Error, TypeError, FilterError};

match executor.execute_str(filter, data) {
    Ok(result) => println!("Success: {:?}", result),
    Err(Error::Filter(FilterError::SyntaxError { message, position })) => {
        eprintln!("Syntax error at {}: {}", position, message);
    }
    Err(Error::Type(TypeError::IncompatibleTypes { expected, got })) => {
        eprintln!("Type error: expected {}, got {}", expected, got);
    }
    Err(e) => eprintln!("Error: {}", e),
}
```

## Complete Examples

### CSV to Parquet Converter

```rust
use dsq_core::io;

fn convert_csv_to_parquet(
    input: &str,
    output: &str
) -> Result<(), Box<dyn std::error::Error>> {
    let data = io::read_file(input, &io::ReadOptions::default())?;
    io::write_file(&data, output, &io::WriteOptions::default())?;
    println!("Converted {} to {}", input, output);
    Ok(())
}
```

### Data Aggregation Tool

```rust
use dsq_core::{api::Dsq, ops::aggregate::AggregationFunction};

fn sales_summary(file: &str) -> Result<String, Box<dyn std::error::Error>> {
    let result = Dsq::from_file(file)?
        .group_by(&["region"])?
        .aggregate(
            &["region"],
            vec![
                AggregationFunction::Count,
                AggregationFunction::Sum("sales".to_string()),
                AggregationFunction::Mean("sales".to_string()),
            ]
        )?
        .sort_by(&["sum_sales"])?
        .to_json()?;

    Ok(result)
}
```

### Batch Processing

```rust
use dsq_core::io;
use std::path::Path;

fn process_directory(
    dir: &Path,
    filter: &str
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().map_or(false, |e| e == "csv") {
            let data = io::read_file(
                path.to_str().unwrap(),
                &io::ReadOptions::default()
            )?;

            let mut executor = FilterExecutor::new();
            let result = executor.execute_str(filter, data)?;

            let output = path.with_extension("parquet");
            io::write_file(&result.value, output.to_str().unwrap(), &io::WriteOptions::default())?;
        }
    }
    Ok(())
}
```

### Streaming Large Files

```rust
use dsq_core::io::stream::StreamProcessor;

fn process_large_file(
    input: &str,
    output: &str
) -> Result<(), Box<dyn std::error::Error>> {
    let processor = StreamProcessor::new(10000) // 10k row chunks
        .with_read_options(io::ReadOptions::default())
        .with_write_options(io::WriteOptions::default());

    processor.process_file(input, output, |chunk| {
        // Process each chunk
        let filtered = ops::basic::filter(&chunk, "amount > 1000")?;
        Ok(Some(filtered))
    })?;

    Ok(())
}
```

## Performance Tips

1. **Use lazy evaluation** for large datasets:
   ```rust
   let opts = ReadOptions { lazy: true, ..Default::default() };
   ```

2. **Select columns early**:
   ```rust
   Dsq::from_file("data.csv")?
       .select(&["needed", "columns"])?
       .filter_expr("...")?  // Filters only selected columns
   ```

3. **Use appropriate formats**:
   - Parquet for analytics
   - JSON Lines for streaming
   - Arrow for interchange

4. **Enable optimizations**:
   ```rust
   let config = ExecutorConfig {
       dataframe_optimizations: true,
       optimization_level: OptimizationLevel::Advanced,
       ..Default::default()
   };
   ```

5. **Batch operations**:
   ```rust
   let pipeline = OperationPipeline::new()
       .select(cols)
       .filter(expr)?
       .sort_by(sort_opts);
   let result = pipeline.execute(data)?;  // Execute once
   ```

## Thread Safety

dsq-core operations are thread-safe. You can process multiple files in parallel:

```rust
use rayon::prelude::*;

files.par_iter().for_each(|file| {
    let data = io::read_file(file, &ReadOptions::default()).unwrap();
    // ... process data
});
```

## Feature Flags

Enable specific features in `Cargo.toml`:

```toml
[dependencies]
dsq-core = { version = "0.1", features = [
    "csv",      # CSV support
    "json",     # JSON support
    "parquet",  # Parquet support
    "avro",     # Avro support
    "filter",   # Filter engine
    "io",       # I/O operations
] }
```

For all features:
```toml
dsq-core = { version = "0.1", features = ["default"] }
```
