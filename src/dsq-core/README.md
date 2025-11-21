# dsq-core

![Build status](https://github.com/durableprogramming/dsq/actions/workflows/check.yml/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/dsq-core.svg)](https://crates.io/crates/dsq-core)
[![Documentation](https://docs.rs/dsq-core/badge.svg)](https://docs.rs/dsq-core)
[![Rust 1.69+](https://img.shields.io/badge/rust-1.69+-orange.svg)](https://www.rust-lang.org)

Core library for dsq data processing.

dsq-core provides the fundamental data processing capabilities for dsq, extending jq-compatible syntax to work with structured data formats like Parquet, Avro, CSV, and more. It leverages Polars DataFrames for high-performance data manipulation.

## Key Components

### Value Type System
The core `Value` enum bridges between JSON-like values and Polars DataFrames:

```rust
use dsq_core::value::Value;

// JSON-like values
let json_val = Value::object([
    ("name".to_string(), Value::string("Alice")),
    ("age".to_string(), Value::int(30)),
].into());

// DataFrame values
let df_val = Value::dataframe(dataframe);
```

### Operations Framework
Comprehensive data operations library:

```rust
use dsq_core::ops::{Operation, basic::*};

// Select columns
let selected = select_columns(&data, &["name", "age"])?;

// Sort data
let sorted = sort_by_columns(&selected, &[SortOptions::desc("age")])?;

// Take first N rows
let result = head(&sorted, 10)?;
```

### I/O Support
Input/output for multiple file formats:

```rust
use dsq_core::io;

// Read CSV file
let data = io::read_file("data.csv", &io::ReadOptions::default())?;

// Write to Parquet
io::write_file(&result, "output.parquet", &io::WriteOptions::default())?;
```

### Filter System
jq-compatible filter compilation and execution:

```rust
use dsq_core::filter::{FilterExecutor, ExecutorConfig};

// Execute jq-style filter
let mut executor = FilterExecutor::with_config(ExecutorConfig::default());
let result = executor.execute_str("map(select(.age > 30)) | sort_by(.name)", data)?;
```

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
dsq-core = "0.1"
```

Or for the latest development version:

```toml
[dependencies]
dsq-core = { git = "https://github.com/durableprogramming/dsq", branch = "main" }
```

## Requirements

- Rust 1.69 or later
- Polars for full DataFrame functionality

## API Reference

Full API documentation is available at [docs.rs/dsq-core](https://docs.rs/dsq-core).

## Quick Start

```rust,no_run
use dsq_core::{Value, ops, io};

// Read data from a file
let data = io::read_file("data.csv", &io::ReadOptions::default())?;

// Apply operations
let result = ops::OperationPipeline::new()
    .select(vec!["name".to_string(), "age".to_string()])
    .filter("age > 25")?
    .sort_by(vec![ops::SortOptions::desc("age".to_string())])
    .head(10)
    .execute(data)?;

// Write to Parquet
io::write_file(&result, "output.parquet", &io::WriteOptions::default())?;
# Ok::<(), dsq_core::Error>(())
```

## High-Level API

For more convenient usage, dsq-core provides a fluent API:

```rust,no_run
use dsq_core::api::Dsq;

// Chain operations easily
let result = Dsq::from_file("data.csv")?
    .select(&["name", "age", "department"])
    .filter_expr("age > 25")
    .sort_by(&["department", "age"])
    .group_by(&["department"])
    .aggregate(&["department"], vec![
        dsq_core::ops::aggregate::AggregationFunction::Count,
        dsq_core::ops::aggregate::AggregationFunction::Mean("salary".to_string()),
    ])
    .to_json()?;
```

## Feature Flags

dsq-core supports optional features for different use cases:

- `default` - Includes `all-formats`, `io`, and `filter` for full functionality
- `all-formats` - Enables all supported data formats
- `io` - File I/O operations and format conversion
- `filter` - jq-compatible filter compilation and execution
- `repl` - Interactive REPL support
- `cli` - Command-line interface components

### Format-Specific Features

- `csv` - CSV/TSV reading and writing
- `json` - JSON and JSON Lines support
- `parquet` - Apache Parquet format support
- `avro` - Apache Avro format support (requires Polars avro feature)

## Dependencies

dsq-core builds on several key dependencies:

- **Polars** - High-performance DataFrame operations
- **Arrow** - Columnar memory format
- **Serde** - Serialization/deserialization
- **Tokio** - Async runtime for streaming operations
- **Nom** - Parser combinators for filter syntax
- **dsq-shared** - Shared types and utilities

## Contributing

Contributions are welcome! Please see the main [CONTRIBUTING.md](../CONTRIBUTING.md) file for guidelines.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.