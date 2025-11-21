# dsq-formats

File format support for DSQ - handles reading and writing various data formats.

## Overview

`dsq-formats` provides comprehensive support for reading and writing multiple structured data formats. It serves as the I/O layer for DSQ, converting between different file formats and DSQ's internal data representations.

## Features

- **Multiple formats**: CSV, JSON, JSON Lines, Parquet, Avro, Arrow IPC
- **Format detection**: Automatic format detection based on file content
- **Streaming support**: Efficient processing of large files
- **Schema inference**: Automatic schema detection for structured data
- **Flexible options**: Configurable parsing and writing options
- **Error handling**: Detailed error messages for format issues

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
dsq-formats = "0.1"
```

Enable specific formats:

```toml
[dependencies]
dsq-formats = { version = "0.1", features = ["csv", "json", "parquet"] }
```

## Usage

### Reading CSV Files

```rust
use dsq_formats::csv::read_csv_file;

fn main() {
    let df = read_csv_file("data.csv")
        .expect("Failed to read CSV");

    println!("Loaded {} rows", df.height());
}
```

### Writing JSON

```rust
use dsq_formats::json::write_json_file;
use polars::prelude::*;

fn main() {
    let df = df! {
        "name" => ["Alice", "Bob"],
        "age" => [30, 25],
    }.unwrap();

    write_json_file(&df, "output.json")
        .expect("Failed to write JSON");
}
```

### Reading Parquet

```rust
use dsq_formats::parquet::read_parquet_file;

fn main() {
    let df = read_parquet_file("data.parquet")
        .expect("Failed to read Parquet");

    println!("Columns: {:?}", df.get_column_names());
}
```

### Format Detection

```rust
use dsq_formats::detect_format;

fn main() {
    let format = detect_format("data.csv")
        .expect("Failed to detect format");

    match format {
        Format::Csv => println!("CSV file detected"),
        Format::Json => println!("JSON file detected"),
        Format::Parquet => println!("Parquet file detected"),
        _ => println!("Other format"),
    }
}
```

### Custom Options

```rust
use dsq_formats::csv::{read_csv_file_with_options, CsvReadOptions};

fn main() {
    let options = CsvReadOptions {
        has_header: true,
        delimiter: b';',
        quote_char: Some(b'"'),
        ..Default::default()
    };

    let df = read_csv_file_with_options("data.csv", &options)
        .expect("Failed to read CSV with options");
}
```

## Supported Formats

### CSV (Comma-Separated Values)

- **Read**: Yes
- **Write**: Yes
- **Features**: Custom delimiters, headers, quotes, null values
- **Streaming**: Yes

### JSON

- **Read**: Yes (standard JSON and JSON Lines)
- **Write**: Yes
- **Features**: Pretty printing, compact format
- **Streaming**: Yes (JSON Lines)

### JSON5

- **Read**: Yes
- **Write**: No
- **Features**: Comments, trailing commas, unquoted keys
- **Streaming**: No

### Parquet

- **Read**: Yes
- **Write**: Yes
- **Features**: Compression, column pruning, predicate pushdown
- **Streaming**: Yes (with chunking)

### Avro

- **Read**: Yes
- **Write**: Yes
- **Features**: Schema evolution, compression
- **Streaming**: Yes

### Arrow IPC

- **Read**: Yes
- **Write**: Yes
- **Features**: Zero-copy reads, compression
- **Streaming**: Yes

## Format Detection

The library can automatically detect file formats based on:

- File extension
- Magic bytes (file signature)
- Content analysis

```rust
use dsq_formats::detect_format;

let format = detect_format("unknown.dat")?;
```

## Configuration Options

Each format supports various configuration options:

### CSV Options

- `delimiter`: Field separator character
- `has_header`: Whether first row contains headers
- `quote_char`: Character for quoting fields
- `null_values`: List of strings to interpret as NULL
- `skip_rows`: Number of rows to skip
- `encoding`: Character encoding

### JSON Options

- `pretty`: Pretty-print output
- `indent`: Indentation level
- `null_handling`: How to handle null values

### Parquet Options

- `compression`: Compression algorithm (snappy, gzip, lz4, zstd)
- `row_group_size`: Rows per row group
- `statistics`: Whether to compute column statistics

## API Documentation

For detailed API documentation, see [docs.rs/dsq-formats](https://docs.rs/dsq-formats).

## Performance

Format readers and writers are optimized for:

- Large file handling with streaming
- Memory-efficient processing
- Parallel parsing where applicable
- Zero-copy operations for compatible formats

## Contributing

Contributions are welcome! To add support for new formats:

1. Create a new module for the format
2. Implement read/write functions
3. Add format detection logic
4. Include tests with sample data
5. Update documentation

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for more details.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT license ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
