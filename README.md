# datasetq 

[![CI](https://github.com/datasetq/datasetq/actions/workflows/ci.yml/badge.svg)](https://github.com/datasetq/datasetq/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/dsq-core.svg)](https://crates.io/crates/dsq-core)
[![Documentation](https://docs.rs/dsq-core/badge.svg)](https://docs.rs/dsq-core)
[![Rust 1.69+](https://img.shields.io/badge/rust-1.69+-orange.svg)](https://www.rust-lang.org)

datasetq is a high-performance data processing tool that extends jq-like syntax to work with structured data formats including Parquet, Avro, CSV, JSON Lines, Arrow, and more. Built on [Polars](https://pola.rs/), dsq provides fast data manipulation across multiple file formats with familiar filter syntax.

## Key Features

* **Format Flexibility** - Process Parquet, Avro, CSV, TSV, JSON Lines, Arrow, and more with automatic format detection
* **Performance** - Built on Polars DataFrames with lazy evaluation, columnar operations, and efficient memory usage
* **Familiar Syntax** - jq-inspired filter syntax extended to tabular data operations
* **Correctness** - Proper type handling and clear error messages

## Installation

### Binaries

Download binaries for Linux, Mac, and Windows from the [releases page](https://github.com/datasetq/datasetq/releases).

On Linux:
```bash
curl -fsSL https://github.com/datasetq/datasetq/releases/latest/download/dsq-$(uname -m)-unknown-linux-musl -o dsq && chmod +x dsq
```

### From Source

Install with Rust toolchain (see <https://rustup.rs/>):
```bash
cargo install --locked dsq-cli
cargo install --locked --git https://github.com/datasetq/datasetq  # development version
```

Or build from the repository:
```bash
cargo build --release  # creates target/release/dsq
cargo install --locked --path dsq-cli  # installs binary
```

## Quick Start

Process CSV data:
```bash
dsq 'map(select(.age > 30))' people.csv
```

Convert between formats:
```bash
dsq '.' data.csv --output data.parquet
```

Aggregate data:
```bash
dsq 'group_by(.department) | map({dept: .[0].department, count: length})' employees.parquet
```

Filter and transform:
```bash
dsq 'map(select(.status == "active") | {name, email})' users.json
```

Process multiple files:
```bash
dsq 'flatten | group_by(.category)' sales_*.csv
```

Use lazy evaluation for large datasets:
```bash
dsq --lazy 'filter(.amount > 1000)' transactions.parquet
```

## Interactive Mode

Start an interactive REPL to experiment with filters:
```bash
dsq --interactive
```

Available REPL commands:
- `load <file>` - Load data from a file
- `show` - Display current data
- `explain <filter>` - Explain what a filter does
- `history` - Show command history
- `help` - Show help message
- `quit` - Exit

## Common Operations

### Format Conversion
```bash
dsq convert input.csv output.parquet
```

### Data Inspection
```bash
dsq inspect data.parquet --schema --sample 10 --stats
```

### File Merging
```bash
dsq merge data1.csv data2.csv --output combined.csv
```

### Shell Completions
```bash
dsq completions bash >> ~/.bashrc
```

## Supported Formats

**Input/Output:**
- CSV/TSV - Delimited text with customizable options
- Parquet - Columnar storage with compression
- JSON/JSON Lines - Standard and newline-delimited JSON
- Arrow - Columnar in-memory format
- Avro - Row-based serialization
- ADT - ASCII delimited text (control characters)

**Output Only:**
- Excel (.xlsx)
- ORC - Optimized row columnar

Format detection is automatic based on file extensions. Override with `--input-format` and `--output-format`.

## Documentation

- [Architecture](docs/ARCHITECTURE.md) - Core library structure and modules
- [Functions](docs/FUNCTIONS.md) - Built-in function reference
- [Formats](docs/FORMATS.md) - Format support and options
- [API](docs/API.md) - Library usage examples
- [Configuration](docs/CONFIGURATION.md) - Configuration file reference
- [Language](docs/LANGUAGE.md) - Filter language syntax

## Command-Line Options

### Input/Output
- `-i, --input-format <FORMAT>` - Specify input format
- `-o, --output <FILE>` - Output file (stdout by default)
- `--output-format <FORMAT>` - Specify output format
- `-f, --filter-file <FILE>` - Read filter from file

### Processing
- `--lazy` - Enable lazy evaluation
- `--dataframe-optimizations` - Enable DataFrame optimizations
- `--threads <N>` - Number of threads
- `--memory-limit <LIMIT>` - Memory limit (e.g., 1GB)

### Output Formatting
- `-c, --compact-output` - Compact output
- `-r, --raw-output` - Raw strings without quotes
- `-S, --sort-keys` - Sort object keys

### Debugging
- `-v, --verbose` - Increase verbosity
- `--explain` - Show execution plan
- `--stats` - Show execution statistics
- `-I, --interactive` - Start REPL mode

## Configuration

Configuration files are searched in:
1. Current directory (`.dsq.toml`, `dsq.yaml`)
2. Home directory (`~/.config/dsq/`)
3. System directory (`/etc/dsq/`)

Manage configuration:
```bash
dsq config show                  # Show current configuration
dsq config set filter.lazy_evaluation true
dsq config init                  # Create default config
```

See [Configuration](docs/CONFIGURATION.md) for details.

## Contributing

Contributions are welcome! Please ensure:
1. Compatibility with jq syntax where possible
2. Tests pass with `cargo test`
3. Documentation updated for new features
4. Performance implications considered

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Acknowledgements

dsq builds on excellent foundations from:

* [jq](https://stedolan.github.io/jq/) - The original and inimitable jq
* [jaq](https://github.com/01mf02/jaq) - jq clone inspiring our syntax compatibility
* [Polars](https://pola.rs/) - High-performance DataFrame library
* [Arrow](https://arrow.apache.org/) - Columnar memory format

Special thanks to **Ronald Duncan** for [defining the ASCII Delimited Text (ADT) format](https://ronaldduncan.wordpress.com/2009/10/31/text-file-formats-ascii-delimited-text-not-csv-or-tab-delimited-text/).

Our GitHub Actions disk space cleanup script was inspired by the [Apache Flink project](https://github.com/apache/flink).

## License

See LICENSE file for details.
