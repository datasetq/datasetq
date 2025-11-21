# DSQ Feature Flags Documentation

This document describes all feature flags available in the DSQ workspace and how to use them.

## Overview

DSQ uses Cargo feature flags to enable optional functionality and manage dependencies. This allows you to build only the parts of DSQ that you need, reducing compile time and binary size.

## Root Crate Features (`datasetq`)

### Default Features

```toml
default = ["all-formats", "bin"]
```

The default feature set includes all data formats and the command-line binary.

### Format Features

Control which data formats are supported:

- **`csv`** - CSV (Comma-Separated Values) format support
  - No additional dependencies
  - Enables reading and writing CSV files

- **`json`** - JSON (JavaScript Object Notation) format support
  - No additional dependencies
  - Enables reading and writing JSON and JSON Lines files

- **`parquet`** - Apache Parquet format support
  - Requires Polars with parquet features
  - Enables reading and writing columnar Parquet files

- **`arrow`** - Apache Arrow IPC format support
  - Requires `polars/ipc` feature
  - Enables reading and writing Arrow IPC/Feather files

- **`avro`** - Apache Avro format support
  - Requires `apache-avro` dependency with Snappy compression
  - Enables reading and writing Avro files

- **`all-formats`** - Enables all format features
  - Equivalent to `["csv", "json", "parquet", "arrow", "avro"]`

### Application Features

- **`cli`** - Command-line interface dependencies
  - Includes: `clap`, `clap_complete`, `atty`, `env_logger`, `num_cpus`, `tokio`, `serde`, `serde_json`, `serde_yaml`, `toml`, `dirs`, `tempfile`, `getrandom`
  - Required for building the CLI application
  - Enables configuration file support (YAML, TOML, JSON)

- **`bin`** - Build the command-line binary
  - Enables the `cli` feature
  - Required for `dsq` executable

- **`filter`** - Data filtering capabilities
  - Requires `polars` for DataFrame operations
  - Enables jq-like filtering syntax

- **`wasm`** - WebAssembly support
  - Requires `serde_json` and `polars`
  - Enables building DSQ for WebAssembly targets
  - Uses modified Polars features for WASM compatibility

## Feature Combinations

### Minimal Build (Library Only)

```bash
cargo build --no-default-features --features csv,json
```

Builds only CSV and JSON support without the CLI.

### CLI with Specific Formats

```bash
cargo build --no-default-features --features bin,csv,parquet
```

Builds the CLI with only CSV and Parquet support.

### WebAssembly Build

```bash
cargo build --target wasm32-unknown-unknown --features wasm
```

Builds for WebAssembly with appropriate feature flags.

### Full Featured Build (Default)

```bash
cargo build
```

Builds with all formats and the CLI binary.

## Per-Crate Features

### dsq-core

No additional feature flags. Core data structures and types.

### dsq-shared

No additional feature flags. Shared utilities and types.

### dsq-parser

No additional feature flags. SQL-like and jq-like query parsing.

### dsq-filter

No additional feature flags. Data filtering and transformation logic.

### dsq-functions

No additional feature flags. Built-in and custom functions for data processing.

### dsq-formats

Format-specific features are controlled at the root level. This crate provides:
- CSV reading/writing (when `csv` feature enabled)
- JSON reading/writing (when `json` feature enabled)
- Parquet reading/writing (when `parquet` feature enabled)
- Arrow IPC reading/writing (when `arrow` feature enabled)
- Avro reading/writing (when `avro` feature enabled)

### dsq-io

No additional feature flags. I/O operations and file handling.

### dsq-cli

Requires the `cli` feature from the root crate. Provides:
- Command-line argument parsing
- Configuration file handling
- Terminal I/O
- Command completion generation

## Platform-Specific Features

### Native (non-WASM) Targets

```toml
[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
polars = { version = "0.35", features = ["lazy", "csv", "json", "parquet", ...] }
```

Full Polars functionality with all data processing features.

### WASM Targets

```toml
[target.'cfg(target_arch = "wasm32")'.dependencies]
polars = { version = "0.35", default-features = false, features = ["csv", "json", ...] }
```

Reduced Polars feature set optimized for WebAssembly:
- No lazy evaluation
- No Parquet support (limited in WASM)
- Basic CSV and JSON support
- Modified `zstd-sys` for WASM compatibility

### WASI Targets

```toml
[target.'cfg(all(target_arch = "wasm32", target_os = "wasi"))'.dependencies]
```

Currently no special dependencies for WASI targets.

## Testing Features

Development dependencies include testing utilities regardless of features:

- **`criterion`** - Benchmarking framework
- **`proptest`** - Property-based testing
- **`tempfile`** - Temporary file creation for tests
- **`assert_cmd`** - CLI testing utilities (root crate only)
- **`predicates`** - Assertion helpers (root crate only)
- **`pretty_assertions`** - Better test output formatting

## Examples

### Example 1: Minimal CSV Parser Library

```toml
[dependencies]
dsq = { version = "0.1", default-features = false, features = ["csv"] }
```

Use DSQ as a library to parse CSV files only.

### Example 2: Full CLI Tool

```toml
[dependencies]
dsq = { version = "0.1" }
```

Use default features to get the full CLI experience.

### Example 3: JSON and Parquet Only

```toml
[dependencies]
dsq = { version = "0.1", default-features = false, features = ["json", "parquet", "filter"] }
```

Library usage with JSON and Parquet support plus filtering capabilities.

### Example 4: Custom Format Combination for CLI

```bash
cargo build --no-default-features --features bin,csv,json,parquet,filter
```

Build CLI with CSV, JSON, and Parquet support, excluding Avro and Arrow.

## Testing Feature Combinations

The CI pipeline tests various feature combinations to ensure compatibility:

```yaml
- features: --no-default-features
- features: --no-default-features --features csv
- features: --no-default-features --features json
- features: --no-default-features --features parquet
- features: --no-default-features --features all-formats
- features: --features bin
- features: --features wasm
```

## Feature Flag Best Practices

1. **Always test your feature combination** before deploying:
   ```bash
   cargo test --no-default-features --features your,features,here
   ```

2. **Document required features** if your application depends on DSQ:
   ```toml
   # Requires JSON and filtering capabilities
   dsq = { version = "0.1", default-features = false, features = ["json", "filter"] }
   ```

3. **Consider binary size** when selecting features:
   - Full build: ~50MB (with all formats)
   - Minimal build: ~10MB (CSV + JSON only)

4. **Use `all-formats`** unless you have specific constraints:
   ```toml
   dsq = { version = "0.1", features = ["all-formats"] }
   ```

## Future Features (Planned)

The following features are planned for future releases:

- **`sql`** - Full SQL query support (beyond current subset)
- **`python`** - Python expression evaluation in filters
- **`remote`** - S3 and HTTP(S) data source support
- **`compression`** - Additional compression formats (gzip, bzip2, xz)
- **`plugins`** - Dynamic plugin loading system
- **`streaming`** - Streaming data processing for large files

## Troubleshooting

### Error: "format not supported"

Make sure you've enabled the appropriate format feature:

```bash
cargo build --features parquet  # if building with default features
# or
cargo build --no-default-features --features bin,csv,json,parquet
```

### Error: "clap not found" or "CLI dependencies missing"

Enable the `cli` or `bin` feature:

```bash
cargo build --features bin
```

### WASM Build Failures

Use the `wasm` feature and appropriate target:

```bash
cargo build --target wasm32-unknown-unknown --features wasm --no-default-features
```

### Feature Combination Incompatibilities

If you encounter build errors with certain feature combinations, please:
1. Check the CI configuration for tested combinations
2. Report the issue on GitHub
3. Try using default features as a workaround

## See Also

- [ARCHITECTURE.md](ARCHITECTURE.md) - System architecture and design
- [CONTRIBUTING.md](CONTRIBUTING.md) - Contributing guidelines
- [README.md](README.md) - Project overview and quick start
- [Cargo Book: Features](https://doc.rust-lang.org/cargo/reference/features.html) - Official Cargo features documentation
