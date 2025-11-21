# dsq-cli

![Build status](https://github.com/durableprogramming/dsq/actions/workflows/check.yml/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/dsq-cli.svg)](https://crates.io/crates/dsq-cli)
[![Documentation](https://docs.rs/dsq-cli/badge.svg)](https://docs.rs/dsq-cli)
[![Rust 1.69+](https://img.shields.io/badge/rust-1.69+-orange.svg)](https://www.rust-lang.org)

Command-line interface for dsq - data processing with jq syntax.

The `dsq-cli` crate provides the command-line interface for dsq, a data processing tool that extends jq-compatible syntax to work with structured data formats like Parquet, Avro, CSV, and more.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
dsq-cli = "0.1"
```

Or for the latest development version:

```toml
[dependencies]
dsq-cli = { git = "https://github.com/durableprogramming/dsq", branch = "main" }
```

## Requirements

- Rust 1.69 or later
- For full functionality: dsq-core with appropriate features

## API Reference

Full API documentation is available at [docs.rs/dsq-cli](https://docs.rs/dsq-cli).

## Usage

The dsq-cli crate provides the main `dsq` binary. See the main project README for detailed usage examples.

## Key Components

### CLI Parsing
Command-line argument parsing and configuration:

```rust
use dsq_cli::cli::{parse_args, CliConfig};

// Parse command line arguments
let config = parse_args()?;
```

### Execution Engine
Data processing execution with various modes:

```rust
use dsq_cli::executor::Executor;

// Execute data processing
let executor = Executor::new(config);
let result = executor.execute().await?;
```

### Interactive REPL
Interactive shell for data exploration:

```rust
use dsq_cli::repl::Repl;

// Start REPL mode
let repl = Repl::new();
repl.run().await?;
```

## Dependencies

- **dsq-core** - Core data processing functionality
- **clap** - Command-line argument parsing
- **tokio** - Async runtime
- **anyhow** - Error handling

## Contributing

Contributions are welcome! Please see the main [CONTRIBUTING.md](../CONTRIBUTING.md) file for guidelines.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.