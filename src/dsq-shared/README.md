# dsq-shared

![Build status](https://github.com/durableprogramming/dsq/actions/workflows/check.yml/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/dsq-shared.svg)](https://crates.io/crates/dsq-shared)
[![Documentation](https://docs.rs/dsq-shared/badge.svg)](https://docs.rs/dsq-shared)
[![Rust 1.69+](https://img.shields.io/badge/rust-1.69+-orange.svg)](https://www.rust-lang.org)

Shared types and utilities for DSQ crates.

The `dsq-shared` crate provides common types, utilities, and operations used across multiple DSQ crates. It serves as the foundation for type-safe data processing and shared functionality.

## Key Components

### Value Type System
The core `Value` enum bridges between JSON-like values and Polars DataFrames:

```rust
use dsq_shared::value::Value;

let json_val = Value::object([
    ("name".to_string(), Value::string("Alice")),
    ("age".to_string(), Value::int(30)),
    ("scores".to_string(), Value::array(vec![Value::float(95.5), Value::float(87.2)]))
].into());

let df_val = Value::dataframe(dataframe); // Polars DataFrame
let series_val = Value::series(series);   // Polars Series
```

### Operations Framework
The `Operation` trait provides a composable interface for data transformations:

```rust
use dsq_shared::ops::{Operation, FieldAccessOperation, AddOperation};

let field_op = FieldAccessOperation::new("age".to_string());
let result = field_op.apply(&json_val)?; // Returns Value::Int(30)
```

### Utility Functions
Common utilities for data processing:

```rust
use dsq_shared::{utils, constants};

// HashMap creation helper
let map = utils::hashmap([("key1", "value1"), ("key2", "value2")]);

// String utilities
assert!(utils::is_blank("   "));
assert_eq!(utils::capitalize_first("hello"), "Hello");

// Constants for configuration
let batch_size = constants::DEFAULT_BATCH_SIZE; // 1000
let buffer_size = constants::LARGE_BUFFER_SIZE; // 128KB
```

### Error Handling
Standardized error types and utilities:

```rust
use dsq_shared::error;

let err = error::operation_error("Invalid operation");
let config_err = error::config_error("Missing configuration");
```

### Build Information
Runtime access to build metadata:

```rust
use dsq_shared::{BuildInfo, VERSION};

println!("dsq-shared version: {}", VERSION);
let build_info = BuildInfo {
    version: VERSION,
    git_hash: Some("abc123"),
    build_date: Some("2024-01-01"),
    rust_version: Some("1.75.0"),
    features: &["default"],
};
println!("{}", build_info);
```

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
dsq-shared = "0.1"
```

Or for the latest development version:

```toml
[dependencies]
dsq-shared = { git = "https://github.com/durableprogramming/dsq", branch = "main" }
```

## Requirements

- Rust 1.69 or later
- For full functionality: Polars with appropriate features

## Dependencies

- **Polars** - High-performance DataFrame operations
- **Serde** - Serialization support
- **Chrono** - Date/time handling
- **Num-bigint** - Arbitrary precision integers
- **Indexmap** - Ordered maps

## API Reference

Full API documentation is available at [docs.rs/dsq-shared](https://docs.rs/dsq-shared).

## Contributing

Contributions are welcome! Please see the main [CONTRIBUTING.md](../CONTRIBUTING.md) file for guidelines.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.