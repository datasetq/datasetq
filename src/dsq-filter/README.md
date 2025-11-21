# dsq-filter

Filter system for DSQ that operates at the AST level, evaluating parsed queries against data.

## Overview

`dsq-filter` is the execution engine for DSQ queries. It takes an Abstract Syntax Tree (AST) produced by `dsq-parser` and evaluates it against data structures, producing filtered and transformed results. The filter system supports both in-memory operations and DataFrame-based operations using Polars.

## Features

- **AST evaluation**: Execute parsed queries against data
- **DataFrame support**: Efficient operations on large datasets using Polars
- **Streaming operations**: Process large datasets with minimal memory overhead
- **Function registry**: Extensible function system
- **Type coercion**: Automatic type conversions where appropriate
- **Error propagation**: Clear error messages for runtime issues

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
dsq-filter = "0.1"
```

## Usage

### Basic Filtering

```rust
use dsq_filter::execute_filter;
use dsq_shared::value::Value;

fn main() {
    let data = r#"{"name": "Alice", "age": 30}"#;
    let value = Value::from_json(serde_json::from_str(data).unwrap());

    let result = execute_filter(".name", &value).expect("Filter failed");
    println!("Result: {:?}", result);
}
```

### Working with DataFrames

```rust
use dsq_filter::execute_dataframe_filter;
use polars::prelude::*;

fn main() {
    // Create a DataFrame
    let df = df! {
        "name" => ["Alice", "Bob", "Charlie"],
        "age" => [30, 25, 35],
    }.unwrap();

    // Filter using DSQ syntax
    let result = execute_dataframe_filter(".[] | select(.age > 26)", df)
        .expect("Filter failed");

    println!("{:?}", result);
}
```

### Complex Queries

```rust
use dsq_filter::execute_filter;
use dsq_shared::value::Value;

fn main() {
    let data = r#"[
        {"name": "Alice", "age": 30, "city": "NYC"},
        {"name": "Bob", "age": 25, "city": "LA"},
        {"name": "Charlie", "age": 35, "city": "NYC"}
    ]"#;

    let value = Value::from_json(serde_json::from_str(data).unwrap());

    // Complex query with filtering and transformation
    let query = ".[] | select(.city == \"NYC\") | {name, age}";
    let result = execute_filter(query, &value).expect("Filter failed");

    println!("NYC residents: {:?}", result);
}
```

## Supported Operations

The filter system supports:

- **Selection**: `select()`, `map()`, `reject()`
- **Transformation**: Field extraction, object construction, array operations
- **Aggregation**: `group_by()`, `sort_by()`, `unique()`
- **Arithmetic**: `+`, `-`, `*`, `/`, `%`
- **Comparison**: `==`, `!=`, `<`, `>`, `<=`, `>=`
- **Logical**: `and`, `or`, `not`
- **String operations**: `split()`, `join()`, `contains()`, `startswith()`, `endswith()`
- **Array operations**: `length`, `first`, `last`, `reverse`, `flatten`

## API Documentation

For detailed API documentation, see [docs.rs/dsq-filter](https://docs.rs/dsq-filter).

## Performance

The filter system is designed for performance:

- Uses Polars for DataFrame operations (optimized for large datasets)
- Supports lazy evaluation to minimize memory usage
- Efficient AST traversal with minimal allocations
- Streaming operations for large files

## Contributing

Contributions are welcome! Please see the [CONTRIBUTING.md](../../CONTRIBUTING.md) file in the repository root for guidelines.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT license ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
