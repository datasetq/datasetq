# dsq-functions

Built-in functions and function registry for DSQ.

## Overview

`dsq-functions` provides a comprehensive library of built-in functions for data manipulation, transformation, and analysis in DSQ queries. The crate includes a function registry system that allows for dynamic function lookup and extensibility.

## Features

- **Extensive function library**: Over 100+ built-in functions
- **Function registry**: Dynamic function registration and lookup
- **Type-safe**: Strong typing with automatic conversions
- **Extensible**: Easy to add custom functions
- **Well-documented**: Each function includes documentation and examples
- **Platform support**: Works on native and WASM targets

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
dsq-functions = "0.1"
```

## Usage

### Using Built-in Functions

```rust
use dsq_functions::{call_function, FunctionRegistry};
use dsq_shared::value::Value;

fn main() {
    let registry = FunctionRegistry::default();

    // Call a string function
    let input = Value::String("hello world".to_string());
    let result = call_function("uppercase", &[input], &registry)
        .expect("Function call failed");

    println!("Result: {:?}", result); // "HELLO WORLD"
}
```

### Registering Custom Functions

```rust
use dsq_functions::{Function, FunctionRegistry};
use dsq_shared::value::Value;

fn main() {
    let mut registry = FunctionRegistry::new();

    // Register a custom function
    registry.register("double", Function {
        name: "double",
        description: "Doubles a number",
        handler: |args| {
            let num = args[0].as_number()?;
            Ok(Value::Number(num * 2.0))
        },
    });

    // Use the custom function
    let result = registry.call("double", &[Value::Number(5.0)])
        .expect("Function call failed");

    println!("Result: {:?}", result); // 10.0
}
```

## Function Categories

### String Functions

- `uppercase`, `lowercase`, `capitalize`
- `split`, `join`, `trim`
- `contains`, `startswith`, `endswith`
- `replace`, `substring`
- `length`, `reverse`
- `slugify`, `unidecode`

### Array Functions

- `length`, `first`, `last`, `nth`
- `map`, `filter`, `reduce`
- `sort`, `sort_by`, `reverse`
- `unique`, `unique_by`
- `flatten`, `group_by`
- `zip`, `unzip`

### Object Functions

- `keys`, `values`, `entries`
- `has`, `get`, `set`
- `merge`, `assign`
- `pick`, `omit`
- `to_entries`, `from_entries`

### Math Functions

- `abs`, `ceil`, `floor`, `round`
- `min`, `max`, `sum`, `avg`
- `sqrt`, `pow`, `log`
- `sin`, `cos`, `tan`

### Date/Time Functions

- `now`, `date`, `time`
- `format_date`, `parse_date`
- `add_days`, `add_hours`, `add_minutes`
- `year`, `month`, `day`, `hour`, `minute`, `second`

### Type Functions

- `type`, `is_string`, `is_number`, `is_boolean`, `is_array`, `is_object`
- `to_string`, `to_number`, `to_boolean`
- `parse_json`, `to_json`

### Encoding Functions

- `base64_encode`, `base64_decode`
- `base32_encode`, `base32_decode`
- `base58_encode`, `base58_decode`
- `url_encode`, `url_decode`
- `hex_encode`, `hex_decode`

### Hash Functions

- `md5`, `sha1`, `sha256`
- `hash`

### UUID Functions

- `uuid_v4`, `uuid_v7`
- `uuid_parse`, `uuid_validate`

### Utility Functions

- `select`, `empty`, `error`
- `range`, `repeat`, `limit`
- `random`, `random_int`
- `debug`, `assert`

## API Documentation

For detailed API documentation, including complete function signatures and examples, see [docs.rs/dsq-functions](https://docs.rs/dsq-functions).

## Performance

Functions are optimized for performance:

- Zero-cost abstractions where possible
- Efficient string operations using `smartstring`
- Minimal allocations for numeric operations
- Lazy evaluation support for collection operations

## Contributing

Contributions are welcome! To add new functions:

1. Define the function in the appropriate module
2. Register it in the function registry
3. Add tests and documentation
4. Submit a pull request

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for more details.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT license ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
