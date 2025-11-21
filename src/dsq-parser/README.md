# dsq-parser

Parser for the DSQ filter language that produces an Abstract Syntax Tree (AST).

## Overview

`dsq-parser` is a core component of the DSQ (DataSet Query) ecosystem that handles parsing of jq-like query syntax into an AST representation. The parser is built using the `nom` parser combinator library and produces a structured AST that can be evaluated by other DSQ components.

## Features

- **jq-compatible syntax**: Supports a subset of jq's filter language
- **Comprehensive AST**: Produces a detailed abstract syntax tree for query evaluation
- **Error handling**: Provides clear error messages for syntax errors
- **Extensible design**: Easy to add new operators and functions
- **Parser combinators**: Built with `nom` for composability and performance

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
dsq-parser = "0.1"
```

## Usage

### Basic Parsing

```rust
use dsq_parser::parse_filter;

fn main() {
    let query = ".name | select(.age > 18)";

    match parse_filter(query) {
        Ok(ast) => println!("Parsed AST: {:?}", ast),
        Err(e) => eprintln!("Parse error: {}", e),
    }
}
```

### Working with the AST

```rust
use dsq_parser::{parse_filter, Expr};

fn main() {
    let query = ".[] | .id";
    let ast = parse_filter(query).expect("Failed to parse");

    // Process the AST
    match ast {
        Expr::Pipe(left, right) => {
            println!("Pipeline detected");
            println!("Left: {:?}", left);
            println!("Right: {:?}", right);
        }
        _ => println!("Other expression type"),
    }
}
```

## Supported Syntax

The parser supports the following jq-like constructs:

- **Field access**: `.field`, `.field.nested`
- **Array operations**: `.[]`, `.[0]`, `.[1:3]`
- **Pipes**: `expr | expr`
- **Filters**: `select()`, `map()`, `sort_by()`
- **Operators**: `+`, `-`, `*`, `/`, `==`, `!=`, `<`, `>`, `<=`, `>=`
- **Functions**: Built-in and custom function calls
- **Literals**: Numbers, strings, booleans, null
- **Object construction**: `{key: value}`
- **Array construction**: `[expr]`

## API Documentation

For detailed API documentation, see [docs.rs/dsq-parser](https://docs.rs/dsq-parser).

## Architecture

The parser is organized into several modules:

- **lexer**: Tokenization of input strings
- **expr**: Expression parsing
- **operators**: Operator parsing and precedence
- **literals**: Literal value parsing
- **combinators**: Helper parser combinators

## Contributing

Contributions are welcome! Please see the [CONTRIBUTING.md](../../CONTRIBUTING.md) file in the repository root for guidelines.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT license ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
