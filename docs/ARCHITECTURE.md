# Architecture

This document describes the internal architecture of dsq, including its core modules and how they work together.

## Overview

dsq is organized into several specialized crates, each handling a distinct aspect of data processing:

```
dsq-core          - Main library coordinating all components
├── dsq-parser    - Parse filter expressions into AST
├── dsq-filter    - Compile and execute filters
├── dsq-functions - Built-in function registry
├── dsq-formats   - Format detection and I/O
├── dsq-io        - Low-level I/O operations
└── dsq-shared    - Shared types and utilities
```

## dsq-core

The core library provides high-level APIs and coordinates all components.

### Key Modules

#### value
Core value type that bridges JSON and Polars DataFrames, enabling seamless data interchange between different representations.

#### ops
Comprehensive data operations library organized by category:

- **basic** - Fundamental operations: `select`, `filter`, `sort`, `head`, `tail`, `unique`
- **aggregate** - Grouping and aggregation: `group_by`, `sum`, `mean`, `count`, `pivot`
- **join** - Join operations: inner, left, right, outer joins
- **transform** - Data transformations: `transpose`, `cast`, `reshape`

#### io
Input/output support for multiple file formats:
- CSV/TSV with automatic delimiter detection
- Parquet for columnar storage
- JSON and JSON Lines
- Arrow IPC format
- Avro (planned)

#### filter
jq-compatible filter compilation and execution engine with DataFrame optimizations.

#### error
Comprehensive error handling with specific error types:
- `TypeError` - Type conversion and compatibility issues
- `FormatError` - File format parsing problems
- `FilterError` - jq filter syntax and execution errors

#### api
High-level fluent API (`Dsq` struct) for ergonomic data processing workflows with operation chaining and automatic optimization.

### Dependencies

- **Polars** - High-performance DataFrame operations
- **Arrow** - Columnar memory format
- **Serde** - Serialization/deserialization
- **Tokio** - Async runtime for streaming operations
- **Nom** - Parser combinators for filter syntax

## dsq-parser

Foundational parsing component that converts filter language strings into Abstract Syntax Tree (AST) representations.

### Key Features

- **Complete DSQ Syntax Support** - Parses all filter language constructs including SQL-like SELECT statements
- **Fast Parsing** - Uses nom for high-performance parsing with zero-copy operations
- **Comprehensive Error Reporting** - Detailed error messages with position information
- **AST Generation** - Produces structured AST for processing by dsq-filter
- **Type Safety** - Compile-time guarantees about AST structure

### Supported Syntax

- **Identity and field access**: `.`, `.field`, `.field.subfield`
- **Array operations**: `.[0]`, `.[1:5]`, `.[]`
- **Function calls**: `length`, `map(select(.age > 30))`
- **Arithmetic**: `+`, `-`, `*`, `/`
- **Comparisons**: `>`, `<`, `==`, `!=`, `>=`, `<=`
- **Logical operations**: `and`, `or`, `not`
- **Object/array construction**: `{name, age}`, `[1, 2, 3]`
- **Pipelines**: `expr1 | expr2 | expr3`
- **Assignments**: `. += value`, `. |= value`
- **Control flow**: `if condition then expr else expr end`
- **Sequences**: `expr1, expr2, expr3`

### Architecture Components

- **`FilterParser`** - Main parser interface with public API
- **`ast.rs`** - AST node definitions with Display implementations
- **`parser.rs`** - nom-based parser combinators implementing the grammar
- **`error.rs`** - Comprehensive error types with position tracking
- **`tests.rs`** - Extensive test suite covering all language features

### Usage Example

```rust
use dsq_parser::{FilterParser, Filter};

// Parse a DSQ filter string into an AST
let parser = FilterParser::new();
let filter: Filter = parser.parse(".name | length")?;

// Access the parsed AST
match &filter.expr {
    dsq_parser::Expr::Pipeline(exprs) => {
        println!("Pipeline with {} expressions", exprs.len());
    }
    _ => {}
}
```

### Dependencies

- **nom** - Parser combinator library
- **serde** - Serialization support for AST
- **num-bigint** - Arbitrary-precision integers
- **dsq-shared** - Shared types and utilities

## dsq-filter

Core filter compilation and execution engine that powers dsq's jq-compatible syntax. Operates at the AST level to transform filter expressions into optimized DataFrame operations.

### Key Components

#### compiler
Transforms jaq AST nodes into dsq operations with support for:
- Variable scoping and function definitions
- Type checking and error reporting
- Multiple optimization levels (None, Basic, Advanced)
- DataFrame-specific optimizations

#### executor
High-performance execution engine with:
- Configurable execution modes (Standard, Lazy, Streaming)
- Execution statistics and performance monitoring
- Error handling with strict/lenient modes
- Built-in caching for compiled filters

#### context
Execution context management for:
- Variable bindings during filter execution
- User-defined and built-in function registry
- Recursion depth control and stack management

### Usage Example

```rust
use dsq_filter::{FilterCompiler, ExecutorConfig, FilterExecutor};

// Compile a jq filter expression
let compiler = FilterCompiler::new();
let compiled = compiler.compile_str(r#"map(select(.age > 30)) | sort_by(.name)"#)?;

// Execute on data
let mut executor = FilterExecutor::with_config(ExecutorConfig {
    collect_stats: true,
    ..Default::default()
});

let result = executor.execute_compiled(&compiled, data)?;
println!("Execution stats: {:?}", result.stats);
```

## dsq-functions

Provides the comprehensive built-in function registry, implementing over 150 functions across multiple categories.

### Architecture

- **`BuiltinRegistry`** - Central registry managing all built-in functions
- **`BuiltinFunction`** - Type alias for function implementations
- **Function categories** - Organized by operation type (math, string, array, etc.)
- **Type polymorphism** - Functions work across different data types

### Key Features

- **Type Safety** - Compile-time function validation and type checking
- **Performance** - Optimized implementations using Polars operations where possible
- **Extensibility** - Easy to add new functions via the registry pattern
- **Cross-Type Support** - Functions automatically handle different input types
- **Error Handling** - Comprehensive error reporting with context

### Dependencies

- **Polars** - High-performance DataFrame operations
- **Serde** - Serialization for complex data structures
- **Chrono** - Date/time manipulation
- **URL** - URL parsing and manipulation
- **Base64/Base58/Base32** - Encoding/decoding operations
- **SHA2/SHA1/MD5** - Cryptographic hashing
- **UUID** - Universally unique identifier generation
- **Heck** - String case conversion
- **Rand** - Random number generation

See [FUNCTIONS.md](FUNCTIONS.md) for the complete function reference.

## dsq-formats

Provides comprehensive support for reading and writing various structured data formats with automatic format detection.

### Core Components

- **`DataFormat` enum** - Represents all supported formats with format-specific capabilities
- **`DataReader`/`DataWriter` traits** - Unified interface for reading/writing across all formats
- **Format-specific implementations** - Optimized readers/writers for each format
- **Automatic format detection** - Extension-based and content-based detection

### Key Features

- **Format Detection** - Automatic detection from file extensions, magic bytes, and content analysis
- **Unified Interface** - Consistent `read_file()` and `write_file()` functions across formats
- **Performance Optimizations** - Lazy reading, streaming, and parallel processing where supported
- **Extensibility** - Easy to add new formats with macro-based boilerplate reduction
- **Type Safety** - Compile-time format validation and option checking

### Architecture Example

```rust
use dsq_formats::{DataFormat, from_path, to_path};

// Generic reading - format auto-detected
let data = from_path("data.csv")?;

// Explicit format specification
let data = from_path_with_format("data.txt", DataFormat::Csv)?;

// Writing with format options
let writer = to_path_with_format("output.parquet", DataFormat::Parquet)?;
writer.write_dataframe(&dataframe, &write_options)?;
```

### Feature Flags

- `csv` - CSV/TSV reading and writing
- `json` - JSON and JSON Lines support
- `json5` - JSON5 format support
- `parquet` - Apache Parquet format
- `avro` - Apache Avro format

See [FORMATS.md](FORMATS.md) for detailed format documentation.

## dsq-io

Provides low-level I/O utilities and high-level data reading/writing interfaces.

### Architecture

Built around two main traits:

- **`DataReader`** - Reading data from various sources into DataFrames or LazyFrames
- **`DataWriter`** - Writing DataFrames and LazyFrames to various destinations

### Key Features

#### Low-Level I/O Operations
- Synchronous and asynchronous file reading/writing
- STDIN/STDOUT handling with proper buffering
- Memory-mapped I/O for large files
- Error handling with specific error types

#### High-Level Data Interfaces
- Format-agnostic reading/writing APIs
- Lazy evaluation support for performance
- Streaming capabilities for large datasets
- Batch processing utilities

#### Advanced Capabilities
- **File inspection** - Extract metadata without full loading
- **Format conversion** - Convert between any supported formats
- **Batch operations** - Process multiple files with consistent operations
- **Streaming processing** - Constant memory usage for large datasets

### Core Types

```rust
pub struct ReadOptions {
    pub max_rows: Option<usize>,           // Limit rows read
    pub infer_schema: bool,                // Auto-detect column types
    pub lazy: bool,                        // Use lazy evaluation
    pub skip_rows: usize,                  // Skip initial rows
    pub columns: Option<Vec<String>>,      // Select specific columns
}

pub struct WriteOptions {
    pub include_header: bool,              // Include headers in output
    pub overwrite: bool,                   // Overwrite existing files
    pub compression: Option<CompressionLevel>, // Compression settings
}
```

### Usage Examples

#### Basic File Reading
```rust
use dsq_io::{from_path, ReadOptions};

let mut reader = from_path("data.csv")?;
let options = ReadOptions::default();
let dataframe = reader.read(&options)?;
```

#### Format Conversion
```rust
use dsq_io::{convert_file, ReadOptions, WriteOptions};

let read_opts = ReadOptions::default();
let write_opts = WriteOptions::default();
convert_file("data.csv", "data.parquet", &read_opts, &write_opts)?;
```

#### Streaming Processing
```rust
use dsq_io::stream::StreamProcessor;

let processor = StreamProcessor::new(10000) // 10k row chunks
    .with_read_options(read_opts)
    .with_write_options(write_opts);

processor.process_file("large.csv", "processed.parquet", |chunk| {
    let filtered = chunk.filter(/* some condition */)?;
    Ok(Some(filtered))
})?;
```

### Dependencies

- **Polars** - DataFrame operations and I/O
- **Tokio** - Async runtime for I/O operations
- **Apache Avro** - Avro format support
- **dsq-shared** - Shared types and utilities
- **dsq-formats** - Format-specific I/O implementations

## High-Level API

The fluent API in dsq-core provides an ergonomic interface for common workflows.

### Example Usage

```rust
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
- `avro` - Apache Avro format support
- `io-arrow` - Apache Arrow IPC format support

## Performance Characteristics

dsq leverages Polars' high-performance engine:

* **Lazy Evaluation** - Operations are optimized before execution
* **Columnar Processing** - Efficient memory layout and SIMD operations
* **Parallel Execution** - Multi-threaded processing where beneficial
* **Memory Efficiency** - Streaming and chunked processing for large datasets
