# dsq-io

I/O utilities for DSQ - handles reading and writing to disk, STDIN, STDOUT.

## Overview

`dsq-io` provides I/O abstractions and utilities for the DSQ ecosystem. It handles reading from various sources (files, STDIN, URLs) and writing to various destinations (files, STDOUT, STDERR), with support for streaming, buffering, and format detection.

## Features

- **Multiple sources**: Files, STDIN, pipes
- **Multiple destinations**: Files, STDOUT, STDERR
- **Streaming I/O**: Efficient processing of large datasets
- **Async support**: Non-blocking I/O with Tokio
- **Format-aware**: Integrates with dsq-formats for automatic format detection
- **Error handling**: Comprehensive error types for I/O operations
- **Buffer management**: Configurable buffering strategies

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
dsq-io = "0.1"
```

## Usage

### Reading from Files

```rust
use dsq_io::read_file;

#[tokio::main]
async fn main() {
    let data = read_file("data.json")
        .await
        .expect("Failed to read file");

    println!("Read {} bytes", data.len());
}
```

### Reading from STDIN

```rust
use dsq_io::read_stdin;

#[tokio::main]
async fn main() {
    let data = read_stdin()
        .await
        .expect("Failed to read from STDIN");

    println!("Received {} bytes from STDIN", data.len());
}
```

### Writing to Files

```rust
use dsq_io::write_file;

#[tokio::main]
async fn main() {
    let data = b"Hello, World!";

    write_file("output.txt", data)
        .await
        .expect("Failed to write file");
}
```

### Writing to STDOUT

```rust
use dsq_io::write_stdout;

#[tokio::main]
async fn main() {
    let data = b"Result data";

    write_stdout(data)
        .await
        .expect("Failed to write to STDOUT");
}
```

### Streaming Data

```rust
use dsq_io::{StreamReader, StreamWriter};
use tokio::io::AsyncBufReadExt;

#[tokio::main]
async fn main() {
    let mut reader = StreamReader::from_file("large_file.csv")
        .await
        .expect("Failed to open file");

    let mut writer = StreamWriter::to_file("output.csv")
        .await
        .expect("Failed to create output file");

    // Process line by line
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await.unwrap() {
        // Process line
        writer.write_line(&line).await.expect("Failed to write");
    }

    writer.flush().await.expect("Failed to flush");
}
```

### Auto-detecting Format

```rust
use dsq_io::read_data_file;
use dsq_formats::Format;

#[tokio::main]
async fn main() {
    // Automatically detects format and parses
    let (format, dataframe) = read_data_file("data.csv")
        .await
        .expect("Failed to read data file");

    println!("Detected format: {:?}", format);
    println!("Loaded {} rows", dataframe.height());
}
```

## API Components

### Input Sources

- `read_file()`: Read from a file path
- `read_stdin()`: Read from standard input
- `read_bytes()`: Read raw bytes
- `read_data_file()`: Read and parse data file with format detection

### Output Destinations

- `write_file()`: Write to a file path
- `write_stdout()`: Write to standard output
- `write_stderr()`: Write to standard error
- `write_bytes()`: Write raw bytes

### Streaming

- `StreamReader`: Async buffered reader
- `StreamWriter`: Async buffered writer
- `LineReader`: Line-by-line reading
- `ChunkReader`: Chunked reading for large files

### Utilities

- `detect_source_type()`: Determine if input is file, STDIN, or pipe
- `is_tty()`: Check if output is a terminal
- `ensure_directory()`: Create directory if it doesn't exist
- `temp_file()`: Create temporary file

## Configuration

### Buffer Sizes

```rust
use dsq_io::{IoConfig, StreamReader};

let config = IoConfig {
    buffer_size: 64 * 1024, // 64 KB buffer
    read_timeout: Some(Duration::from_secs(30)),
    ..Default::default()
};

let reader = StreamReader::with_config("file.dat", config)
    .await?;
```

### Error Handling

```rust
use dsq_io::{read_file, IoError};

#[tokio::main]
async fn main() {
    match read_file("missing.txt").await {
        Ok(data) => println!("Success: {} bytes", data.len()),
        Err(IoError::FileNotFound(path)) => {
            eprintln!("File not found: {}", path);
        }
        Err(IoError::PermissionDenied(path)) => {
            eprintln!("Permission denied: {}", path);
        }
        Err(e) => eprintln!("I/O error: {}", e),
    }
}
```

## Platform Support

- **Linux**: Full support
- **macOS**: Full support
- **Windows**: Full support
- **WASM**: Limited support (no file system access)

## API Documentation

For detailed API documentation, see [docs.rs/dsq-io](https://docs.rs/dsq-io).

## Performance

I/O operations are optimized for:

- Large file handling with buffering
- Async I/O to prevent blocking
- Memory-efficient streaming
- Minimal system calls

## Contributing

Contributions are welcome! Please see the [CONTRIBUTING.md](../../CONTRIBUTING.md) file in the repository root for guidelines.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT license ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
