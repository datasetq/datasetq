//! dsq-io: Low-level I/O utilities for dsq
//!
//! This crate provides basic functions for reading and writing bytes to/from
//! files, STDIN, and STDOUT. It handles the low-level I/O operations without
//! any format-specific logic.
//!
//! # Features
//!
//! - Synchronous and asynchronous I/O
//! - File reading/writing
//! - STDIN/STDOUT handling
//! - Network I/O (planned)
//!
//! # Examples
//!
//! Reading from a file:
//! ```rust,ignore
//! use dsq_io::read_file;
//!
//! let data = read_file("data.txt").await.unwrap();
//! ```
//!
//! Writing to STDOUT:
//! ```rust,ignore
//! use dsq_io::write_stdout;
//!
//! write_stdout(b"Hello, world!").await.unwrap();
//! ```

use std::io::{self as std_io, Read, Write};
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Low-level I/O only - format parsing is in dsq-formats

// Re-export from dsq-formats for convenience
pub use dsq_formats::{serialize, CompressionLevel, DataFormat, FormatWriteOptions, WriteOptions};

// Writer modules
pub mod file_writer;
pub mod memory_writer;
pub mod traits;

// Re-export writer types
pub use file_writer::{to_path, to_path_with_format, FileWriter};
pub use memory_writer::{to_memory, MemoryWriter};
pub use traits::DataWriter;

/// Error type for I/O operations
pub type Result<T> = std::result::Result<T, Error>;

/// I/O error type
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),
    #[error("Format error: {0}")]
    Format(String),
    #[error("Other error: {0}")]
    Other(String),
}

impl From<dsq_formats::Error> for Error {
    fn from(e: dsq_formats::Error) -> Self {
        Error::Other(e.to_string())
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::Other(e.to_string())
    }
}

impl From<apache_avro::Error> for Error {
    fn from(e: apache_avro::Error) -> Self {
        Error::Other(e.to_string())
    }
}

impl Error {
    /// Create an operation error with a custom message
    pub fn operation(msg: impl Into<String>) -> Self {
        Error::Other(msg.into())
    }
}

/// Read all bytes from a file asynchronously
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_io::read_file;
///
/// let data = read_file("data.txt").await.unwrap();
/// ```
pub async fn read_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    fs::read(path).await.map_err(Error::from)
}

/// Write bytes to a file asynchronously
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_io::write_file;
///
/// write_file("output.txt", b"Hello, world!").await.unwrap();
/// ```
pub async fn write_file<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
    fs::write(path, data).await.map_err(Error::from)
}

/// Read all bytes from STDIN asynchronously
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_io::read_stdin;
///
/// let data = read_stdin().await.unwrap();
/// ```
pub async fn read_stdin() -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    tokio::io::stdin().read_to_end(&mut buffer).await?;
    Ok(buffer)
}

/// Write bytes to STDOUT asynchronously
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_io::write_stdout;
///
/// write_stdout(b"Hello, world!").await.unwrap();
/// ```
pub async fn write_stdout(data: &[u8]) -> Result<()> {
    let mut stdout = tokio::io::stdout();
    stdout.write_all(data).await?;
    stdout.flush().await?;
    Ok(())
}

/// Write bytes to STDERR asynchronously
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_io::write_stderr;
///
/// write_stderr(b"Error message").await.unwrap();
/// ```
pub async fn write_stderr(data: &[u8]) -> Result<()> {
    let mut stderr = tokio::io::stderr();
    stderr.write_all(data).await?;
    stderr.flush().await?;
    Ok(())
}

/// Synchronous versions for compatibility
/// Read all bytes from a file synchronously
pub fn read_file_sync<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(Error::from)
}

/// Write bytes to a file synchronously
pub fn write_file_sync<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
    std::fs::write(path, data).map_err(Error::from)
}

/// Read all bytes from STDIN synchronously
pub fn read_stdin_sync() -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    std_io::stdin().read_to_end(&mut buffer)?;
    Ok(buffer)
}

/// Write bytes to STDOUT synchronously
pub fn write_stdout_sync(data: &[u8]) -> Result<()> {
    std_io::stdout().write_all(data)?;
    std_io::stdout().flush()?;
    Ok(())
}

/// Write bytes to STDERR synchronously
pub fn write_stderr_sync(data: &[u8]) -> Result<()> {
    std_io::stderr().write_all(data)?;
    std_io::stderr().flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_read_write_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let test_data = b"Hello, world!";

        write_file(temp_file.path(), test_data).await.unwrap();
        let read_data = read_file(temp_file.path()).await.unwrap();

        assert_eq!(read_data, test_data);
    }

    #[test]
    fn test_read_write_file_sync() {
        let temp_file = NamedTempFile::new().unwrap();
        let test_data = b"Hello, world!";

        write_file_sync(temp_file.path(), test_data).unwrap();
        let read_data = read_file_sync(temp_file.path()).unwrap();

        assert_eq!(read_data, test_data);
    }
}
