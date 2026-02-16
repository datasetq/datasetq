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

use std::path::Path;

// Low-level I/O only - format parsing is in dsq-formats

// Re-export from dsq-formats for convenience
pub use dsq_formats::{serialize, CompressionLevel, DataFormat, FormatWriteOptions, WriteOptions};

// I/O plugins
pub use dsq_io_filesystem as filesystem;
#[cfg(feature = "http")]
pub use dsq_io_https as https;
#[cfg(feature = "huggingface")]
pub use dsq_io_huggingface as huggingface;
pub use dsq_io_uri as uri;

// Writer modules
pub mod file_writer;
pub mod memory_writer;
pub mod traits;

// Options
pub mod options;

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

/// Read all bytes from a file or URL asynchronously
///
/// Supports:
/// - Local file paths
/// - HTTP(S) URLs (when `http` feature is enabled)
/// - HuggingFace URLs with `hf://` scheme (when `huggingface` feature is enabled)
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_io::read_file;
///
/// // Local file
/// let data = read_file("data.txt").await.unwrap();
///
/// // HTTP URL (requires `http` feature)
/// let data = read_file("https://example.com/data.csv").await.unwrap();
///
/// // HuggingFace URL (requires `huggingface` feature)
/// let data = read_file("hf://datasets/user/repo/data.csv").await.unwrap();
/// ```
pub async fn read_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let path_str = path.as_ref().to_string_lossy();

    // Parse URI to determine the type
    let uri_info =
        uri::parse_uri(&path_str).map_err(|e| Error::Other(format!("Failed to parse URI: {e}")))?;

    match uri_info.scheme {
        uri::IoScheme::Http => {
            #[cfg(feature = "http")]
            {
                return https::fetch_http(&path_str)
                    .await
                    .map_err(|e| Error::Other(format!("HTTP fetch error: {e}")));
            }
            #[cfg(not(feature = "http"))]
            {
                return Err(Error::Other(
                    "HTTP support not enabled. Rebuild with --features http".to_string(),
                ));
            }
        }
        uri::IoScheme::HuggingFace => {
            #[cfg(feature = "huggingface")]
            {
                return huggingface::fetch_huggingface(&path_str)
                    .await
                    .map_err(|e| Error::Other(format!("HuggingFace fetch error: {e}")));
            }
            #[cfg(not(feature = "huggingface"))]
            {
                Err(Error::Other(
                    "HuggingFace support not enabled. Rebuild with --features huggingface"
                        .to_string(),
                ))
            }
        }
        uri::IoScheme::File => {
            // Use filesystem plugin for local files
            filesystem::read_file(path)
                .await
                .map_err(|e| Error::Other(format!("Filesystem read error: {e}")))
        }
    }
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
    filesystem::write_file(path, data)
        .await
        .map_err(|e| Error::Other(format!("Filesystem write error: {e}")))
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
    filesystem::read_stdin()
        .await
        .map_err(|e| Error::Other(format!("Stdin read error: {e}")))
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
    filesystem::write_stdout(data)
        .await
        .map_err(|e| Error::Other(format!("Stdout write error: {e}")))
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
    filesystem::write_stderr(data)
        .await
        .map_err(|e| Error::Other(format!("Stderr write error: {e}")))
}

/// Synchronous versions for compatibility
/// Read all bytes from a file or URL synchronously
///
/// Supports the same sources as `read_file()`.
pub fn read_file_sync<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let path_str = path.as_ref().to_string_lossy();

    // Parse URI to determine the type
    let uri_info =
        uri::parse_uri(&path_str).map_err(|e| Error::Other(format!("Failed to parse URI: {e}")))?;

    match uri_info.scheme {
        uri::IoScheme::Http => {
            #[cfg(feature = "http")]
            {
                https::fetch_http_sync(&path_str)
                    .map_err(|e| Error::Other(format!("HTTP fetch error: {e}")))
            }
            #[cfg(not(feature = "http"))]
            {
                return Err(Error::Other(
                    "HTTP support not enabled. Rebuild with --features http".to_string(),
                ));
            }
        }
        uri::IoScheme::HuggingFace => {
            #[cfg(feature = "huggingface")]
            {
                return huggingface::fetch_huggingface_sync(&path_str)
                    .map_err(|e| Error::Other(format!("HuggingFace fetch error: {e}")));
            }
            #[cfg(not(feature = "huggingface"))]
            {
                Err(Error::Other(
                    "HuggingFace support not enabled. Rebuild with --features huggingface"
                        .to_string(),
                ))
            }
        }
        uri::IoScheme::File => filesystem::read_file_sync(path)
            .map_err(|e| Error::Other(format!("Filesystem read error: {e}"))),
    }
}

/// Write bytes to a file synchronously
pub fn write_file_sync<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
    filesystem::write_file_sync(path, data)
        .map_err(|e| Error::Other(format!("Filesystem write error: {e}")))
}

/// Read all bytes from STDIN synchronously
pub fn read_stdin_sync() -> Result<Vec<u8>> {
    filesystem::read_stdin_sync().map_err(|e| Error::Other(format!("Stdin read error: {e}")))
}

/// Write bytes to STDOUT synchronously
pub fn write_stdout_sync(data: &[u8]) -> Result<()> {
    filesystem::write_stdout_sync(data)
        .map_err(|e| Error::Other(format!("Stdout write error: {e}")))
}

/// Write bytes to STDERR synchronously
pub fn write_stderr_sync(data: &[u8]) -> Result<()> {
    filesystem::write_stderr_sync(data)
        .map_err(|e| Error::Other(format!("Stderr write error: {e}")))
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
