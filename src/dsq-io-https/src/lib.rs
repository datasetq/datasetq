//! HTTP(S) I/O plugin for dsq
//!
//! This crate provides functionality for fetching files from HTTP and HTTPS URLs.

use reqwest::Client;
use std::time::Duration;

/// Error type for HTTP I/O operations
pub type Result<T> = std::result::Result<T, Error>;

/// HTTP I/O error type
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("Other error: {0}")]
    Other(String),
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Http(e.to_string())
    }
}

/// Fetch a file from an HTTP(S) URL
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_io_https::fetch_http;
///
/// let data = fetch_http("https://example.com/data.csv").await.unwrap();
/// ```
pub async fn fetch_http(url: &str) -> Result<Vec<u8>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(300)) // 5 minute timeout
        .build()
        .map_err(|e| Error::Other(format!("Failed to create HTTP client: {e}")))?;

    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| Error::Http(format!("Failed to fetch URL {url}: {e}")))?;

    if !response.status().is_success() {
        return Err(Error::Http(format!(
            "HTTP request failed with status: {}",
            response.status()
        )));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| Error::Http(format!("Failed to read response body: {e}")))?;

    Ok(bytes.to_vec())
}

/// Synchronous version using tokio runtime
pub fn fetch_http_sync(url: &str) -> Result<Vec<u8>> {
    tokio::runtime::Runtime::new()
        .map_err(|e| Error::Other(format!("Failed to create runtime: {e}")))?
        .block_on(fetch_http(url))
}

/// Check if a string is an HTTP(S) URL
pub fn is_http_url(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_http_url() {
        assert!(is_http_url("http://example.com/data.csv"));
        assert!(is_http_url("https://example.com/data.csv"));
        assert!(!is_http_url("file:///data.csv"));
        assert!(!is_http_url("/path/to/file.csv"));
        assert!(!is_http_url("data.csv"));
    }
}
