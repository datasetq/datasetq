//! HTTP(S) fetching support for dsq-io
//!
//! This module provides functionality for fetching files from HTTP and HTTPS URLs.

use crate::{Error, Result};
use reqwest::Client;
use std::time::Duration;

/// Fetch a file from an HTTP(S) URL
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_io::http::fetch_http;
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
        .map_err(|e| Error::Other(format!("Failed to fetch URL {url}: {e}")))?;

    if !response.status().is_success() {
        return Err(Error::Other(format!(
            "HTTP request failed with status: {}",
            response.status()
        )));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| Error::Other(format!("Failed to read response body: {e}")))?;

    Ok(bytes.to_vec())
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
