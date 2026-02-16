//! URI routing for dsq I/O plugins
//!
//! This crate provides functionality for parsing URIs and determining which
//! I/O plugin should handle them.

/// Error type for URI operations
pub type Result<T> = std::result::Result<T, Error>;

/// URI error type
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid URI: {0}")]
    InvalidUri(String),
    #[error("Unsupported URI scheme: {0}")]
    UnsupportedScheme(String),
    #[error("Other error: {0}")]
    Other(String),
}

/// Represents the type of I/O operation to perform
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoScheme {
    /// Local filesystem
    File,
    /// HTTP or HTTPS URL
    Http,
    /// HuggingFace Hub URL (hf://)
    HuggingFace,
}

/// Information about a URI
#[derive(Debug, Clone)]
pub struct UriInfo {
    /// The original URI string
    pub uri: String,
    /// The scheme/protocol
    pub scheme: IoScheme,
    /// The path component (without scheme)
    pub path: String,
}

impl UriInfo {
    /// Parse a URI string and determine its type
    pub fn parse<S: AsRef<str>>(uri: S) -> Result<Self> {
        let uri_str = uri.as_ref();

        // Check for HTTP(S)
        if uri_str.starts_with("http://") || uri_str.starts_with("https://") {
            return Ok(Self {
                uri: uri_str.to_string(),
                scheme: IoScheme::Http,
                path: uri_str.to_string(),
            });
        }

        // Check for HuggingFace
        if uri_str.starts_with("hf://") {
            return Ok(Self {
                uri: uri_str.to_string(),
                scheme: IoScheme::HuggingFace,
                path: uri_str[5..].to_string(), // Strip "hf://"
            });
        }

        // Default to local filesystem
        Ok(Self {
            uri: uri_str.to_string(),
            scheme: IoScheme::File,
            path: uri_str.to_string(),
        })
    }

    /// Check if this URI represents a URL
    pub fn is_url(&self) -> bool {
        matches!(self.scheme, IoScheme::Http | IoScheme::HuggingFace)
    }

    /// Check if this URI represents a local file
    pub fn is_file(&self) -> bool {
        matches!(self.scheme, IoScheme::File)
    }
}

/// Parse a path/URI and determine its type
pub fn parse_uri<S: AsRef<str>>(uri: S) -> Result<UriInfo> {
    UriInfo::parse(uri)
}

/// Check if a string is an HTTP(S) URL
pub fn is_http_url(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://")
}

/// Check if a string is a HuggingFace URL
pub fn is_huggingface_url(s: &str) -> bool {
    s.starts_with("hf://")
}

/// Check if a string is a URL (any remote resource)
pub fn is_url(s: &str) -> bool {
    is_http_url(s) || is_huggingface_url(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_http_url() {
        let info = parse_uri("https://example.com/data.csv").unwrap();
        assert_eq!(info.scheme, IoScheme::Http);
        assert!(info.is_url());
        assert!(!info.is_file());
    }

    #[test]
    fn test_parse_huggingface_url() {
        let info = parse_uri("hf://datasets/user/repo/data.csv").unwrap();
        assert_eq!(info.scheme, IoScheme::HuggingFace);
        assert!(info.is_url());
        assert!(!info.is_file());
    }

    #[test]
    fn test_parse_local_file() {
        let info = parse_uri("/path/to/file.csv").unwrap();
        assert_eq!(info.scheme, IoScheme::File);
        assert!(!info.is_url());
        assert!(info.is_file());
    }

    #[test]
    fn test_is_http_url() {
        assert!(is_http_url("http://example.com"));
        assert!(is_http_url("https://example.com"));
        assert!(!is_http_url("hf://example"));
        assert!(!is_http_url("/path/to/file"));
    }

    #[test]
    fn test_is_huggingface_url() {
        assert!(is_huggingface_url("hf://datasets/user/repo"));
        assert!(!is_huggingface_url("https://example.com"));
        assert!(!is_huggingface_url("/path/to/file"));
    }
}
