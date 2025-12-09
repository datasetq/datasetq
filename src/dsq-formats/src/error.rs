use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt;
use std::io;

/// Result type alias for dsq operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for dsq operations
#[derive(Debug)]
pub enum Error {
    /// I/O errors (file operations, etc.)
    Io(io::Error),

    /// Polars errors (`DataFrame` operations)
    Polars(polars::error::PolarsError),

    /// JSON parsing errors
    Json(serde_json::Error),

    /// Format detection or parsing errors
    Format(FormatError),

    /// General operation errors
    Operation(Cow<'static, str>),

    /// Configuration errors
    Config(String),

    /// Multiple errors collected during processing
    Multiple(Vec<Error>),
}

/// Errors related to file format handling
#[derive(Debug, Clone)]
pub enum FormatError {
    /// Unknown or unsupported file format
    Unknown(String),

    /// Failed to detect format from file extension
    DetectionFailed(String),

    /// Format is supported but specific feature is not
    UnsupportedFeature(String),

    /// Schema mismatch between expected and actual
    SchemaMismatch {
        /// Expected schema
        expected: String,
        /// Actual schema
        actual: String,
    },

    /// Invalid format-specific options
    InvalidOption(String),

    /// Serialization/deserialization errors
    SerializationError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Polars(e) => write!(f, "DataFrame error: {}", e),
            Error::Json(e) => write!(f, "JSON error: {}", e),
            Error::Format(e) => write!(f, "Format error: {}", e),
            Error::Operation(msg) => write!(f, "Operation error: {}", msg),
            Error::Config(msg) => write!(f, "Configuration error: {}", msg),
            Error::Multiple(errors) => {
                write!(f, "Multiple errors occurred:")?;
                for (i, e) in errors.iter().enumerate() {
                    write!(f, "\n  {}. {}", i + 1, e)?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FormatError::Unknown(format) => {
                write!(f, "Unknown format: {}", format)
            }
            FormatError::DetectionFailed(path) => {
                write!(f, "Failed to detect format for: {}", path)
            }
            FormatError::UnsupportedFeature(feature) => {
                write!(f, "Unsupported feature: {}", feature)
            }
            FormatError::SchemaMismatch { expected, actual } => {
                write!(f, "Schema mismatch: expected {}, got {}", expected, actual)
            }
            FormatError::InvalidOption(option) => {
                write!(f, "Invalid format option: {}", option)
            }
            FormatError::SerializationError(msg) => {
                write!(f, "Serialization error: {}", msg)
            }
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Polars(e) => Some(e),
            Error::Json(e) => Some(e),
            _ => None,
        }
    }
}

impl StdError for FormatError {}

// Conversion implementations
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<polars::error::PolarsError> for Error {
    fn from(e: polars::error::PolarsError) -> Self {
        Error::Polars(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

impl From<apache_avro::Error> for Error {
    fn from(e: apache_avro::Error) -> Self {
        Error::Format(FormatError::SerializationError(e.to_string()))
    }
}

impl From<FormatError> for Error {
    fn from(e: FormatError) -> Self {
        Error::Format(e)
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::Operation(Cow::Owned(e.to_string()))
    }
}

// Helper functions for creating common errors
impl Error {
    /// Create an operation error with a custom message
    pub fn operation(msg: impl Into<Cow<'static, str>>) -> Self {
        Error::Operation(msg.into())
    }

    /// Create a configuration error with a custom message
    pub fn config(msg: impl Into<String>) -> Self {
        Error::Config(msg.into())
    }

    /// Combine multiple errors into a single error
    pub fn combine(errors: Vec<Error>) -> Self {
        match errors.len() {
            0 => Error::operation(Cow::Borrowed("No errors")),
            1 => errors.into_iter().next().unwrap(),
            _ => Error::Multiple(errors),
        }
    }
}

impl FormatError {
    /// Create an unknown format error
    pub fn unknown(format: impl Into<String>) -> Self {
        FormatError::Unknown(format.into())
    }

    /// Create a detection failed error
    pub fn detection_failed(path: impl Into<String>) -> Self {
        FormatError::DetectionFailed(path.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        // Test Error variants
        let err = Error::operation(Cow::Borrowed("test operation failed"));
        assert_eq!(err.to_string(), "Operation error: test operation failed");

        let err = Error::config("test config failed".to_string());
        assert_eq!(err.to_string(), "Configuration error: test config failed");

        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = Error::Io(io_err);
        assert_eq!(err.to_string(), "I/O error: file not found");

        // For Polars and Json, we can't easily test exact string without creating real errors
        // But we can test the prefix
        let polars_err = polars::error::PolarsError::from(io::Error::new(
            io::ErrorKind::Other,
            "test polars error",
        ));
        let err = Error::Polars(polars_err);
        assert!(err.to_string().starts_with("DataFrame error:"));

        let json_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let err = Error::Json(json_err);
        assert!(err.to_string().starts_with("JSON error:"));

        let format_err = FormatError::unknown("xyz");
        let err = Error::Format(format_err);
        assert_eq!(err.to_string(), "Format error: Unknown format: xyz");

        // Test Multiple
        let errors = vec![
            Error::operation(Cow::Borrowed("error 1")),
            Error::operation(Cow::Borrowed("error 2")),
        ];
        let combined = Error::combine(errors);
        let display = combined.to_string();
        assert!(display.starts_with("Multiple errors occurred:"));
        assert!(display.contains("1. Operation error: error 1"));
        assert!(display.contains("2. Operation error: error 2"));
    }

    #[test]
    fn test_format_error_display() {
        let err = FormatError::Unknown("xyz".to_string());
        assert_eq!(err.to_string(), "Unknown format: xyz");

        let err = FormatError::DetectionFailed("path/to/file".to_string());
        assert_eq!(err.to_string(), "Failed to detect format for: path/to/file");

        let err = FormatError::UnsupportedFeature("streaming".to_string());
        assert_eq!(err.to_string(), "Unsupported feature: streaming");

        let err = FormatError::SchemaMismatch {
            expected: "schema1".to_string(),
            actual: "schema2".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Schema mismatch: expected schema1, got schema2"
        );

        let err = FormatError::InvalidOption("option=value".to_string());
        assert_eq!(err.to_string(), "Invalid format option: option=value");

        let err = FormatError::SerializationError("failed to serialize".to_string());
        assert_eq!(err.to_string(), "Serialization error: failed to serialize");
    }

    #[test]
    fn test_error_conversion() {
        // Io
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));

        // Polars
        let polars_err =
            polars::error::PolarsError::from(io::Error::new(io::ErrorKind::Other, "test"));
        let err: Error = polars_err.into();
        assert!(matches!(err, Error::Polars(_)));

        // Json
        let json_err = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let err: Error = json_err.into();
        assert!(matches!(err, Error::Json(_)));

        // Format
        let format_err = FormatError::unknown("test");
        let err: Error = format_err.into();
        assert!(matches!(err, Error::Format(_)));

        // Anyhow (if available, but since it's in impl, assume it's there)
        // Note: anyhow::Error might not be imported, but the impl is there
    }

    #[test]
    fn test_error_source() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = Error::Io(io_err);
        assert!(err.source().is_some());

        let polars_err = polars::error::PolarsError::from(io::Error::other("test"));
        let err = Error::Polars(polars_err);
        assert!(err.source().is_some());

        let json_err = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let err = Error::Json(json_err);
        assert!(err.source().is_some());

        let err = Error::operation("test");
        assert!(err.source().is_none());

        let err = Error::config("test");
        assert!(err.source().is_none());

        let format_err = FormatError::unknown("test");
        let err = Error::Format(format_err);
        assert!(err.source().is_none());

        let errors = vec![Error::operation("1"), Error::operation("2")];
        let err = Error::combine(errors);
        assert!(err.source().is_none());
    }

    #[test]
    fn test_helper_functions() {
        // Error::operation
        let err = Error::operation("test msg");
        assert!(matches!(err, Error::Operation(_)));
        assert_eq!(err.to_string(), "Operation error: test msg");

        let err = Error::operation(Cow::Owned("owned".to_string()));
        assert!(matches!(err, Error::Operation(_)));

        // Error::config
        let err = Error::config("config msg");
        assert!(matches!(err, Error::Config(_)));
        assert_eq!(err.to_string(), "Configuration error: config msg");

        // FormatError::unknown
        let err = FormatError::unknown("fmt");
        assert!(matches!(err, FormatError::Unknown(_)));
        assert_eq!(err.to_string(), "Unknown format: fmt");

        // FormatError::detection_failed
        let err = FormatError::detection_failed("path");
        assert!(matches!(err, FormatError::DetectionFailed(_)));
        assert_eq!(err.to_string(), "Failed to detect format for: path");
    }

    #[test]
    fn test_error_combine() {
        // Empty
        let combined = Error::combine(vec![]);
        assert!(matches!(combined, Error::Operation(_)));
        assert_eq!(combined.to_string(), "Operation error: No errors");

        // Single
        let single = Error::operation("single");
        let combined = Error::combine(vec![single]);
        assert!(matches!(combined, Error::Operation(_)));
        assert_eq!(combined.to_string(), "Operation error: single".to_string());

        // Multiple
        let errors = vec![
            Error::operation("error 1"),
            Error::operation("error 2"),
            Error::config("config error"),
        ];
        let combined = Error::combine(errors);
        assert!(matches!(combined, Error::Multiple(_)));
        let display = combined.to_string();
        assert!(display.contains("Multiple errors occurred:"));
        assert!(display.contains("1. Operation error: error 1"));
        assert!(display.contains("2. Operation error: error 2"));
        assert!(display.contains("3. Configuration error: config error"));
    }

    #[test]
    fn test_format_error_clone() {
        let err = FormatError::Unknown("test".to_string());
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());
    }
}
