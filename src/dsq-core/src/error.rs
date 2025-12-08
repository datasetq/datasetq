use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt;
use std::io;

use dsq_formats;
#[cfg(feature = "io")]
use dsq_io;

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

    /// Filter compilation or execution errors
    Filter(FilterError),

    /// Type conversion errors
    Type(TypeError),

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
}

/// Errors related to filter compilation and execution
#[derive(Debug, Clone)]
pub enum FilterError {
    /// Parse error from jaq parser
    Parse(String),

    /// Compilation error when converting jaq AST to dsq operations
    Compile(String),

    /// Runtime error during filter execution
    Runtime(String),

    /// Undefined variable or function
    Undefined(String),

    /// Type mismatch in filter operation
    TypeMismatch {
        /// Expected type
        expected: String,
        /// Actual type
        actual: String,
    },

    /// Invalid argument count for function
    ArgumentCount {
        /// Expected number of arguments
        expected: usize,
        /// Actual number of arguments
        actual: usize,
    },
}

/// Type conversion and compatibility errors
#[derive(Debug, Clone)]
pub enum TypeError {
    /// Cannot convert between types
    InvalidConversion {
        /// Source type
        from: String,
        /// Target type
        to: String,
    },

    /// Operation not supported for type
    UnsupportedOperation {
        /// Operation name
        operation: String,
        /// Type name
        typ: String,
    },

    /// Field not found in object or `DataFrame`
    FieldNotFound {
        /// Field name
        field: String,
        /// Type name
        typ: String,
    },

    /// Value out of range for target type
    OutOfRange(String),

    /// Null value where not expected
    UnexpectedNull(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Polars(e) => write!(f, "DataFrame error: {e}"),
            Error::Json(e) => write!(f, "JSON error: {e}"),
            Error::Format(e) => write!(f, "Format error: {e}"),
            Error::Filter(e) => write!(f, "Filter error: {e}"),
            Error::Type(e) => write!(f, "Type error: {e}"),
            Error::Operation(msg) => write!(f, "Operation error: {msg}"),
            Error::Config(msg) => write!(f, "Configuration error: {msg}"),
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
                write!(f, "Unknown format: {format}")
            }
            FormatError::DetectionFailed(path) => {
                write!(f, "Failed to detect format for: {path}")
            }
            FormatError::UnsupportedFeature(feature) => {
                write!(f, "Unsupported feature: {feature}")
            }
            FormatError::SchemaMismatch { expected, actual } => {
                write!(f, "Schema mismatch: expected {expected}, got {actual}")
            }
            FormatError::InvalidOption(option) => {
                write!(f, "Invalid format option: {option}")
            }
        }
    }
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterError::Parse(msg) => write!(f, "Parse error: {msg}"),
            FilterError::Compile(msg) => write!(f, "Compilation error: {msg}"),
            FilterError::Runtime(msg) => write!(f, "Runtime error: {msg}"),
            FilterError::Undefined(name) => write!(f, "Undefined: {name}"),
            FilterError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {expected}, got {actual}")
            }
            FilterError::ArgumentCount { expected, actual } => {
                write!(f, "Wrong argument count: expected {expected}, got {actual}")
            }
        }
    }
}

impl fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeError::InvalidConversion { from, to } => {
                write!(f, "Cannot convert from {from} to {to}")
            }
            TypeError::UnsupportedOperation { operation, typ } => {
                write!(f, "Operation '{operation}' not supported for type {typ}")
            }
            TypeError::FieldNotFound { field, typ } => {
                write!(f, "Field '{field}' not found in {typ}")
            }
            TypeError::OutOfRange(msg) => write!(f, "Value out of range: {msg}"),
            TypeError::UnexpectedNull(context) => {
                write!(f, "Unexpected null value in: {context}")
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
impl StdError for FilterError {}
impl StdError for TypeError {}

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

impl From<FormatError> for Error {
    fn from(e: FormatError) -> Self {
        Error::Format(e)
    }
}

impl From<FilterError> for Error {
    fn from(e: FilterError) -> Self {
        Error::Filter(e)
    }
}

impl From<TypeError> for Error {
    fn from(e: TypeError) -> Self {
        Error::Type(e)
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::Operation(Cow::Owned(e.to_string()))
    }
}

#[cfg(feature = "io")]
impl From<dsq_io::Error> for Error {
    fn from(e: dsq_io::Error) -> Self {
        Error::Operation(Cow::Owned(e.to_string()))
    }
}

impl From<dsq_formats::Error> for Error {
    fn from(e: dsq_formats::Error) -> Self {
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

    /// Create an operation error from shared utility
    pub fn from_operation_error(msg: impl Into<Cow<'static, str>>) -> Self {
        Error::Operation(msg.into())
    }

    /// Create a configuration error from shared utility
    pub fn from_config_error(msg: impl Into<String>) -> Self {
        Error::Config(msg.into())
    }

    /// Combine multiple errors into a single error
    #[must_use]
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

impl FilterError {
    /// Create a parse error
    pub fn parse(msg: impl Into<String>) -> Self {
        FilterError::Parse(msg.into())
    }

    /// Create a compilation error
    pub fn compile(msg: impl Into<String>) -> Self {
        FilterError::Compile(msg.into())
    }

    /// Create a runtime error
    pub fn runtime(msg: impl Into<String>) -> Self {
        FilterError::Runtime(msg.into())
    }
}

impl TypeError {
    /// Create an invalid conversion error
    pub fn invalid_conversion(from: impl Into<String>, to: impl Into<String>) -> Self {
        TypeError::InvalidConversion {
            from: from.into(),
            to: to.into(),
        }
    }

    /// Create an unsupported operation error
    pub fn unsupported_operation(operation: impl Into<String>, typ: impl Into<String>) -> Self {
        TypeError::UnsupportedOperation {
            operation: operation.into(),
            typ: typ.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::operation(Cow::Borrowed("test operation failed"));
        assert_eq!(err.to_string(), "Operation error: test operation failed");

        let err = FormatError::unknown("xyz");
        assert_eq!(err.to_string(), "Unknown format: xyz");

        let err = FilterError::parse("unexpected token");
        assert_eq!(err.to_string(), "Parse error: unexpected token");

        let err = TypeError::invalid_conversion("string", "number");
        assert_eq!(err.to_string(), "Cannot convert from string to number");
    }

    #[test]
    fn test_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));

        let format_err = FormatError::unknown("test");
        let err: Error = format_err.into();
        assert!(matches!(err, Error::Format(_)));
    }

    #[test]
    fn test_multiple_errors() {
        let errors = vec![
            Error::operation(Cow::Borrowed("error 1")),
            Error::operation(Cow::Borrowed("error 2")),
        ];
        let combined = Error::combine(errors);
        assert!(matches!(combined, Error::Multiple(_)));
    }
}
