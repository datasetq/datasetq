//! Error types for the DSQ parser

use std::fmt;

/// Errors that can occur during parsing
#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    /// Unexpected token encountered
    UnexpectedToken {
        /// The unexpected token
        found: String,
        /// Expected tokens (if known)
        expected: Vec<String>,
        /// Position in the input
        position: usize,
    },

    /// Invalid syntax
    InvalidSyntax {
        /// Description of the syntax error
        message: String,
        /// Position in the input
        position: usize,
    },

    /// Unterminated string literal
    UnterminatedString {
        /// Position where the string starts
        position: usize,
    },

    /// Invalid number literal
    InvalidNumber {
        /// The invalid number string
        number: String,
        /// Position in the input
        position: usize,
    },

    /// Unknown function name
    UnknownFunction {
        /// The function name
        name: String,
        /// Position in the input
        position: usize,
    },

    /// Invalid field access
    InvalidFieldAccess {
        /// The invalid field string
        field: String,
        /// Position in the input
        position: usize,
    },

    /// Mismatched parentheses or brackets
    MismatchedBrackets {
        /// The opening bracket
        opening: char,
        /// Position of the opening bracket
        position: usize,
    },

    /// Empty input
    EmptyInput,

    /// General parsing error
    General {
        /// Error message
        message: String,
    },

    /// Nom parsing error (internal)
    NomError {
        /// Error message from nom
        message: String,
    },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedToken {
                found,
                expected,
                position,
            } => {
                write!(f, "Unexpected token '{}' at position {}", found, position)?;
                if !expected.is_empty() {
                    write!(f, ". Expected one of: {}", expected.join(", "))?;
                }
                Ok(())
            }
            ParseError::InvalidSyntax { message, position } => {
                write!(f, "Invalid syntax at position {}: {}", position, message)
            }
            ParseError::UnterminatedString { position } => {
                write!(
                    f,
                    "Unterminated string literal starting at position {}",
                    position
                )
            }
            ParseError::InvalidNumber { number, position } => {
                write!(f, "Invalid number '{}' at position {}", number, position)
            }
            ParseError::UnknownFunction { name, position } => {
                write!(f, "Unknown function '{}' at position {}", name, position)
            }
            ParseError::InvalidFieldAccess { field, position } => {
                write!(
                    f,
                    "Invalid field access '{}' at position {}",
                    field, position
                )
            }
            ParseError::MismatchedBrackets { opening, position } => {
                write!(
                    f,
                    "Mismatched brackets. Opening '{}' at position {} has no matching close",
                    opening, position
                )
            }
            ParseError::EmptyInput => write!(f, "Empty input"),
            ParseError::General { message } => write!(f, "{}", message),
            ParseError::NomError { message } => write!(f, "Nom error: {}", message),
        }
    }
}

impl std::error::Error for ParseError {}

/// Result type for parsing operations
pub type Result<T> = std::result::Result<T, ParseError>;

impl From<nom::Err<nom::error::Error<&str>>> for ParseError {
    fn from(err: nom::Err<nom::error::Error<&str>>) -> Self {
        match err {
            nom::Err::Error(e) | nom::Err::Failure(e) => {
                // Position calculation is not accurate without original input length
                let position = 0;
                ParseError::InvalidSyntax {
                    message: format!("Parse error: {}", e.code.description()),
                    position,
                }
            }
            nom::Err::Incomplete(_) => ParseError::General {
                message: "Incomplete input".to_string(),
            },
        }
    }
}
