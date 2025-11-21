//! dsq-parser: Parser for DSQ filter language
//!
//! This crate provides parsing functionality for the DSQ filter language,
//! converting filter strings into Abstract Syntax Tree (AST) representations
//! using the nom parser combinator library.
//!
//! # Features
//!
//! - **Complete DSQ Syntax Support**: Parses all DSQ filter language constructs
//! - **Fast Parsing**: Uses nom for high-performance parsing
//! - **Comprehensive Error Reporting**: Detailed error messages with position information
//! - **AST Generation**: Produces structured AST for further processing
//!
//! # Quick Start
//!
//! ```rust
//! use dsq_parser::{FilterParser, Filter};
//!
//! let parser = FilterParser::new();
//! let filter: Filter = parser.parse(".name | length")?;
//!
//! // Access the parsed AST
//! match &filter.expr {
//!     dsq_parser::Expr::Pipeline(exprs) => {
//!         println!("Pipeline with {} expressions", exprs.len());
//!     }
//!     _ => {}
//! }
//! # Ok::<(), dsq_parser::ParseError>(())
//! ```
//!
//! # Supported Syntax
//!
//! The parser supports the full DSQ filter language including:
//!
//! - **Identity and field access**: `.`, `.field`, `.field.subfield`
//! - **Array operations**: `.[0]`, `.[1:5]`, `.[]`
//! - **Function calls**: `length`, `map(select(.age > 30))`
//! - **Arithmetic**: `+`, `-`, `*`, `/`
//! - **Comparisons**: `>`, `<`, `==`, `!=`, `>=`, `<=`
//! - **Logical operations**: `and`, `or`, `not`
//! - **Object/array construction**: `{name, age}`, `[1, 2, 3]`
//! - **Pipelines**: `expr1 | expr2 | expr3`
//! - **Assignment**: `. += value`
//!
//! # Error Handling
//!
//! Parse errors include position information and expected tokens:
//!
//! ```rust
//! use dsq_parser::{FilterParser, ParseError};
//!
//! let parser = FilterParser::new();
//! match parser.parse("invalid syntax +++") {
//!     Ok(_) => {}
//!     Err(ParseError::UnexpectedToken { found, position, .. }) => {
//!         eprintln!("Unexpected '{}' at position {}", found, position);
//!     }
//!     Err(e) => eprintln!("Parse error: {}", e),
//! }
//! ```

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::similar_names,
    clippy::too_many_lines
)]

pub mod ast;
pub mod error;
mod parser;
#[cfg(test)]
mod tests;

// Re-export main types
pub use ast::*;
pub use error::*;
pub use parser::*;

// Re-export shared types
pub use dsq_shared::VERSION;
