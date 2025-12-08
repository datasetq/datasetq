//! Parser implementation for DSQ filter language
//!
//! This module contains the main parser that converts DSQ filter strings
//! into AST representations using nom parser combinators.

use nom::{
    branch::alt,
    character::complete::multispace1,
    combinator::{all_consuming, map, opt},
    sequence::preceded,
    IResult, Parser,
};

use crate::ast::{OrderBy, OrderDirection, *};
use crate::error::{ParseError, Result};

mod expressions;
mod identifiers;
mod literals;
mod operators;
mod utils;

use expressions::{parse_comma_sequence, parse_if};
use identifiers::parse_identifier;
use utils::keyword;

/// Main parser for DSQ filter expressions
pub struct FilterParser {
    // future parser configuration could go here
}

impl FilterParser {
    /// Create a new parser instance
    pub fn new() -> Self {
        Self {}
    }

    /// Parse a DSQ filter string into an AST
    pub fn parse(&self, input: &str) -> Result<Filter> {
        let input = input.trim();
        if input.is_empty() {
            return Err(ParseError::EmptyInput);
        }

        match all_consuming(parse_filter).parse(input) {
            Ok((_, filter)) => Ok(filter),
            Err(e) => Err(ParseError::from(e)),
        }
    }
}

/// Parse a complete filter
fn parse_filter(input: &str) -> IResult<&str, Filter> {
    all_consuming(map(alt((parse_if, parse_comma_sequence)), |expr| Filter {
        expr,
    }))
    .parse(input)
}

/// Parse order by clause
#[allow(dead_code)]
fn parse_order_by(input: &str) -> IResult<&str, OrderBy> {
    map(
        (
            parse_identifier,
            opt(preceded(
                multispace1,
                alt((
                    map(keyword("asc"), |_| OrderDirection::Asc),
                    map(keyword("desc"), |_| OrderDirection::Desc),
                )),
            )),
        ),
        |(column, direction)| OrderBy {
            column,
            direction: direction.unwrap_or(OrderDirection::Asc),
        },
    )
    .parse(input)
}

impl Default for FilterParser {
    fn default() -> Self {
        Self::new()
    }
}
