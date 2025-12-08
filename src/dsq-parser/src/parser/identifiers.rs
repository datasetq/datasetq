//! Identifier and variable parsing
//!
//! This module contains parsers for identifiers, variables, and related constructs.

use nom::{
    bytes::complete::{take_while, take_while1},
    character::complete::char,
    combinator::{map, recognize},
    sequence::preceded,
    IResult, Parser,
};

use crate::ast::Expr;

/// Parse identifiers
pub(crate) fn parse_identifier(input: &str) -> IResult<&str, String> {
    map(
        recognize((
            take_while1(|c: char| c.is_alphabetic() || c == '_'),
            take_while(|c: char| c.is_alphanumeric() || c == '_'),
        )),
        |s: &str| s.to_string(),
    )
    .parse(input)
}

/// Parse variable references ($name)
pub(crate) fn parse_variable(input: &str) -> IResult<&str, Expr> {
    map(preceded(char('$'), parse_identifier), |name: String| {
        Expr::Variable(name)
    })
    .parse(input)
}
