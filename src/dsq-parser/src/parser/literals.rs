//! Literal value parsing
//!
//! This module contains parsers for literal values including strings, numbers,
//! booleans, and null.

use nom::{
    branch::alt,
    bytes::complete::take_while,
    character::complete::{char, digit1},
    combinator::{map, map_res, opt, recognize, verify},
    error::ErrorKind,
    sequence::delimited,
    IResult, Parser,
};

use crate::ast::{Expr, Literal};
use num_bigint::BigInt;

use super::utils::keyword;

/// Parse literals
pub(crate) fn parse_literal(input: &str) -> IResult<&str, Expr> {
    alt((
        parse_string_literal,
        parse_number_literal,
        parse_boolean_literal,
        parse_null_literal,
    ))
    .parse(input)
}

/// Parse string content with escapes
fn parse_string_content(input: &str) -> IResult<&str, String> {
    let mut result = String::new();
    let mut chars = input.char_indices();
    while let Some((i, ch)) = chars.next() {
        if ch == '"' {
            return Ok((&input[i..], result));
        } else if ch == '\\' {
            if let Some((_, esc_ch)) = chars.next() {
                let ch = match esc_ch {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    'b' => '\x08',
                    'f' => '\x0c',
                    '"' => '"',
                    '\\' => '\\',
                    '/' => '/',
                    _ => esc_ch, // unknown escapes are kept as is
                };
                result.push(ch);
            } else {
                return Err(nom::Err::Error(nom::error::Error::new(
                    &input[i..],
                    ErrorKind::Eof,
                )));
            }
        } else {
            result.push(ch);
        }
    }
    Err(nom::Err::Error(nom::error::Error::new("", ErrorKind::Eof)))
}

/// Parse string literals
pub(crate) fn parse_string_literal(input: &str) -> IResult<&str, Expr> {
    alt((
        // Double-quoted strings with escapes
        map(
            delimited(char('"'), parse_string_content, char('"')),
            |s: String| Expr::Literal(Literal::String(s)),
        ),
        // Single-quoted strings (simple, no escapes)
        map(
            delimited(char('\''), take_while(|c| c != '\''), char('\'')),
            |s: &str| Expr::Literal(Literal::String(s.to_string())),
        ),
    ))
    .parse(input)
}

/// Parse number literals
pub(crate) fn parse_number_literal(input: &str) -> IResult<&str, Expr> {
    map_res(
        verify(
            recognize((
                opt(char('-')),
                digit1,
                opt((char('.'), digit1)),
                opt((
                    alt((char('e'), char('E'))),
                    opt(alt((char('+'), char('-')))),
                    digit1,
                )),
            )),
            |s: &str| {
                // Reject numbers with leading zero unless it's just 0 or 0.something
                if s.starts_with('0')
                    && s.len() > 1
                    && !s.starts_with("0.")
                    && !s.starts_with("0e")
                    && !s.starts_with("0E")
                    && !s.starts_with("-0")
                {
                    false
                } else if s.starts_with("-0")
                    && s.len() > 2
                    && !s.starts_with("-0.")
                    && !s.starts_with("-0e")
                    && !s.starts_with("-0E")
                {
                    false
                } else {
                    true
                }
            },
        ),
        |s: &str| {
            if let Ok(int_val) = s.parse::<i64>() {
                Ok(Expr::Literal(Literal::Int(int_val)))
            } else if let Ok(big_int_val) = s.parse::<BigInt>() {
                Ok(Expr::Literal(Literal::BigInt(big_int_val)))
            } else if let Ok(float_val) = s.parse::<f64>() {
                Ok(Expr::Literal(Literal::Float(float_val)))
            } else {
                Err(format!("Invalid number: {}", s))
            }
        },
    )
    .parse(input)
}

/// Parse boolean literals
pub(crate) fn parse_boolean_literal(input: &str) -> IResult<&str, Expr> {
    alt((
        map(keyword("true"), |_| Expr::Literal(Literal::Bool(true))),
        map(keyword("false"), |_| Expr::Literal(Literal::Bool(false))),
    ))
    .parse(input)
}

/// Parse null literal
pub(crate) fn parse_null_literal(input: &str) -> IResult<&str, Expr> {
    map(keyword("null"), |_| Expr::Literal(Literal::Null)).parse(input)
}
