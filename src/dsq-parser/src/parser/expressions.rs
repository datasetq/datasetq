//! Expression parsing
//!
//! This module contains parsers for complex expressions including pipelines,
//! function calls, arrays, objects, and control flow expressions.

use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{char, multispace1},
    combinator::{map, not, opt, peek, verify},
    multi::{many0, many1, separated_list0},
    sequence::{delimited, preceded},
    IResult, Parser,
};

use crate::ast::{Expr, ObjectEntry};

use super::identifiers::{parse_identifier, parse_variable};
use super::literals::{parse_literal, parse_string_literal};
use super::operators::parse_assignment;
use super::utils::{keyword, ws, BUILTIN_FUNCTIONS, KEYWORDS};

/// Parse an expression (main expression parser with precedence)
#[inline]
pub(crate) fn parse_expr(input: &str) -> IResult<&str, Expr> {
    alt((parse_if, parse_pipeline)).parse(input)
}

/// Parse if-then-else expression
pub(crate) fn parse_if(input: &str) -> IResult<&str, Expr> {
    map(
        (
            keyword("if"),
            map(opt(multispace1), |_| ()),
            parse_pipeline,
            (
                map(opt(multispace1), |_| ()),
                keyword("then"),
                map(opt(multispace1), |_| ()),
            ),
            parse_pipeline,
            (
                map(opt(multispace1), |_| ()),
                keyword("else"),
                map(opt(multispace1), |_| ()),
            ),
            parse_pipeline,
            (
                map(opt(multispace1), |_| ()),
                keyword("end"),
                map(opt(multispace1), |_| ()),
            ),
        ),
        |(_, _, condition, _, then_branch, _, else_branch, _)| Expr::If {
            condition: Box::new(condition),
            then_branch: Box::new(then_branch),
            else_branch: Box::new(else_branch),
        },
    )
    .parse(input)
}

/// Parse try-catch expression
pub(crate) fn parse_try(input: &str) -> IResult<&str, Expr> {
    map(
        (
            keyword("try"),
            ws,
            parse_pipeline,
            (ws, keyword("catch"), ws),
            parse_pipeline,
        ),
        |(_, _, try_expr, _, catch_expr)| Expr::FunctionCall {
            name: "iferror".to_string(),
            args: vec![try_expr, catch_expr],
        },
    )
    .parse(input)
}

/// Parse comma-separated expressions
pub(crate) fn parse_comma_sequence(input: &str) -> IResult<&str, Expr> {
    alt((
        map(
            (
                parse_pipeline,
                many1(preceded(delimited(ws, char(','), ws), parse_pipeline)),
            ),
            |(first, rest)| {
                let mut exprs = vec![first];
                exprs.extend(rest);
                Expr::Sequence(exprs)
            },
        ),
        parse_pipeline,
    ))
    .parse(input)
}

/// Parse pipeline expressions (lowest precedence)
pub(crate) fn parse_pipeline(input: &str) -> IResult<&str, Expr> {
    alt((
        // Pipeline starting with expression: expr | expr | ...
        map(
            (
                parse_assignment,
                many0(preceded(delimited(ws, char('|'), ws), parse_assignment)),
            ),
            |(first, rest)| {
                let mut exprs = vec![first];
                exprs.extend(rest);
                if exprs.len() == 1 {
                    // Safe: we just pushed `first`, so the vec has at least one element
                    exprs
                        .into_iter()
                        .next()
                        .expect("pipeline has at least one expression")
                } else {
                    Expr::Pipeline(exprs)
                }
            },
        ),
        // Pipeline starting with '|': | expr | expr | ...
        map(
            (
                char('|'),
                parse_assignment,
                many0(preceded(delimited(ws, char('|'), ws), parse_assignment)),
            ),
            |(_, first, rest)| {
                let mut exprs = vec![first];
                exprs.extend(rest);
                if exprs.len() == 1 {
                    // Safe: we just pushed `first`, so the vec has at least one element
                    exprs
                        .into_iter()
                        .next()
                        .expect("pipeline has at least one expression")
                } else {
                    Expr::Pipeline(exprs)
                }
            },
        ),
    ))
    .parse(input)
}

/// Parse function call with parentheses (func(arg, ...))
pub(crate) fn parse_function_call_with_paren(input: &str) -> IResult<&str, Expr> {
    verify(
        map(
            (
                parse_identifier,
                delimited(ws, char('('), ws),
                separated_list0(
                    delimited(ws, alt((char(','), char(';'))), ws),
                    parse_pipeline,
                ),
                delimited(ws, char(')'), ws),
            ),
            |(name, _, args, _)| Expr::FunctionCall { name, args },
        ),
        |expr| {
            if let Expr::FunctionCall { name, args } = expr {
                !(name == "group_by" && (args.is_empty() || args.len() > 1))
            } else {
                true
            }
        },
    )
    .parse(input)
}

/// Parse primary expressions (basic expressions without postfix operators)
#[inline]
pub(crate) fn parse_primary_expr(input: &str) -> IResult<&str, Expr> {
    preceded(
        ws,
        alt((
            parse_object_construction,
            parse_array_construction,
            parse_paren_expr,
            parse_try,
            parse_if,
            parse_literal,
            // Bare identifiers - check if they're builtin functions
            verify(
                map(parse_identifier, |name: String| {
                    if BUILTIN_FUNCTIONS.contains(&name.as_str()) {
                        Expr::FunctionCall { name, args: vec![] }
                    } else {
                        Expr::Identifier(name)
                    }
                }),
                |expr| {
                    if let Expr::Identifier(name) = expr {
                        !KEYWORDS.contains(&name.as_str())
                    } else {
                        true
                    }
                },
            ),
            // Try field access before identity since ".name" should be field access, "." should be identity
            parse_field_access,
            parse_variable,
            // Function calls
            parse_function_call_with_paren,
            parse_identity,
        )),
    )
    .parse(input)
}

/// Parse postfix expressions (primary + postfix operators)
pub(crate) fn parse_postfix_expr(input: &str) -> IResult<&str, Expr> {
    let (input, mut expr) = parse_primary_expr(input)?;

    // Collect postfix operators
    let (input, ops) = many0(alt((
        map(
            preceded(not(peek(tag(".."))), many1((char('.'), parse_identifier))),
            |field_pairs| {
                let fields = field_pairs.into_iter().map(|(_, field)| field).collect();
                PostfixOp::FieldAccess(fields)
            },
        ),
        map(
            delimited(char('['), parse_array_index, char(']')),
            PostfixOp::ArrayIndex,
        ),
        map(
            (
                char('('),
                separated_list0(
                    delimited(ws, alt((char(','), char(';'))), ws),
                    parse_pipeline,
                ),
                char(')'),
            ),
            |(_, args, _)| PostfixOp::FunctionCall(args),
        ),
    )))
    .parse(input)?;

    // Apply postfix operators
    for op in ops {
        expr = match op {
            PostfixOp::FieldAccess(fields) => Expr::FieldAccess {
                base: Box::new(expr),
                fields,
            },
            PostfixOp::ArrayIndex(index) => match index {
                ArrayIndex::Single(idx_expr) => Expr::ArrayAccess {
                    array: Box::new(expr),
                    index: Box::new(idx_expr),
                },
                ArrayIndex::Slice { start, end } => Expr::ArraySlice {
                    array: Box::new(expr),
                    start: start.map(Box::new),
                    end: end.map(Box::new),
                },
                ArrayIndex::Iteration => Expr::ArrayIteration(Box::new(expr)),
            },
            PostfixOp::FunctionCall(args) => {
                let name = match &expr {
                    Expr::FieldAccess { fields, .. } => {
                        // Safe: FieldAccess is constructed with at least one field
                        fields
                            .last()
                            .expect("field access has at least one field")
                            .clone()
                    }
                    Expr::Identifier(name) => name.clone(),
                    Expr::Identity => ".".to_string(),
                    Expr::FunctionCall { name, .. } => name.clone(),
                    _ => continue, // Invalid, skip
                };
                // Special case: group_by requires exactly one argument
                if name == "group_by" && (args.is_empty() || args.len() > 1) {
                    return Err(nom::Err::Error(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Verify,
                    )));
                }
                Expr::FunctionCall { name, args }
            }
        };
    }

    Ok((input, expr))
}

#[derive(Debug)]
enum PostfixOp {
    FieldAccess(Vec<String>),
    ArrayIndex(ArrayIndex),
    FunctionCall(Vec<Expr>),
}

/// Array index types
#[derive(Debug)]
enum ArrayIndex {
    Single(Expr),
    Slice {
        start: Option<Expr>,
        end: Option<Expr>,
    },
    Iteration,
}

/// Parse array index
fn parse_array_index(input: &str) -> IResult<&str, ArrayIndex> {
    // Handle empty brackets (for .[])
    if input.trim().is_empty() || input.starts_with(']') {
        return Ok((input, ArrayIndex::Iteration));
    }

    alt((
        map(
            (opt(parse_expr), char(':'), opt(parse_expr)),
            |(start, _, end)| ArrayIndex::Slice { start, end },
        ),
        map(parse_expr, ArrayIndex::Single),
    ))
    .parse(input)
}

/// Parse field access
pub(crate) fn parse_field_access(input: &str) -> IResult<&str, Expr> {
    // Reject inputs that start with consecutive dots
    let (input, _) = not(tag("..")).parse(input)?;
    map(many1(preceded(char('.'), parse_identifier)), |fields| {
        Expr::FieldAccess {
            base: Box::new(Expr::Identity),
            fields,
        }
    })
    .parse(input)
}

/// Parse parenthesized expressions
fn parse_paren_expr(input: &str) -> IResult<&str, Expr> {
    map(delimited(char('('), parse_expr, char(')')), |expr| {
        Expr::Paren(Box::new(expr))
    })
    .parse(input)
}

/// Parse object construction {key: value, key, ...}
fn parse_object_construction(input: &str) -> IResult<&str, Expr> {
    map(
        delimited(
            char('{'),
            delimited(
                ws,
                separated_list0(delimited(ws, char(','), ws), parse_object_entry),
                ws,
            ),
            char('}'),
        ),
        |entries| Expr::Object { pairs: entries },
    )
    .parse(input)
}

/// Parse object key (identifier or string literal)
fn parse_object_key(input: &str) -> IResult<&str, String> {
    alt((
        map(parse_string_literal, |e| match e {
            Expr::Literal(crate::ast::Literal::String(s)) => s,
            _ => unreachable!(),
        }),
        parse_identifier,
    ))
    .parse(input)
}

/// Parse object entry (key: value or key)
fn parse_object_entry(input: &str) -> IResult<&str, ObjectEntry> {
    preceded(
        ws,
        alt((
            map(
                (
                    parse_object_key,
                    delimited(ws, char(':'), ws),
                    parse_pipeline,
                ),
                |(key, _, value)| ObjectEntry::KeyValue { key, value },
            ),
            map(parse_identifier, ObjectEntry::Shorthand),
        )),
    )
    .parse(input)
}

/// Parse array construction [item1, item2, ...]
fn parse_array_construction(input: &str) -> IResult<&str, Expr> {
    map(
        delimited(
            char('['),
            separated_list0(delimited(ws, char(','), ws), parse_expr),
            char(']'),
        ),
        Expr::Array,
    )
    .parse(input)
}

/// Parse identity
fn parse_identity(input: &str) -> IResult<&str, Expr> {
    map(delimited(ws, char('.'), not(peek(char('.')))), |_| {
        Expr::Identity
    })
    .parse(input)
}
