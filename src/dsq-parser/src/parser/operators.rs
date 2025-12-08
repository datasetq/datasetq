//! Operator expression parsing
//!
//! This module contains parsers for operators and operator expressions including
//! binary operators, unary operators, and assignments.

use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{char, multispace1},
    combinator::{map, opt},
    multi::many0,
    sequence::{delimited, preceded},
    IResult, Parser,
};

use crate::ast::{AssignmentOperator, BinaryOperator, Expr, UnaryOperator};

use super::utils::{keyword, ws};

/// Parse assignment expressions
pub(crate) fn parse_assignment(input: &str) -> IResult<&str, Expr> {
    alt((
        map(
            (
                parse_or_expr,
                (opt(ws), alt((tag("+="), tag("|="))), opt(ws)),
                parse_or_expr,
            ),
            |(target, (_, op, _), value)| {
                let op = match op {
                    "+=" => AssignmentOperator::AddAssign,
                    "|=" => AssignmentOperator::UpdateAssign,
                    _ => unreachable!(),
                };
                Expr::Assignment {
                    op,
                    target: Box::new(target),
                    value: Box::new(value),
                }
            },
        ),
        parse_or_expr,
    ))
    .parse(input)
}

/// Parse logical OR expressions
pub(crate) fn parse_or_expr(input: &str) -> IResult<&str, Expr> {
    map(
        (
            parse_and_expr,
            many0(preceded(delimited(ws, keyword("or"), ws), parse_and_expr)),
        ),
        |(first, rest)| {
            rest.into_iter().fold(first, |left, right| Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            })
        },
    )
    .parse(input)
}

/// Parse logical AND expressions
pub(crate) fn parse_and_expr(input: &str) -> IResult<&str, Expr> {
    map(
        (
            parse_comparison_expr,
            many0(preceded(
                delimited(ws, keyword("and"), ws),
                parse_comparison_expr,
            )),
        ),
        |(first, rest)| {
            rest.into_iter().fold(first, |left, right| Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            })
        },
    )
    .parse(input)
}

/// Parse comparison expressions
pub(crate) fn parse_comparison_expr(input: &str) -> IResult<&str, Expr> {
    alt((
        map(
            (
                parse_additive_expr,
                (
                    opt(ws),
                    alt((
                        tag(">="),
                        tag("<="),
                        tag("!="),
                        tag("=="),
                        tag(">"),
                        tag("<"),
                    )),
                    opt(ws),
                ),
                parse_additive_expr,
            ),
            |(left, (_, op, _), right)| {
                let op = match op {
                    ">=" => BinaryOperator::Ge,
                    "<=" => BinaryOperator::Le,
                    "!=" => BinaryOperator::Ne,
                    "==" => BinaryOperator::Eq,
                    ">" => BinaryOperator::Gt,
                    "<" => BinaryOperator::Lt,
                    _ => unreachable!(),
                };
                Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                }
            },
        ),
        parse_additive_expr,
    ))
    .parse(input)
}

/// Parse additive expressions (+, -)
pub(crate) fn parse_additive_expr(input: &str) -> IResult<&str, Expr> {
    map(
        (
            parse_multiplicative_expr,
            many0((
                (opt(ws), alt((char('+'), char('-'))), opt(ws)),
                parse_multiplicative_expr,
            )),
        ),
        |(first, rest)| {
            rest.into_iter().fold(first, |left, ((_, op, _), right)| {
                let op = match op {
                    '+' => BinaryOperator::Add,
                    '-' => BinaryOperator::Sub,
                    _ => unreachable!(),
                };
                Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                }
            })
        },
    )
    .parse(input)
}

/// Parse multiplicative expressions (*, /)
pub(crate) fn parse_multiplicative_expr(input: &str) -> IResult<&str, Expr> {
    map(
        (
            parse_unary_expr,
            many0((
                (opt(ws), alt((char('*'), char('/'))), opt(ws)),
                parse_unary_expr,
            )),
        ),
        |(first, rest)| {
            rest.into_iter().fold(first, |left, ((_, op, _), right)| {
                let op = match op {
                    '*' => BinaryOperator::Mul,
                    '/' => BinaryOperator::Div,
                    _ => unreachable!(),
                };
                Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                }
            })
        },
    )
    .parse(input)
}

/// Parse unary expressions
pub(crate) fn parse_unary_expr(input: &str) -> IResult<&str, Expr> {
    use super::expressions::parse_postfix_expr;

    alt((
        map(
            (
                preceded(ws, keyword("not")),
                opt(multispace1),
                parse_unary_expr,
            ),
            |(_, _, expr)| Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            },
        ),
        map(
            (
                preceded(ws, keyword("del")),
                opt(multispace1),
                parse_unary_expr,
            ),
            |(_, _, expr)| Expr::UnaryOp {
                op: UnaryOperator::Del,
                expr: Box::new(expr),
            },
        ),
        parse_postfix_expr,
    ))
    .parse(input)
}
