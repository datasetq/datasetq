//! Parser implementation for DSQ filter language
//!
//! This module contains the main parser that converts DSQ filter strings
//! into AST representations using nom parser combinators.

use nom::{
    branch::alt,
    bytes::complete::{tag, take_while, take_while1},
    character::complete::{alphanumeric1, char, digit1, multispace1},
    combinator::{all_consuming, map, map_res, not, opt, peek, recognize, verify},
    error::{ParseError as NomParseError, VerboseError},
    multi::{many0, many1, separated_list0},
    sequence::{delimited, preceded, terminated, tuple},
    IResult,
};

use crate::ast::{AssignmentOperator, OrderBy, OrderDirection, *};
use crate::error::{ParseError, Result};
use num_bigint::BigInt;

/// Parse a keyword, ensuring it's not followed by alphanumeric characters
fn keyword<'a>(
    word: &'static str,
) -> impl Fn(&'a str) -> IResult<&'a str, &'a str, VerboseError<&'a str>> {
    move |input| terminated(tag(word), not(alphanumeric1))(input)
}

/// Parse optional whitespace including newlines
fn ws(input: &str) -> IResult<&str, (), VerboseError<&str>> {
    map(
        many0(alt((tag(" "), tag("\t"), tag("\n"), tag("\r")))),
        |_| (),
    )(input)
}

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

        match all_consuming(parse_filter)(input) {
            Ok((_, filter)) => Ok(filter),
            Err(e) => Err(ParseError::from(e)),
        }
    }
}

// Parser functions using nom

/// Known builtin function names that can be used without parentheses
const BUILTIN_FUNCTIONS: &[&str] = &[
    "length",
    "map",
    "sort",
    "keys",
    "values",
    "add",
    "sum",
    "min",
    "max",
    "count",
    "count_if",
    "avg_if",
    "avg_ifs",
    "least_frequent",
    "mean",
    "avg",
    "median",
    "std",
    "var",
    "first",
    "last",
    "reverse",
    "unique",
    "flatten",
    "transpose",
    "pivot",
    "unpivot",
    "join",
    "inner_join",
    "left_join",
    "right_join",
    "outer_join",
    "concat",
    "contains",
    "startswith",
    "endswith",
    "lstrip",
    "rstrip",
    "tolower",
    "toupper",
    "trim",
    "replace",
    "dos2unix",
    "type",
    "isnan",
    "isinf",
    "isnormal",
    "isfinite",
    "transform_values",
    "map_values",
    "url_set_query_string",
    "month",
    "array_shift",
    "array_unshift",
    "array_push",
    "transliterate",
    "fromjson",
    "tojson",
    "base32_encode",
    "base58_encode",
    "base58_decode",
    "base64_encode",
    "base64_decode",
    "sha512",
    "sha256",
    "url_strip_fragment",
    "abs",
    "to_ascii",
    "is_valid_utf8",
    "to_valid_utf8",
    "snake_case",
    "camel_case",
    "tostring",
    "tonumber",
    "split",
    "join",
    "concat",
    "coalesce",
    "columns",
    "shape",
    "dtypes",
    "group_by",
    "pi",
    "rand",
    "randarray",
    "randbetween",
    "floor",
    "roundup",
    "rounddown",
    "ceil",
    "mround",
    "pow",
    "atan",
    "acos",
    "asin",
    "sin",
    "tan",
    "exp",
    "minute",
    "has",
    "empty",
    "error",
    "sort_by",
    "repeat",
    "zip",
    "md5",
    "sha1",
    "SHA1",
];

/// Keywords that cannot be used as identifiers
const KEYWORDS: &[&str] = &[
    "if", "then", "else", "end", "try", "catch", "and", "or", "not", "del", "true", "false", "null",
];

/// Parse a complete filter
fn parse_filter(input: &str) -> IResult<&str, Filter, VerboseError<&str>> {
    all_consuming(map(alt((parse_if, parse_comma_sequence)), |expr| Filter {
        expr,
    }))(input)
}

/// Parse an expression (main expression parser with precedence)
#[inline]
fn parse_expr(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    alt((parse_if, parse_pipeline))(input)
}

/// Parse if-then-else expression
fn parse_if(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(
        tuple((
            keyword("if"),
            map(opt(multispace1), |_| ()),
            parse_pipeline,
            tuple((
                map(opt(multispace1), |_| ()),
                keyword("then"),
                map(opt(multispace1), |_| ()),
            )),
            parse_pipeline,
            tuple((
                map(opt(multispace1), |_| ()),
                keyword("else"),
                map(opt(multispace1), |_| ()),
            )),
            parse_pipeline,
            tuple((
                map(opt(multispace1), |_| ()),
                keyword("end"),
                map(opt(multispace1), |_| ()),
            )),
        )),
        |(_, _, condition, _, then_branch, _, else_branch, _)| Expr::If {
            condition: Box::new(condition),
            then_branch: Box::new(then_branch),
            else_branch: Box::new(else_branch),
        },
    )(input)
}

/// Parse try-catch expression
fn parse_try(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(
        tuple((
            keyword("try"),
            ws,
            parse_pipeline,
            tuple((ws, keyword("catch"), ws)),
            parse_pipeline,
        )),
        |(_, _, try_expr, _, catch_expr)| Expr::FunctionCall {
            name: "iferror".to_string(),
            args: vec![try_expr, catch_expr],
        },
    )(input)
}

/// Parse comma-separated expressions
fn parse_comma_sequence(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    alt((
        map(
            tuple((
                parse_pipeline,
                many1(preceded(delimited(ws, char(','), ws), parse_pipeline)),
            )),
            |(first, rest)| {
                let mut exprs = vec![first];
                exprs.extend(rest);
                Expr::Sequence(exprs)
            },
        ),
        parse_pipeline,
    ))(input)
}

/// Parse order by clause
#[allow(dead_code)]
fn parse_order_by(input: &str) -> IResult<&str, OrderBy, VerboseError<&str>> {
    map(
        tuple((
            parse_identifier,
            opt(preceded(
                multispace1,
                alt((
                    map(keyword("asc"), |_| OrderDirection::Asc),
                    map(keyword("desc"), |_| OrderDirection::Desc),
                )),
            )),
        )),
        |(column, direction)| OrderBy {
            column,
            direction: direction.unwrap_or(OrderDirection::Asc),
        },
    )(input)
}

/// Parse pipeline expressions (lowest precedence)
fn parse_pipeline(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    alt((
        // Pipeline starting with expression: expr | expr | ...
        map(
            tuple((
                parse_assignment,
                many0(preceded(delimited(ws, char('|'), ws), parse_assignment)),
            )),
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
            tuple((
                char('|'),
                parse_assignment,
                many0(preceded(delimited(ws, char('|'), ws), parse_assignment)),
            )),
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
    ))(input)
}

/// Parse assignment expressions
fn parse_assignment(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    alt((
        map(
            tuple((
                parse_or_expr,
                tuple((opt(ws), alt((tag("+="), tag("|="))), opt(ws))),
                parse_or_expr,
            )),
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
    ))(input)
}

/// Parse logical OR expressions
fn parse_or_expr(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(
        tuple((
            parse_and_expr,
            many0(preceded(delimited(ws, keyword("or"), ws), parse_and_expr)),
        )),
        |(first, rest)| {
            rest.into_iter().fold(first, |left, right| Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            })
        },
    )(input)
}

/// Parse logical AND expressions
fn parse_and_expr(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(
        tuple((
            parse_comparison_expr,
            many0(preceded(
                delimited(ws, keyword("and"), ws),
                parse_comparison_expr,
            )),
        )),
        |(first, rest)| {
            rest.into_iter().fold(first, |left, right| Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            })
        },
    )(input)
}

/// Parse comparison expressions
fn parse_comparison_expr(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    alt((
        map(
            tuple((
                parse_additive_expr,
                tuple((
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
                )),
                parse_additive_expr,
            )),
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
    ))(input)
}

/// Parse additive expressions (+, -)
fn parse_additive_expr(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(
        tuple((
            parse_multiplicative_expr,
            many0(tuple((
                tuple((opt(ws), alt((char('+'), char('-'))), opt(ws))),
                parse_multiplicative_expr,
            ))),
        )),
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
    )(input)
}

/// Parse multiplicative expressions (*, /)
fn parse_multiplicative_expr(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(
        tuple((
            parse_unary_expr,
            many0(tuple((
                tuple((opt(ws), alt((char('*'), char('/'))), opt(ws))),
                parse_unary_expr,
            ))),
        )),
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
    )(input)
}

/// Parse unary expressions
fn parse_unary_expr(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    alt((
        map(
            tuple((
                preceded(ws, keyword("not")),
                opt(multispace1),
                parse_unary_expr,
            )),
            |(_, _, expr)| Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            },
        ),
        map(
            tuple((
                preceded(ws, keyword("del")),
                opt(multispace1),
                parse_unary_expr,
            )),
            |(_, _, expr)| Expr::UnaryOp {
                op: UnaryOperator::Del,
                expr: Box::new(expr),
            },
        ),
        parse_postfix_expr,
    ))(input)
}

/// Parse function call with parentheses (func(arg, ...))
fn parse_function_call_with_paren(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    verify(
        map(
            tuple((
                parse_identifier,
                delimited(ws, char('('), ws),
                separated_list0(
                    delimited(ws, alt((char(','), char(';'))), ws),
                    parse_pipeline,
                ),
                delimited(ws, char(')'), ws),
            )),
            |(name, _, args, _)| Expr::FunctionCall { name, args },
        ),
        |expr| {
            if let Expr::FunctionCall { name, args } = expr {
                !(name == "group_by" && (args.is_empty() || args.len() > 1))
            } else {
                true
            }
        },
    )(input)
}

/// Parse primary expressions (basic expressions without postfix operators)
#[inline]
fn parse_primary_expr(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
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
    )(input)
}

/// Parse postfix expressions (primary + postfix operators)
fn parse_postfix_expr(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    let (input, mut expr) = parse_primary_expr(input)?;

    // Collect postfix operators
    let (input, ops) = many0(alt((
        map(
            preceded(
                not(peek(tag(".."))),
                many1(tuple((char('.'), parse_identifier))),
            ),
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
            tuple((
                char('('),
                separated_list0(
                    delimited(ws, alt((char(','), char(';'))), ws),
                    parse_pipeline,
                ),
                char(')'),
            )),
            |(_, args, _)| PostfixOp::FunctionCall(args),
        ),
    )))(input)?;

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
                    return Err(nom::Err::Error(VerboseError::from_error_kind(
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
fn parse_array_index(input: &str) -> IResult<&str, ArrayIndex, VerboseError<&str>> {
    // Handle empty brackets (for .[])
    if input.trim().is_empty() || input.starts_with(']') {
        return Ok((input, ArrayIndex::Iteration));
    }

    alt((
        map(
            tuple((opt(parse_expr), char(':'), opt(parse_expr))),
            |(start, _, end)| ArrayIndex::Slice { start, end },
        ),
        map(parse_expr, ArrayIndex::Single),
    ))(input)
}

/// Parse field access
fn parse_field_access(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    // Reject inputs that start with consecutive dots
    let (input, _) = not(tag(".."))(input)?;
    map(many1(preceded(char('.'), parse_identifier)), |fields| {
        Expr::FieldAccess {
            base: Box::new(Expr::Identity),
            fields,
        }
    })(input)
}

/// Parse parenthesized expressions
fn parse_paren_expr(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(delimited(char('('), parse_expr, char(')')), |expr| {
        Expr::Paren(Box::new(expr))
    })(input)
}

/// Parse object construction {key: value, key, ...}
fn parse_object_construction(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(
        delimited(
            char('{'),
            terminated(
                separated_list0(delimited(ws, char(','), ws), parse_object_entry),
                ws,
            ),
            char('}'),
        ),
        |entries| Expr::Object { pairs: entries },
    )(input)
}

/// Parse object key (identifier or string literal)
fn parse_object_key(input: &str) -> IResult<&str, String, VerboseError<&str>> {
    alt((
        map(parse_string_literal, |e| match e {
            Expr::Literal(Literal::String(s)) => s,
            _ => unreachable!(),
        }),
        parse_identifier,
    ))(input)
}

/// Parse object entry (key: value or key)
fn parse_object_entry(input: &str) -> IResult<&str, ObjectEntry, VerboseError<&str>> {
    preceded(
        ws,
        alt((
            map(
                tuple((
                    parse_object_key,
                    delimited(ws, char(':'), ws),
                    parse_pipeline,
                )),
                |(key, _, value)| ObjectEntry::KeyValue { key, value },
            ),
            map(parse_identifier, ObjectEntry::Shorthand),
        )),
    )(input)
}

/// Parse array construction [item1, item2, ...]
fn parse_array_construction(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(
        delimited(
            char('['),
            separated_list0(delimited(ws, char(','), ws), parse_expr),
            char(']'),
        ),
        Expr::Array,
    )(input)
}

/// Parse literals
fn parse_literal(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    alt((
        parse_string_literal,
        parse_number_literal,
        parse_boolean_literal,
        parse_null_literal,
    ))(input)
}

/// Parse string content with escapes
fn parse_string_content(input: &str) -> IResult<&str, String, VerboseError<&str>> {
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
                return Err(nom::Err::Error(VerboseError::from_error_kind(
                    &input[i..],
                    nom::error::ErrorKind::Eof,
                )));
            }
        } else {
            result.push(ch);
        }
    }
    Err(nom::Err::Error(VerboseError::from_error_kind(
        "",
        nom::error::ErrorKind::Eof,
    )))
}

/// Parse string literals
fn parse_string_literal(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
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
    ))(input)
}

/// Parse number literals
fn parse_number_literal(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map_res(
        verify(
            recognize(tuple((
                opt(char('-')),
                digit1,
                opt(tuple((char('.'), digit1))),
                opt(tuple((
                    alt((char('e'), char('E'))),
                    opt(alt((char('+'), char('-')))),
                    digit1,
                ))),
            ))),
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
    )(input)
}

/// Parse boolean literals
fn parse_boolean_literal(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    alt((
        map(keyword("true"), |_| Expr::Literal(Literal::Bool(true))),
        map(keyword("false"), |_| Expr::Literal(Literal::Bool(false))),
    ))(input)
}

/// Parse null literal
fn parse_null_literal(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(keyword("null"), |_| Expr::Literal(Literal::Null))(input)
}

/// Parse identity
fn parse_identity(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(terminated(char('.'), not(peek(char('.')))), |_| {
        Expr::Identity
    })(input)
}

/// Parse variable references ($name)
fn parse_variable(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(preceded(char('$'), parse_identifier), |name: String| {
        Expr::Variable(name)
    })(input)
}

/// Parse identifiers
fn parse_identifier(input: &str) -> IResult<&str, String, VerboseError<&str>> {
    map(
        recognize(tuple((
            take_while1(|c: char| c.is_alphabetic() || c == '_'),
            take_while(|c: char| c.is_alphanumeric() || c == '_'),
        ))),
        |s: &str| s.to_string(),
    )(input)
}

impl Default for FilterParser {
    fn default() -> Self {
        Self::new()
    }
}
