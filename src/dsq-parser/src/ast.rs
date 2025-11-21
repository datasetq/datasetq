//! Abstract Syntax Tree (AST) definitions for DSQ filter language
//!
//! This module defines the AST nodes that represent the parsed structure
//! of DSQ filter expressions.

use std::fmt;

/// Represents a complete DSQ filter expression (possibly a pipeline)
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Filter {
    /// The root expression of the filter
    pub expr: Expr,
}

/// Core expression types in DSQ
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum Expr {
    /// Identity filter (.)
    Identity,

    /// Field access (.field or .field.subfield)
    FieldAccess {
        /// Base expression (usually Identity for .field)
        base: Box<Expr>,
        /// Field names
        fields: Vec<String>,
    },

    /// Array access (.[index])
    ArrayAccess {
        /// The array expression
        array: Box<Expr>,
        /// The index expression
        index: Box<Expr>,
    },

    /// Array slicing (.[start:end])
    ArraySlice {
        /// The array expression
        array: Box<Expr>,
        /// Start index (None means 0)
        start: Option<Box<Expr>>,
        /// End index (None means end)
        end: Option<Box<Expr>>,
    },

    /// Array iteration (.[])
    ArrayIteration(Box<Expr>),

    /// Function call (func(args...))
    FunctionCall {
        /// Function name
        name: String,
        /// Arguments
        args: Vec<Expr>,
    },

    /// Binary operation (left op right)
    BinaryOp {
        /// Left operand
        left: Box<Expr>,
        /// Operator
        op: BinaryOperator,
        /// Right operand
        right: Box<Expr>,
    },

    /// Unary operation (op expr)
    UnaryOp {
        /// Operator
        op: UnaryOperator,
        /// Operand
        expr: Box<Expr>,
    },

    /// Assignment operation (target += value or target |= value)
    Assignment {
        /// Assignment operator
        op: AssignmentOperator,
        /// Target expression
        target: Box<Expr>,
        /// Value expression
        value: Box<Expr>,
    },

    /// Object construction ({key: value, ...})
    Object {
        /// Key-value pairs
        pairs: Vec<ObjectEntry>,
    },

    /// Array construction ([item1, item2, ...])
    Array(Vec<Expr>),

    /// Literal value
    Literal(Literal),

    /// Identifier (bare word, could be function name or variable)
    Identifier(String),

    /// Variable reference ($name)
    Variable(String),

    /// Parenthesized expression
    Paren(Box<Expr>),

    /// Pipeline (expr1 | expr2 | ...)
    Pipeline(Vec<Expr>),

    /// If-then-else expression (if condition then expr else expr end)
    If {
        /// Condition expression
        condition: Box<Expr>,
        /// Then branch expression
        then_branch: Box<Expr>,
        /// Else branch expression
        else_branch: Box<Expr>,
    },

    /// Sequence (expr1, expr2, ...)
    Sequence(Vec<Expr>),
}

/// Binary operators
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum BinaryOperator {
    /// Addition (+)
    Add,
    /// Subtraction (-)
    Sub,
    /// Multiplication (*)
    Mul,
    /// Division (/)
    Div,
    /// Greater than (>)
    Gt,
    /// Less than (<)
    Lt,
    /// Equal (==)
    Eq,
    /// Not equal (!=)
    Ne,
    /// Greater than or equal (>=)
    Ge,
    /// Less than or equal (<=)
    Le,
    /// Logical AND (and)
    And,
    /// Logical OR (or)
    Or,
}

/// Assignment operators
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum AssignmentOperator {
    /// Add assign (+=)
    AddAssign,
    /// Update assign (|=)
    UpdateAssign,
}

/// Unary operators
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum UnaryOperator {
    /// Logical NOT (not)
    Not,
    /// Delete field (del)
    Del,
}

/// Object construction entry
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum ObjectEntry {
    /// Explicit key-value pair (key: value)
    KeyValue {
        /// Key name
        key: String,
        /// Value expression
        value: Expr,
    },
    /// Shorthand entry (just key name)
    Shorthand(String),
}

/// Order by specification
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct OrderBy {
    /// Column to order by
    pub column: String,
    /// Sort direction
    pub direction: OrderDirection,
}

/// Order direction
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum OrderDirection {
    /// Ascending order
    Asc,
    /// Descending order
    Desc,
}

/// Literal values
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum Literal {
    /// Integer literal
    Int(i64),
    /// Big integer literal
    BigInt(num_bigint::BigInt),
    /// Float literal
    Float(f64),
    /// String literal
    String(String),
    /// Boolean literal
    Bool(bool),
    /// Null literal
    Null,
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Identity => write!(f, "."),

            Expr::FieldAccess { base, fields } => {
                if matches!(**base, Expr::Identity) {
                    write!(f, ".")?;
                } else {
                    write!(f, "{}", base)?;
                }
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 || !matches!(**base, Expr::Identity) {
                        write!(f, ".")?;
                    }
                    write!(f, "{}", field)?;
                }
                Ok(())
            }
            Expr::ArrayAccess { array, index } => write!(f, "{}[{}]", array, index),
            Expr::ArraySlice { array, start, end } => {
                write!(f, "{}[", array)?;
                if let Some(start) = start {
                    write!(f, "{}", start)?;
                }
                write!(f, ":")?;
                if let Some(end) = end {
                    write!(f, "{}", end)?;
                }
                write!(f, "]")
            }
            Expr::ArrayIteration(expr) => write!(f, "{}[]", expr),
            Expr::FunctionCall { name, args } => {
                write!(f, "{}(", name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ")")
            }
            Expr::BinaryOp { left, op, right } => write!(f, "{} {} {}", left, op, right),
            Expr::UnaryOp { op, expr } => write!(f, "{} {}", op, expr),
            Expr::Assignment { op, target, value } => write!(f, "{} {} {}", target, op, value),
            Expr::Object { pairs } => {
                write!(f, "{{")?;
                for (i, pair) in pairs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    match pair {
                        ObjectEntry::KeyValue { key, value } => write!(f, "{}: {}", key, value)?,
                        ObjectEntry::Shorthand(key) => write!(f, "{}", key)?,
                    }
                }
                write!(f, "}}")
            }
            Expr::Array(elements) => {
                write!(f, "[")?;
                for (i, elem) in elements.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", elem)?;
                }
                write!(f, "]")
            }
            Expr::Literal(lit) => write!(f, "{}", lit),
            Expr::Identifier(name) => write!(f, "{}", name),
            Expr::Variable(name) => write!(f, "${}", name),
            Expr::Paren(expr) => write!(f, "({})", expr),
            Expr::Pipeline(exprs) => {
                for (i, expr) in exprs.iter().enumerate() {
                    if i > 0 {
                        write!(f, " | ")?;
                    }
                    write!(f, "{}", expr)?;
                }
                Ok(())
            }
            Expr::If {
                condition,
                then_branch,
                else_branch,
            } => {
                write!(
                    f,
                    "if {} then {} else {} end",
                    condition, then_branch, else_branch
                )
            }
            Expr::Sequence(exprs) => {
                for (i, expr) in exprs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", expr)?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOperator::Add => write!(f, "+"),
            BinaryOperator::Sub => write!(f, "-"),
            BinaryOperator::Mul => write!(f, "*"),
            BinaryOperator::Div => write!(f, "/"),
            BinaryOperator::Gt => write!(f, ">"),
            BinaryOperator::Lt => write!(f, "<"),
            BinaryOperator::Eq => write!(f, "=="),
            BinaryOperator::Ne => write!(f, "!="),
            BinaryOperator::Ge => write!(f, ">="),
            BinaryOperator::Le => write!(f, "<="),
            BinaryOperator::And => write!(f, "and"),
            BinaryOperator::Or => write!(f, "or"),
        }
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOperator::Not => write!(f, "not"),
            UnaryOperator::Del => write!(f, "del"),
        }
    }
}

impl fmt::Display for AssignmentOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AssignmentOperator::AddAssign => write!(f, "+="),
            AssignmentOperator::UpdateAssign => write!(f, "|="),
        }
    }
}

impl fmt::Display for OrderDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderDirection::Asc => write!(f, "asc"),
            OrderDirection::Desc => write!(f, "desc"),
        }
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Int(i) => write!(f, "{}", i),
            Literal::BigInt(bi) => write!(f, "{}", bi),
            Literal::Float(fl) => write!(f, "{}", fl),
            Literal::String(s) => write!(f, "\"{}\"", s),
            Literal::Bool(b) => write!(f, "{}", b),
            Literal::Null => write!(f, "null"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;

    #[test]
    fn test_literal_display() {
        assert_eq!(format!("{}", Literal::Int(42)), "42");
        assert_eq!(format!("{}", Literal::Int(-42)), "-42");
        assert_eq!(
            format!("{}", Literal::BigInt(BigInt::from(123456789))),
            "123456789"
        );
        assert_eq!(format!("{}", Literal::Float(3.14)), "3.14");
        assert_eq!(format!("{}", Literal::Float(-3.14)), "-3.14");
        assert_eq!(
            format!("{}", Literal::String("hello".to_string())),
            "\"hello\""
        );
        assert_eq!(
            format!("{}", Literal::String("with \"quotes\"".to_string())),
            "\"with \"quotes\"\""
        );
        assert_eq!(format!("{}", Literal::Bool(true)), "true");
        assert_eq!(format!("{}", Literal::Bool(false)), "false");
        assert_eq!(format!("{}", Literal::Null), "null");
    }

    #[test]
    fn test_binary_operator_display() {
        assert_eq!(format!("{}", BinaryOperator::Add), "+");
        assert_eq!(format!("{}", BinaryOperator::Sub), "-");
        assert_eq!(format!("{}", BinaryOperator::Mul), "*");
        assert_eq!(format!("{}", BinaryOperator::Div), "/");
        assert_eq!(format!("{}", BinaryOperator::Gt), ">");
        assert_eq!(format!("{}", BinaryOperator::Lt), "<");
        assert_eq!(format!("{}", BinaryOperator::Eq), "==");
        assert_eq!(format!("{}", BinaryOperator::Ne), "!=");
        assert_eq!(format!("{}", BinaryOperator::Ge), ">=");
        assert_eq!(format!("{}", BinaryOperator::Le), "<=");
        assert_eq!(format!("{}", BinaryOperator::And), "and");
        assert_eq!(format!("{}", BinaryOperator::Or), "or");
    }

    #[test]
    fn test_unary_operator_display() {
        assert_eq!(format!("{}", UnaryOperator::Not), "not");
        assert_eq!(format!("{}", UnaryOperator::Del), "del");
    }

    #[test]
    fn test_assignment_operator_display() {
        assert_eq!(format!("{}", AssignmentOperator::AddAssign), "+=");
        assert_eq!(format!("{}", AssignmentOperator::UpdateAssign), "|=");
    }

    #[test]
    fn test_order_direction_display() {
        assert_eq!(format!("{}", OrderDirection::Asc), "asc");
        assert_eq!(format!("{}", OrderDirection::Desc), "desc");
    }

    #[test]
    fn test_expr_display_identity() {
        let expr = Expr::Identity;
        assert_eq!(format!("{}", expr), ".");
    }

    #[test]
    fn test_expr_display_field_access() {
        // Simple field access
        let expr = Expr::FieldAccess {
            base: Box::new(Expr::Identity),
            fields: vec!["name".to_string()],
        };
        assert_eq!(format!("{}", expr), ".name");

        // Nested field access
        let expr = Expr::FieldAccess {
            base: Box::new(Expr::Identity),
            fields: vec!["user".to_string(), "name".to_string()],
        };
        assert_eq!(format!("{}", expr), ".user.name");

        // Field access on expression
        let expr = Expr::FieldAccess {
            base: Box::new(Expr::Identifier("obj".to_string())),
            fields: vec!["field".to_string()],
        };
        assert_eq!(format!("{}", expr), "obj.field");
    }

    #[test]
    fn test_expr_display_array_access() {
        let expr = Expr::ArrayAccess {
            array: Box::new(Expr::Identity),
            index: Box::new(Expr::Literal(Literal::Int(0))),
        };
        assert_eq!(format!("{}", expr), ".[0]");
    }

    #[test]
    fn test_expr_display_array_slice() {
        // Full slice
        let expr = Expr::ArraySlice {
            array: Box::new(Expr::Identity),
            start: None,
            end: None,
        };
        assert_eq!(format!("{}", expr), ".[:]");

        // Start only
        let expr = Expr::ArraySlice {
            array: Box::new(Expr::Identity),
            start: Some(Box::new(Expr::Literal(Literal::Int(1)))),
            end: None,
        };
        assert_eq!(format!("{}", expr), ".[1:]");

        // End only
        let expr = Expr::ArraySlice {
            array: Box::new(Expr::Identity),
            start: None,
            end: Some(Box::new(Expr::Literal(Literal::Int(5)))),
        };
        assert_eq!(format!("{}", expr), ".[:5]");

        // Both start and end
        let expr = Expr::ArraySlice {
            array: Box::new(Expr::Identity),
            start: Some(Box::new(Expr::Literal(Literal::Int(1)))),
            end: Some(Box::new(Expr::Literal(Literal::Int(5)))),
        };
        assert_eq!(format!("{}", expr), ".[1:5]");
    }

    #[test]
    fn test_expr_display_array_iteration() {
        let expr = Expr::ArrayIteration(Box::new(Expr::Identity));
        assert_eq!(format!("{}", expr), ".[]");
    }

    #[test]
    fn test_expr_display_function_call() {
        // No args
        let expr = Expr::FunctionCall {
            name: "length".to_string(),
            args: vec![],
        };
        assert_eq!(format!("{}", expr), "length()");

        // One arg
        let expr = Expr::FunctionCall {
            name: "map".to_string(),
            args: vec![Expr::Identity],
        };
        assert_eq!(format!("{}", expr), "map(.)");

        // Multiple args
        let expr = Expr::FunctionCall {
            name: "add".to_string(),
            args: vec![
                Expr::Literal(Literal::Int(1)),
                Expr::Literal(Literal::Int(2)),
            ],
        };
        assert_eq!(format!("{}", expr), "add(1, 2)");
    }

    #[test]
    fn test_expr_display_binary_op() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Int(1))),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Literal(Literal::Int(2))),
        };
        assert_eq!(format!("{}", expr), "1 + 2");
    }

    #[test]
    fn test_expr_display_unary_op() {
        let expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Literal(Literal::Bool(true))),
        };
        assert_eq!(format!("{}", expr), "not true");
    }

    #[test]
    fn test_expr_display_assignment() {
        let expr = Expr::Assignment {
            op: AssignmentOperator::AddAssign,
            target: Box::new(Expr::FieldAccess {
                base: Box::new(Expr::Identity),
                fields: vec!["salary".to_string()],
            }),
            value: Box::new(Expr::Literal(Literal::Int(5000))),
        };
        assert_eq!(format!("{}", expr), ".salary += 5000");
    }

    #[test]
    fn test_expr_display_object() {
        // Empty object
        let expr = Expr::Object { pairs: vec![] };
        assert_eq!(format!("{}", expr), "{}");

        // Object with shorthand
        let expr = Expr::Object {
            pairs: vec![ObjectEntry::Shorthand("name".to_string())],
        };
        assert_eq!(format!("{}", expr), "{name}");

        // Object with key-value
        let expr = Expr::Object {
            pairs: vec![ObjectEntry::KeyValue {
                key: "age".to_string(),
                value: Expr::Literal(Literal::Int(30)),
            }],
        };
        assert_eq!(format!("{}", expr), "{age: 30}");

        // Mixed
        let expr = Expr::Object {
            pairs: vec![
                ObjectEntry::Shorthand("name".to_string()),
                ObjectEntry::KeyValue {
                    key: "age".to_string(),
                    value: Expr::Literal(Literal::Int(30)),
                },
            ],
        };
        assert_eq!(format!("{}", expr), "{name, age: 30}");
    }

    #[test]
    fn test_expr_display_array() {
        // Empty array
        let expr = Expr::Array(vec![]);
        assert_eq!(format!("{}", expr), "[]");

        // Array with elements
        let expr = Expr::Array(vec![
            Expr::Literal(Literal::Int(1)),
            Expr::Literal(Literal::Int(2)),
            Expr::Literal(Literal::Int(3)),
        ]);
        assert_eq!(format!("{}", expr), "[1, 2, 3]");
    }

    #[test]
    fn test_expr_display_literal() {
        let expr = Expr::Literal(Literal::String("hello".to_string()));
        assert_eq!(format!("{}", expr), "\"hello\"");
    }

    #[test]
    fn test_expr_display_identifier() {
        let expr = Expr::Identifier("func".to_string());
        assert_eq!(format!("{}", expr), "func");
    }

    #[test]
    fn test_expr_display_variable() {
        let expr = Expr::Variable("config".to_string());
        assert_eq!(format!("{}", expr), "$config");
    }

    #[test]
    fn test_expr_display_paren() {
        let expr = Expr::Paren(Box::new(Expr::Literal(Literal::Int(42))));
        assert_eq!(format!("{}", expr), "(42)");
    }

    #[test]
    fn test_expr_display_pipeline() {
        let expr = Expr::Pipeline(vec![
            Expr::Identity,
            Expr::FunctionCall {
                name: "map".to_string(),
                args: vec![Expr::FieldAccess {
                    base: Box::new(Expr::Identity),
                    fields: vec!["name".to_string()],
                }],
            },
        ]);
        assert_eq!(format!("{}", expr), ". | map(.name)");
    }

    #[test]
    fn test_expr_display_if() {
        let expr = Expr::If {
            condition: Box::new(Expr::Literal(Literal::Bool(true))),
            then_branch: Box::new(Expr::Literal(Literal::Int(1))),
            else_branch: Box::new(Expr::Literal(Literal::Int(0))),
        };
        assert_eq!(format!("{}", expr), "if true then 1 else 0 end");
    }

    #[test]
    fn test_expr_display_sequence() {
        let expr = Expr::Sequence(vec![
            Expr::FieldAccess {
                base: Box::new(Expr::Identity),
                fields: vec!["name".to_string()],
            },
            Expr::FieldAccess {
                base: Box::new(Expr::Identity),
                fields: vec!["age".to_string()],
            },
        ]);
        assert_eq!(format!("{}", expr), ".name, .age");
    }

    #[test]
    fn test_filter_display() {
        let filter = Filter {
            expr: Expr::Identity,
        };
        assert_eq!(format!("{}", filter), ".");
    }

    #[test]
    fn test_partial_eq() {
        // Test equality
        let expr1 = Expr::Literal(Literal::Int(42));
        let expr2 = Expr::Literal(Literal::Int(42));
        assert_eq!(expr1, expr2);

        // Test inequality
        let expr3 = Expr::Literal(Literal::Int(43));
        assert_ne!(expr1, expr3);

        // Test complex structures
        let obj1 = Expr::Object {
            pairs: vec![ObjectEntry::Shorthand("name".to_string())],
        };
        let obj2 = Expr::Object {
            pairs: vec![ObjectEntry::Shorthand("name".to_string())],
        };
        assert_eq!(obj1, obj2);

        let obj3 = Expr::Object {
            pairs: vec![ObjectEntry::Shorthand("age".to_string())],
        };
        assert_ne!(obj1, obj3);
    }

    #[test]
    fn test_clone() {
        let original = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Int(1))),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Literal(Literal::Int(2))),
        };
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_serde_serialize() {
        let expr = Expr::Literal(Literal::Int(42));
        let serialized = serde_json::to_string(&expr).unwrap();
        // Just check it serializes without error
        assert!(serialized.contains("42"));
    }
}
