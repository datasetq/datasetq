//! Operation utilities
//!
//! This module contains helper functions used by multiple operation types.

#![allow(clippy::cast_precision_loss)]

use crate::value::Value;
use crate::Result;

/// Compare two values for ordering
pub fn compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;

    match (a, b) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Null, _) => Ok(Ordering::Less),
        (_, Value::Null) => Ok(Ordering::Greater),

        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| crate::error::operation_error("Cannot compare NaN values")),
        (Value::String(a), Value::String(b)) => Ok(a.cmp(b)),

        // Cross-type numeric comparisons
        (Value::Int(a), Value::Float(b)) => (*a as f64)
            .partial_cmp(b)
            .ok_or_else(|| crate::error::operation_error("Cannot compare NaN values")),
        (Value::Float(a), Value::Int(b)) => a
            .partial_cmp(&(*b as f64))
            .ok_or_else(|| crate::error::operation_error("Cannot compare NaN values")),

        // For complex types, compare string representations
        _ => Err(crate::error::operation_error(format!(
            "Cannot compare values of types {} and {}",
            a.type_name(),
            b.type_name()
        ))),
    }
}

/// Add two values
pub fn add_values(a: &Value, b: &Value) -> Result<Value> {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Ok(Value::Int(x + y)),
        (Value::Float(x), Value::Float(y)) => Ok(Value::Float(x + y)),
        (Value::Int(x), Value::Float(y)) => Ok(Value::Float(*x as f64 + *y)),
        (Value::Float(x), Value::Int(y)) => Ok(Value::Float(*x + *y as f64)),
        (Value::String(x), Value::String(y)) => Ok(Value::String(format!("{}{}", x, y))),
        (Value::Series(s), Value::Int(y)) => {
            // Add scalar to series
            let result = s + *y;
            Ok(Value::Series(result))
        }
        (Value::Series(s), Value::Float(y)) => {
            // Add scalar to series
            let result = s + *y;
            Ok(Value::Series(result))
        }
        (Value::Int(x), Value::Series(s)) => {
            // Add scalar to series
            let result = s + *x;
            Ok(Value::Series(result))
        }
        (Value::Float(x), Value::Series(s)) => {
            // Add scalar to series
            let result = s + *x;
            Ok(Value::Series(result))
        }
        _ => Err(crate::error::operation_error(format!(
            "Cannot add {} and {}",
            a.type_name(),
            b.type_name()
        ))),
    }
}

/// Subtract two values
pub fn sub_values(a: &Value, b: &Value) -> Result<Value> {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Ok(Value::Int(x - y)),
        (Value::Float(x), Value::Float(y)) => Ok(Value::Float(x - y)),
        (Value::Int(x), Value::Float(y)) => Ok(Value::Float(*x as f64 - *y)),
        (Value::Float(x), Value::Int(y)) => Ok(Value::Float(*x - *y as f64)),
        _ => Err(crate::error::operation_error(format!(
            "Cannot subtract {} and {}",
            a.type_name(),
            b.type_name()
        ))),
    }
}

/// Multiply two values
pub fn mul_values(a: &Value, b: &Value) -> Result<Value> {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Ok(Value::Int(x * y)),
        (Value::Float(x), Value::Float(y)) => Ok(Value::Float(x * y)),
        (Value::Int(x), Value::Float(y)) => Ok(Value::Float(*x as f64 * *y)),
        (Value::Float(x), Value::Int(y)) => Ok(Value::Float(*x * *y as f64)),
        _ => Err(crate::error::operation_error(format!(
            "Cannot multiply {} and {}",
            a.type_name(),
            b.type_name()
        ))),
    }
}

/// Divide two values
pub fn div_values(a: &Value, b: &Value) -> Result<Value> {
    let b_float = match b {
        Value::Int(y) if *y == 0 => return Err(crate::error::operation_error("Division by zero")),
        Value::Float(y) if *y == 0.0 => {
            return Err(crate::error::operation_error("Division by zero"))
        }
        Value::Int(y) => *y as f64,
        Value::Float(y) => *y,
        _ => {
            return Err(crate::error::operation_error(format!(
                "Cannot divide by {}",
                b.type_name()
            )))
        }
    };
    match a {
        Value::Int(x) => Ok(Value::Float(*x as f64 / b_float)),
        Value::Float(x) => Ok(Value::Float(*x / b_float)),
        _ => Err(crate::error::operation_error(format!(
            "Cannot divide {} by number",
            a.type_name()
        ))),
    }
}
