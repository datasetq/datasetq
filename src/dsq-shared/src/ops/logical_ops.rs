//! Logical and conditional operations
//!
//! This module contains boolean logic and conditional execution operations.

use crate::value::Value;
use crate::Result;
use std::any::Any;

use super::traits::{Context, Operation};

/// Logical AND operation
///
/// Performs logical AND on boolean values using truthiness rules.
pub struct AndOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl AndOperation {
    /// Creates a new logical AND operation with the given operand operations
    pub fn new(
        left_ops: Vec<Box<dyn Operation + Send + Sync>>,
        right_ops: Vec<Box<dyn Operation + Send + Sync>>,
    ) -> Self {
        Self {
            left_ops,
            right_ops,
        }
    }
}

impl Operation for AndOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut context = None;
        self.apply_with_context(value, &mut context)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply_with_context(&left_val, context)?;
        }

        let left_truthy = match left_val {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            _ => false,
        };

        if !left_truthy {
            return Ok(Value::Bool(false));
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply_with_context(&right_val, context)?;
        }

        let right_truthy = match right_val {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            _ => false,
        };

        Ok(Value::Bool(right_truthy))
    }

    fn description(&self) -> String {
        "logical and".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Logical OR operation
///
/// Performs logical OR on boolean values using truthiness rules.
pub struct OrOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl OrOperation {
    /// Creates a new logical OR operation with the given operand operations
    pub fn new(
        left_ops: Vec<Box<dyn Operation + Send + Sync>>,
        right_ops: Vec<Box<dyn Operation + Send + Sync>>,
    ) -> Self {
        Self {
            left_ops,
            right_ops,
        }
    }
}

impl Operation for OrOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut context = None;
        self.apply_with_context(value, &mut context)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply_with_context(&left_val, context)?;
        }

        let left_truthy = match left_val {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            _ => false,
        };

        if left_truthy {
            return Ok(Value::Bool(true));
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply_with_context(&right_val, context)?;
        }

        let right_truthy = match right_val {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            _ => false,
        };

        Ok(Value::Bool(right_truthy))
    }

    fn description(&self) -> String {
        "logical or".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Conditional (if-then-else) operation
///
/// Evaluates a condition and returns the result of either the then or else branch.
pub struct IfOperation {
    /// Operations that produce the condition value
    pub condition_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations to evaluate if condition is truthy
    pub then_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations to evaluate if condition is falsy
    pub else_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl IfOperation {
    /// Creates a new conditional operation with the given condition, then, and else operations
    pub fn new(
        condition_ops: Vec<Box<dyn Operation + Send + Sync>>,
        then_ops: Vec<Box<dyn Operation + Send + Sync>>,
        else_ops: Vec<Box<dyn Operation + Send + Sync>>,
    ) -> Self {
        Self {
            condition_ops,
            then_ops,
            else_ops,
        }
    }
}

impl Operation for IfOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        self.apply_with_context(value, &mut None)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        let mut condition_val = value.clone();
        for op in &self.condition_ops {
            condition_val = op.apply_with_context(&condition_val, context)?;
        }

        let condition_truthy = match condition_val {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            _ => false,
        };

        if condition_truthy {
            let mut then_val = value.clone();
            for op in &self.then_ops {
                then_val = op.apply_with_context(&then_val, context)?;
            }
            Ok(then_val)
        } else {
            let mut else_val = value.clone();
            for op in &self.else_ops {
                else_val = op.apply_with_context(&else_val, context)?;
            }
            Ok(else_val)
        }
    }

    fn description(&self) -> String {
        "conditional".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Logical negation operation
///
/// Negates the truthiness of a value.
pub struct NegationOperation {
    /// Operations that produce the value to negate
    pub expr_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl NegationOperation {
    /// Creates a new negation operation with the given expression operations
    pub fn new(expr_ops: Vec<Box<dyn Operation + Send + Sync>>) -> Self {
        Self { expr_ops }
    }
}

impl Operation for NegationOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut context = None;
        self.apply_with_context(value, &mut context)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        let mut expr_val = value.clone();
        for op in &self.expr_ops {
            expr_val = op.apply_with_context(&expr_val, context)?;
        }

        let truthy = match expr_val {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            _ => false,
        };

        Ok(Value::Bool(!truthy))
    }

    fn description(&self) -> String {
        "logical not".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
