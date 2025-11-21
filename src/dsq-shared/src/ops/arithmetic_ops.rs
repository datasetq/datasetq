//! Arithmetic operations
//!
//! This module contains mathematical operations on numeric values.

use crate::value::Value;
use crate::Result;
use std::any::Any;

use super::traits::{Context, Operation};
use super::utils::{add_values, div_values, mul_values, sub_values};

/// Addition operation
///
/// Performs addition on numeric values or concatenation on strings/arrays.
pub struct AddOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl AddOperation {
    /// Creates a new addition operation with the given operand operations
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

impl Operation for AddOperation {
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

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply_with_context(&right_val, context)?;
        }

        add_values(&left_val, &right_val)
    }

    fn description(&self) -> String {
        "add".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Subtraction operation
///
/// Performs subtraction on numeric values.
pub struct SubOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl SubOperation {
    /// Creates a new subtraction operation with the given operand operations
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

impl Operation for SubOperation {
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

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply_with_context(&right_val, context)?;
        }

        sub_values(&left_val, &right_val)
    }

    fn description(&self) -> String {
        "subtract".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Multiplication operation
///
/// Performs multiplication on numeric values.
pub struct MulOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl MulOperation {
    /// Creates a new multiplication operation with the given operand operations
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

impl Operation for MulOperation {
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

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply_with_context(&right_val, context)?;
        }

        mul_values(&left_val, &right_val)
    }

    fn description(&self) -> String {
        "multiply".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Division operation
///
/// Performs division on numeric values.
pub struct DivOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl DivOperation {
    /// Creates a new division operation with the given operand operations
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

impl Operation for DivOperation {
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

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply_with_context(&right_val, context)?;
        }

        div_values(&left_val, &right_val)
    }

    fn description(&self) -> String {
        "divide".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
