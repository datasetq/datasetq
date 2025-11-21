//! Comparison operations
//!
//! This module contains value comparison operations for filtering and conditional logic.

use crate::value::Value;
use crate::Result;
use std::any::Any;

use super::traits::{Context, Operation};
use super::utils::compare_values;

/// Equality comparison operation
///
/// Compares two values for equality.
pub struct EqOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl EqOperation {
    /// Creates a new equality operation with the given operand operations
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

impl Operation for EqOperation {
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

        Ok(Value::Bool(left_val == right_val))
    }

    fn description(&self) -> String {
        "equals".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Inequality comparison operation
///
/// Compares two values for inequality.
pub struct NeOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl NeOperation {
    /// Creates a new inequality operation with the given operand operations
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

impl Operation for NeOperation {
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

        Ok(Value::Bool(left_val != right_val))
    }

    fn description(&self) -> String {
        "not equals".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Less than comparison operation
///
/// Compares if the left value is less than the right value.
pub struct LtOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl LtOperation {
    /// Creates a new less than operation with the given operand operations
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

impl Operation for LtOperation {
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

        let ordering = compare_values(&left_val, &right_val)?;
        Ok(Value::Bool(ordering == std::cmp::Ordering::Less))
    }

    fn description(&self) -> String {
        "less than".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Less than or equal comparison operation
///
/// Compares if the left value is less than or equal to the right value.
pub struct LeOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl LeOperation {
    /// Creates a new less than or equal operation with the given operand operations
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

impl Operation for LeOperation {
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

        let ordering = compare_values(&left_val, &right_val)?;
        Ok(Value::Bool(ordering != std::cmp::Ordering::Greater))
    }

    fn description(&self) -> String {
        "less than or equal".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Greater than comparison operation
///
/// Compares if the left value is greater than the right value.
pub struct GtOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl GtOperation {
    /// Creates a new greater than operation with the given operand operations
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

impl Operation for GtOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        self.apply_with_context(value, &mut None)
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

        let ordering = compare_values(&left_val, &right_val)?;
        Ok(Value::Bool(ordering == std::cmp::Ordering::Greater))
    }

    fn description(&self) -> String {
        "greater than".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Greater than or equal comparison operation
///
/// Compares if the left value is greater than or equal to the right value.
pub struct GeOperation {
    /// Operations that produce the left operand
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Operations that produce the right operand
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl GeOperation {
    /// Creates a new greater than or equal operation with the given operand operations
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

impl Operation for GeOperation {
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

        let ordering = compare_values(&left_val, &right_val)?;
        Ok(Value::Bool(ordering != std::cmp::Ordering::Less))
    }

    fn description(&self) -> String {
        "greater than or equal".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
