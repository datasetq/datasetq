//! Data construction operations
//!
//! This module contains operations that create new data structures from existing values.

use crate::value::Value;
use crate::Result;
use std::any::Any;

use super::traits::{Context, Operation};

/// Object construction operation
///
/// Creates a new object by evaluating key-value pairs from operations.
pub struct ObjectConstructOperation {
    /// Vector of (key_operation, optional_value_operations) pairs
    ///
    /// For each pair, the key operation produces the field name,
    /// and the value operations (if present) produce the field value.
    /// If no value operations are provided, the key is used as a field access.
    pub field_ops: Vec<(
        Box<dyn Operation + Send + Sync>,
        Option<Vec<Box<dyn Operation + Send + Sync>>>,
    )>,
}

impl ObjectConstructOperation {
    /// Creates a new object construction operation with the given field operations
    pub fn new(
        field_ops: Vec<(
            Box<dyn Operation + Send + Sync>,
            Option<Vec<Box<dyn Operation + Send + Sync>>>,
        )>,
    ) -> Self {
        Self { field_ops }
    }
}

impl Operation for ObjectConstructOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut context = None;
        self.apply_with_context(value, &mut context)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        // If the input value is null (e.g., from a failed select), return null
        if matches!(value, Value::Null) {
            return Ok(Value::Null);
        }

        // Apply object construction to the value (do not iterate over arrays)
        let mut obj = std::collections::HashMap::new();

        for (key_op, value_op) in &self.field_ops {
            let key_value = key_op.apply_with_context(value, context)?;
            let key = match key_value {
                Value::String(s) => s,
                _ => return Err(crate::error::operation_error("Object key must be a string")),
            };

            let field_value = if let Some(ref ops) = value_op {
                let mut current = value.clone();
                for op in ops {
                    current = op.apply_with_context(&current, context)?;
                }
                current
            } else {
                // Shorthand: use the key as a field access
                value.field(&key)?
            };

            obj.insert(key, field_value);
        }

        Ok(Value::Object(obj))
    }

    fn description(&self) -> String {
        "object construction".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Array construction operation
///
/// Creates a new array by evaluating a sequence of element operations.
pub struct ArrayConstructOperation {
    /// The operations that produce each element of the array
    pub element_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl ArrayConstructOperation {
    /// Creates a new array construction operation with the given element operations
    pub fn new(element_ops: Vec<Box<dyn Operation + Send + Sync>>) -> Self {
        Self { element_ops }
    }
}

impl Operation for ArrayConstructOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut context = None;
        self.apply_with_context(value, &mut context)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        let mut arr = Vec::new();
        for op in &self.element_ops {
            arr.push(op.apply_with_context(value, context)?);
        }
        Ok(Value::Array(arr))
    }

    fn description(&self) -> String {
        "array construction".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Sequence operation (comma operator)
///
/// Evaluates multiple expressions and returns their results as an array.
pub struct SequenceOperation {
    /// Vector of expression sequences, where each inner vector represents one expression
    pub expr_ops: Vec<Vec<Box<dyn Operation + Send + Sync>>>,
}

impl SequenceOperation {
    /// Creates a new sequence operation with the given expression operations
    pub fn new(expr_ops: Vec<Vec<Box<dyn Operation + Send + Sync>>>) -> Self {
        Self { expr_ops }
    }
}

impl Operation for SequenceOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut context = None;
        self.apply_with_context(value, &mut context)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        let mut results = Vec::new();
        for ops in &self.expr_ops {
            let mut val = value.clone();
            for op in ops {
                val = op.apply_with_context(&val, context)?;
            }
            results.push(val);
        }
        Ok(Value::Array(results))
    }

    fn description(&self) -> String {
        "sequence".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
