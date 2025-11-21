//! Core traits and types for operations
//!
//! This module defines the fundamental traits and basic types that other operations depend on.

use crate::value::Value;
use crate::Result;
use std::any::Any;

/// Assignment operators for variable assignments and updates
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AssignmentOperator {
    /// Add assignment operator (+=)
    AddAssign,
    /// Update assignment operator (=)
    UpdateAssign,
}

/// Context trait for operations that need additional context
pub trait Context {
    /// Get a variable value by name
    fn get_variable(&self, name: &str) -> Option<&Value>;

    /// Set a variable value
    fn set_variable(&mut self, name: &str, value: Value);

    /// Downcast to Any for concrete type checking
    fn as_any(&self) -> &dyn std::any::Any;

    /// Downcast mutably
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Simple context implementation that holds a single value
pub struct SimpleContext {
    /// The current value being processed
    pub value: Value,
}

impl Context for SimpleContext {
    /// Returns the current value for any variable name (since SimpleContext has no named variables)
    fn get_variable(&self, _name: &str) -> Option<&Value> {
        // For simple context, we don't have named variables, just the current value
        // But for joins, we might need to access the current row value
        Some(&self.value)
    }

    /// Does nothing since SimpleContext doesn't support setting variables
    fn set_variable(&mut self, _name: &str, _value: Value) {
        // Simple context doesn't support setting variables
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Trait for operations that can be applied to values
///
/// This trait provides a common interface for all data operations,
/// allowing them to be composed and chained together.
pub trait Operation {
    /// Apply the operation to a value
    fn apply(&self, value: &Value) -> Result<Value>;

    /// Apply the operation to a value with optional context
    fn apply_with_context(
        &self,
        value: &Value,
        _context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        // Default implementation ignores context
        self.apply(value)
    }

    /// Get a description of what this operation does
    fn description(&self) -> String;

    /// Check if this operation can be applied to the given value type
    fn is_applicable(&self, value: &Value) -> bool {
        // Default implementation: try to apply and see if it works
        self.apply(value).is_ok()
    }

    /// Get as Any for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}
