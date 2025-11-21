//! Advanced/specialized operations
//!
//! This module contains complex operations that don't fit other categories.

use crate::value::Value;
use crate::Result;
use std::any::Any;

use super::traits::{AssignmentOperator, Context, Operation};

/// Function call operation
pub struct FunctionCallOperation {
    /// Function name
    pub function_name: String,
    /// Argument operations
    pub arg_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// Built-in function implementation
    pub builtin_func: Option<
        std::sync::Arc<dyn Fn(&[crate::value::Value]) -> Result<crate::value::Value> + Send + Sync>,
    >,
}

impl FunctionCallOperation {
    /// Create a new function call operation
    pub fn new(
        function_name: String,
        arg_ops: Vec<Box<dyn Operation + Send + Sync>>,
        builtin_func: Option<
            std::sync::Arc<
                dyn Fn(&[crate::value::Value]) -> Result<crate::value::Value> + Send + Sync,
            >,
        >,
    ) -> Self {
        Self {
            function_name,
            arg_ops,
            builtin_func,
        }
    }
}

impl Operation for FunctionCallOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut context = None;
        self.apply_with_context(value, &mut context)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        if let Some(func) = &self.builtin_func {
            // Evaluate arguments
            let mut args = Vec::new();
            for arg_op in &self.arg_ops {
                let arg_value = arg_op.apply_with_context(value, context)?;
                args.push(arg_value);
            }

            // Special handling for select: pass the current input as the first argument
            if self.function_name == "select" && args.len() == 1 {
                args.insert(0, value.clone());
            }

            // Call the built-in function
            func(&args)
        } else {
            Err(crate::error::operation_error(format!(
                "Unknown function '{}'",
                self.function_name
            )))
        }
    }

    fn description(&self) -> String {
        format!("call function {}", self.function_name)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Delete operation
///
/// Removes fields or elements from objects/arrays (currently a placeholder implementation).
pub struct DelOperation {
    /// Operations that produce the paths to delete
    pub path_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl DelOperation {
    /// Creates a new delete operation with the given path operations
    pub fn new(path_ops: Vec<Box<dyn Operation + Send + Sync>>) -> Self {
        Self { path_ops }
    }
}

impl Operation for DelOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut context = None;
        self.apply_with_context(value, &mut context)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        _context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        // For now, just return the value unchanged
        // Del operations are complex and would need to modify the input structure
        Ok(value.clone())
    }

    fn description(&self) -> String {
        "delete".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Assignment operation
///
/// Assigns a value to a target location (field, variable, etc.) using the specified operator.
pub struct AssignmentOperation {
    /// Operations that produce the target location
    pub target_ops: Vec<Box<dyn Operation + Send + Sync>>,
    /// The assignment operator to use
    pub operator: AssignmentOperator,
    /// Operations that produce the value to assign
    pub value_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl AssignmentOperation {
    /// Creates a new assignment operation with the given target, operator, and value operations
    pub fn new(
        target_ops: Vec<Box<dyn Operation + Send + Sync>>,
        operator: AssignmentOperator,
        value_ops: Vec<Box<dyn Operation + Send + Sync>>,
    ) -> Self {
        Self {
            target_ops,
            operator,
            value_ops,
        }
    }
}

impl Operation for AssignmentOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut context = None;
        self.apply_with_context(value, &mut context)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        // Handle field assignment on objects
        if let Value::Object(obj) = value {
            // Check if this is a simple field assignment like .field |= value
            // target_ops should contain [IdentityOperation, FieldAccessOperation(field)]
            if self.target_ops.len() == 2 {
                // Check if second operation is field access
                if let Some(field_op) = self.target_ops[1]
                    .as_any()
                    .downcast_ref::<super::basic_ops::FieldAccessOperation>()
                {
                    if field_op.fields.len() == 1 {
                        let field_name = &field_op.fields[0];

                        // Get current value of the field
                        let current_value = obj.get(field_name).cloned().unwrap_or(Value::Null);

                        // Evaluate the value_ops to get the new value
                        let new_value = if self.value_ops.len() == 1 {
                            self.value_ops[0].apply_with_context(value, context)?
                        } else {
                            Value::Null
                        };

                        // Apply the operator
                        let final_value = match self.operator {
                            AssignmentOperator::AddAssign => {
                                match (&current_value, &new_value) {
                                    (Value::Int(a), Value::Int(b)) => Value::Int(a + b),
                                    (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
                                    (Value::String(a), Value::String(b)) => {
                                        Value::String(format!("{}{}", a, b))
                                    }
                                    _ => new_value, // For other types, just use the new value
                                }
                            }
                            AssignmentOperator::UpdateAssign => new_value,
                        };

                        // Create new object with modified field
                        let mut new_obj = obj.clone();
                        new_obj.insert(field_name.clone(), final_value);
                        return Ok(Value::Object(new_obj));
                    }
                }
            }
        }

        // For non-object values or complex paths, return the evaluated value
        // This handles cases where assignment doesn't make sense
        if self.value_ops.len() == 1 {
            self.value_ops[0].apply_with_context(value, context)
        } else {
            Ok(value.clone())
        }
    }

    fn description(&self) -> String {
        "assignment".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Join from file operation
///
/// Performs a join operation by loading data from a file and joining on specified keys.
pub struct JoinFromFileOperation {
    /// Path to the file to join with
    pub file_path: String,
    /// Key field name in the left (current) data
    pub left_key: String,
    /// Key field name in the right (file) data
    pub right_key: String,
}

impl JoinFromFileOperation {
    /// Creates a new join from file operation with the given file path and key fields
    pub fn new(file_path: String, left_key: String, right_key: String) -> Self {
        Self {
            file_path,
            left_key,
            right_key,
        }
    }
}

impl Operation for JoinFromFileOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut context = None;
        self.apply_with_context(value, &mut context)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        _context: &mut Option<&mut dyn Context>,
    ) -> Result<Value> {
        // This should be implemented for actual join functionality
        // For now, return the value unchanged
        Ok(value.clone())
    }

    fn description(&self) -> String {
        "join from file".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
