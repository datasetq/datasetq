//! Basic data access operations
//!
//! This module contains fundamental operations for accessing and manipulating basic data structures.

use crate::value::Value;
use crate::Result;
use std::any::Any;

use super::traits::Operation;

/// Identity operation - returns input unchanged
pub struct IdentityOperation;

impl Operation for IdentityOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        Ok(value.clone())
    }

    fn description(&self) -> String {
        "identity".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Literal value operation
///
/// Represents a literal value that is returned unchanged when applied.
pub struct LiteralOperation {
    /// The literal value to return
    pub value: Value,
}

impl LiteralOperation {
    /// Creates a new literal operation with the given value
    pub fn new(value: Value) -> Self {
        Self { value }
    }
}

impl Operation for LiteralOperation {
    fn apply(&self, _value: &Value) -> Result<Value> {
        Ok(self.value.clone())
    }

    fn description(&self) -> String {
        format!("literal: {:?}", self.value)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Field access operation
pub struct FieldAccessOperation {
    /// The sequence of field names to access
    pub fields: Vec<String>,
}

impl FieldAccessOperation {
    /// Create a new field access operation for a single field
    pub fn new(field: String) -> Self {
        Self {
            fields: vec![field],
        }
    }

    /// Create a new field access operation for multiple fields
    pub fn with_fields(fields: Vec<String>) -> Self {
        Self { fields }
    }
}

impl Operation for FieldAccessOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut current = value.clone();
        for field in &self.fields {
            current = current.field(field)?;
        }
        Ok(current)
    }

    fn description(&self) -> String {
        format!("field access: {}", self.fields.join("."))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Array indexing operation
///
/// Represents indexing into arrays, strings, or DataFrames using a sequence of index operations.
pub struct IndexOperation {
    /// The sequence of operations that produce the index values
    pub index_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl IndexOperation {
    /// Creates a new index operation with the given index operations
    pub fn new(index_ops: Vec<Box<dyn Operation + Send + Sync>>) -> Self {
        Self { index_ops }
    }
}

impl Operation for IndexOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut current = value.clone();
        for op in &self.index_ops {
            let index_value = op.apply(&current)?;
            match index_value {
                Value::Int(idx) => current = current.index(idx)?,
                _ => return Err(crate::error::operation_error("Index must be an integer")),
            }
        }
        Ok(current)
    }

    fn description(&self) -> String {
        "array index".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Array iteration operation (.[])
pub struct IterateOperation;

impl Operation for IterateOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        match value {
            Value::Array(arr) => Ok(Value::Array(arr.clone())),
            Value::Object(obj) => {
                let values: Vec<Value> = obj.values().cloned().collect();
                Ok(Value::Array(values))
            }
            Value::DataFrame(df) => {
                // Convert DataFrame to array of objects
                let mut rows = Vec::new();
                for i in 0..df.height() {
                    let mut row_obj = std::collections::HashMap::new();
                    for col_name in df.get_column_names() {
                        if let Ok(series) = df.column(col_name) {
                            if let Ok(val) = series.get(i) {
                                let value = match val {
                                    polars::prelude::AnyValue::Int64(i) => Value::Int(i),
                                    polars::prelude::AnyValue::Float64(f) => Value::Float(f),
                                    polars::prelude::AnyValue::String(s) => {
                                        Value::String(s.to_string())
                                    }
                                    polars::prelude::AnyValue::Boolean(b) => Value::Bool(b),
                                    _ => Value::Null,
                                };
                                row_obj.insert(col_name.to_string(), value);
                            }
                        }
                    }
                    rows.push(Value::Object(row_obj));
                }
                Ok(Value::Array(rows))
            }
            _ => Ok(value.clone()),
        }
    }

    fn description(&self) -> String {
        "iterate array/object".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
