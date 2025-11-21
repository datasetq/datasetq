use super::Operation;
use crate::error::Result;
use crate::Value;

/// Literal value operation
pub struct LiteralOperation {
    pub value: Value,
}

impl LiteralOperation {
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
}

/// Variable access operation
pub struct VariableOperation {
    pub name: String,
}

impl VariableOperation {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl Operation for VariableOperation {
    fn apply(&self, _value: &Value) -> Result<Value> {
        // For now, variables are not supported
        Err(crate::error::Error::operation(format!(
            "Variable '{}' not found",
            self.name
        )))
    }

    fn description(&self) -> String {
        format!("variable: {}", self.name)
    }
}

/// Object construction operation
pub struct ObjectConstructOperation {
    pub field_ops: Vec<(
        Box<dyn Operation + Send + Sync>,
        Option<Vec<Box<dyn Operation + Send + Sync>>>,
    )>,
}

impl ObjectConstructOperation {
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
        if matches!(value, Value::Null) {
            return Ok(Value::Null);
        }

        let mut obj = std::collections::HashMap::new();

        for (key_op, value_op) in &self.field_ops {
            let key_value = key_op.apply(value)?;
            let key = match key_value {
                Value::String(s) => s,
                _ => {
                    return Err(crate::error::Error::operation(
                        "Object key must be a string",
                    ))
                }
            };

            let field_value = if let Some(ref ops) = value_op {
                let mut current = value.clone();
                for op in ops {
                    current = op.apply(&current)?;
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
}

/// Array construction operation
pub struct ArrayConstructOperation {
    pub element_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl ArrayConstructOperation {
    pub fn new(element_ops: Vec<Box<dyn Operation + Send + Sync>>) -> Self {
        Self { element_ops }
    }
}

impl Operation for ArrayConstructOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut arr = Vec::new();
        for op in &self.element_ops {
            arr.push(op.apply(value)?);
        }
        Ok(Value::Array(arr))
    }

    fn description(&self) -> String {
        "array construction".to_string()
    }
}
