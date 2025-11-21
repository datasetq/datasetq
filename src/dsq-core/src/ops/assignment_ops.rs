use super::Operation;
use crate::error::{Error, Result};
use crate::Value;

pub struct AssignAddOperation {
    pub target_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub value_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl AssignAddOperation {
    pub fn new(
        target_ops: Vec<Box<dyn Operation + Send + Sync>>,
        value_ops: Vec<Box<dyn Operation + Send + Sync>>,
    ) -> Self {
        Self {
            target_ops,
            value_ops,
        }
    }

    fn get_target_field(&self) -> Option<String> {
        // Check if the last operation is FieldAccessOperation
        if let Some(last_op) = self.target_ops.last() {
            // This is a bit hacky, but we can downcast to FieldAccessOperation
            // For now, assume it's FieldAccessOperation and get the field
            // Since we can't downcast easily, let's check the description
            let desc = last_op.description();
            if desc.starts_with("field access: ") {
                Some(desc[14..].to_string()) // Remove "field access: "
            } else {
                None
            }
        } else {
            None
        }
    }

    fn apply_to_dataframe(
        &self,
        df: &polars::prelude::DataFrame,
        field_name: &str,
        add_val: &Value,
    ) -> Result<Value> {
        // Get the current column
        let current_series = df
            .column(field_name)
            .map_err(|_| Error::operation(format!("Field '{}' not found", field_name)))?;

        // Add the value to the series
        let new_series = match add_val {
            Value::Int(i) => current_series + *i,
            Value::Float(f) => current_series + *f,
            _ => return Err(Error::operation("Can only add numeric values to columns")),
        };

        // Create new DataFrame with the modified column
        let mut new_df = df.clone();
        new_df
            .replace(field_name, new_series)
            .map_err(|e| Error::operation(format!("Failed to replace column: {}", e)))?;

        Ok(Value::DataFrame(new_df))
    }
}

impl Operation for AssignAddOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        match value {
            Value::DataFrame(df) => {
                // For DataFrame, find the field name from target_ops
                if let Some(field_name) = self.get_target_field() {
                    // Get the value to add
                    let mut add_val = value.clone();
                    for op in &self.value_ops {
                        add_val = op.apply(&add_val)?;
                    }

                    // Modify the DataFrame
                    self.apply_to_dataframe(df, &field_name, &add_val)
                } else {
                    Err(Error::operation("Assignment target must be a field access"))
                }
            }
            _ => {
                // For other values, apply as before (though this may not work well)
                let mut target_val = value.clone();
                for op in &self.target_ops {
                    target_val = op.apply(&target_val)?;
                }

                let mut add_val = value.clone();
                for op in &self.value_ops {
                    add_val = op.apply(&add_val)?;
                }

                Ok(dsq_shared::ops::add_values(&target_val, &add_val)?)
            }
        }
    }

    fn description(&self) -> String {
        "assign add".to_string()
    }
}

pub struct AssignUpdateOperation {
    pub target_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub value_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl AssignUpdateOperation {
    pub fn new(
        target_ops: Vec<Box<dyn Operation + Send + Sync>>,
        value_ops: Vec<Box<dyn Operation + Send + Sync>>,
    ) -> Self {
        Self {
            target_ops,
            value_ops,
        }
    }

    /// Helper method to update nested fields in an object
    fn update_nested_field(
        obj: &mut std::collections::HashMap<String, Value>,
        fields: &[&str],
        value: Value,
    ) -> Result<()> {
        if fields.is_empty() {
            return Err(crate::error::Error::operation("Empty field path"));
        }

        if fields.len() == 1 {
            obj.insert(fields[0].to_string(), value);
            return Ok(());
        }

        // Navigate to the nested object
        let mut current = obj;
        for &field in &fields[..fields.len() - 1] {
            match current.get_mut(field) {
                Some(Value::Object(ref mut nested_obj)) => {
                    current = nested_obj;
                }
                Some(_) => {
                    return Err(crate::error::Error::operation(format!(
                        "Field '{}' is not an object",
                        field
                    )));
                }
                None => {
                    return Err(crate::error::Error::operation(format!(
                        "Field '{}' not found",
                        field
                    )));
                }
            }
        }

        // Update the final field
        let last_field = fields[fields.len() - 1];
        current.insert(last_field.to_string(), value);
        Ok(())
    }
}

impl Operation for AssignUpdateOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        // For assignment on objects, we need to modify the current value
        if let Value::Object(ref obj) = value {
            // Check if target is a field access
            if self.target_ops.len() == 1 {
                if let Some(field_op) = self.target_ops.first() {
                    // Check if it's a FieldAccessOperation by trying to downcast
                    // For now, check description for field access
                    let desc = field_op.description();
                    if desc.starts_with("field access: ") {
                        let field_path = &desc[14..]; // Remove "field access: "
                        let fields: Vec<&str> = field_path.split('.').collect();

                        // Evaluate the value
                        let mut value_val = value.clone();
                        for op in &self.value_ops {
                            value_val = op.apply(&value_val)?;
                        }

                        // Create new object with updated field
                        let mut new_obj = obj.clone();
                        if fields.len() == 1 {
                            // Simple field update
                            new_obj.insert(field_path.to_string(), value_val);
                        } else {
                            // Nested field update
                            Self::update_nested_field(&mut new_obj, &fields, value_val)?;
                        }
                        return Ok(Value::Object(new_obj));
                    }
                }
            }
        }

        // Fallback: evaluate target and value, then return value
        let mut value_val = value.clone();
        for op in &self.value_ops {
            value_val = op.apply(&value_val)?;
        }

        Ok(value_val)
    }

    fn description(&self) -> String {
        "assign update".to_string()
    }
}
