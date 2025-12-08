use crate::error::Result;
use crate::Value;

use super::Operation;

/// Identity operation - returns input unchanged
pub struct IdentityOperation;

impl Operation for IdentityOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        Ok(value.clone())
    }

    fn description(&self) -> String {
        "identity".to_string()
    }
}

/// Field access operation
pub struct FieldAccessOperation {
    pub fields: Vec<String>,
}

impl FieldAccessOperation {
    #[must_use]
    pub fn new(field: String) -> Self {
        Self {
            fields: vec![field],
        }
    }

    #[must_use]
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
}

/// Array indexing operation
pub struct IndexOperation {
    pub index_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl IndexOperation {
    #[must_use]
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
                _ => return Err(crate::error::Error::operation("Index must be an integer")),
            }
        }
        Ok(current)
    }

    fn description(&self) -> String {
        "array index".to_string()
    }
}

/// Array slicing operation
pub struct SliceOperation {
    pub start_ops: Option<Vec<Box<dyn Operation + Send + Sync>>>,
    pub end_ops: Option<Vec<Box<dyn Operation + Send + Sync>>>,
}

impl SliceOperation {
    #[must_use]
    pub fn new(
        start_ops: Option<Vec<Box<dyn Operation + Send + Sync>>>,
        end_ops: Option<Vec<Box<dyn Operation + Send + Sync>>>,
    ) -> Self {
        Self { start_ops, end_ops }
    }
}

impl Operation for SliceOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let start = if let Some(ref ops) = self.start_ops {
            let mut start_val = value.clone();
            for op in ops {
                start_val = op.apply(&start_val)?;
            }
            match start_val {
                Value::Int(i) => Some(usize::try_from(i).map_err(|_| {
                    crate::error::Error::operation("Slice start index out of range")
                })?),
                _ => {
                    return Err(crate::error::Error::operation(
                        "Slice start must be an integer",
                    ));
                }
            }
        } else {
            None
        };

        let end =
            if let Some(ref ops) = self.end_ops {
                let mut end_val = value.clone();
                for op in ops {
                    end_val = op.apply(&end_val)?;
                }
                match end_val {
                    Value::Int(i) => Some(usize::try_from(i).map_err(|_| {
                        crate::error::Error::operation("Slice end index out of range")
                    })?),
                    _ => {
                        return Err(crate::error::Error::operation(
                            "Slice end must be an integer",
                        ));
                    }
                }
            } else {
                None
            };

        match value {
            Value::Array(arr) => {
                let start_idx = start.unwrap_or(0);
                let end_idx = end.unwrap_or(arr.len());
                Ok(Value::Array(arr[start_idx..end_idx].to_vec()))
            }
            Value::DataFrame(df) => {
                let start_idx = i64::try_from(start.unwrap_or(0)).map_err(|_| {
                    crate::error::Error::operation("Start index out of range for i64")
                })?;
                let end_idx = end.unwrap_or(df.height());
                let end_idx_i64 = i64::try_from(end_idx).map_err(|_| {
                    crate::error::Error::operation("End index out of range for i64")
                })?;
                let length = usize::try_from(end_idx_i64 - start_idx)
                    .map_err(|_| crate::error::Error::operation("Slice length out of range"))?;
                Ok(Value::DataFrame(df.slice(start_idx, length)))
            }
            _ => Err(crate::error::Error::operation(
                "Slice operation requires array or DataFrame",
            )),
        }
    }

    fn description(&self) -> String {
        "array slice".to_string()
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
                                    polars::prelude::AnyValue::Utf8(s) => {
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
}
