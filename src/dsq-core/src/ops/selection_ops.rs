use polars::prelude::NewChunkedArray;

use crate::error::{Error, Result};
use crate::Value;

use super::Operation;

/// Select condition operation for filtering (select(condition))
pub struct SelectConditionOperation {
    pub condition_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl SelectConditionOperation {
    #[must_use]
    pub fn new(condition_ops: Vec<Box<dyn Operation + Send + Sync>>) -> Self {
        Self { condition_ops }
    }
}

impl Operation for SelectConditionOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        // Evaluate the condition
        let mut condition_value = value.clone();
        for op in &self.condition_ops {
            condition_value = op.apply(&condition_value)?;
        }

        // Handle DataFrame filtering
        if let Value::DataFrame(df) = value {
            if let Value::Series(mask_series) = &condition_value {
                // Convert the series to a boolean mask
                let mut mask_vec = Vec::new();
                for i in 0..mask_series.len() {
                    match mask_series.get(i) {
                        Ok(val) => {
                            let is_true = match val {
                                polars::prelude::AnyValue::Boolean(b) => b,
                                polars::prelude::AnyValue::Int64(i) => i != 0,
                                polars::prelude::AnyValue::Float64(f) => f != 0.0,
                                polars::prelude::AnyValue::String(s) => !s.is_empty(),
                                _ => false,
                            };
                            mask_vec.push(is_true);
                        }
                        Err(_) => mask_vec.push(false),
                    }
                }

                if mask_vec.len() == df.height() {
                    let mask_chunked =
                        polars::prelude::BooleanChunked::from_slice("mask".into(), &mask_vec);
                    match df.filter(&mask_chunked) {
                        Ok(filtered_df) => return Ok(Value::DataFrame(filtered_df)),
                        Err(e) => {
                            return Err(Error::Operation(
                                format!("select() failed to filter DataFrame: {e}").into(),
                            ));
                        }
                    }
                }
            }
        }

        // For non-DataFrame values or when condition is not a series, check if condition is truthy
        let is_truthy = match condition_value {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            Value::DataFrame(df) if df.height() > 0 => true,
            Value::Series(series) if !series.is_empty() => true,
            _ => false,
        };

        if is_truthy {
            Ok(value.clone())
        } else {
            Ok(Value::Null)
        }
    }

    fn description(&self) -> String {
        "select with condition".to_string()
    }
}
