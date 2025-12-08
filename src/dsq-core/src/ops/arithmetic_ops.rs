use crate::error::Result;
use crate::Value;

use super::Operation;

/// Binary operations
pub struct AddOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl AddOperation {
    #[must_use]
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
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        Ok(dsq_shared::ops::add_values(&left_val, &right_val)?)
    }

    fn description(&self) -> String {
        "add".to_string()
    }
}

pub struct SubOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl SubOperation {
    #[must_use]
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
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        Ok(dsq_shared::ops::sub_values(&left_val, &right_val)?)
    }

    fn description(&self) -> String {
        "subtract".to_string()
    }
}

pub struct MulOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl MulOperation {
    #[must_use]
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
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        Ok(dsq_shared::ops::mul_values(&left_val, &right_val)?)
    }

    fn description(&self) -> String {
        "multiply".to_string()
    }
}

pub struct DivOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl DivOperation {
    #[must_use]
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
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        Ok(dsq_shared::ops::div_values(&left_val, &right_val)?)
    }

    fn description(&self) -> String {
        "divide".to_string()
    }
}
