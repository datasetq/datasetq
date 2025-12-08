use crate::error::Result;
use crate::Value;

use super::Operation;

pub struct EqOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl EqOperation {
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

impl Operation for EqOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        Ok(Value::Bool(left_val == right_val))
    }

    fn description(&self) -> String {
        "equals".to_string()
    }
}

pub struct NeOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl NeOperation {
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

impl Operation for NeOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        Ok(Value::Bool(left_val != right_val))
    }

    fn description(&self) -> String {
        "not equals".to_string()
    }
}

pub struct LtOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl LtOperation {
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

impl Operation for LtOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        let ordering = dsq_shared::ops::compare_values(&left_val, &right_val)?;
        Ok(Value::Bool(ordering == std::cmp::Ordering::Less))
    }

    fn description(&self) -> String {
        "less than".to_string()
    }
}

pub struct LeOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl LeOperation {
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

impl Operation for LeOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        let ordering = dsq_shared::ops::compare_values(&left_val, &right_val)?;
        Ok(Value::Bool(ordering != std::cmp::Ordering::Greater))
    }

    fn description(&self) -> String {
        "less than or equal".to_string()
    }
}

pub struct GtOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl GtOperation {
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

impl Operation for GtOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        let ordering = dsq_shared::ops::compare_values(&left_val, &right_val)?;
        Ok(Value::Bool(ordering == std::cmp::Ordering::Greater))
    }

    fn description(&self) -> String {
        "greater than".to_string()
    }
}

pub struct GeOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl GeOperation {
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

impl Operation for GeOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        let ordering = dsq_shared::ops::compare_values(&left_val, &right_val)?;
        Ok(Value::Bool(ordering != std::cmp::Ordering::Less))
    }

    fn description(&self) -> String {
        "greater than or equal".to_string()
    }
}
