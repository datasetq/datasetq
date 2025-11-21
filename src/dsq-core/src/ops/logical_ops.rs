use super::Operation;
use crate::error::Result;
use crate::Value;

pub struct AndOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl AndOperation {
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

impl Operation for AndOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let left_truthy = match left_val {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            _ => false,
        };

        if !left_truthy {
            return Ok(Value::Bool(false));
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        let right_truthy = match right_val {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            _ => false,
        };

        Ok(Value::Bool(right_truthy))
    }

    fn description(&self) -> String {
        "logical and".to_string()
    }
}

pub struct OrOperation {
    pub left_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub right_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl OrOperation {
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

impl Operation for OrOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut left_val = value.clone();
        for op in &self.left_ops {
            left_val = op.apply(&left_val)?;
        }

        let left_truthy = match left_val {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            _ => false,
        };

        if left_truthy {
            return Ok(Value::Bool(true));
        }

        let mut right_val = value.clone();
        for op in &self.right_ops {
            right_val = op.apply(&right_val)?;
        }

        let right_truthy = match right_val {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            _ => false,
        };

        Ok(Value::Bool(right_truthy))
    }

    fn description(&self) -> String {
        "logical or".to_string()
    }
}

pub struct NegationOperation {
    pub expr_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl NegationOperation {
    pub fn new(expr_ops: Vec<Box<dyn Operation + Send + Sync>>) -> Self {
        Self { expr_ops }
    }
}

impl Operation for NegationOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut expr_val = value.clone();
        for op in &self.expr_ops {
            expr_val = op.apply(&expr_val)?;
        }

        let truthy = match expr_val {
            Value::Bool(b) => b,
            Value::Int(i) if i != 0 => true,
            Value::Float(f) if f != 0.0 => true,
            Value::String(s) if !s.is_empty() => true,
            Value::Array(arr) if !arr.is_empty() => true,
            Value::Object(obj) if !obj.is_empty() => true,
            _ => false,
        };

        Ok(Value::Bool(!truthy))
    }

    fn description(&self) -> String {
        "logical not".to_string()
    }
}
