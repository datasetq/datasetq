use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "pi",
        func: builtin_pi,
    }
}

pub fn builtin_pi(_args: &[Value]) -> Result<Value> {
    Ok(Value::Float(std::f64::consts::PI))
}
