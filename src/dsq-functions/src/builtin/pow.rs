use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "pow",
        func: builtin_pow,
    }
}

pub fn builtin_pow(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "pow() expects 2 arguments: base and exponent",
        ));
    }

    let base = &args[0];
    let exponent = &args[1];

    match (base, exponent) {
        (Value::Int(b), Value::Int(e)) => Ok(Value::Float((*b as f64).powf(*e as f64))),
        (Value::Int(b), Value::Float(e)) => Ok(Value::Float((*b as f64).powf(*e))),
        (Value::Float(b), Value::Int(e)) => Ok(Value::Float(b.powf(*e as f64))),
        (Value::Float(b), Value::Float(e)) => Ok(Value::Float(b.powf(*e))),
        _ => Err(dsq_shared::error::operation_error(
            "pow() requires numeric base and exponent",
        )),
    }
}
