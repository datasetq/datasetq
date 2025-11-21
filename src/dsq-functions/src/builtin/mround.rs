use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "mround",
        func: builtin_mround,
    }
}

pub fn builtin_mround(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "mround() expects 2 arguments: value and multiple",
        ));
    }

    let value = match &args[0] {
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        Value::String(s) => s.parse::<f64>().map_err(|_| {
            dsq_shared::error::operation_error("mround() first argument must be numeric")
        })?,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "mround() first argument must be numeric",
            ))
        }
    };

    let multiple = match &args[1] {
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        Value::String(s) => s.parse::<f64>().map_err(|_| {
            dsq_shared::error::operation_error("mround() second argument must be numeric")
        })?,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "mround() second argument must be numeric",
            ))
        }
    };

    let rounded = if multiple == 0.0 {
        value
    } else {
        (value / multiple).round() * multiple
    };

    if rounded.fract() == 0.0 {
        Ok(Value::Int(rounded as i64))
    } else {
        Ok(Value::Float(rounded))
    }
}
