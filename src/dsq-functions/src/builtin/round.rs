use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "round",
        func: builtin_round,
    }
}

pub fn builtin_round(args: &[Value]) -> Result<Value> {
    if args.len() < 1 || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "round() expects 1 or 2 arguments",
        ));
    }

    let value = match &args[0] {
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "round() first argument must be numeric",
            ))
        }
    };

    let decimals = if args.len() == 2 {
        match &args[1] {
            Value::Int(i) => *i as i32,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "round() second argument must be an integer",
                ))
            }
        }
    } else {
        0
    };

    let factor = 10f64.powi(decimals);
    let rounded = (value * factor).round() / factor;

    if decimals == 0 && rounded.fract() == 0.0 {
        Ok(Value::Int(rounded as i64))
    } else {
        Ok(Value::Float(rounded))
    }
}
