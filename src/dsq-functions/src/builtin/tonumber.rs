use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use num_traits::ToPrimitive;

pub fn builtin_tonumber(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "tonumber() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            if let Ok(i) = s.parse::<i64>() {
                Ok(Value::Int(i))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(Value::Float(f))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Int(i) => Ok(Value::Int(*i)),
        Value::Float(f) => Ok(Value::Float(*f)),
        Value::BigInt(bi) => {
            // Try to convert BigInt to i64 if possible
            if let Some(i) = bi.to_i64() {
                Ok(Value::Int(i))
            } else {
                Ok(Value::Float(bi.to_string().parse::<f64>().unwrap_or(0.0)))
            }
        }
        _ => Ok(Value::Null),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "tonumber",
        func: builtin_tonumber,
    }
}
