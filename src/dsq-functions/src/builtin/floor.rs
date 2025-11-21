use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "floor",
        func: builtin_floor,
    }
}

pub fn builtin_floor(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "floor() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(i) => Ok(Value::Int(*i)),
        Value::Float(f) => Ok(Value::Float(f.floor())),
        _ => Err(dsq_shared::error::operation_error(
            "floor() requires numeric argument",
        )),
    }
}
