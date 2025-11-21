use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "ceil",
        func: builtin_ceil,
    }
}

pub fn builtin_ceil(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "ceil() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(i) => Ok(Value::Int(*i)),
        Value::Float(f) => Ok(Value::Float(f.ceil())),
        _ => Err(dsq_shared::error::operation_error(
            "ceil() requires numeric argument",
        )),
    }
}
