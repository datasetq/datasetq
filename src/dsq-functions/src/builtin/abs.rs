use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use num_traits::Signed;

inventory::submit! {
    FunctionRegistration {
        name: "abs",
        func: builtin_abs,
    }
}

pub fn builtin_abs(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "abs() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Int(i) => Ok(Value::Int(i.abs())),
        Value::Float(f) => Ok(Value::Float(f.abs())),
        Value::BigInt(bi) => Ok(Value::BigInt(bi.abs())),
        _ => Err(dsq_shared::error::operation_error(
            "abs() requires numeric argument",
        )),
    }
}
