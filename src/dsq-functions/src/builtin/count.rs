use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_count(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "count() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => Ok(Value::Int(arr.len() as i64)),
        Value::Object(obj) => Ok(Value::Int(obj.len() as i64)),
        Value::DataFrame(df) => Ok(Value::Int(df.height() as i64)),
        Value::Series(series) => Ok(Value::Int(series.len() as i64)),
        Value::String(s) => Ok(Value::Int(s.chars().count() as i64)),
        _ => Ok(Value::Int(1)),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "count",
        func: builtin_count,
    }
}
