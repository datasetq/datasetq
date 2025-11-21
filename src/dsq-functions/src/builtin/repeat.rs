use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_repeat(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "repeat() expects 2 arguments: value and count",
        ));
    }

    let count = match &args[1] {
        Value::Int(c) if *c >= 0 => *c as usize,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "repeat() second argument must be a non-negative integer",
            ))
        }
    };

    let value = &args[0];
    let repeated: Vec<Value> = (0..count).map(|_| value.clone()).collect();
    Ok(Value::Array(repeated))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "repeat",
        func: builtin_repeat,
    }
}
