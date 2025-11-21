use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "generate_uuidv4",
        func: builtin_generate_uuidv4,
    }
}

pub fn builtin_generate_uuidv4(args: &[Value]) -> Result<Value> {
    if !args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "generate_uuidv4() expects no arguments",
        ));
    }
    let uuid = uuid::Uuid::new_v4();
    Ok(Value::String(uuid.to_string()))
}
