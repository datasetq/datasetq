use chrono::Utc;
use dsq_shared::value::Value;
use dsq_shared::Result;

pub fn builtin_today(args: &[Value]) -> Result<Value> {
    if !args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "today() expects no arguments",
        ));
    }
    let today = Utc::now().date_naive();
    Ok(Value::String(today.format("%Y-%m-%d").to_string()))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "today",
        func: builtin_today,
    }
}
