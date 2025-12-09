use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use inventory;
use std::collections::HashMap;

pub fn builtin_last(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "last() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            if arr.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(arr[arr.len() - 1].clone())
            }
        }
        Value::DataFrame(df) => {
            if df.height() == 0 {
                Ok(Value::Null)
            } else {
                // Return last row as object
                let last_idx = df.height() - 1;
                let mut row_obj = HashMap::new();
                for col_name in df.get_column_names() {
                    if let Ok(series) = df.column(col_name) {
                        if let Ok(val) = series.get(last_idx) {
                            let value = value_from_any_value(val).unwrap_or(Value::Null);
                            row_obj.insert(col_name.to_string(), value);
                        }
                    }
                }
                Ok(Value::Object(row_obj))
            }
        }
        _ => Ok(args[0].clone()),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "last",
        func: builtin_last,
    }
}
