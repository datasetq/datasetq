use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_keys(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "keys() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Object(obj) => {
            let mut keys: Vec<String> = obj.keys().cloned().collect();
            keys.sort();
            let keys: Vec<Value> = keys.into_iter().map(Value::String).collect();
            Ok(Value::Array(keys))
        }
        Value::Array(arr) => {
            let indices: Vec<Value> = (0..arr.len()).map(|i| Value::Int(i as i64)).collect();
            Ok(Value::Array(indices))
        }
        Value::DataFrame(df) => {
            let columns: Vec<Value> = df
                .get_column_names()
                .iter()
                .map(|name| Value::String(name.to_string()))
                .collect();
            Ok(Value::Array(columns))
        }
        Value::LazyFrame(lf) => {
            // Collect the LazyFrame to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_keys(&[Value::DataFrame(df)])
        }
        _ => Ok(Value::Array(Vec::new())),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "keys",
        func: builtin_keys,
    }
}
