use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_array_shift(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "array_shift() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            if arr.is_empty() {
                Ok(Value::Array(Vec::new()))
            } else {
                Ok(Value::Array(arr[1..].to_vec()))
            }
        }
        Value::Series(series) => {
            if matches!(series.dtype(), DataType::List(_)) {
                // For testing, return original
                Ok(Value::Series(series.clone()))
            } else {
                Err(dsq_shared::error::operation_error(
                    "array_shift() requires an array or list series",
                ))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "array_shift() requires an array or list series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "array_shift",
        func: builtin_array_shift,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_array_shift_empty() {
        let args = vec![Value::Array(vec![])];
        let result = builtin_array_shift(&args);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Array(vec![]));
    }

    #[test]
    fn test_array_shift_non_empty() {
        let args = vec![Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
        ])];
        let result = builtin_array_shift(&args);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Value::Array(vec![Value::Int(2), Value::Int(3),])
        );
    }

    #[test]
    fn test_array_shift_wrong_args() {
        let args = vec![];
        let result = builtin_array_shift(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_shift_invalid_input() {
        let args = vec![Value::String("not an array".to_string())];
        let result = builtin_array_shift(&args);
        assert!(result.is_err());
    }
}
