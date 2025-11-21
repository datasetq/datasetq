use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    crate::FunctionRegistration {
        name: "iferror",
        func: builtin_iferror,
    }
}

pub fn builtin_iferror(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "iferror() expects 2 arguments",
        ));
    }

    // Return first argument if it's not null, otherwise return second argument
    match &args[0] {
        Value::Null => Ok(args[1].clone()),
        _ => Ok(args[0].clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iferror_with_null() {
        let result = builtin_iferror(&[Value::Null, Value::String("default".to_string())]).unwrap();
        assert_eq!(result, Value::String("default".to_string()));
    }

    #[test]
    fn test_iferror_with_non_null() {
        let result = builtin_iferror(&[
            Value::String("value".to_string()),
            Value::String("default".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("value".to_string()));
    }

    #[test]
    fn test_iferror_with_int() {
        let result = builtin_iferror(&[Value::Int(42), Value::Int(0)]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_iferror_with_null_second_arg() {
        let result = builtin_iferror(&[Value::Null, Value::Null]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_iferror_wrong_number_of_args() {
        let result = builtin_iferror(&[Value::Null]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("iferror() expects 2 arguments"));
    }

    #[test]
    fn test_iferror_with_array() {
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let result = builtin_iferror(&[arr.clone(), Value::String("default".to_string())]).unwrap();
        assert_eq!(result, arr);
    }

    #[test]
    fn test_iferror_with_object() {
        let obj = Value::Object(
            [("key".to_string(), Value::String("value".to_string()))]
                .into_iter()
                .collect(),
        );
        let result = builtin_iferror(&[obj.clone(), Value::String("default".to_string())]).unwrap();
        assert_eq!(result, obj);
    }
}
