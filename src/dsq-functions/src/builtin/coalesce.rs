use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_coalesce(args: &[Value]) -> Result<Value> {
    for arg in args {
        if !matches!(arg, Value::Null) {
            return Ok(arg.clone());
        }
    }
    Ok(Value::Null)
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "coalesce",
        func: builtin_coalesce,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn test_coalesce_null_and_value() {
        let result =
            builtin_coalesce(&[Value::Null, Value::String("default".to_string())]).unwrap();
        assert_eq!(result, Value::String("default".to_string()));
    }

    #[test]
    fn test_coalesce_first_non_null() {
        let result = builtin_coalesce(&[
            Value::String("first".to_string()),
            Value::String("second".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("first".to_string()));
    }

    #[test]
    fn test_coalesce_all_null() {
        let result = builtin_coalesce(&[Value::Null, Value::Null]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_coalesce_empty_args() {
        let result = builtin_coalesce(&[]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_coalesce_with_dataframe() {
        let df = DataFrame::new(vec![Series::new("a".into().into(), vec![1, 2, 3])]).unwrap();
        let result = builtin_coalesce(&[Value::Null, Value::DataFrame(df.clone())]).unwrap();
        match result {
            Value::DataFrame(res_df) => {
                assert_eq!(res_df.height(), 3);
                assert_eq!(res_df.width(), 1);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_coalesce_with_int() {
        let result = builtin_coalesce(&[Value::Null, Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_coalesce_with_float() {
        let result = builtin_coalesce(&[Value::Null, Value::Float(3.14)]).unwrap();
        assert_eq!(result, Value::Float(3.14));
    }

    #[test]
    fn test_coalesce_with_bool() {
        let result = builtin_coalesce(&[Value::Null, Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_coalesce_with_array() {
        let arr = vec![Value::Int(1), Value::Int(2)];
        let result = builtin_coalesce(&[Value::Null, Value::Array(arr.clone())]).unwrap();
        assert_eq!(result, Value::Array(arr));
    }

    #[test]
    fn test_coalesce_with_object() {
        let mut obj = std::collections::HashMap::new();
        obj.insert("key".to_string(), Value::String("value".to_string()));
        let result = builtin_coalesce(&[Value::Null, Value::Object(obj.clone())]).unwrap();
        assert_eq!(result, Value::Object(obj));
    }
}
