use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_length(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "length() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => Ok(Value::Int(arr.len() as i64)),
        Value::String(s) => Ok(Value::Int(s.chars().count() as i64)),
        Value::Object(obj) => Ok(Value::Int(obj.len() as i64)),
        Value::DataFrame(df) => Ok(Value::Int(df.height() as i64)),
        Value::Series(s) => Ok(Value::Int(s.len() as i64)),
        Value::Null => Ok(Value::Int(0)),
        _ => Ok(Value::Int(1)),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "length",
        func: builtin_length,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::datatypes::PlSmallStr;
    use polars::prelude::{Column, DataFrame, NamedFrom, Series};

    #[test]
    fn test_length_array() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_length(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn test_length_empty_array() {
        let arr: Vec<Value> = vec![];
        let result = builtin_length(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn test_length_string() {
        let result = builtin_length(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(result, Value::Int(5));
    }

    #[test]
    fn test_length_string_unicode() {
        let result = builtin_length(&[Value::String("héllo".to_string())]).unwrap();
        assert_eq!(result, Value::Int(5)); // Unicode characters count as 1
    }

    #[test]
    fn test_length_empty_string() {
        let result = builtin_length(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn test_length_object() {
        let mut obj = std::collections::HashMap::new();
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("b".to_string(), Value::Int(2));
        let result = builtin_length(&[Value::Object(obj)]).unwrap();
        assert_eq!(result, Value::Int(2));
    }

    #[test]
    fn test_length_empty_object() {
        let obj = std::collections::HashMap::new();
        let result = builtin_length(&[Value::Object(obj)]).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn test_length_null() {
        let result = builtin_length(&[Value::Null]).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn test_length_other_values() {
        let result = builtin_length(&[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(1));

        let result = builtin_length(&[Value::Float(3.14)]).unwrap();
        assert_eq!(result, Value::Int(1));

        let result = builtin_length(&[Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_length_dataframe() {
        let df = DataFrame::new(vec![
            Column::new(PlSmallStr::from("a"), vec![1i64, 2, 3]),
            Column::new(PlSmallStr::from("b"), vec![4i64, 5, 6]),
        ])
        .unwrap();
        let result = builtin_length(&[Value::DataFrame(df)]).unwrap();
        assert_eq!(result, Value::Int(3)); // height is 3
    }

    #[test]
    fn test_length_series() {
        let series = Series::new(PlSmallStr::from("test"), vec![1, 2, 3, 4, 5]);
        let result = builtin_length(&[Value::Series(series)]).unwrap();
        assert_eq!(result, Value::Int(5));
    }

    #[test]
    fn test_length_empty_series() {
        let series = Series::new(PlSmallStr::from("empty"), Vec::<i32>::new());
        let result = builtin_length(&[Value::Series(series)]).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn test_length_no_args() {
        let result = builtin_length(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_length_too_many_args() {
        let result = builtin_length(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }
}
