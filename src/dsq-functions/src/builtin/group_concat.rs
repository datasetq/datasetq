use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_group_concat(args: &[Value]) -> Result<Value> {
    if args.len() < 1 || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "group_concat() expects 1 or 2 arguments",
        ));
    }

    let separator = if args.len() == 2 {
        match &args[1] {
            Value::String(s) => s.clone(),
            _ => ",".to_string(),
        }
    } else {
        ",".to_string()
    };

    match &args[0] {
        Value::Array(arr) => {
            let strings: Vec<String> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    _ => v.to_string(),
                })
                .collect();
            Ok(Value::String(strings.join(&separator)))
        }
        _ => Err(dsq_shared::error::operation_error(
            "group_concat() requires array argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "group_concat",
        func: builtin_group_concat,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_group_concat_basic() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ];
        let result = builtin_group_concat(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::String("a,b,c".to_string()));
    }

    #[test]
    fn test_group_concat_custom_separator() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ];
        let result =
            builtin_group_concat(&[Value::Array(arr), Value::String(";".to_string())]).unwrap();
        assert_eq!(result, Value::String("a;b;c".to_string()));
    }

    #[test]
    fn test_group_concat_mixed_types() {
        let arr = vec![
            Value::String("hello".to_string()),
            Value::Int(42),
            Value::Bool(true),
        ];
        let result = builtin_group_concat(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::String("hello,42,true".to_string()));
    }

    #[test]
    fn test_group_concat_empty_array() {
        let arr = vec![];
        let result = builtin_group_concat(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_group_concat_single_element() {
        let arr = vec![Value::String("single".to_string())];
        let result = builtin_group_concat(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::String("single".to_string()));
    }

    #[test]
    fn test_group_concat_invalid_separator() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let result = builtin_group_concat(&[Value::Array(arr), Value::Int(1)]).unwrap();
        assert_eq!(result, Value::String("a,b".to_string())); // Should use default separator
    }

    #[test]
    fn test_group_concat_no_args() {
        let result = builtin_group_concat(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_group_concat_too_many_args() {
        let arr = vec![Value::String("a".to_string())];
        let result = builtin_group_concat(&[
            Value::Array(arr),
            Value::String(",".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_group_concat_non_array() {
        let result = builtin_group_concat(&[Value::String("not an array".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_group_concat_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("group_concat"));
    }
}
