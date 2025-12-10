use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_flatten(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "flatten() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let mut flattened = Vec::new();
            for item in arr {
                match item {
                    Value::Array(inner) => flattened.extend(inner.clone()),
                    _ => flattened.push(item.clone()),
                }
            }
            Ok(Value::Array(flattened))
        }
        _ => Ok(args[0].clone()),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "flatten",
        func: builtin_flatten,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_flatten_simple() {
        let arr = Value::Array(vec![
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            Value::Array(vec![Value::Int(3), Value::Int(4)]),
        ]);
        let result = builtin_flatten(&[arr]).unwrap();
        match result {
            Value::Array(flattened) => {
                assert_eq!(flattened.len(), 4);
                assert_eq!(flattened[0], Value::Int(1));
                assert_eq!(flattened[1], Value::Int(2));
                assert_eq!(flattened[2], Value::Int(3));
                assert_eq!(flattened[3], Value::Int(4));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_flatten_mixed() {
        let arr = Value::Array(vec![
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            Value::String("hello".to_string()),
            Value::Array(vec![Value::Int(3)]),
        ]);
        let result = builtin_flatten(&[arr]).unwrap();
        match result {
            Value::Array(flattened) => {
                assert_eq!(flattened.len(), 4);
                assert_eq!(flattened[0], Value::Int(1));
                assert_eq!(flattened[1], Value::Int(2));
                assert_eq!(flattened[2], Value::String("hello".to_string()));
                assert_eq!(flattened[3], Value::Int(3));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_flatten_empty() {
        let arr = Value::Array(vec![]);
        let result = builtin_flatten(&[arr]).unwrap();
        match result {
            Value::Array(flattened) => {
                assert_eq!(flattened.len(), 0);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_flatten_no_arrays() {
        let arr = Value::Array(vec![
            Value::Int(1),
            Value::String("test".to_string()),
            Value::Bool(true),
        ]);
        let result = builtin_flatten(&[arr]).unwrap();
        match result {
            Value::Array(flattened) => {
                assert_eq!(flattened.len(), 3);
                assert_eq!(flattened[0], Value::Int(1));
                assert_eq!(flattened[1], Value::String("test".to_string()));
                assert_eq!(flattened[2], Value::Bool(true));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_flatten_non_array() {
        let val = Value::String("hello".to_string());
        let result = builtin_flatten(std::slice::from_ref(&val)).unwrap();
        assert_eq!(result, val);
    }

    #[test]
    fn test_flatten_wrong_args() {
        let result = builtin_flatten(&[]);
        assert!(result.is_err());
        let result = builtin_flatten(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_flatten_registered_via_inventory() {
        let mut found = false;
        for func in inventory::iter::<crate::FunctionRegistration> {
            if func.name == "flatten" {
                found = true;
                // Test that the function works
                let arr = Value::Array(vec![
                    Value::Array(vec![Value::Int(1), Value::Int(2)]),
                    Value::Int(3),
                ]);
                let result = (func.func)(&[arr]).unwrap();
                match result {
                    Value::Array(flattened) => {
                        assert_eq!(flattened.len(), 3);
                        assert_eq!(flattened[0], Value::Int(1));
                        assert_eq!(flattened[1], Value::Int(2));
                        assert_eq!(flattened[2], Value::Int(3));
                    }
                    _ => panic!("Expected Array"),
                }
                break;
            }
        }
        assert!(found, "flatten function not found in inventory");
    }
}
