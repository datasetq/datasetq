use crate::inventory;
use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;

inventory::submit! {
    FunctionRegistration {
        name: "iif",
        func: builtin_iif,
    }
}

pub fn builtin_iif(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(dsq_shared::error::operation_error(
            "iif() expects 3 arguments",
        ));
    }

    let condition = dsq_shared::value::is_truthy(&args[0]);
    if condition {
        Ok(args[1].clone())
    } else {
        Ok(args[2].clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_iif_function() {
        let result = builtin_iif(&[
            Value::Bool(true),
            Value::String("yes".to_string()),
            Value::String("no".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("yes".to_string()));

        let result = builtin_iif(&[
            Value::Bool(false),
            Value::String("yes".to_string()),
            Value::String("no".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("no".to_string()));
    }

    #[test]
    fn test_iif_with_numbers() {
        let result = builtin_iif(&[Value::Bool(true), Value::Int(42), Value::Int(24)]).unwrap();
        assert_eq!(result, Value::Int(42));

        let result = builtin_iif(&[Value::Bool(false), Value::Int(42), Value::Int(24)]).unwrap();
        assert_eq!(result, Value::Int(24));
    }

    #[test]
    fn test_iif_with_null() {
        let result = builtin_iif(&[
            Value::Bool(true),
            Value::Null,
            Value::String("not null".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Null);

        let result = builtin_iif(&[
            Value::Bool(false),
            Value::Null,
            Value::String("not null".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("not null".to_string()));
    }

    #[test]
    fn test_iif_invalid_args() {
        // Too few arguments
        let result = builtin_iif(&[Value::Bool(true), Value::String("yes".to_string())]);
        assert!(result.is_err());

        // Too many arguments
        let result = builtin_iif(&[
            Value::Bool(true),
            Value::String("yes".to_string()),
            Value::String("no".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_iif_truthy_conditions() {
        // Test various truthy values
        let result = builtin_iif(&[
            Value::Int(1),
            Value::String("true".to_string()),
            Value::String("false".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("true".to_string()));

        let result = builtin_iif(&[
            Value::String("hello".to_string()),
            Value::String("true".to_string()),
            Value::String("false".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("true".to_string()));

        let result = builtin_iif(&[
            Value::Array(vec![Value::Int(1)]),
            Value::String("true".to_string()),
            Value::String("false".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("true".to_string()));

        // Test falsy values
        let result = builtin_iif(&[
            Value::Int(0),
            Value::String("true".to_string()),
            Value::String("false".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("false".to_string()));

        let result = builtin_iif(&[
            Value::Null,
            Value::String("true".to_string()),
            Value::String("false".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("false".to_string()));

        let result = builtin_iif(&[
            Value::Array(vec![]),
            Value::String("true".to_string()),
            Value::String("false".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("false".to_string()));
    }

    #[test]
    fn test_iif_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("iif"));
    }
}
