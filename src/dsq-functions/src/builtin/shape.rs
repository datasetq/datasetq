use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_shape(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "shape() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::DataFrame(df) => {
            let shape = vec![
                Value::Int(df.height() as i64),
                Value::Int(df.width() as i64),
            ];
            Ok(Value::Array(shape))
        }
        Value::Array(arr) => {
            let shape = vec![Value::Int(arr.len() as i64)];
            Ok(Value::Array(shape))
        }
        _ => Err(dsq_shared::error::operation_error(
            "shape() requires DataFrame or Array argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "shape",
        func: builtin_shape,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;

    #[test]
    fn test_shape_dataframe() {
        let df = DataFrame::new(vec![
            Series::new("name".into().into(), &["Alice", "Bob", "Charlie"]),
            Series::new("age".into().into(), &[25, 30, 35]),
            Series::new("score".into().into(), &[85.5, 92.0, 78.3]),
        ])
        .unwrap();

        let result = builtin_shape(&[Value::DataFrame(df)]).unwrap();
        if let Value::Array(shape) = result {
            assert_eq!(shape.len(), 2);
            assert_eq!(shape[0], Value::Int(3)); // 3 rows
            assert_eq!(shape[1], Value::Int(3)); // 3 columns
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_shape_array() {
        let arr = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
        ]);
        let result = builtin_shape(&[arr]).unwrap();
        if let Value::Array(shape) = result {
            assert_eq!(shape.len(), 1);
            assert_eq!(shape[0], Value::Int(4));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_shape_empty_array() {
        let arr = Value::Array(vec![]);
        let result = builtin_shape(&[arr]).unwrap();
        if let Value::Array(shape) = result {
            assert_eq!(shape.len(), 1);
            assert_eq!(shape[0], Value::Int(0));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_shape_invalid_type() {
        let result = builtin_shape(&[Value::String("test".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires DataFrame or Array argument"));
    }

    #[test]
    fn test_shape_no_args() {
        let result = builtin_shape(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_shape_multiple_args() {
        let arr = Value::Array(vec![Value::Int(1)]);
        let result = builtin_shape(&[arr, Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_shape_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("shape"));
    }
}
