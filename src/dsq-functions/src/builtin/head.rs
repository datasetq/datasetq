use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "head",
        func: builtin_head,
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "limit",
        func: builtin_head,
    }
}

pub fn builtin_head(args: &[Value]) -> Result<Value> {
    let n = if args.len() == 1 {
        5 // default
    } else if args.len() == 2 {
        match &args[1] {
            Value::Int(i) if *i >= 0 => *i as usize,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "head() second argument must be a non-negative integer",
                ));
            }
        }
    } else {
        return Err(dsq_shared::error::operation_error(
            "head() expects 1 or 2 arguments",
        ));
    };

    match &args[0] {
        Value::Array(arr) => {
            let head: Vec<Value> = arr.iter().take(n).cloned().collect();
            Ok(Value::Array(head))
        }
        Value::DataFrame(df) => {
            let head_df = df.head(Some(n));
            Ok(Value::DataFrame(head_df))
        }
        Value::Series(series) => {
            let head_series = series.head(Some(n));
            Ok(Value::Series(head_series))
        }
        _ => Ok(args[0].clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;

    #[test]
    fn test_head_array_default() {
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
            Value::Int(6),
        ];
        let result = builtin_head(&[Value::Array(arr)]).unwrap();
        let expected = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        assert_eq!(result, Value::Array(expected));
    }

    #[test]
    fn test_head_array_custom_n() {
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        let result = builtin_head(&[Value::Array(arr), Value::Int(2)]).unwrap();
        let expected = vec![Value::Int(1), Value::Int(2)];
        assert_eq!(result, Value::Array(expected));
    }

    #[test]
    fn test_head_array_smaller_than_n() {
        let arr = vec![Value::Int(1), Value::Int(2)];
        let result = builtin_head(&[Value::Array(arr.clone()), Value::Int(5)]).unwrap();
        assert_eq!(result, Value::Array(arr));
    }

    #[test]
    fn test_head_dataframe() {
        let series = Series::new("col".into().into(), vec![1i64, 2, 3, 4, 5, 6]);
        let df = DataFrame::new(vec![series]).unwrap();
        let result = builtin_head(&[Value::DataFrame(df), Value::Int(3)]).unwrap();
        if let Value::DataFrame(head_df) = result {
            assert_eq!(head_df.height(), 3);
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_head_series() {
        let series = Series::new("col".into().into(), vec![1i64, 2, 3, 4, 5, 6]);
        let result = builtin_head(&[Value::Series(series), Value::Int(2)]).unwrap();
        if let Value::Series(head_series) = result {
            assert_eq!(head_series.len(), 2);
        } else {
            panic!("Expected Series");
        }
    }

    #[test]
    fn test_head_other_types() {
        let result = builtin_head(&[Value::Int(42)]);
        assert_eq!(result.unwrap(), Value::Int(42));
    }

    #[test]
    fn test_head_wrong_args() {
        let result = builtin_head(&[]);
        assert!(result.is_err());

        let result = builtin_head(&[Value::Int(1), Value::Int(2), Value::Int(3)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_head_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("head"));
    }
}
