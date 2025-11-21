use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "tail",
        func: builtin_tail,
    }
}

pub fn builtin_tail(args: &[Value]) -> Result<Value> {
    let n = if args.len() == 1 {
        5 // default
    } else if args.len() == 2 {
        match &args[1] {
            Value::Int(i) if *i >= 0 => *i as usize,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "tail() second argument must be a non-negative integer",
                ));
            }
        }
    } else {
        return Err(dsq_shared::error::operation_error(
            "tail() expects 1 or 2 arguments",
        ));
    };

    match &args[0] {
        Value::Array(arr) => {
            if arr.len() <= n {
                Ok(Value::Array(arr.clone()))
            } else {
                let tail: Vec<Value> = arr[arr.len() - n..].to_vec();
                Ok(Value::Array(tail))
            }
        }
        Value::DataFrame(df) => {
            let tail_df = df.tail(Some(n));
            Ok(Value::DataFrame(tail_df))
        }
        Value::Series(series) => {
            let tail_series = series.tail(Some(n));
            Ok(Value::Series(tail_series))
        }
        _ => Ok(args[0].clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_tail_array_default() {
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
            Value::Int(6),
        ];
        let result = builtin_tail(&[Value::Array(arr)]).unwrap();
        let expected = vec![
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
            Value::Int(6),
        ];
        assert_eq!(result, Value::Array(expected));
    }

    #[test]
    fn test_tail_array_custom_n() {
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        let result = builtin_tail(&[Value::Array(arr), Value::Int(2)]).unwrap();
        let expected = vec![Value::Int(4), Value::Int(5)];
        assert_eq!(result, Value::Array(expected));
    }

    #[test]
    fn test_tail_array_smaller_than_n() {
        let arr = vec![Value::Int(1), Value::Int(2)];
        let result = builtin_tail(&[Value::Array(arr.clone()), Value::Int(5)]).unwrap();
        assert_eq!(result, Value::Array(arr));
    }

    #[test]
    fn test_tail_dataframe() {
        let series = Series::new("col", vec![1i64, 2, 3, 4, 5, 6]);
        let df = DataFrame::new(vec![series]).unwrap();
        let result = builtin_tail(&[Value::DataFrame(df), Value::Int(3)]).unwrap();
        if let Value::DataFrame(tail_df) = result {
            assert_eq!(tail_df.height(), 3);
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_tail_series() {
        let series = Series::new("col", vec![1i64, 2, 3, 4, 5, 6]);
        let result = builtin_tail(&[Value::Series(series), Value::Int(2)]).unwrap();
        if let Value::Series(tail_series) = result {
            assert_eq!(tail_series.len(), 2);
        } else {
            panic!("Expected Series");
        }
    }

    #[test]
    fn test_tail_other_types() {
        let result = builtin_tail(&[Value::Int(42)]);
        assert_eq!(result.unwrap(), Value::Int(42));
    }

    #[test]
    fn test_tail_wrong_args() {
        let result = builtin_tail(&[]);
        assert!(result.is_err());

        let result = builtin_tail(&[Value::Int(1), Value::Int(2), Value::Int(3)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_tail_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("tail"));
    }
}
