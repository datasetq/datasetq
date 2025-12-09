use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_range(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 3 {
        return Err(dsq_shared::error::operation_error(
            "range() expects 1-3 arguments",
        ));
    }

    let start = if args.len() >= 2 {
        match &args[0] {
            Value::Int(i) => *i,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "range() start must be integer",
                ));
            }
        }
    } else {
        0
    };

    let end = match &args[if args.len() >= 2 { 1 } else { 0 }] {
        Value::Int(i) => *i,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "range() end must be integer",
            ));
        }
    };

    let step = if args.len() == 3 {
        match &args[2] {
            Value::Int(i) if *i != 0 => *i,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "range() step must be non-zero integer",
                ));
            }
        }
    } else if start <= end {
        1
    } else {
        -1
    };

    let mut result = Vec::new();
    let mut current = start;
    if step > 0 {
        while current < end {
            result.push(Value::Int(current));
            current += step;
        }
    } else {
        while current > end {
            result.push(Value::Int(current));
            current += step;
        }
    }
    Ok(Value::Array(result))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "range",
        func: builtin_range,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_range_single_arg() {
        let result = builtin_range(&[Value::Int(5)]).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Int(0),
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(4)
            ])
        );
    }

    #[test]
    fn test_range_two_args() {
        let result = builtin_range(&[Value::Int(1), Value::Int(5)]).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(4)
            ])
        );
    }

    #[test]
    fn test_range_three_args() {
        let result = builtin_range(&[Value::Int(0), Value::Int(10), Value::Int(2)]).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Int(0),
                Value::Int(2),
                Value::Int(4),
                Value::Int(6),
                Value::Int(8)
            ])
        );
    }

    #[test]
    fn test_range_negative_step() {
        let result = builtin_range(&[Value::Int(5), Value::Int(0), Value::Int(-1)]).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Int(5),
                Value::Int(4),
                Value::Int(3),
                Value::Int(2),
                Value::Int(1)
            ])
        );
    }

    #[test]
    fn test_range_descending() {
        let result = builtin_range(&[Value::Int(5), Value::Int(0)]).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::Int(5),
                Value::Int(4),
                Value::Int(3),
                Value::Int(2),
                Value::Int(1)
            ])
        );
    }

    #[test]
    fn test_range_empty() {
        let result = builtin_range(&[Value::Int(0), Value::Int(0)]).unwrap();
        assert_eq!(result, Value::Array(vec![]));
    }

    #[test]
    fn test_range_invalid_args() {
        let result = builtin_range(&[]);
        assert!(result.is_err());

        let result = builtin_range(&[Value::Int(1), Value::Int(5), Value::Int(0)]);
        assert!(result.is_err());

        let result = builtin_range(&[Value::String("1".to_string())]);
        assert!(result.is_err());
    }
}
