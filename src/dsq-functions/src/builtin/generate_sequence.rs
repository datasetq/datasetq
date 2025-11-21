use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    crate::FunctionRegistration {
        name: "generate_sequence",
        func: builtin_generate_sequence,
    }
}

pub fn builtin_generate_sequence(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(dsq_shared::error::operation_error(
            "generate_sequence() expects 3 arguments: start, end, step",
        ));
    }

    let start = match &args[0] {
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "generate_sequence() start must be a number",
            ))
        }
    };

    let end = match &args[1] {
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "generate_sequence() end must be a number",
            ))
        }
    };

    let step = match &args[2] {
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "generate_sequence() step must be a number",
            ))
        }
    };

    if step == 0.0 {
        return Err(dsq_shared::error::operation_error(
            "generate_sequence() step cannot be zero",
        ));
    }

    let mut sequence = Vec::new();
    let mut current = start;

    if step > 0.0 {
        while current <= end {
            if current.fract() == 0.0 && current >= i64::MIN as f64 && current <= i64::MAX as f64 {
                sequence.push(Value::Int(current as i64));
            } else {
                sequence.push(Value::Float(current));
            }
            current += step;
        }
    } else {
        while current >= end {
            if current.fract() == 0.0 && current >= i64::MIN as f64 && current <= i64::MAX as f64 {
                sequence.push(Value::Int(current as i64));
            } else {
                sequence.push(Value::Float(current));
            }
            current += step;
        }
    }

    Ok(Value::Array(sequence))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_generate_sequence_basic() {
        // Test basic sequence: 0, 1, 2, 3, 4, 5
        let result =
            builtin_generate_sequence(&[Value::Int(0), Value::Int(5), Value::Int(1)]).unwrap();
        match result {
            Value::Array(seq) => {
                assert_eq!(seq.len(), 6);
                assert_eq!(seq[0], Value::Int(0));
                assert_eq!(seq[1], Value::Int(1));
                assert_eq!(seq[2], Value::Int(2));
                assert_eq!(seq[3], Value::Int(3));
                assert_eq!(seq[4], Value::Int(4));
                assert_eq!(seq[5], Value::Int(5));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_generate_sequence_with_start() {
        // Test sequence: 5, 6, 7, 8, 9, 10
        let result =
            builtin_generate_sequence(&[Value::Int(5), Value::Int(10), Value::Int(1)]).unwrap();
        match result {
            Value::Array(seq) => {
                assert_eq!(seq.len(), 6);
                assert_eq!(seq[0], Value::Int(5));
                assert_eq!(seq[1], Value::Int(6));
                assert_eq!(seq[2], Value::Int(7));
                assert_eq!(seq[3], Value::Int(8));
                assert_eq!(seq[4], Value::Int(9));
                assert_eq!(seq[5], Value::Int(10));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_generate_sequence_with_step() {
        // Test sequence: 0, 2, 4, 6, 8, 10
        let result =
            builtin_generate_sequence(&[Value::Int(0), Value::Int(10), Value::Int(2)]).unwrap();
        match result {
            Value::Array(seq) => {
                assert_eq!(seq.len(), 6);
                assert_eq!(seq[0], Value::Int(0));
                assert_eq!(seq[1], Value::Int(2));
                assert_eq!(seq[2], Value::Int(4));
                assert_eq!(seq[3], Value::Int(6));
                assert_eq!(seq[4], Value::Int(8));
                assert_eq!(seq[5], Value::Int(10));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_generate_sequence_negative_step() {
        // Test sequence: 10, 8, 6, 4, 2, 0
        let result =
            builtin_generate_sequence(&[Value::Int(10), Value::Int(0), Value::Int(-2)]).unwrap();
        match result {
            Value::Array(seq) => {
                assert_eq!(seq.len(), 6);
                assert_eq!(seq[0], Value::Int(10));
                assert_eq!(seq[1], Value::Int(8));
                assert_eq!(seq[2], Value::Int(6));
                assert_eq!(seq[3], Value::Int(4));
                assert_eq!(seq[4], Value::Int(2));
                assert_eq!(seq[5], Value::Int(0));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_generate_sequence_float_values() {
        // Test sequence: 0.0, 0.5, 1.0, 1.5, 2.0
        let result =
            builtin_generate_sequence(&[Value::Float(0.0), Value::Float(2.0), Value::Float(0.5)])
                .unwrap();
        match result {
            Value::Array(seq) => {
                assert_eq!(seq.len(), 5);
                assert_eq!(seq[0], Value::Float(0.0));
                assert_eq!(seq[1], Value::Float(0.5));
                assert_eq!(seq[2], Value::Float(1.0));
                assert_eq!(seq[3], Value::Float(1.5));
                assert_eq!(seq[4], Value::Float(2.0));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_generate_sequence_mixed_int_float() {
        // Test sequence: 1, 2, 3, 4
        let result =
            builtin_generate_sequence(&[Value::Int(1), Value::Float(4.0), Value::Int(1)]).unwrap();
        match result {
            Value::Array(seq) => {
                assert_eq!(seq.len(), 4);
                assert_eq!(seq[0], Value::Int(1));
                assert_eq!(seq[1], Value::Int(2));
                assert_eq!(seq[2], Value::Int(3));
                assert_eq!(seq[3], Value::Int(4));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_generate_sequence_empty() {
        // Test empty sequence when start >= end with positive step
        let result =
            builtin_generate_sequence(&[Value::Int(5), Value::Int(0), Value::Int(1)]).unwrap();
        match result {
            Value::Array(seq) => {
                assert_eq!(seq.len(), 0);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_generate_sequence_single_element() {
        // Test sequence: 5, 6
        let result =
            builtin_generate_sequence(&[Value::Int(5), Value::Int(6), Value::Int(1)]).unwrap();
        match result {
            Value::Array(seq) => {
                assert_eq!(seq.len(), 2);
                assert_eq!(seq[0], Value::Int(5));
                assert_eq!(seq[1], Value::Int(6));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_generate_sequence_invalid_args() {
        // Test with wrong number of arguments
        let result = builtin_generate_sequence(&[Value::Int(0), Value::Int(5)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 3 arguments"));

        let result = builtin_generate_sequence(&[
            Value::Int(0),
            Value::Int(5),
            Value::Int(1),
            Value::Int(2),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 3 arguments"));

        // Test with invalid start type
        let result = builtin_generate_sequence(&[
            Value::String("invalid".to_string()),
            Value::Int(5),
            Value::Int(1),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("start must be a number"));

        // Test with invalid end type
        let result = builtin_generate_sequence(&[
            Value::Int(0),
            Value::String("invalid".to_string()),
            Value::Int(1),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("end must be a number"));

        // Test with invalid step type
        let result = builtin_generate_sequence(&[
            Value::Int(0),
            Value::Int(5),
            Value::String("invalid".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("step must be a number"));

        // Test with zero step
        let result = builtin_generate_sequence(&[Value::Int(0), Value::Int(5), Value::Int(0)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("step cannot be zero"));
    }
}
