use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_date_diff(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "date_diff() expects 2 arguments",
        ));
    }

    match (&args[0], &args[1]) {
        (Value::Array(arr1), Value::Array(arr2)) => {
            if arr1.len() != arr2.len() {
                return Err(dsq_shared::error::operation_error(
                    "date_diff() arrays must have same length",
                ));
            }
            let mut results = Vec::new();
            for (v1, v2) in arr1.iter().zip(arr2.iter()) {
                let dt1 = crate::extract_timestamp(v1)?;
                let dt2 = crate::extract_timestamp(v2)?;
                let duration = (dt1 - dt2).abs();
                results.push(Value::Int(duration.num_days() as i64));
            }
            Ok(Value::Array(results))
        }
        _ => {
            let dt1 = crate::extract_timestamp(&args[0])?;
            let dt2 = crate::extract_timestamp(&args[1])?;
            let duration = (dt1 - dt2).abs();
            Ok(Value::Int(duration.num_days() as i64))
        }
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "date_diff",
        func: builtin_date_diff,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_date_diff_function() {
        // Test with date strings
        let result = builtin_date_diff(&[
            Value::String("2023-01-01".to_string()),
            Value::String("2023-01-05".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Int(4));

        // Test with same date
        let result = builtin_date_diff(&[
            Value::String("2023-01-01".to_string()),
            Value::String("2023-01-01".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Int(0));

        // Test with RFC3339 strings
        let result = builtin_date_diff(&[
            Value::String("2023-01-01T00:00:00Z".to_string()),
            Value::String("2023-01-05T00:00:00Z".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Int(4));

        // Test with arrays
        let arr1 = Value::Array(vec![
            Value::String("2023-01-01".to_string()),
            Value::String("2023-02-01".to_string()),
        ]);
        let arr2 = Value::Array(vec![
            Value::String("2023-01-05".to_string()),
            Value::String("2023-02-05".to_string()),
        ]);
        let result = builtin_date_diff(&[arr1, arr2]).unwrap();
        assert_eq!(result, Value::Array(vec![Value::Int(4), Value::Int(4)]));
    }
}
