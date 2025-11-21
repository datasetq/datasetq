use dsq_shared::value::Value;
use dsq_shared::Result;
use std::collections::HashMap;

pub fn builtin_stdev_p(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "stdev_p() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let values: Vec<f64> = arr
                .iter()
                .filter_map(|v| match *v {
                    Value::Int(i) => Some(i as f64),
                    Value::Float(f) => Some(f),
                    _ => None,
                })
                .collect();
            if values.len() < 2 {
                return Ok(Value::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            Ok(Value::Float(variance.sqrt()))
        }
        Value::DataFrame(df) => {
            let mut stds = HashMap::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() {
                        // std calculation for DataFrame columns - placeholder
                        stds.insert(col_name.to_string(), Value::Null);
                    }
                }
            }
            Ok(Value::Object(stds))
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                Ok(Value::Null) // Placeholder - std calculation for series
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "stdev_p() requires array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "stdev_p",
        func: builtin_stdev_p,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_stdev_p() {
        // Test with array of integers
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        let result = builtin_stdev_p(&[Value::Array(arr)]).unwrap();
        // Population standard deviation of [1,2,3,4,5] is sqrt(2) ≈ 1.414213562
        match result {
            Value::Float(val) => assert!((val - 1.414213562).abs() < 1e-6),
            _ => panic!("Expected Float"),
        }

        // Test with empty array
        let result = builtin_stdev_p(&[Value::Array(vec![])]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with single element
        let arr = vec![Value::Int(5)];
        let result = builtin_stdev_p(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Null); // Since len < 2

        // Test with floats
        let arr = vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)];
        let result = builtin_stdev_p(&[Value::Array(arr)]).unwrap();
        // Population std of [1,2,3] is sqrt(2/3) ≈ 0.816496581
        match result {
            Value::Float(val) => assert!((val - 0.816496581).abs() < 1e-6),
            _ => panic!("Expected Float"),
        }

        // Test error: no args
        let result = builtin_stdev_p(&[]);
        assert!(result.is_err());

        // Test error: too many args
        let result = builtin_stdev_p(&[Value::Array(vec![Value::Int(1)]), Value::Int(2)]);
        assert!(result.is_err());
    }
}
