use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "var",
        func: builtin_var,
    }
}

pub fn builtin_var(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "var() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let values: Vec<f64> = arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            if values.len() < 2 {
                return Ok(Value::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            Ok(Value::Float(variance))
        }
        Value::DataFrame(df) => {
            let mut vars = std::collections::HashMap::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() {
                        // Calculate variance for numeric columns
                        let mut values = Vec::new();
                        for i in 0..series.len() {
                            if let Ok(val) = series.get(i) {
                                match val {
                                    AnyValue::Int8(n) => values.push(n as f64),
                                    AnyValue::Int16(n) => values.push(n as f64),
                                    AnyValue::Int32(n) => values.push(n as f64),
                                    AnyValue::Int64(n) => values.push(n as f64),
                                    AnyValue::UInt8(n) => values.push(n as f64),
                                    AnyValue::UInt16(n) => values.push(n as f64),
                                    AnyValue::UInt32(n) => values.push(n as f64),
                                    AnyValue::UInt64(n) => values.push(n as f64),
                                    AnyValue::Float32(n) => values.push(n as f64),
                                    AnyValue::Float64(n) => values.push(n),
                                    _ => {}
                                }
                            }
                        }
                        if values.len() >= 2 {
                            let mean = values.iter().sum::<f64>() / values.len() as f64;
                            let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                                / (values.len() - 1) as f64;
                            vars.insert(col_name.to_string(), Value::Float(variance));
                        } else {
                            vars.insert(col_name.to_string(), Value::Null);
                        }
                    } else {
                        // Non-numeric columns get null
                        vars.insert(col_name.to_string(), Value::Null);
                    }
                }
            }
            Ok(Value::Object(vars))
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                let mut values = Vec::new();
                for i in 0..series.len() {
                    if let Ok(val) = series.get(i) {
                        match val {
                            AnyValue::Int8(n) => values.push(n as f64),
                            AnyValue::Int16(n) => values.push(n as f64),
                            AnyValue::Int32(n) => values.push(n as f64),
                            AnyValue::Int64(n) => values.push(n as f64),
                            AnyValue::UInt8(n) => values.push(n as f64),
                            AnyValue::UInt16(n) => values.push(n as f64),
                            AnyValue::UInt32(n) => values.push(n as f64),
                            AnyValue::UInt64(n) => values.push(n as f64),
                            AnyValue::Float32(n) => values.push(n as f64),
                            AnyValue::Float64(n) => values.push(n),
                            _ => {}
                        }
                    }
                }
                if values.len() >= 2 {
                    let mean = values.iter().sum::<f64>() / values.len() as f64;
                    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                        / (values.len() - 1) as f64;
                    Ok(Value::Float(variance))
                } else {
                    Ok(Value::Null)
                }
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "var() requires array, DataFrame, or Series",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_var_array() {
        // Test with array of integers
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        let result = builtin_var(&[Value::Array(arr)]).unwrap();
        // Variance of [1,2,3,4,5] = 2.5
        assert_eq!(result, Value::Float(2.5));

        // Test with array of floats
        let arr = vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)];
        let result = builtin_var(&[Value::Array(arr)]).unwrap();
        // Variance of [1.0,2.0,3.0] = 1.0
        assert_eq!(result, Value::Float(1.0));

        // Test with mixed types
        let arr = vec![Value::Int(1), Value::Float(2.0), Value::Int(3)];
        let result = builtin_var(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Float(1.0));
    }

    #[test]
    fn test_builtin_var_array_small() {
        // Test with array of 1 element
        let arr = vec![Value::Int(5)];
        let result = builtin_var(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with empty array
        let arr = vec![];
        let result = builtin_var(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_var_array_non_numeric() {
        // Test with non-numeric values
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let result = builtin_var(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_var_series() {
        // Create a numeric series
        let series = Series::new("numbers", vec![1i64, 2, 3, 4, 5]);
        let result = builtin_var(&[Value::Series(series)]).unwrap();
        assert_eq!(result, Value::Float(2.5));
    }

    #[test]
    fn test_builtin_var_series_small() {
        // Create a series with 1 element
        let series = Series::new("numbers", vec![5i64]);
        let result = builtin_var(&[Value::Series(series)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_var_series_non_numeric() {
        // Create a non-numeric series
        let series = Series::new("strings", vec!["a", "b", "c"]);
        let result = builtin_var(&[Value::Series(series)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_var_dataframe() {
        // Create a DataFrame with numeric column
        let series1 = Series::new("numbers", vec![1i64, 2, 3, 4, 5]);
        let series2 = Series::new("strings", vec!["a", "b", "c", "d", "e"]);
        let df = DataFrame::new(vec![series1, series2]).unwrap();
        let result = builtin_var(&[Value::DataFrame(df)]).unwrap();

        if let Value::Object(obj) = result {
            assert_eq!(obj.get("numbers"), Some(&Value::Float(2.5)));
            assert_eq!(obj.get("strings"), Some(&Value::Null));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_builtin_var_invalid_args() {
        // No arguments
        let result = builtin_var(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Too many arguments
        let result = builtin_var(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_builtin_var_invalid_type() {
        let result = builtin_var(&[Value::String("test".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires array, DataFrame, or Series"));
    }

    #[test]
    fn test_var_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("var"));
    }
}
