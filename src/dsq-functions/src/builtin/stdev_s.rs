use dsq_shared::value::Value;
use dsq_shared::Result;
use polars::prelude::*;
use std::collections::HashMap;

pub fn builtin_stdev_s(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "stdev_s() expects 1 argument",
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
            Ok(Value::Float(variance.sqrt()))
        }
        Value::DataFrame(df) => {
            let mut stds = HashMap::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
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
                            stds.insert(col_name.to_string(), Value::Float(variance.sqrt()));
                        } else {
                            stds.insert(col_name.to_string(), Value::Null);
                        }
                    } else {
                        stds.insert(col_name.to_string(), Value::Null);
                    }
                }
            }
            Ok(Value::Object(stds))
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
                    Ok(Value::Float(variance.sqrt()))
                } else {
                    Ok(Value::Null)
                }
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "stdev_s() requires array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "stdev_s",
        func: builtin_stdev_s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_stdev_s_array() {
        // Test with array of integers
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        let result = builtin_stdev_s(&[Value::Array(arr)]).unwrap();
        // Sample standard deviation of [1,2,3,4,5] is sqrt(2.5) ≈ 1.5811388
        match result {
            Value::Float(val) => assert!((val - 1.5811388).abs() < 1e-6),
            _ => panic!("Expected Float"),
        }

        // Test with array of floats
        let arr = vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)];
        let result = builtin_stdev_s(&[Value::Array(arr)]).unwrap();
        // Sample std of [1.0,2.0,3.0] = 1.0
        assert_eq!(result, Value::Float(1.0));

        // Test with mixed types
        let arr = vec![Value::Int(1), Value::Float(2.0), Value::Int(3)];
        let result = builtin_stdev_s(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Float(1.0));
    }

    #[test]
    fn test_builtin_stdev_s_array_small() {
        // Test with array of 1 element
        let arr = vec![Value::Int(5)];
        let result = builtin_stdev_s(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with empty array
        let arr = vec![];
        let result = builtin_stdev_s(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_stdev_s_array_non_numeric() {
        // Test with non-numeric values
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let result = builtin_stdev_s(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_stdev_s_series() {
        // Create a numeric series
        let series = Series::new(
            <&str as Into<PlSmallStr>>::into("numbers"),
            vec![1i64, 2, 3, 4, 5],
        );
        let result = builtin_stdev_s(&[Value::Series(series)]).unwrap();
        match result {
            Value::Float(val) => assert!((val - 1.5811388).abs() < 1e-6),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_builtin_stdev_s_series_small() {
        // Create a series with 1 element
        let series = Series::new(<&str as Into<PlSmallStr>>::into("numbers"), vec![5i64]);
        let result = builtin_stdev_s(&[Value::Series(series)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_stdev_s_series_non_numeric() {
        // Create a non-numeric series
        let series = Series::new(
            <&str as Into<PlSmallStr>>::into("strings"),
            &["a", "b", "c"],
        );
        let result = builtin_stdev_s(&[Value::Series(series)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_stdev_s_dataframe() {
        // Create a DataFrame with numeric column
        let series1 = Series::new(
            <&str as Into<PlSmallStr>>::into("numbers"),
            vec![1i64, 2, 3, 4, 5],
        );
        let series2 = Series::new(
            <&str as Into<PlSmallStr>>::into("strings"),
            vec!["a", "b", "c", "d", "e"],
        );
        let df = DataFrame::new(vec![series1.into(), series2.into()]).unwrap();
        let result = builtin_stdev_s(&[Value::DataFrame(df)]).unwrap();

        if let Value::Object(obj) = result {
            if let Some(Value::Float(val)) = obj.get("numbers") {
                assert!((val - 1.5811388).abs() < 1e-6);
            } else {
                panic!("Expected Float for numbers column");
            }
            assert_eq!(obj.get("strings"), Some(&Value::Null));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_builtin_stdev_s_invalid_args() {
        // No arguments
        let result = builtin_stdev_s(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Too many arguments
        let result = builtin_stdev_s(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_builtin_stdev_s_invalid_type() {
        let result = builtin_stdev_s(&[Value::String("test".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires array, DataFrame, or Series"));
    }

    #[test]
    fn test_stdev_s_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("stdev_s"));
    }
}
