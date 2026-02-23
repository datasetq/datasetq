use crate::inventory;
use crate::FunctionRegistration;
use dsq_shared::value::{is_truthy, Value};
use dsq_shared::Result;
use polars::prelude::*;
use std::collections::HashMap;

inventory::submit! {
    FunctionRegistration {
        name: "avg_if",
        func: builtin_avg_if,
    }
}

pub fn builtin_avg_if(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "avg_if() expects 2 arguments: values and mask",
        ));
    }

    let values = &args[0];
    let mask = &args[1];

    match (values, mask) {
        (Value::LazyFrame(lf), Value::Series(mask_series)) => {
            // Collect LazyFrame to DataFrame
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;

            // Recursively call with the collected DataFrame
            builtin_avg_if(&[Value::DataFrame(df), Value::Series(mask_series.clone())])
        }
        (Value::Array(arr), Value::Array(mask_arr)) => {
            if arr.len() != mask_arr.len() {
                return Err(dsq_shared::error::operation_error(
                    "avg_if() values and mask arrays must have same length",
                ));
            }
            let mut sum = 0.0;
            let mut count = 0;
            for (val, m) in arr.iter().zip(mask_arr.iter()) {
                if is_truthy(m) {
                    match val {
                        Value::Int(i) => {
                            sum += *i as f64;
                            count += 1;
                        }
                        Value::Float(f) => {
                            sum += *f;
                            count += 1;
                        }
                        _ => {}
                    }
                }
            }
            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Float(sum / count as f64))
            }
        }
        (Value::DataFrame(df), Value::Series(mask_series)) => {
            if df.height() != mask_series.len() {
                return Err(dsq_shared::error::operation_error(
                    "avg_if() DataFrame and mask series must have same length",
                ));
            }
            // For DataFrame, average all numeric columns where mask is true
            let mut result = HashMap::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() {
                        let mut sum = 0.0;
                        let mut count = 0;
                        for i in 0..series.len() {
                            if mask_series
                                .get(i)
                                .ok()
                                .map(|v| matches!(v, AnyValue::Boolean(true)))
                                .unwrap_or(false)
                            {
                                if let Ok(val) = series.get(i) {
                                    match val {
                                        AnyValue::Int8(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::Int16(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::Int32(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::Int64(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::UInt8(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::UInt16(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::UInt32(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::UInt64(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::Float32(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::Float64(n) => {
                                            sum += n;
                                            count += 1;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        if count > 0 {
                            result.insert(col_name.to_string(), Value::Float(sum / count as f64));
                        }
                    }
                }
            }
            Ok(Value::Object(result))
        }
        (Value::Series(series), Value::Series(mask_series)) => {
            if series.len() != mask_series.len() {
                return Err(dsq_shared::error::operation_error(
                    "avg_if() series and mask series must have same length",
                ));
            }
            if series.dtype().is_numeric() {
                let mut sum = 0.0;
                let mut count = 0;
                for i in 0..series.len() {
                    if mask_series
                        .get(i)
                        .ok()
                        .map(|v| matches!(v, AnyValue::Boolean(true)))
                        .unwrap_or(false)
                    {
                        if let Ok(val) = series.get(i) {
                            match val {
                                AnyValue::Int8(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::Int16(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::Int32(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::Int64(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::UInt8(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::UInt16(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::UInt32(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::UInt64(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::Float32(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::Float64(n) => {
                                    sum += n;
                                    count += 1;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                if count == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float(sum / count as f64))
                }
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "avg_if() requires (array, array) or (dataframe/lazyframe/series, series)",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_avg_if_array() {
        // Test with arrays
        let values = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
        ]);
        let mask = Value::Array(vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(false),
        ]);
        let result = builtin_avg_if(&[values, mask]).unwrap();
        assert_eq!(result, Value::Float(2.0)); // (1 + 3) / 2 = 2.0
    }

    #[test]
    fn test_avg_if_array_no_matches() {
        let values = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let mask = Value::Array(vec![Value::Bool(false), Value::Bool(false)]);
        let result = builtin_avg_if(&[values, mask]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_avg_if_array_mixed_types() {
        let values = Value::Array(vec![
            Value::Int(1),
            Value::Float(2.5),
            Value::String("ignore".to_string()),
        ]);
        let mask = Value::Array(vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
        ]);
        let result = builtin_avg_if(&[values, mask]).unwrap();
        assert_eq!(result, Value::Float(1.75)); // (1 + 2.5) / 2 = 1.75
    }

    #[test]
    fn test_avg_if_series() {
        let series = Series::new(PlSmallStr::from("test"), vec![1i64, 2, 3, 4]);
        let mask_series = Series::new(PlSmallStr::from("mask"), vec![true, false, true, false]);
        let result = builtin_avg_if(&[Value::Series(series), Value::Series(mask_series)]).unwrap();
        assert_eq!(result, Value::Float(2.0));
    }

    #[test]
    fn test_avg_if_series_no_matches() {
        let series = Series::new(PlSmallStr::from("test"), vec![1i64, 2]);
        let mask_series = Series::new(PlSmallStr::from("mask"), vec![false, false]);
        let result = builtin_avg_if(&[Value::Series(series), Value::Series(mask_series)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_avg_if_dataframe() {
        let df = DataFrame::new(vec![
            Series::new(PlSmallStr::from("a"), vec![1i64, 2, 3]).into(),
            Series::new(PlSmallStr::from("b"), vec![4i64, 5, 6]).into(),
        ])
        .unwrap();
        let mask_series = Series::new(PlSmallStr::from("mask"), vec![true, false, true]);
        let result = builtin_avg_if(&[Value::DataFrame(df), Value::Series(mask_series)]).unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.get("a"), Some(&Value::Float(2.0))); // (1 + 3) / 2
            assert_eq!(obj.get("b"), Some(&Value::Float(5.0))); // (4 + 6) / 2
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_avg_if_invalid_args() {
        // Wrong number of args
        let result = builtin_avg_if(&[Value::Int(1)]);
        assert!(result.is_err());

        // Mismatched lengths
        let values = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let mask = Value::Array(vec![Value::Bool(true)]);
        let result = builtin_avg_if(&[values, mask]);
        assert!(result.is_err());

        // Invalid types
        let result = builtin_avg_if(&[Value::String("test".to_string()), Value::Int(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_avg_if_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("avg_if"));
    }
}
