use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "sum",
        func: builtin_sum,
    }
}

pub fn builtin_sum(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "sum() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(Value::Null);
            }

            // Try numeric addition
            if arr
                .iter()
                .all(|v| matches!(v, Value::Int(_) | Value::Float(_) | Value::BigInt(_)))
            {
                // Check if any value is BigInt
                if arr.iter().any(|v| matches!(v, Value::BigInt(_))) {
                    // Use BigInt arithmetic
                    let mut sum = num_bigint::BigInt::from(0);
                    for val in arr {
                        match val {
                            Value::Int(i) => sum += num_bigint::BigInt::from(*i),
                            Value::BigInt(bi) => sum += bi,
                            Value::Float(f) => {
                                // For simplicity, convert float to BigInt by truncating
                                sum += num_bigint::BigInt::from(*f as i64);
                            }
                            _ => unreachable!(),
                        }
                    }
                    Ok(Value::BigInt(sum))
                } else {
                    // Check if all are Int
                    if arr.iter().all(|v| matches!(v, Value::Int(_))) {
                        // Use i128 arithmetic for Int only to handle overflow
                        let mut sum: i128 = 0;
                        for val in arr {
                            if let Value::Int(i) = val {
                                sum = sum.saturating_add(*i as i128);
                            }
                        }
                        if sum >= i64::MIN as i128 && sum <= i64::MAX as i128 {
                            Ok(Value::Int(sum as i64))
                        } else {
                            Ok(Value::BigInt(num_bigint::BigInt::from(sum)))
                        }
                    } else {
                        // Use f64 arithmetic for Float mixed
                        let mut sum = 0.0;
                        for val in arr {
                            match val {
                                Value::Int(i) => sum += *i as f64,
                                Value::Float(f) => sum += f,
                                _ => unreachable!(),
                            }
                        }

                        if sum.fract() == 0.0 && sum <= i64::MAX as f64 && sum >= i64::MIN as f64 {
                            Ok(Value::Int(sum as i64))
                        } else {
                            Ok(Value::Float(sum))
                        }
                    }
                }
            } else {
                Err(dsq_shared::error::operation_error(
                    "sum() requires homogeneous numeric array",
                ))
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                match series.sum::<f64>() {
                    Ok(sum) => {
                        if sum.fract() == 0.0 && sum <= i64::MAX as f64 && sum >= i64::MIN as f64 {
                            Ok(Value::Int(sum as i64))
                        } else {
                            Ok(Value::Float(sum))
                        }
                    }
                    Err(e) => Err(dsq_shared::error::operation_error(format!(
                        "sum() failed to sum series: {}",
                        e
                    ))),
                }
            } else {
                Err(dsq_shared::error::operation_error(
                    "sum() requires numeric series",
                ))
            }
        }
        Value::DataFrame(df) => {
            // Sum all numeric values in the DataFrame
            let mut sum = 0.0;
            let mut has_values = false;
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() {
                        for i in 0..series.len() {
                            if let Ok(val) = series.get(i) {
                                has_values = true;
                                match val {
                                    AnyValue::Int8(n) => sum += n as f64,
                                    AnyValue::Int16(n) => sum += n as f64,
                                    AnyValue::Int32(n) => sum += n as f64,
                                    AnyValue::Int64(n) => sum += n as f64,
                                    AnyValue::UInt8(n) => sum += n as f64,
                                    AnyValue::UInt16(n) => sum += n as f64,
                                    AnyValue::UInt32(n) => sum += n as f64,
                                    AnyValue::UInt64(n) => sum += n as f64,
                                    AnyValue::Float32(n) => sum += n as f64,
                                    AnyValue::Float64(n) => sum += n,
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            if has_values {
                if sum.fract() == 0.0 && sum <= i64::MAX as f64 && sum >= i64::MIN as f64 {
                    Ok(Value::Int(sum as i64))
                } else {
                    Ok(Value::Float(sum))
                }
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "sum() requires an array, Series, or DataFrame",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_sum_array_integers() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_sum(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Int(6));
    }

    #[test]
    fn test_sum_array_floats() {
        let arr = vec![Value::Float(1.1), Value::Float(2.2), Value::Float(3.3)];
        let result = builtin_sum(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Float(6.6));
    }

    #[test]
    fn test_sum_array_mixed() {
        let arr = vec![Value::Int(1), Value::Float(2.5), Value::Int(3)];
        let result = builtin_sum(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Float(6.5));
    }

    #[test]
    fn test_sum_array_empty() {
        let arr = vec![];
        let result = builtin_sum(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_sum_array_non_numeric() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let result = builtin_sum(&[Value::Array(arr)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_sum_series_numeric() {
        let series = Series::new("col".into(), vec![1i64, 2, 3]);
        let result = builtin_sum(&[Value::Series(series)]).unwrap();
        assert_eq!(result, Value::Int(6));
    }

    #[test]
    fn test_sum_series_float() {
        let series = Series::new("col".into(), vec![1.1f64, 2.2, 3.3]);
        let result = builtin_sum(&[Value::Series(series)]).unwrap();
        assert_eq!(result, Value::Float(6.6));
    }

    #[test]
    fn test_sum_series_non_numeric() {
        let series = Series::new("col".into(), vec!["a", "b", "c"]);
        let result = builtin_sum(&[Value::Series(series)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_sum_dataframe() {
        let series1 = Series::new("col1".into(), vec![1i64, 2, 3]);
        let series2 = Series::new("col2".into(), vec![4i64, 5, 6]);
        let df = DataFrame::new(vec![series1, series2]).unwrap();
        let result = builtin_sum(&[Value::DataFrame(df)]).unwrap();
        assert_eq!(result, Value::Int(21)); // 1+2+3+4+5+6=21
    }

    #[test]
    fn test_sum_dataframe_no_numeric() {
        let series = Series::new("col".into(), vec!["a", "b", "c"]);
        let df = DataFrame::new(vec![series]).unwrap();
        let result = builtin_sum(&[Value::DataFrame(df)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_sum_wrong_args() {
        let result = builtin_sum(&[]);
        assert!(result.is_err());

        let result = builtin_sum(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_sum_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("sum"));
    }
}
