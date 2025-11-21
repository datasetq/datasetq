use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "add",
        func: builtin_add,
    }
}

pub fn builtin_add(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "add() expects at least 1 argument",
        ));
    }

    // If single argument, handle as before
    if args.len() == 1 {
        return builtin_add_single(args);
    }

    // Multiple arguments: sum them all
    builtin_add_multiple(args)
}

fn builtin_add_single(args: &[Value]) -> Result<Value> {
    match &args[0] {
        Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(Value::Null);
            }

            // Try numeric addition first
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
                                // In a real implementation, you might want to handle this differently
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
            }
            // Try string concatenation
            else if arr.iter().all(|v| matches!(v, Value::String(_))) {
                let mut result = String::new();
                for val in arr {
                    if let Value::String(s) = val {
                        result.push_str(s);
                    }
                }
                Ok(Value::String(result))
            } else {
                Err(dsq_shared::error::operation_error(
                    "add() requires homogeneous array",
                ))
            }
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                let sum_result = series.sum::<f64>();
                match sum_result {
                    Some(sum) => {
                        if sum.fract() == 0.0 && sum <= i64::MAX as f64 && sum >= i64::MIN as f64 {
                            Ok(Value::Int(sum as i64))
                        } else {
                            Ok(Value::Float(sum))
                        }
                    }
                    None => Ok(Value::Null),
                }
            } else {
                Err(dsq_shared::error::operation_error(
                    "add() requires numeric series",
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
            "add() requires an array, Series, or DataFrame",
        )),
    }
}

fn builtin_add_multiple(args: &[Value]) -> Result<Value> {
    // Sum all arguments
    let mut sum_bigint = num_bigint::BigInt::from(0);
    let mut sum_float = 0.0;
    let mut has_float = false;
    let mut has_bigint = false;
    let mut string_parts = Vec::new();
    let mut all_strings = true;
    let mut all_numeric = true;

    for arg in args {
        match arg {
            Value::Int(i) => {
                all_strings = false;
                if has_float {
                    sum_float += *i as f64;
                } else if has_bigint {
                    sum_bigint += num_bigint::BigInt::from(*i);
                } else {
                    sum_bigint += num_bigint::BigInt::from(*i);
                    has_bigint = true;
                }
            }
            Value::Float(f) => {
                all_strings = false;
                has_float = true;
                sum_float += *f;
                if has_bigint {
                    sum_float += sum_bigint.to_string().parse::<f64>().unwrap_or(0.0);
                    has_bigint = false;
                }
            }
            Value::BigInt(bi) => {
                all_strings = false;
                has_bigint = true;
                if has_float {
                    sum_float += bi.to_string().parse::<f64>().unwrap_or(0.0);
                } else {
                    sum_bigint += bi;
                }
            }
            Value::String(s) => {
                all_numeric = false;
                string_parts.push(s.clone());
            }
            Value::Array(_arr) => {
                // For arrays, we could flatten and sum, but for simplicity, treat as error
                return Err(dsq_shared::error::operation_error(
                    "add() with multiple arguments does not support arrays",
                ));
            }
            Value::DataFrame(_) | Value::LazyFrame(_) | Value::Series(_) | Value::Object(_) => {
                return Err(dsq_shared::error::operation_error(
                    "add() with multiple arguments only supports numeric and string values",
                ));
            }
            Value::Bool(_b) => {
                return Err(dsq_shared::error::operation_error(
                    "add() with multiple arguments does not support boolean values",
                ));
            }
            Value::Null => {
                // Null adds nothing
            }
        }
    }

    if all_strings && !string_parts.is_empty() {
        let result = string_parts.join("");
        Ok(Value::String(result))
    } else if all_numeric {
        if has_float {
            if sum_float.fract() == 0.0
                && sum_float <= i64::MAX as f64
                && sum_float >= i64::MIN as f64
            {
                Ok(Value::Int(sum_float as i64))
            } else {
                Ok(Value::Float(sum_float))
            }
        } else if has_bigint {
            Ok(Value::BigInt(sum_bigint))
        } else {
            // All were Int, sum_bigint contains the sum
            if sum_bigint >= num_bigint::BigInt::from(i64::MIN)
                && sum_bigint <= num_bigint::BigInt::from(i64::MAX)
            {
                Ok(Value::Int(sum_bigint.to_string().parse().unwrap()))
            } else {
                Ok(Value::BigInt(sum_bigint))
            }
        }
    } else {
        Err(dsq_shared::error::operation_error(
            "add() with multiple arguments requires all arguments to be numeric or all strings",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;
    use std::collections::HashMap;

    fn create_test_dataframe() -> DataFrame {
        let names = Series::new("name", &["Alice", "Bob", "Charlie"]);
        let ages = Series::new("age", &[25, 30, 35]);
        let scores = Series::new("score", &[85.5, 92.0, 78.3]);
        DataFrame::new(vec![names, ages, scores]).unwrap()
    }

    #[test]
    fn test_builtin_add_with_dataframe() {
        let mut df = DataFrame::new(vec![Series::new("value", &[10.0, 20.0, 30.0])]).unwrap();
        let df_value = Value::DataFrame(df);
        let result = builtin_add(&[df_value]).unwrap();
        assert_eq!(result, Value::Float(60.0));
    }

    #[test]
    fn test_builtin_add_multiple_integers() {
        let result = builtin_add(&[Value::Int(1), Value::Int(2), Value::Int(3)]).unwrap();
        assert_eq!(result, Value::Int(6));
    }

    #[test]
    fn test_builtin_add_multiple_floats() {
        let result =
            builtin_add(&[Value::Float(1.5), Value::Float(2.5), Value::Float(3.0)]).unwrap();
        assert_eq!(result, Value::Float(7.0));
    }

    #[test]
    fn test_builtin_add_mixed_numeric() {
        let result = builtin_add(&[Value::Int(1), Value::Float(2.5), Value::Int(3)]).unwrap();
        assert_eq!(result, Value::Float(6.5));
    }

    #[test]
    fn test_builtin_add_strings() {
        let result = builtin_add(&[
            Value::String("hello".to_string()),
            Value::String(" ".to_string()),
            Value::String("world".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    #[test]
    fn test_builtin_add_bigints() {
        use num_bigint::BigInt;
        let big1 = Value::BigInt(BigInt::from(1000000000000i64));
        let big2 = Value::BigInt(BigInt::from(2000000000000i64));
        let result = builtin_add(&[big1, big2]).unwrap();
        assert_eq!(result, Value::BigInt(BigInt::from(3000000000000i64)));
    }

    #[test]
    fn test_builtin_add_bigint_and_int() {
        use num_bigint::BigInt;
        let big = Value::BigInt(BigInt::from(1000000000000i64));
        let int = Value::Int(1);
        let result = builtin_add(&[big, int]).unwrap();
        assert_eq!(result, Value::BigInt(BigInt::from(1000000000001i64)));
    }

    #[test]
    fn test_builtin_add_with_null() {
        let result = builtin_add(&[Value::Int(1), Value::Null, Value::Int(2)]).unwrap();
        assert_eq!(result, Value::Int(3)); // Null should be ignored
    }

    #[test]
    fn test_builtin_add_empty_args() {
        let result = builtin_add(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("add() expects at least 1 argument"));
    }

    #[test]
    fn test_builtin_add_mixed_types_error() {
        let result = builtin_add(&[Value::Int(1), Value::String("test".to_string())]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "add() with multiple arguments requires all arguments to be numeric or all strings"
        ));
    }

    #[test]
    fn test_builtin_add_array_with_multiple_args_error() {
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let result = builtin_add(&[arr, Value::Int(3)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("add() with multiple arguments does not support arrays"));
    }

    #[test]
    fn test_builtin_add_with_series() {
        use polars::prelude::*;
        let series = Series::new("test", &[1, 2, 3, 4, 5]);
        let series_value = Value::Series(series);
        let result = builtin_add(&[series_value]).unwrap();
        assert_eq!(result, Value::Int(15));
    }

    #[test]
    fn test_add_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("add"));
    }
}
