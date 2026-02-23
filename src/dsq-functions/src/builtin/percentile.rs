use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::collections::HashMap;

pub fn builtin_percentile(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "percentile() expects 2 arguments",
        ));
    }

    let percentile = match &args[1] {
        Value::Int(i) if *i >= 0 && *i <= 100 => *i as f64 / 100.0,
        Value::Float(f) if *f >= 0.0 && *f <= 1.0 => *f,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "percentile() second argument must be 0-100 or 0.0-1.0",
            ));
        }
    };

    match &args[0] {
        Value::LazyFrame(lf) => {
            // Collect LazyFrame to DataFrame
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;

            // Recursively call with the collected DataFrame
            builtin_percentile(&[Value::DataFrame(df), args[1].clone()])
        }
        Value::Array(arr) => {
            let mut values: Vec<f64> = arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(Value::Null);
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let len = values.len();
            let idx = ((len - 1) as f64 * percentile) as usize;
            Ok(Value::Float(values[idx]))
        }
        Value::DataFrame(df) => {
            let mut percentiles = HashMap::new();
            for col_name in df.get_column_names() {
                if let Ok(column) = df.column(col_name) {
                    let series = column.as_materialized_series();
                    if series.dtype().is_numeric() {
                        if let Ok(result) =
                            series.quantile_reduce(percentile, QuantileMethod::default())
                        {
                            let av = result.as_any_value();
                            if let Ok(f) = av.try_extract::<f64>() {
                                percentiles.insert(col_name.to_string(), Value::Float(f));
                            }
                        }
                    }
                }
            }
            Ok(Value::Object(percentiles))
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                match series.quantile_reduce(percentile, QuantileMethod::default()) {
                    Ok(result) => {
                        let av = result.as_any_value();
                        match av.try_extract::<f64>() {
                            Ok(f) => Ok(Value::Float(f)),
                            Err(_) => Ok(Value::Null),
                        }
                    }
                    Err(_) => Ok(Value::Null),
                }
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "percentile() requires array, DataFrame, LazyFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "percentile",
        func: builtin_percentile,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_percentile_array() {
        // Test 50th percentile (median) of [1, 2, 3, 4, 5] should be 3
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        let result = builtin_percentile(&[Value::Array(arr), Value::Int(50)]).unwrap();
        assert_eq!(result, Value::Float(3.0));

        // Test 0th percentile should be 1
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        let result = builtin_percentile(&[Value::Array(arr), Value::Int(0)]).unwrap();
        assert_eq!(result, Value::Float(1.0));

        // Test 100th percentile should be 5
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        let result = builtin_percentile(&[Value::Array(arr), Value::Int(100)]).unwrap();
        assert_eq!(result, Value::Float(5.0));
    }

    #[test]
    fn test_builtin_percentile_float_percentile() {
        // Test with float percentile (0.5 = 50th percentile)
        let arr = vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ];
        let result = builtin_percentile(&[Value::Array(arr), Value::Float(0.5)]).unwrap();
        assert_eq!(result, Value::Float(3.0));
    }

    #[test]
    fn test_builtin_percentile_empty_array() {
        let arr = vec![];
        let result = builtin_percentile(&[Value::Array(arr), Value::Int(50)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_percentile_invalid_args() {
        // Wrong number of arguments
        let result = builtin_percentile(&[Value::Array(vec![Value::Int(1)])]);
        assert!(result.is_err());

        // Invalid percentile value
        let result = builtin_percentile(&[Value::Array(vec![Value::Int(1)]), Value::Int(150)]);
        assert!(result.is_err());

        // Invalid input type
        let result = builtin_percentile(&[Value::String("not array".to_string()), Value::Int(50)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_percentile_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("percentile"));
    }
}
