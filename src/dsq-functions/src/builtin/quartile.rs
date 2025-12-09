use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::collections::HashMap;

pub fn builtin_quartile(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "quartile() expects 1 or 2 arguments",
        ));
    }

    let quartile = if args.len() == 2 {
        match &args[1] {
            Value::Int(i) if *i >= 1 && *i <= 3 => *i as usize,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "quartile() second argument must be 1, 2, or 3",
                ));
            }
        }
    } else {
        2 // default to median
    };

    match &args[0] {
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
            let p = quartile as f64 / 4.0;
            let pos = (len - 1) as f64 * p;
            let idx = pos as usize;
            if idx + 1 < len {
                let frac = pos - idx as f64;
                let val = values[idx] + frac * (values[idx + 1] - values[idx]);
                Ok(Value::Float(val))
            } else {
                Ok(Value::Float(values[idx]))
            }
        }
        Value::DataFrame(df) => {
            let mut quartiles = HashMap::new();
            for col_name in df.get_column_names() {
                if let Ok(column) = df.column(col_name) {
                    let series = column.as_materialized_series();
                    if series.dtype().is_numeric() {
                        if let Ok(result) =
                            series.quantile_reduce(quartile as f64 / 4.0, QuantileMethod::Linear)
                        {
                            let av = result.as_any_value();
                            if let Ok(f) = av.try_extract::<f64>() {
                                quartiles.insert(col_name.to_string(), Value::Float(f));
                            }
                        }
                    }
                }
            }
            Ok(Value::Object(quartiles))
        }
        Value::Series(series) => {
            if series.dtype().is_numeric() {
                match series.quantile_reduce(quartile as f64 / 4.0, QuantileMethod::default()) {
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
            "quartile() requires array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "quartile",
        func: builtin_quartile,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_quartile_array() {
        // Test quartile with array - Q2 (median) default
        let result = builtin_quartile(&[Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ])])
        .unwrap();
        assert_eq!(result, Value::Float(3.0));

        // Test quartile with array - Q1
        let result = builtin_quartile(&[
            Value::Array(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(4),
                Value::Int(5),
            ]),
            Value::Int(1),
        ])
        .unwrap();
        assert_eq!(result, Value::Float(2.0));

        // Test quartile with array - Q3
        let result = builtin_quartile(&[
            Value::Array(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(4),
                Value::Int(5),
            ]),
            Value::Int(3),
        ])
        .unwrap();
        assert_eq!(result, Value::Float(4.0));

        // Test quartile with even length array - Q2 (interpolated median)
        let result = builtin_quartile(&[Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
        ])])
        .unwrap();
        assert_eq!(result, Value::Float(2.5));

        // Test quartile with floats
        let result = builtin_quartile(&[Value::Array(vec![
            Value::Float(1.5),
            Value::Float(2.5),
            Value::Float(3.5),
            Value::Float(4.5),
            Value::Float(5.5),
        ])])
        .unwrap();
        assert_eq!(result, Value::Float(3.5));
    }

    #[test]
    fn test_builtin_quartile_empty_array() {
        // Test quartile with empty array
        let result = builtin_quartile(&[Value::Array(vec![])]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_quartile_with_non_numeric() {
        // Test quartile with array containing non-numeric (filtered out)
        let result = builtin_quartile(&[Value::Array(vec![
            Value::Int(1),
            Value::String("not a number".to_string()),
            Value::Int(3),
        ])])
        .unwrap();
        assert_eq!(result, Value::Float(2.0));
    }

    #[test]
    fn test_builtin_quartile_invalid_args() {
        // Test invalid number of arguments
        let result = builtin_quartile(&[]);
        assert!(result.is_err());

        let result = builtin_quartile(&[Value::Int(1), Value::Int(2), Value::Int(3)]);
        assert!(result.is_err());

        // Test invalid quartile number
        let result = builtin_quartile(&[Value::Array(vec![Value::Int(1)]), Value::Int(0)]);
        assert!(result.is_err());

        let result = builtin_quartile(&[Value::Array(vec![Value::Int(1)]), Value::Int(4)]);
        assert!(result.is_err());

        let result = builtin_quartile(&[
            Value::Array(vec![Value::Int(1)]),
            Value::String("invalid".to_string()),
        ]);
        assert!(result.is_err());

        let result = builtin_quartile(&[Value::String("not array".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_quartile_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("quartile"));
    }
}
