use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::collections::HashMap;

inventory::submit! {
    crate::FunctionRegistration {
        name: "min_by",
        func: builtin_min_by,
    }
}

fn compare_values_for_sorting(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Bool(a_val), Value::Bool(b_val)) => a_val.cmp(b_val),
        (Value::Int(a_val), Value::Int(b_val)) => a_val.cmp(b_val),
        (Value::Float(a_val), Value::Float(b_val)) => a_val
            .partial_cmp(b_val)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(a_val), Value::String(b_val)) => a_val.cmp(b_val),
        (Value::Int(a_val), Value::Float(b_val)) => (*a_val as f64)
            .partial_cmp(b_val)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(a_val), Value::Int(b_val)) => a_val
            .partial_cmp(&(*b_val as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        _ => std::cmp::Ordering::Equal,
    }
}

pub fn builtin_min_by(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "min_by() expects 2 arguments",
        ));
    }

    match (&args[0], &args[1]) {
        (Value::LazyFrame(lf), Value::String(column)) => {
            // Collect LazyFrame to DataFrame
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;

            // Recursively call with the collected DataFrame
            builtin_min_by(&[Value::DataFrame(df), Value::String(column.clone())])
        }
        (Value::LazyFrame(lf), Value::Array(key_arr)) => {
            // Collect LazyFrame to DataFrame
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;

            // Recursively call with the collected DataFrame
            builtin_min_by(&[Value::DataFrame(df), Value::Array(key_arr.clone())])
        }
        (Value::Array(arr), Value::Array(key_arr)) if arr.len() == key_arr.len() => {
            if arr.is_empty() {
                return Ok(Value::Null);
            }
            let mut min_idx = 0;
            let mut min_key = &key_arr[0];
            for (i, key) in key_arr.iter().enumerate().skip(1) {
                if compare_values_for_sorting(key, min_key) == std::cmp::Ordering::Less {
                    min_idx = i;
                    min_key = key;
                }
            }
            Ok(arr[min_idx].clone())
        }
        (Value::DataFrame(df), Value::String(column)) => {
            // Find row with min value in column
            if let Ok(series) = df.column(column) {
                if series.dtype().is_numeric() {
                    let mut min_idx = 0;
                    let mut min_val = f64::INFINITY;
                    for i in 0..series.len() {
                        if let Ok(val) = series.get(i) {
                            match val {
                                AnyValue::Int64(v) => {
                                    let vf = v as f64;
                                    if vf < min_val {
                                        min_val = vf;
                                        min_idx = i;
                                    }
                                }
                                AnyValue::Float64(v) => {
                                    if v < min_val {
                                        min_val = v;
                                        min_idx = i;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    // Return the row as object
                    let mut row_obj = HashMap::new();
                    for col_name in df.get_column_names() {
                        if let Ok(s) = df.column(col_name) {
                            if let Ok(val) = s.get(min_idx) {
                                let value = value_from_any_value(val).unwrap_or(Value::Null);
                                row_obj.insert(col_name.to_string(), value);
                            }
                        }
                    }
                    Ok(Value::Object(row_obj))
                } else {
                    Ok(Value::Null)
                }
            } else {
                Err(dsq_shared::error::operation_error(format!(
                    "Column '{}' not found",
                    column
                )))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "min_by() requires (array, array), (dataframe, string), (lazyframe, string), or (lazyframe, array)",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_min_by_array() {
        // Test with arrays: min_by([1,2,3], [3,1,2]) should return 2 (index 1 has min key 1)
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let keys = vec![Value::Int(3), Value::Int(1), Value::Int(2)];
        let result = builtin_min_by(&[Value::Array(arr), Value::Array(keys)]).unwrap();
        assert_eq!(result, Value::Int(2));
    }

    #[test]
    fn test_builtin_min_by_dataframe_string() {
        // This would require creating a DataFrame, but for simplicity, test the error case
        let df = Value::DataFrame(DataFrame::new(vec![]).unwrap()); // Empty DF
        let result = builtin_min_by(&[df, Value::String("nonexistent".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_builtin_min_by_invalid_args() {
        let result = builtin_min_by(&[Value::Int(1)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_min_by_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("min_by"));
    }
}
