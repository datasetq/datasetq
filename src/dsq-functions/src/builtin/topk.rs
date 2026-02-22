use dsq_shared::{
    value::{value_from_any_value, Value},
    Result,
};
use inventory;
use polars::prelude::*;
use serde_json;
use std::collections::HashMap;

use crate::FunctionRegistration;

pub fn builtin_topk(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(dsq_shared::error::operation_error(
            "topk() expects 2 or 3 arguments (column, k, descending)",
        ));
    }

    let k = match &args[1] {
        Value::Int(i) => {
            if *i < 0 {
                return Err(dsq_shared::error::operation_error(
                    "topk() k must be a positive integer",
                ));
            }
            *i as usize
        }
        Value::Float(f) => {
            if *f < 0.0 {
                return Err(dsq_shared::error::operation_error(
                    "topk() k must be a positive integer",
                ));
            }
            *f as usize
        }
        _ => {
            return Err(dsq_shared::error::operation_error(
                "topk() k must be a number",
            ))
        }
    };

    let descending = if args.len() == 3 {
        match &args[2] {
            Value::Bool(b) => *b,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "topk() descending must be a boolean",
                ))
            }
        }
    } else {
        true // default: descending (top k largest)
    };

    match &args[0] {
        Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(Value::Array(vec![]));
            }

            // Count frequency of each value
            let mut frequency_map: HashMap<String, (usize, Value)> = HashMap::new();
            for value in arr {
                let key = serde_json::to_string(value).unwrap_or_default();
                frequency_map
                    .entry(key)
                    .and_modify(|(count, _)| *count += 1)
                    .or_insert((1, value.clone()));
            }

            // Sort by frequency
            let mut freq_vec: Vec<(usize, Value)> = frequency_map.into_values().collect();
            freq_vec.sort_by(|a, b| {
                let cmp = a.0.cmp(&b.0);
                if descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            });

            let result: Vec<Value> = freq_vec.iter().take(k).map(|(_, v)| v.clone()).collect();

            Ok(Value::Array(result))
        }
        Value::DataFrame(df) => {
            if df.height() == 0 {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

            let col_names = df.get_column_names();
            if col_names.is_empty() {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

            let column = df.column(col_names[0]).map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get first column: {}", e))
            })?;
            let series = column.as_materialized_series();

            // Use Polars value_counts for frequency analysis
            let counts_df = series
                .value_counts(true, false, "count".into(), false)
                .map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to count values: {}", e))
                })?;

            // Sort by count
            let sorted = counts_df
                .sort(
                    ["count"],
                    SortMultipleOptions::default().with_order_descending(descending),
                )
                .map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to sort: {}", e))
                })?;

            // Take top k
            let top_k_df = sorted.head(Some(k));

            // Extract just the value column (remove count column)
            let value_series = top_k_df.column(series.name()).map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get values column: {}", e))
            })?;

            let result_df = DataFrame::new(vec![value_series.clone()]).map_err(|e| {
                dsq_shared::error::operation_error(format!(
                    "Failed to create result DataFrame: {}",
                    e
                ))
            })?;

            Ok(Value::DataFrame(result_df))
        }
        Value::Series(series) => {
            if series.is_empty() {
                return Ok(Value::Array(vec![]));
            }

            // Use Polars value_counts for frequency analysis
            let counts_df = series
                .value_counts(true, false, "count".into(), false)
                .map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to count values: {}", e))
                })?;

            // Sort by count
            let sorted = counts_df
                .sort(
                    ["count"],
                    SortMultipleOptions::default().with_order_descending(descending),
                )
                .map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to sort: {}", e))
                })?;

            // Take top k
            let top_k_df = sorted.head(Some(k));

            // Extract the values column
            let values_column = top_k_df.column(series.name()).map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get values column: {}", e))
            })?;
            let values_series = values_column.as_materialized_series();

            let result: Vec<Value> = (0..values_series.len())
                .filter_map(|i| {
                    values_series
                        .get(i)
                        .ok()
                        .and_then(|v| value_from_any_value(v))
                })
                .collect();

            Ok(Value::Array(result))
        }
        Value::LazyFrame(lf) => {
            // Select only the first column before collecting to avoid materializing entire LazyFrame
            let schema = (**lf).clone().collect_schema().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get LazyFrame schema: {}", e))
            })?;

            let col_names: Vec<_> = schema.iter_names().map(|s| s.as_str()).collect();
            if col_names.is_empty() {
                return Ok(Value::Array(vec![]));
            }

            let first_col = col_names[0];
            let df = lf.clone().select([col(first_col)]).collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;

            // Recursively call with the collected DataFrame
            builtin_topk(&[
                Value::DataFrame(df),
                args[1].clone(),
                if args.len() == 3 {
                    args[2].clone()
                } else {
                    Value::Bool(true)
                },
            ])
        }
        _ => Err(dsq_shared::error::operation_error(
            "topk() requires array, DataFrame, LazyFrame, or Series",
        )),
    }
}

pub fn builtin_topk_with_counts(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(dsq_shared::error::operation_error(
            "topk_with_counts() expects 2 or 3 arguments (column, k, descending)",
        ));
    }

    let k = match &args[1] {
        Value::Int(i) => {
            if *i < 0 {
                return Err(dsq_shared::error::operation_error(
                    "topk_with_counts() k must be a positive integer",
                ));
            }
            *i as usize
        }
        Value::Float(f) => {
            if *f < 0.0 {
                return Err(dsq_shared::error::operation_error(
                    "topk_with_counts() k must be a positive integer",
                ));
            }
            *f as usize
        }
        _ => {
            return Err(dsq_shared::error::operation_error(
                "topk_with_counts() k must be a number",
            ))
        }
    };

    let descending = if args.len() == 3 {
        match &args[2] {
            Value::Bool(b) => *b,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "topk_with_counts() descending must be a boolean",
                ))
            }
        }
    } else {
        true // default: descending (top k largest)
    };

    match &args[0] {
        Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

            // Count frequency of each value
            let mut frequency_map: HashMap<String, (usize, Value)> = HashMap::new();
            for value in arr {
                let key = serde_json::to_string(value).unwrap_or_default();
                frequency_map
                    .entry(key)
                    .and_modify(|(count, _)| *count += 1)
                    .or_insert((1, value.clone()));
            }

            // Sort by frequency
            let mut freq_vec: Vec<(usize, Value)> = frequency_map.into_values().collect();
            freq_vec.sort_by(|a, b| {
                let cmp = a.0.cmp(&b.0);
                if descending {
                    cmp.reverse()
                } else {
                    cmp
                }
            });

            // Build result DataFrame
            let values: Vec<Value> = freq_vec.iter().take(k).map(|(_, v)| v.clone()).collect();
            let counts: Vec<u64> = freq_vec.iter().take(k).map(|(c, _)| *c as u64).collect();

            // Create Series from values and counts
            let value_series = Series::from_any_values(
                "value".into(),
                &values
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => AnyValue::String(s),
                        Value::Int(i) => AnyValue::Int64(*i),
                        Value::Float(f) => AnyValue::Float64(*f),
                        Value::Bool(b) => AnyValue::Boolean(*b),
                        Value::Null => AnyValue::Null,
                        _ => AnyValue::String(""),
                    })
                    .collect::<Vec<_>>(),
                false,
            )
            .map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to create value series: {}", e))
            })?;

            let count_series = Series::new("count".into(), counts);

            let df =
                DataFrame::new(vec![value_series.into(), count_series.into()]).map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to create DataFrame: {}", e))
                })?;

            Ok(Value::DataFrame(df))
        }
        Value::DataFrame(df) => {
            if df.height() == 0 {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

            let col_names = df.get_column_names();
            if col_names.is_empty() {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

            let column = df.column(col_names[0]).map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get first column: {}", e))
            })?;
            let series = column.as_materialized_series();

            // Use Polars value_counts for frequency analysis
            let counts_df = series
                .value_counts(true, false, "count".into(), false)
                .map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to count values: {}", e))
                })?;

            // Sort by count
            let sorted = counts_df
                .sort(
                    ["count"],
                    SortMultipleOptions::default().with_order_descending(descending),
                )
                .map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to sort: {}", e))
                })?;

            // Take top k
            let top_k_df = sorted.head(Some(k));

            // Cast count column to UInt64 for consistency
            let count_col = top_k_df.column("count").map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get count column: {}", e))
            })?;
            let count_u64 = count_col.cast(&DataType::UInt64).map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to cast count to UInt64: {}", e))
            })?;

            let value_col = top_k_df.column(series.name()).map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get value column: {}", e))
            })?;

            let result_df = DataFrame::new(vec![value_col.clone(), count_u64]).map_err(|e| {
                dsq_shared::error::operation_error(format!(
                    "Failed to create result DataFrame: {}",
                    e
                ))
            })?;

            Ok(Value::DataFrame(result_df))
        }
        Value::Series(series) => {
            if series.is_empty() {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

            // Use Polars value_counts for frequency analysis
            let counts_df = series
                .value_counts(true, false, "count".into(), false)
                .map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to count values: {}", e))
                })?;

            // Sort by count
            let sorted = counts_df
                .sort(
                    ["count"],
                    SortMultipleOptions::default().with_order_descending(descending),
                )
                .map_err(|e| {
                    dsq_shared::error::operation_error(format!("Failed to sort: {}", e))
                })?;

            // Take top k
            let top_k_df = sorted.head(Some(k));

            // Cast count column to UInt64 for consistency
            let count_col = top_k_df.column("count").map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get count column: {}", e))
            })?;
            let count_u64 = count_col.cast(&DataType::UInt64).map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to cast count to UInt64: {}", e))
            })?;

            let value_col = top_k_df.column(series.name()).map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get value column: {}", e))
            })?;

            let result_df = DataFrame::new(vec![value_col.clone(), count_u64]).map_err(|e| {
                dsq_shared::error::operation_error(format!(
                    "Failed to create result DataFrame: {}",
                    e
                ))
            })?;

            Ok(Value::DataFrame(result_df))
        }
        Value::LazyFrame(lf) => {
            // Select only the first column before collecting to avoid materializing entire LazyFrame
            let schema = (**lf).clone().collect_schema().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to get LazyFrame schema: {}", e))
            })?;

            let col_names: Vec<_> = schema.iter_names().map(|s| s.as_str()).collect();
            if col_names.is_empty() {
                return Ok(Value::DataFrame(DataFrame::empty()));
            }

            let first_col = col_names[0];
            let df = lf.clone().select([col(first_col)]).collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;

            // Recursively call with the collected DataFrame
            builtin_topk_with_counts(&[
                Value::DataFrame(df),
                args[1].clone(),
                if args.len() == 3 {
                    args[2].clone()
                } else {
                    Value::Bool(true)
                },
            ])
        }
        _ => Err(dsq_shared::error::operation_error(
            "topk_with_counts() requires array, DataFrame, LazyFrame, or Series",
        )),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "topk",
        func: builtin_topk,
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "topk_with_counts",
        func: builtin_topk_with_counts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topk_array_basic() {
        let arr = vec![
            Value::String("apple".to_string()),
            Value::String("banana".to_string()),
            Value::String("apple".to_string()),
            Value::String("cherry".to_string()),
            Value::String("apple".to_string()),
            Value::String("banana".to_string()),
        ];
        let result = builtin_topk(&[Value::Array(arr), Value::Int(2)]).unwrap();
        if let Value::Array(vals) = result {
            assert_eq!(vals.len(), 2);
            assert_eq!(vals[0], Value::String("apple".to_string()));
            assert_eq!(vals[1], Value::String("banana".to_string()));
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_topk_array_single_value() {
        let arr = vec![
            Value::String("same".to_string()),
            Value::String("same".to_string()),
            Value::String("same".to_string()),
        ];
        let result = builtin_topk(&[Value::Array(arr), Value::Int(1)]).unwrap();
        if let Value::Array(vals) = result {
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0], Value::String("same".to_string()));
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_topk_array_ascending() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("a".to_string()),
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ];
        let result = builtin_topk(&[Value::Array(arr), Value::Int(2), Value::Bool(false)]).unwrap();
        if let Value::Array(vals) = result {
            assert_eq!(vals.len(), 2);
            // With ascending=true (false for descending), should get least frequent
            // Both "b" and "c" have count 1, so either could be first
            let val_strings: Vec<String> = vals
                .iter()
                .map(|v| {
                    if let Value::String(s) = v {
                        s.clone()
                    } else {
                        String::new()
                    }
                })
                .collect();
            assert!(
                val_strings.contains(&"b".to_string()) || val_strings.contains(&"c".to_string())
            );
            assert_eq!(val_strings.len(), 2);
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_topk_array_empty() {
        let arr: Vec<Value> = vec![];
        let result = builtin_topk(&[Value::Array(arr), Value::Int(5)]).unwrap();
        assert_eq!(result, Value::Array(vec![]));
    }

    #[test]
    fn test_topk_array_k_larger_than_unique() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let result = builtin_topk(&[Value::Array(arr), Value::Int(10)]).unwrap();
        if let Value::Array(vals) = result {
            assert_eq!(vals.len(), 2); // Only 2 unique values
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_topk_series() {
        let series = Series::new("test".into(), vec!["a", "b", "a", "c", "a", "b", "d"]);
        let result = builtin_topk(&[Value::Series(series), Value::Int(2)]).unwrap();
        if let Value::Array(vals) = result {
            assert_eq!(vals.len(), 2);
            assert_eq!(vals[0], Value::String("a".to_string()));
            assert_eq!(vals[1], Value::String("b".to_string()));
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_topk_dataframe() {
        let series = Series::new("col".into(), vec!["x", "y", "x", "z", "x", "y"]);
        let df = DataFrame::new(vec![series.into()]).unwrap();
        let result = builtin_topk(&[Value::DataFrame(df), Value::Int(2)]).unwrap();
        if let Value::DataFrame(result_df) = result {
            assert_eq!(result_df.height(), 2);
        } else {
            panic!("Expected DataFrame result");
        }
    }

    #[test]
    fn test_topk_invalid_args() {
        let arr = vec![Value::Int(1)];
        let result = builtin_topk(&[Value::Array(arr)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_topk_negative_k() {
        let arr = vec![Value::Int(1)];
        let result = builtin_topk(&[Value::Array(arr), Value::Int(-1)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_topk_with_counts_array_basic() {
        let arr = vec![
            Value::String("apple".to_string()),
            Value::String("banana".to_string()),
            Value::String("apple".to_string()),
            Value::String("cherry".to_string()),
            Value::String("apple".to_string()),
        ];
        let result = builtin_topk_with_counts(&[Value::Array(arr), Value::Int(2)]).unwrap();
        if let Value::DataFrame(df) = result {
            assert_eq!(df.height(), 2);
            assert_eq!(df.width(), 2);
            let columns = df.get_column_names();
            assert!(columns.iter().any(|c| c.as_str() == "value"));
            assert!(columns.iter().any(|c| c.as_str() == "count"));

            // First value should be "apple" with count 3
            let value_col = df.column("value").unwrap().as_materialized_series();
            let count_col = df.column("count").unwrap().as_materialized_series();

            if let Ok(AnyValue::String(s)) = value_col.get(0) {
                assert_eq!(s, "apple");
            } else {
                panic!("Expected first value to be 'apple'");
            }

            if let Ok(count_val) = count_col.get(0) {
                match count_val {
                    AnyValue::UInt32(c) => assert_eq!(c, 3),
                    AnyValue::UInt64(c) => assert_eq!(c, 3),
                    _ => panic!("Expected count to be UInt32 or UInt64, got {:?}", count_val),
                }
            } else {
                panic!("Failed to get first count");
            }
        } else {
            panic!("Expected DataFrame result");
        }
    }

    #[test]
    fn test_topk_with_counts_series() {
        let series = Series::new("test".into(), vec!["a", "b", "a", "c", "a", "b"]);
        let result = builtin_topk_with_counts(&[Value::Series(series), Value::Int(2)]).unwrap();
        if let Value::DataFrame(df) = result {
            assert_eq!(df.height(), 2);
            assert_eq!(df.width(), 2);

            let count_col = df.column("count").unwrap().as_materialized_series();
            if let Ok(count_val) = count_col.get(0) {
                match count_val {
                    AnyValue::UInt32(c) => assert_eq!(c, 3),
                    AnyValue::UInt64(c) => assert_eq!(c, 3),
                    _ => panic!("Expected count to be UInt32 or UInt64, got {:?}", count_val),
                }
            }
            if let Ok(count_val) = count_col.get(1) {
                match count_val {
                    AnyValue::UInt32(c) => assert_eq!(c, 2),
                    AnyValue::UInt64(c) => assert_eq!(c, 2),
                    _ => panic!("Expected count to be UInt32 or UInt64, got {:?}", count_val),
                }
            }
        } else {
            panic!("Expected DataFrame result");
        }
    }

    #[test]
    fn test_topk_with_counts_dataframe() {
        let series = Series::new("col".into(), vec!["x", "y", "x", "z", "x"]);
        let df = DataFrame::new(vec![series.into()]).unwrap();
        let result = builtin_topk_with_counts(&[Value::DataFrame(df), Value::Int(2)]).unwrap();
        if let Value::DataFrame(result_df) = result {
            assert_eq!(result_df.height(), 2);
            assert_eq!(result_df.width(), 2);
        } else {
            panic!("Expected DataFrame result");
        }
    }

    #[test]
    fn test_topk_with_counts_ascending() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("a".to_string()),
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ];
        let result =
            builtin_topk_with_counts(&[Value::Array(arr), Value::Int(2), Value::Bool(false)])
                .unwrap();
        if let Value::DataFrame(df) = result {
            let count_col = df.column("count").unwrap().as_materialized_series();
            if let Ok(count_val) = count_col.get(0) {
                match count_val {
                    AnyValue::UInt32(c) => assert_eq!(c, 1),
                    AnyValue::UInt64(c) => assert_eq!(c, 1),
                    _ => panic!("Expected count to be UInt32 or UInt64, got {:?}", count_val),
                }
            }
        } else {
            panic!("Expected DataFrame result");
        }
    }

    #[test]
    fn test_topk_with_counts_empty() {
        let arr: Vec<Value> = vec![];
        let result = builtin_topk_with_counts(&[Value::Array(arr), Value::Int(5)]).unwrap();
        if let Value::DataFrame(df) = result {
            assert_eq!(df.height(), 0);
        } else {
            panic!("Expected DataFrame result");
        }
    }

    #[test]
    fn test_topk_with_counts_invalid_args() {
        let arr = vec![Value::Int(1)];
        let result = builtin_topk_with_counts(&[Value::Array(arr)]);
        assert!(result.is_err());
    }
}
