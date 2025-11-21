use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "sort_by",
        func: builtin_sort_by,
    }
}

pub fn builtin_sort_by(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "sort_by() expects 2 arguments",
        ));
    }

    match (&args[0], &args[1]) {
        (Value::Array(arr), Value::Array(key_arr)) if arr.len() == key_arr.len() => {
            // Sort array by key array
            let mut indices: Vec<usize> = (0..arr.len()).collect();
            indices.sort_by(|&i, &j| crate::compare_values_for_sorting(&key_arr[i], &key_arr[j]));
            let sorted_arr: Vec<Value> = indices.into_iter().map(|i| arr[i].clone()).collect();
            Ok(Value::Array(sorted_arr))
        }
        (Value::Array(arr), Value::String(field)) => {
            // Sort array of objects by field
            let mut key_arr = Vec::new();
            for item in arr {
                if let Value::Object(obj) = item {
                    if let Some(value) = obj.get(field) {
                        key_arr.push(value.clone());
                    } else {
                        key_arr.push(Value::Null);
                    }
                } else {
                    key_arr.push(Value::Null);
                }
            }
            let mut indices: Vec<usize> = (0..arr.len()).collect();
            indices.sort_by(|&i, &j| crate::compare_values_for_sorting(&key_arr[i], &key_arr[j]));
            let sorted_arr: Vec<Value> = indices.into_iter().map(|i| arr[i].clone()).collect();
            Ok(Value::Array(sorted_arr))
        }
        (Value::DataFrame(df), Value::String(column)) => {
            // Sort DataFrame by column name
            match df.sort([column.as_str()], false, false) {
                Ok(sorted_df) => Ok(Value::DataFrame(sorted_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!("sort_by() failed: {}", e))),
            }
        }
        (Value::DataFrame(df), Value::Array(keys)) if df.height() == keys.len() => {
            // Sort DataFrame by the provided keys array
            let mut indices: Vec<usize> = (0..keys.len()).collect();
            indices.sort_by(|&i, &j| crate::compare_values_for_sorting(&keys[i], &keys[j]));
            let indices_u32: Vec<u32> = indices.into_iter().map(|i| i as u32).collect();
            let indices_ca = UInt32Chunked::from_vec("indices".into(), indices_u32);
            match df.take(&indices_ca) {
                Ok(sorted_df) => Ok(Value::DataFrame(sorted_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!("sort_by() failed: {}", e))),
            }
        }
        (Value::DataFrame(df), Value::Series(series)) => {
            // Sort DataFrame by the provided series
            // Add the series as a temporary column, sort by it, then remove it
            let temp_col_name = "__sort_by_temp_col";
            let mut df_clone = df.clone();
            let mut temp_series = series.clone();
            temp_series.rename(temp_col_name);
            match df_clone.with_column(temp_series) {
                Ok(df_with_sort) => {
                    match df_with_sort.sort([temp_col_name], false, false) {
                        Ok(sorted_df) => {
                            match sorted_df.drop(temp_col_name) {
                                Ok(final_df) => Ok(Value::DataFrame(final_df)),
                                Err(e) => Err(dsq_shared::error::operation_error(format!("sort_by() failed to drop temp column: {}", e))),
                            }
                        }
                        Err(e) => Err(dsq_shared::error::operation_error(format!("sort_by() failed to sort: {}", e))),
                    }
                }
                Err(e) => Err(dsq_shared::error::operation_error(format!("sort_by() failed to add temp column: {}", e))),
            }
        }
        (Value::Series(series), Value::Series(key_series)) => {
            // Sort series by key_series
            let temp_col_name = "__sort_by_temp_col";
            let mut df = DataFrame::new(vec![series.clone()]).map_err(|e| dsq_shared::error::operation_error(format!("sort_by() failed to create df: {}", e)))?;
            let mut temp_series = key_series.clone();
            temp_series.rename(temp_col_name);
            match df.with_column(temp_series) {
                Ok(df_with_sort) => {
                    match df_with_sort.sort([temp_col_name], false, false) {
                        Ok(sorted_df) => {
                            match sorted_df.drop(temp_col_name) {
                                Ok(final_df) => {
                                    if let Some(sorted_series) = final_df.get_columns().first() {
                                        Ok(Value::Series(sorted_series.clone()))
                                    } else {
                                        Ok(Value::Series(series.clone()))
                                    }
                                }
                                Err(e) => Err(dsq_shared::error::operation_error(format!("sort_by() failed to drop temp column: {}", e))),
                            }
                        }
                        Err(e) => Err(dsq_shared::error::operation_error(format!("sort_by() failed to sort: {}", e))),
                    }
                }
                Err(e) => Err(dsq_shared::error::operation_error(format!("sort_by() failed to add temp column: {}", e))),
            }
        }
        _ => Err(dsq_shared::error::operation_error("sort_by() requires (array, array), (array, string), (dataframe, string/array/series), or (series, series)")),
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
    fn test_sort_by_array_by_key_array() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ];
        let key_arr = vec![Value::Int(3), Value::Int(1), Value::Int(2)];
        let result = builtin_sort_by(&[Value::Array(arr), Value::Array(key_arr)]).unwrap();
        if let Value::Array(sorted) = result {
            assert_eq!(sorted[0], Value::String("b".to_string()));
            assert_eq!(sorted[1], Value::String("c".to_string()));
            assert_eq!(sorted[2], Value::String("a".to_string()));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_sort_by_array_of_objects_by_field() {
        let mut obj1 = HashMap::new();
        obj1.insert("name".to_string(), Value::String("Alice".to_string()));
        obj1.insert("age".to_string(), Value::Int(25));

        let mut obj2 = HashMap::new();
        obj2.insert("name".to_string(), Value::String("Bob".to_string()));
        obj2.insert("age".to_string(), Value::Int(30));

        let mut obj3 = HashMap::new();
        obj3.insert("name".to_string(), Value::String("Charlie".to_string()));
        obj3.insert("age".to_string(), Value::Int(20));

        let arr = vec![
            Value::Object(obj1),
            Value::Object(obj2),
            Value::Object(obj3),
        ];
        let result =
            builtin_sort_by(&[Value::Array(arr), Value::String("age".to_string())]).unwrap();
        if let Value::Array(sorted) = result {
            if let Value::Object(obj) = &sorted[0] {
                assert_eq!(obj.get("name"), Some(&Value::String("Charlie".to_string())));
            }
            if let Value::Object(obj) = &sorted[1] {
                assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
            }
            if let Value::Object(obj) = &sorted[2] {
                assert_eq!(obj.get("name"), Some(&Value::String("Bob".to_string())));
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_sort_by_dataframe_by_column() {
        let df = create_test_dataframe();
        let result =
            builtin_sort_by(&[Value::DataFrame(df), Value::String("age".to_string())]).unwrap();
        if let Value::DataFrame(sorted_df) = result {
            let names = sorted_df.column("name").unwrap().utf8().unwrap();
            assert_eq!(names.get(0), Some("Alice"));
            assert_eq!(names.get(1), Some("Bob"));
            assert_eq!(names.get(2), Some("Charlie"));
        } else {
            panic!("Expected DataFrame result");
        }
    }

    #[test]
    fn test_sort_by_dataframe_by_key_array() {
        let df = create_test_dataframe();
        let keys = vec![Value::Int(30), Value::Int(25), Value::Int(35)];
        let result = builtin_sort_by(&[Value::DataFrame(df), Value::Array(keys)]).unwrap();
        if let Value::DataFrame(sorted_df) = result {
            let names = sorted_df.column("name").unwrap().utf8().unwrap();
            assert_eq!(names.get(0), Some("Bob"));
            assert_eq!(names.get(1), Some("Alice"));
            assert_eq!(names.get(2), Some("Charlie"));
        } else {
            panic!("Expected DataFrame result");
        }
    }

    #[test]
    fn test_sort_by_series_by_key_series() {
        let series = Series::new("values", &[3, 1, 2]);
        let key_series = Series::new("keys", &[30, 10, 20]);
        let result = builtin_sort_by(&[Value::Series(series), Value::Series(key_series)]).unwrap();
        if let Value::Series(sorted_series) = result {
            let values = sorted_series.i32().unwrap();
            assert_eq!(values.get(0), Some(1));
            assert_eq!(values.get(1), Some(2));
            assert_eq!(values.get(2), Some(3));
        } else {
            panic!("Expected Series result");
        }
    }

    #[test]
    fn test_sort_by_wrong_number_of_args() {
        let result = builtin_sort_by(&[Value::Array(vec![])]);
        assert!(result.is_err());
    }

    #[test]
    fn test_sort_by_invalid_args() {
        let result = builtin_sort_by(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_sort_by_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("sort_by"));
    }
}
