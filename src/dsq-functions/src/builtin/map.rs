use crate::FunctionRegistration;
use dsq_shared::error::operation_error;
use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::collections::HashMap;

pub fn builtin_map(args: &[Value]) -> Result<Value> {
    match args.len() {
        1 => Ok(args[0].clone()),
        2 => {
            match (&args[0], &args[1]) {
                (Value::Array(arr), Value::String(field)) => {
                    let mut result = Vec::new();
                    for item in arr {
                        if let Value::Object(obj) = item {
                            if let Some(val) = obj.get(field) {
                                result.push(val.clone());
                            } else {
                                result.push(Value::Null);
                            }
                        } else {
                            result.push(Value::Null);
                        }
                    }
                    Ok(Value::Array(result))
                }
                (Value::Array(arr), Value::Object(template)) => {
                    let mut result = Vec::new();
                    for item in arr {
                        if let Value::Object(obj) = item {
                            let mut new_obj = HashMap::new();
                            for (key, _) in template {
                                if let Some(val) = obj.get(key) {
                                    new_obj.insert(key.clone(), val.clone());
                                } else {
                                    new_obj.insert(key.clone(), Value::Null);
                                }
                            }
                            result.push(Value::Object(new_obj));
                        } else {
                            result.push(Value::Null);
                        }
                    }
                    Ok(Value::Array(result))
                }
                (Value::DataFrame(df), Value::String(field)) => {
                    if let Ok(series) = df.column(field) {
                        let mut values = Vec::new();
                        for i in 0..series.len() {
                            if let Ok(val) = series.get(i) {
                                let value = value_from_any_value(val).unwrap_or(Value::Null);
                                values.push(value);
                            }
                        }
                        Ok(Value::Array(values))
                    } else {
                        Err(operation_error(format!("Column '{}' not found", field)))
                    }
                }
                (Value::DataFrame(df), Value::Object(template)) => {
                    // Select columns specified in template
                    let mut selected_series = Vec::new();
                    for (key, _) in template {
                        if let Ok(series) = df.column(key) {
                            let mut s = series.clone();
                            s.rename(key.as_str().into());
                            selected_series.push(s.into());
                        }
                    }
                    match DataFrame::new(selected_series) {
                        Ok(selected_df) => Ok(Value::DataFrame(selected_df)),
                        Err(e) => Err(operation_error(format!("map() failed on DataFrame: {}", e))),
                    }
                }
                _ => Ok(args[0].clone()),
            }
        }
        _ => Err(operation_error("map() expects 1 or 2 arguments")),
    }
}

inventory::submit!(FunctionRegistration {
    name: "map",
    func: builtin_map
});

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_map_single_arg() {
        let args = vec![Value::Int(42)];
        let result = builtin_map(&args).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_map_array_with_string_field() {
        let mut obj1 = HashMap::new();
        obj1.insert("name".to_string(), Value::String("Alice".to_string()));
        obj1.insert("age".to_string(), Value::Int(25));

        let mut obj2 = HashMap::new();
        obj2.insert("name".to_string(), Value::String("Bob".to_string()));
        obj2.insert("age".to_string(), Value::Int(30));

        let arr = vec![Value::Object(obj1), Value::Object(obj2)];
        let args = vec![Value::Array(arr), Value::String("name".to_string())];
        let result = builtin_map(&args).unwrap();

        let expected = vec![
            Value::String("Alice".to_string()),
            Value::String("Bob".to_string()),
        ];
        assert_eq!(result, Value::Array(expected));
    }

    #[test]
    fn test_map_array_with_string_field_missing() {
        let mut obj1 = HashMap::new();
        obj1.insert("name".to_string(), Value::String("Alice".to_string()));

        let mut obj2 = HashMap::new();
        obj2.insert("other".to_string(), Value::String("Bob".to_string()));

        let arr = vec![Value::Object(obj1), Value::Object(obj2)];
        let args = vec![Value::Array(arr), Value::String("name".to_string())];
        let result = builtin_map(&args).unwrap();

        let expected = vec![Value::String("Alice".to_string()), Value::Null];
        assert_eq!(result, Value::Array(expected));
    }

    #[test]
    fn test_map_array_with_object_template() {
        let mut obj1 = HashMap::new();
        obj1.insert("name".to_string(), Value::String("Alice".to_string()));
        obj1.insert("age".to_string(), Value::Int(25));
        obj1.insert("city".to_string(), Value::String("NYC".to_string()));

        let mut obj2 = HashMap::new();
        obj2.insert("name".to_string(), Value::String("Bob".to_string()));
        obj2.insert("age".to_string(), Value::Int(30));
        obj2.insert("city".to_string(), Value::String("LA".to_string()));

        let arr = vec![Value::Object(obj1), Value::Object(obj2)];

        let mut template = HashMap::new();
        template.insert("name".to_string(), Value::Null);
        template.insert("age".to_string(), Value::Null);

        let args = vec![Value::Array(arr), Value::Object(template)];
        let result = builtin_map(&args).unwrap();

        if let Value::Array(result_arr) = result {
            assert_eq!(result_arr.len(), 2);
            if let Value::Object(obj) = &result_arr[0] {
                assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
                assert_eq!(obj.get("age"), Some(&Value::Int(25)));
                assert!(!obj.contains_key("city"));
            } else {
                panic!("Expected object");
            }
            if let Value::Object(obj) = &result_arr[1] {
                assert_eq!(obj.get("name"), Some(&Value::String("Bob".to_string())));
                assert_eq!(obj.get("age"), Some(&Value::Int(30)));
            } else {
                panic!("Expected object");
            }
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_map_dataframe_with_string_field() {
        let names = Column::new(PlSmallStr::from("name"), &["Alice", "Bob"]);
        let ages = Column::new(PlSmallStr::from("age"), &[25, 30]);
        let df = DataFrame::new(vec![names, ages]).unwrap();

        let args = vec![Value::DataFrame(df), Value::String("name".to_string())];
        let result = builtin_map(&args).unwrap();

        let expected = vec![
            Value::String("Alice".to_string()),
            Value::String("Bob".to_string()),
        ];
        assert_eq!(result, Value::Array(expected));
    }

    #[test]
    fn test_map_dataframe_with_missing_column() {
        let names = Column::new(PlSmallStr::from("name"), vec!["Alice", "Bob"]);
        let df = DataFrame::new(vec![names]).unwrap();

        let args = vec![Value::DataFrame(df), Value::String("age".to_string())];
        let result = builtin_map(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_map_dataframe_with_object_template() {
        let names = Series::new(PlSmallStr::from("name"), &["Alice", "Bob"]).into();
        let ages = Series::new(PlSmallStr::from("age"), &[25, 30]).into();
        let cities = Series::new(PlSmallStr::from("city"), &["NYC", "LA"]).into();
        let df = DataFrame::new(vec![names, ages, cities]).unwrap();

        let mut template = HashMap::new();
        template.insert("name".to_string(), Value::Null);
        template.insert("age".to_string(), Value::Null);

        let args = vec![Value::DataFrame(df), Value::Object(template)];
        let result = builtin_map(&args).unwrap();

        if let Value::DataFrame(result_df) = result {
            let col_names = result_df.get_column_names();
            assert_eq!(col_names.len(), 2);
            assert!(col_names.contains(&&PlSmallStr::from("name")));
            assert!(col_names.contains(&&PlSmallStr::from("age")));
            assert_eq!(result_df.height(), 2);
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_map_invalid_args() {
        let args = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_map(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_map_fallback() {
        let args = vec![Value::String("hello".to_string()), Value::Int(42)];
        let result = builtin_map(&args).unwrap();
        assert_eq!(result, Value::String("hello".to_string()));
    }

    #[test]
    fn test_map_identity_preserves_nulls() {
        // Test that map with identity preserves null values
        let arr = vec![Value::Int(1), Value::Null, Value::Int(2)];
        let args = vec![Value::Array(arr)];
        let result = builtin_map(&args).unwrap();
        let expected = vec![Value::Int(1), Value::Null, Value::Int(2)];
        assert_eq!(result, Value::Array(expected));
    }
}
