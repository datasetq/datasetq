use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_cut(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "cut() expects 2 arguments: DataFrame and array of column names",
        ));
    }

    let df = match &args[0] {
        Value::DataFrame(df) => df,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "cut() first argument must be a DataFrame",
            ))
        }
    };

    let columns = match &args[1] {
        Value::Array(arr) => {
            let mut cols = Vec::new();
            for item in arr {
                match item {
                    Value::String(s) => cols.push(s.clone()),
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "cut() column names must be strings",
                        ))
                    }
                }
            }
            cols
        }
        _ => {
            return Err(dsq_shared::error::operation_error(
                "cut() second argument must be an array of column names",
            ))
        }
    };

    // Select columns that exist, ignore non-existent ones
    let mut selected_series = Vec::new();
    for col_name in &columns {
        if let Ok(series) = df.column(col_name) {
            let mut s = series.clone();
            s.rename(col_name);
            selected_series.push(s);
        }
        // Ignore non-existent columns
    }

    if selected_series.is_empty() {
        // Return empty DataFrame if no columns matched
        match DataFrame::new(Vec::<Series>::new()) {
            Ok(empty_df) => Ok(Value::DataFrame(empty_df)),
            Err(e) => Err(dsq_shared::error::operation_error(format!(
                "cut() failed to create empty DataFrame: {}",
                e
            ))),
        }
    } else {
        match DataFrame::new(selected_series) {
            Ok(selected_df) => Ok(Value::DataFrame(selected_df)),
            Err(e) => Err(dsq_shared::error::operation_error(format!(
                "cut() failed: {}",
                e
            ))),
        }
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "cut",
        func: builtin_cut,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;

    fn create_test_dataframe() -> DataFrame {
        let s1 = Series::new("name", &["Alice", "Bob", "Charlie"]);
        let s2 = Series::new("age", &[25, 30, 35]);
        let s3 = Series::new("city", &["NYC", "LA", "Chicago"]);
        DataFrame::new(vec![s1, s2, s3]).unwrap()
    }

    #[test]
    fn test_cut_select_single_column() {
        let df = create_test_dataframe();
        let df_value = Value::DataFrame(df);
        let columns = Value::Array(vec![Value::String("name".to_string())]);

        let result = builtin_cut(&[df_value, columns]).unwrap();

        match result {
            Value::DataFrame(result_df) => {
                assert_eq!(result_df.get_column_names(), &["name"]);
                assert_eq!(result_df.height(), 3);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_cut_select_multiple_columns() {
        let df = create_test_dataframe();
        let df_value = Value::DataFrame(df);
        let columns = Value::Array(vec![
            Value::String("name".to_string()),
            Value::String("age".to_string()),
        ]);

        let result = builtin_cut(&[df_value, columns]).unwrap();

        match result {
            Value::DataFrame(result_df) => {
                assert_eq!(result_df.get_column_names(), &["name", "age"]);
                assert_eq!(result_df.height(), 3);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_cut_select_non_existent_column() {
        let df = create_test_dataframe();
        let df_value = Value::DataFrame(df);
        let columns = Value::Array(vec![
            Value::String("name".to_string()),
            Value::String("nonexistent".to_string()),
        ]);

        let result = builtin_cut(&[df_value, columns]).unwrap();

        match result {
            Value::DataFrame(result_df) => {
                // Should only include existing columns
                assert_eq!(result_df.get_column_names(), &["name"]);
                assert_eq!(result_df.height(), 3);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_cut_select_no_columns() {
        let df = create_test_dataframe();
        let df_value = Value::DataFrame(df);
        let columns = Value::Array(vec![
            Value::String("nonexistent1".to_string()),
            Value::String("nonexistent2".to_string()),
        ]);

        let result = builtin_cut(&[df_value, columns]).unwrap();

        match result {
            Value::DataFrame(result_df) => {
                // Should return empty DataFrame
                assert_eq!(result_df.get_column_names().len(), 0);
                assert_eq!(result_df.height(), 0);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_cut_wrong_first_argument() {
        let wrong_value = Value::String("not a dataframe".to_string());
        let columns = Value::Array(vec![Value::String("name".to_string())]);

        let result = builtin_cut(&[wrong_value, columns]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("first argument must be a DataFrame"));
    }

    #[test]
    fn test_cut_wrong_second_argument() {
        let df = create_test_dataframe();
        let df_value = Value::DataFrame(df);
        let wrong_columns = Value::String("not an array".to_string());

        let result = builtin_cut(&[df_value, wrong_columns]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("second argument must be an array"));
    }

    #[test]
    fn test_cut_non_string_column_names() {
        let df = create_test_dataframe();
        let df_value = Value::DataFrame(df);
        let columns = Value::Array(vec![Value::Int(123)]);

        let result = builtin_cut(&[df_value, columns]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("column names must be strings"));
    }

    #[test]
    fn test_cut_wrong_number_of_arguments() {
        let df = create_test_dataframe();
        let df_value = Value::DataFrame(df);

        // Too few arguments
        let result = builtin_cut(&[df_value.clone()]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));

        // Too many arguments
        let columns = Value::Array(vec![Value::String("name".to_string())]);
        let result = builtin_cut(&[df_value, columns, Value::Int(1)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_cut_empty_dataframe() {
        let empty_df = DataFrame::new(Vec::<Series>::new()).unwrap();
        let df_value = Value::DataFrame(empty_df);
        let columns = Value::Array(vec![Value::String("name".to_string())]);

        let result = builtin_cut(&[df_value, columns]).unwrap();

        match result {
            Value::DataFrame(result_df) => {
                assert_eq!(result_df.get_column_names().len(), 0);
                assert_eq!(result_df.height(), 0);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_cut_registered_via_inventory() {
        // Test that the function is registered via inventory
        let mut found = false;
        for func in inventory::iter::<crate::FunctionRegistration> {
            if func.name == "cut" {
                found = true;
                // Test that the function works
                let df = create_test_dataframe();
                let df_value = Value::DataFrame(df);
                let columns = Value::Array(vec![Value::String("name".to_string())]);
                let result = (func.func)(&[df_value, columns]).unwrap();
                match result {
                    Value::DataFrame(result_df) => {
                        assert_eq!(result_df.get_column_names(), &["name"]);
                    }
                    _ => panic!("Expected DataFrame"),
                }
                break;
            }
        }
        assert!(found, "cut function not found in inventory");
    }
}
