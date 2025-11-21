use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_columns(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "columns() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::DataFrame(df) => {
            let columns: Vec<Value> = df
                .get_column_names()
                .iter()
                .map(|name| Value::String(name.to_string()))
                .collect();
            Ok(Value::Array(columns))
        }
        _ => Err(dsq_shared::error::operation_error(
            "columns() requires DataFrame argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "columns",
        func: builtin_columns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;

    fn create_test_dataframe() -> DataFrame {
        let names = Series::new("name", &["Alice", "Bob", "Charlie"]);
        let ages = Series::new("age", &[25, 30, 35]);
        let scores = Series::new("score", &[85.5, 92.0, 78.3]);
        DataFrame::new(vec![names, ages, scores]).unwrap()
    }

    #[test]
    fn test_builtin_columns_dataframe() {
        let df = create_test_dataframe();
        let result = builtin_columns(&[Value::DataFrame(df)]).unwrap();
        if let Value::Array(cols) = result {
            assert_eq!(cols.len(), 3);
            assert!(cols.contains(&Value::String("name".to_string())));
            assert!(cols.contains(&Value::String("age".to_string())));
            assert!(cols.contains(&Value::String("score".to_string())));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_builtin_columns_invalid_args() {
        // No arguments
        let result = builtin_columns(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Too many arguments
        let result = builtin_columns(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_builtin_columns_invalid_type() {
        let result = builtin_columns(&[Value::Int(42)]).unwrap_err();
        assert!(result.to_string().contains("requires DataFrame argument"));
    }

    #[test]
    fn test_columns_registered_via_inventory() {
        // Test that the function is registered via inventory
        let mut found = false;
        for func in inventory::iter::<crate::FunctionRegistration> {
            if func.name == "columns" {
                found = true;
                // Test that the function works
                let df = create_test_dataframe();
                let result = (func.func)(&[Value::DataFrame(df)]).unwrap();
                if let Value::Array(cols) = result {
                    assert_eq!(cols.len(), 3);
                } else {
                    panic!("Expected array result");
                }
                break;
            }
        }
        assert!(found, "columns function not found in inventory");
    }
}
