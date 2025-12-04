use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_systime_int(args: &[Value]) -> Result<Value> {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| dsq_shared::error::operation_error("System time is before UNIX epoch"))?;
    let seconds = duration.as_secs() as i64;

    if args.is_empty() {
        return Ok(Value::Int(seconds));
    }

    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "systime_int() expects 0 or 1 arguments",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let current_times: Vec<Value> = (0..arr.len()).map(|_| Value::Int(seconds)).collect();
            Ok(Value::Array(current_times))
        }
        Value::DataFrame(df) => {
            // Add a new column with current time for each row
            let mut new_df = df.clone();
            let time_series = Series::new("systime_int", vec![seconds; df.height()]);
            match new_df.with_column(time_series) {
                Ok(_) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "systime_int() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            // Add a new series with current time for each element
            let time_series = Series::new("systime_int", vec![seconds; series.len()]);
            Ok(Value::Series(time_series))
        }
        _ => Ok(Value::Int(seconds)),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "systime_int",
        func: builtin_systime_int,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_systime_int_no_args() {
        let result = builtin_systime_int(&[]).unwrap();
        match result {
            Value::Int(timestamp) => {
                // Check that timestamp is reasonable (between 2020 and 2050)
                let min_time = 1577836800; // 2020-01-01
                let max_time = 2524608000; // 2050-01-01
                assert!(timestamp >= min_time && timestamp <= max_time);
            }
            _ => panic!("Expected Int, got {:?}", result),
        }
    }

    #[test]
    fn test_builtin_systime_int_with_array() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let result = builtin_systime_int(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(result_arr) => {
                assert_eq!(result_arr.len(), 2);
                for val in result_arr {
                    match val {
                        Value::Int(_) => {} // Valid
                        _ => panic!("Expected Int in array, got {:?}", val),
                    }
                }
            }
            _ => panic!("Expected Array, got {:?}", result),
        }
    }

    #[test]
    fn test_builtin_systime_int_with_dataframe() {
        let df = DataFrame::new(vec![
            Series::new("name", &["Alice", "Bob"]),
            Series::new("age", &[25, 30]),
        ])
        .unwrap();
        let result = builtin_systime_int(&[Value::DataFrame(df)]).unwrap();
        match result {
            Value::DataFrame(result_df) => {
                assert!(result_df.get_column_names().contains(&"systime_int"));
                assert_eq!(result_df.height(), 2);
            }
            _ => panic!("Expected DataFrame, got {:?}", result),
        }
    }

    #[test]
    fn test_builtin_systime_int_with_series() {
        let series = Series::new("test", &[1, 2, 3]);
        let result = builtin_systime_int(&[Value::Series(series)]).unwrap();
        match result {
            Value::Series(result_series) => {
                assert_eq!(result_series.name(), "systime_int");
                assert_eq!(result_series.len(), 3);
            }
            _ => panic!("Expected Series, got {:?}", result),
        }
    }

    #[test]
    fn test_builtin_systime_int_with_other_value() {
        let result = builtin_systime_int(&[Value::String("test".to_string())]).unwrap();
        match result {
            Value::Int(_) => {} // Should return current time
            _ => panic!("Expected Int, got {:?}", result),
        }
    }

    #[test]
    fn test_builtin_systime_int_too_many_args() {
        let result = builtin_systime_int(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 0 or 1 arguments"));
    }

    #[test]
    fn test_systime_int_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("systime_int"));
    }
}
