use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_systime(args: &[Value]) -> Result<Value> {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| dsq_shared::error::operation_error("System time is before UNIX epoch"))?;
    let nanos = duration.as_nanos() as i64;

    if args.is_empty() {
        return Ok(Value::Int(nanos));
    }

    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "systime() expects 0 or 1 arguments",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let current_times: Vec<Value> = (0..arr.len()).map(|_| Value::Int(nanos)).collect();
            Ok(Value::Array(current_times))
        }
        Value::DataFrame(df) => {
            // Add a new column with current time for each row
            let mut new_df = df.clone();
            let time_series = Series::new("systime".into().into(), vec![nanos; df.height()]);
            match new_df.with_column(time_series) {
                Ok(_) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "systime() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            let time_series = Series::new("systime".into().into(), vec![nanos; series.len()]);
            Ok(Value::Series(time_series))
        }
        _ => Ok(Value::Int(nanos)),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "systime",
        func: builtin_systime,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_systime_no_args() {
        let result = builtin_systime(&[]).unwrap();
        match result {
            Value::Int(timestamp) => {
                // Check that timestamp is reasonable (between 2020 and 2050 in nanoseconds)
                let min_time = 1577836800i64 * 1_000_000_000; // 2020-01-01 in nanoseconds
                let max_time = 2524608000i64 * 1_000_000_000; // 2050-01-01 in nanoseconds
                assert!(timestamp >= min_time && timestamp <= max_time);
            }
            _ => panic!("Expected Int, got {:?}", result),
        }
    }

    #[test]
    fn test_builtin_systime_with_array() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let result = builtin_systime(&[Value::Array(arr)]).unwrap();
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
    fn test_builtin_systime_with_dataframe() {
        let df = DataFrame::new(vec![
            Series::new("name".into().into(), &["Alice", "Bob"]),
            Series::new("age".into().into(), &[25, 30]),
        ])
        .unwrap();
        let result = builtin_systime(&[Value::DataFrame(df)]).unwrap();
        match result {
            Value::DataFrame(result_df) => {
                assert!(result_df.get_column_names().contains(&"systime".into()));
                assert_eq!(result_df.height(), 2);
            }
            _ => panic!("Expected DataFrame, got {:?}", result),
        }
    }

    #[test]
    fn test_builtin_systime_with_series() {
        let series = Series::new("test".into().into(), &[1, 2, 3]);
        let result = builtin_systime(&[Value::Series(series)]).unwrap();
        match result {
            Value::Series(result_series) => {
                assert_eq!(result_series.name(), "systime");
                assert_eq!(result_series.len(), 3);
            }
            _ => panic!("Expected Series, got {:?}", result),
        }
    }

    #[test]
    fn test_builtin_systime_with_other_value() {
        let result = builtin_systime(&[Value::String("test".to_string())]).unwrap();
        match result {
            Value::Int(_) => {} // Should return current time
            _ => panic!("Expected Int, got {:?}", result),
        }
    }

    #[test]
    fn test_builtin_systime_too_many_args() {
        let result = builtin_systime(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 0 or 1 arguments"));
    }

    #[test]
    fn test_systime_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("systime"));
    }
}
