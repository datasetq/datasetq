use dsq_shared::Result;
use dsq_shared::value::Value;
use inventory;
use polars::prelude::*;
use std::time::SystemTime;

pub fn builtin_systime_ns(args: &[Value]) -> Result<Value> {
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
            "systime_ns() expects 0 or 1 arguments",
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
            let time_series = Series::new("systime_ns".into(), vec![nanos; df.height()]);
            match new_df.with_column(time_series) {
                Ok(_) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "systime_ns() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            let time_series = Series::new(series.name(), vec![nanos; series.len()]);
            Ok(Value::Series(time_series))
        }
        _ => Ok(Value::Int(nanos)),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "systime_ns",
        func: builtin_systime_ns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;

    #[test]
    fn test_systime_ns_no_args() {
        let result = builtin_systime_ns(&[]).unwrap();
        match result {
            Value::Int(nanos) => {
                // Should be a positive number representing nanoseconds since epoch
                assert!(nanos > 0);
                // Should be reasonable (after 2020)
                assert!(nanos > 1577836800_000_000_000); // 2020-01-01 in nanoseconds
            }
            _ => panic!("Expected Int result"),
        }
    }

    #[test]
    fn test_systime_ns_with_int() {
        let result = builtin_systime_ns(&[Value::Int(123)]).unwrap();
        match result {
            Value::Int(nanos) => {
                assert!(nanos > 0);
            }
            _ => panic!("Expected Int result"),
        }
    }

    #[test]
    fn test_systime_ns_with_array() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_systime_ns(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(times) => {
                assert_eq!(times.len(), 3);
                for time in times {
                    match time {
                        Value::Int(nanos) => assert!(nanos > 0),
                        _ => panic!("Expected Int in array"),
                    }
                }
            }
            _ => panic!("Expected Array result"),
        }
    }

    #[test]
    fn test_systime_ns_with_dataframe() {
        let df = DataFrame::new(vec![
            Series::new("name".into(), &["Alice", "Bob"]),
            Series::new("age".into(), &[25, 30]),
        ])
        .unwrap();
        let result = builtin_systime_ns(&[Value::DataFrame(df.clone())]).unwrap();
        match result {
            Value::DataFrame(new_df) => {
                assert_eq!(new_df.height(), 2);
                assert_eq!(new_df.width(), 3); // original 2 + 1 new column
                assert!(new_df.column("systime_ns").is_ok());
                let time_col = new_df.column("systime_ns").unwrap();
                assert_eq!(time_col.len(), 2);
            }
            _ => panic!("Expected DataFrame result"),
        }
    }

    #[test]
    fn test_systime_ns_with_series() {
        let series = Series::new("values".into(), vec![1, 2, 3]);
        let result = builtin_systime_ns(&[Value::Series(series.clone())]).unwrap();
        match result {
            Value::Series(time_series) => {
                assert_eq!(time_series.len(), 3);
                assert_eq!(time_series.name(), "values");
            }
            _ => panic!("Expected Series result"),
        }
    }

    #[test]
    fn test_systime_ns_too_many_args() {
        let result = builtin_systime_ns(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expects 0 or 1 arguments")
        );
    }

    #[test]
    fn test_systime_ns_registered_via_inventory() {
        // Test that the function is registered via inventory
        let mut found = false;
        for func in inventory::iter::<crate::FunctionRegistration> {
            if func.name == "systime_ns" {
                found = true;
                // Test that the function works
                let result = (func.func)(&[]).unwrap();
                match result {
                    Value::Int(nanos) => assert!(nanos > 0),
                    _ => panic!("Expected Int result from inventory function"),
                }
                break;
            }
        }
        assert!(found, "systime_ns function not found in inventory");
    }
}
