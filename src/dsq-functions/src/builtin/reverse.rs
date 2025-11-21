use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_reverse(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "reverse() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let mut reversed = arr.clone();
            reversed.reverse();
            Ok(Value::Array(reversed))
        }
        Value::String(s) => {
            let reversed: String = s.chars().rev().collect();
            Ok(Value::String(reversed))
        }
        Value::DataFrame(df) => {
            // Reverse the rows of the DataFrame
            let reversed_df = df.reverse();
            Ok(Value::DataFrame(reversed_df))
        }
        Value::Series(series) => {
            // Reverse the series
            let reversed_series = series.reverse();
            Ok(Value::Series(reversed_series))
        }
        _ => Ok(args[0].clone()),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "reverse",
        func: builtin_reverse,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_reverse_array() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_reverse(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(reversed) => {
                assert_eq!(reversed.len(), 3);
                assert_eq!(reversed[0], Value::Int(3));
                assert_eq!(reversed[1], Value::Int(2));
                assert_eq!(reversed[2], Value::Int(1));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_reverse_string() {
        let result = builtin_reverse(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(result, Value::String("olleh".to_string()));
    }

    #[test]
    fn test_reverse_dataframe() {
        let names = Series::new("name", &["Alice", "Bob", "Charlie"]);
        let ages = Series::new("age", &[25, 30, 35]);
        let df = DataFrame::new(vec![names, ages]).unwrap();

        let result = builtin_reverse(&[Value::DataFrame(df.clone())]).unwrap();
        match result {
            Value::DataFrame(reversed_df) => {
                // After reverse, Charlie should be first
                let first_name = reversed_df
                    .column("name")
                    .unwrap()
                    .utf8()
                    .unwrap()
                    .get(0)
                    .unwrap();
                assert_eq!(first_name, "Charlie");
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_reverse_series() {
        let series = Series::new("test", &[1, 2, 3]);
        let result = builtin_reverse(&[Value::Series(series)]).unwrap();
        match result {
            Value::Series(reversed_series) => {
                assert_eq!(reversed_series.get(0).unwrap(), AnyValue::Int32(3));
                assert_eq!(reversed_series.get(2).unwrap(), AnyValue::Int32(1));
            }
            _ => panic!("Expected Series"),
        }
    }

    #[test]
    fn test_reverse_other_types() {
        let result = builtin_reverse(&[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(42));

        let result = builtin_reverse(&[Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_reverse_wrong_args() {
        let result = builtin_reverse(&[]);
        assert!(result.is_err());

        let result = builtin_reverse(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_reverse_registered_via_inventory() {
        let mut found = false;
        for func in inventory::iter::<crate::FunctionRegistration> {
            if func.name == "reverse" {
                found = true;
                // Test that the function works
                let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
                let result = (func.func)(&[Value::Array(arr)]).unwrap();
                match result {
                    Value::Array(reversed) => {
                        assert_eq!(reversed[0], Value::Int(3));
                    }
                    _ => panic!("Expected array"),
                }
                break;
            }
        }
        assert!(found, "reverse function not found in inventory");
    }
}
