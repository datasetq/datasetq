use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

inventory::submit! {
    FunctionRegistration {
        name: "max",
        func: builtin_max,
    }
}

pub fn builtin_max(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "max() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) if !arr.is_empty() => {
            let mut max_val = &arr[0];
            for val in arr.iter().skip(1) {
                if crate::compare_values_for_sorting(val, max_val) == std::cmp::Ordering::Greater {
                    max_val = val;
                }
            }
            Ok(max_val.clone())
        }
        Value::DataFrame(df) => {
            // Get max of first numeric column
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() {
                        // Collect numeric values and find max
                        let mut values = Vec::new();
                        for i in 0..series.len() {
                            if let Ok(val) = series.get(i) {
                                match val {
                                    AnyValue::Int8(n) => values.push(n as f64),
                                    AnyValue::Int16(n) => values.push(n as f64),
                                    AnyValue::Int32(n) => values.push(n as f64),
                                    AnyValue::Int64(n) => values.push(n as f64),
                                    AnyValue::UInt8(n) => values.push(n as f64),
                                    AnyValue::UInt16(n) => values.push(n as f64),
                                    AnyValue::UInt32(n) => values.push(n as f64),
                                    AnyValue::UInt64(n) => values.push(n as f64),
                                    AnyValue::Float32(n) => values.push(n as f64),
                                    AnyValue::Float64(n) => values.push(n),
                                    _ => {}
                                }
                            }
                        }
                        if let Some(&max_val) =
                            values.iter().max_by(|a, b| a.partial_cmp(b).unwrap())
                        {
                            return Ok(Value::Float(max_val));
                        }
                    }
                }
            }
            Ok(Value::Null)
        }
        _ => Ok(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_max_array_integers() {
        let arr = vec![Value::Int(1), Value::Int(5), Value::Int(3)];
        let result = builtin_max(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Int(5));
    }

    #[test]
    fn test_max_array_floats() {
        let arr = vec![Value::Float(1.1), Value::Float(5.5), Value::Float(3.3)];
        let result = builtin_max(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Float(5.5));
    }

    #[test]
    fn test_max_array_mixed() {
        let arr = vec![Value::Int(1), Value::Float(5.5), Value::Int(3)];
        let result = builtin_max(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Float(5.5));
    }

    #[test]
    fn test_max_array_strings() {
        let arr = vec![
            Value::String("apple".to_string()),
            Value::String("zebra".to_string()),
            Value::String("banana".to_string()),
        ];
        let result = builtin_max(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::String("zebra".to_string()));
    }

    #[test]
    fn test_max_array_empty() {
        let arr = vec![];
        let result = builtin_max(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_max_dataframe() {
        let series = Series::new(PlSmallStr::from("col"), vec![1i64, 5, 3]);
        let column = Column::from(series);
        let df = DataFrame::new(vec![column]).unwrap();
        let result = builtin_max(&[Value::DataFrame(df)]).unwrap();
        assert_eq!(result, Value::Float(5.0));
    }

    #[test]
    fn test_max_dataframe_no_numeric() {
        let series = Series::new(
            PlSmallStr::from("col"),
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        );
        let column = Column::from(series);
        let df = DataFrame::new(vec![column]).unwrap();
        let result = builtin_max(&[Value::DataFrame(df)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_max_other_types() {
        let result = builtin_max(&[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Null);

        let result = builtin_max(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_max_wrong_args() {
        let result = builtin_max(&[]);
        assert!(result.is_err());

        let result = builtin_max(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_max_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("max"));
    }
}
