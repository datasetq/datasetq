use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_rstrip(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "rstrip() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.trim_end().to_string())),
        Value::Array(arr) => {
            let rstripped: Vec<Value> = arr
                .iter()
                .map(|v| {
                    match v {
                        Value::String(s) => Value::String(s.trim_end().to_string()),
                        _ => v.clone(), // Leave non-string values unchanged
                    }
                })
                .collect();
            Ok(Value::Array(rstripped))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::String {
                        let rstrip_series = series
                            .str()
                            .unwrap()
                            .apply(|s| s.map(|s| Cow::Owned(s.trim_end().to_string())))
                            .into_series();
                        let mut s = rstrip_series;
                        s.rename(col_name.clone());
                        new_series.push(s.into());
                    } else {
                        let mut s = series.clone();
                        s.rename(col_name.clone());
                        new_series.push(s);
                    }
                }
            }
            match DataFrame::new(new_series) {
                Ok(new_df) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "rstrip() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let rstrip_series = series
                    .str()
                    .unwrap()
                    .apply(|s| s.map(|s| Cow::Owned(s.trim_end().to_string())))
                    .into_series();
                Ok(Value::Series(rstrip_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        Value::LazyFrame(lf) => {
            // Collect the LazyFrame to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_rstrip(&[Value::DataFrame(df)])
        }
        _ => Ok(args[0].clone()),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "rstrip",
        func: builtin_rstrip,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::{DataFrame, Series};

    #[test]
    fn test_rstrip_string() {
        let result = builtin_rstrip(&[Value::String("  hello  ".to_string())]).unwrap();
        assert_eq!(result, Value::String("  hello".to_string()));
    }

    #[test]
    fn test_rstrip_string_no_trailing() {
        let result = builtin_rstrip(&[Value::String("hello".to_string())]).unwrap();
        assert_eq!(result, Value::String("hello".to_string()));
    }

    #[test]
    fn test_rstrip_string_only_spaces() {
        let result = builtin_rstrip(&[Value::String("   ".to_string())]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_rstrip_empty_string() {
        let result = builtin_rstrip(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_rstrip_array() {
        let arr = vec![
            Value::String("  hello  ".to_string()),
            Value::String("world   ".to_string()),
            Value::Int(42),
        ];
        let result = builtin_rstrip(&[Value::Array(arr)]).unwrap();
        let expected = vec![
            Value::String("  hello".to_string()),
            Value::String("world".to_string()),
            Value::Int(42),
        ];
        assert_eq!(result, Value::Array(expected));
    }

    #[test]
    fn test_rstrip_dataframe() {
        let df = DataFrame::new(vec![
            Series::new("name".into(), &["  Alice  ", "Bob   ", "Charlie"]).into(),
            Series::new("age".into(), &[25, 30, 35]).into(),
        ])
        .unwrap();
        let result = builtin_rstrip(&[Value::DataFrame(df.clone())]).unwrap();
        if let Value::DataFrame(result_df) = result {
            let name_series = result_df.column("name").unwrap();
            let name_values: Vec<String> = name_series
                .str()
                .unwrap()
                .into_iter()
                .map(|s| s.unwrap().to_string())
                .collect();
            assert_eq!(name_values, vec!["  Alice", "Bob", "Charlie"]);
            let age_series = result_df.column("age").unwrap();
            assert_eq!(age_series, df.column("age").unwrap());
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_rstrip_series() {
        let series = Series::new("test".into(), &["  hello  ", "world   ", "no spaces"]);
        let result = builtin_rstrip(&[Value::Series(series)]).unwrap();
        if let Value::Series(result_series) = result {
            let values: Vec<String> = result_series
                .str()
                .unwrap()
                .into_iter()
                .map(|s| s.unwrap().to_string())
                .collect();
            assert_eq!(values, vec!["  hello", "world", "no spaces"]);
        } else {
            panic!("Expected Series");
        }
    }

    #[test]
    fn test_rstrip_non_string_series() {
        let series = Series::new("numbers".into(), vec![1i32, 2, 3]);
        let result = builtin_rstrip(&[Value::Series(series.clone())]).unwrap();
        if let Value::Series(result_series) = result {
            assert_eq!(result_series.name(), series.name());
            assert_eq!(result_series.dtype(), series.dtype());
            assert_eq!(result_series.len(), series.len());
        } else {
            panic!("Expected Series");
        }
    }

    #[test]
    fn test_rstrip_other_types() {
        let result = builtin_rstrip(&[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(42));

        let result = builtin_rstrip(&[Value::Float(3.14)]).unwrap();
        assert_eq!(result, Value::Float(3.14));

        let result = builtin_rstrip(&[Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::Bool(true));

        let result = builtin_rstrip(&[Value::Null]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_rstrip_no_args() {
        let result = builtin_rstrip(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_rstrip_too_many_args() {
        let result = builtin_rstrip(&[
            Value::String("test".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }
}
