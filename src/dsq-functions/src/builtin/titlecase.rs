use dsq_shared::value::Value;
use dsq_shared::Result;
use heck::ToTitleCase;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_titlecase(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "titlecase() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.to_title_case())),
        Value::Array(arr) => {
            let titlecased: Vec<Value> = arr
                .iter()
                .map(|v| {
                    match v {
                        Value::String(s) => Value::String(s.to_title_case()),
                        _ => v.clone(), // Leave non-string values unchanged
                    }
                })
                .collect();
            Ok(Value::Array(titlecased))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::Utf8 {
                        let titlecase_series = series
                            .utf8()
                            .unwrap()
                            .apply(|s| s.map(|s| Cow::Owned(s.to_title_case())))
                            .into_series();
                        let mut s = titlecase_series;
                        s.rename(col_name);
                        new_series.push(s);
                    } else {
                        let mut s = series.clone();
                        s.rename(col_name);
                        new_series.push(s);
                    }
                }
            }
            match DataFrame::new(new_series) {
                Ok(new_df) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "titlecase() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::Utf8 {
                let titlecase_series = series
                    .utf8()
                    .unwrap()
                    .apply(|s| s.map(|s| Cow::Owned(s.to_title_case())))
                    .into_series();
                Ok(Value::Series(titlecase_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Ok(args[0].clone()),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "titlecase",
        func: builtin_titlecase,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;

    #[test]
    fn test_titlecase_string() {
        let result = builtin_titlecase(&[Value::String("hello world".to_string())]).unwrap();
        assert_eq!(result, Value::String("Hello World".to_string()));
    }

    #[test]
    fn test_titlecase_mixed_case() {
        let result = builtin_titlecase(&[Value::String("HELLO WORLD".to_string())]).unwrap();
        assert_eq!(result, Value::String("Hello World".to_string()));
    }

    #[test]
    fn test_titlecase_array() {
        let arr = vec![
            Value::String("hello".to_string()),
            Value::String("WORLD".to_string()),
            Value::Int(42),
        ];
        let result = builtin_titlecase(&[Value::Array(arr)]).unwrap();
        let expected = vec![
            Value::String("Hello".to_string()),
            Value::String("World".to_string()),
            Value::Int(42),
        ];
        assert_eq!(result, Value::Array(expected));
    }

    #[test]
    fn test_titlecase_series() {
        let series = Series::new("test", &["hello", "world", "test"]);
        let result = builtin_titlecase(&[Value::Series(series)]).unwrap();
        if let Value::Series(result_series) = result {
            let expected = Series::new("test", &["Hello", "World", "Test"]);
            assert_eq!(result_series, expected);
        } else {
            panic!("Expected Series result");
        }
    }

    #[test]
    fn test_titlecase_dataframe() {
        let names = Series::new("name", &["alice", "bob"]);
        let ages = Series::new("age", &[25, 30]);
        let df = DataFrame::new(vec![names, ages]).unwrap();
        let result = builtin_titlecase(&[Value::DataFrame(df)]).unwrap();
        if let Value::DataFrame(result_df) = result {
            let expected_names = Series::new("name", &["Alice", "Bob"]);
            let expected_ages = Series::new("age", &[25, 30]);
            let expected_df = DataFrame::new(vec![expected_names, expected_ages]).unwrap();
            assert_eq!(result_df, expected_df);
        } else {
            panic!("Expected DataFrame result");
        }
    }

    #[test]
    fn test_titlecase_non_string_types() {
        let result = builtin_titlecase(&[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(42));

        let result = builtin_titlecase(&[Value::Float(3.14)]).unwrap();
        assert_eq!(result, Value::Float(3.14));
    }

    #[test]
    fn test_titlecase_no_args() {
        let result = builtin_titlecase(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_titlecase_multiple_args() {
        let result = builtin_titlecase(&[
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }
}
