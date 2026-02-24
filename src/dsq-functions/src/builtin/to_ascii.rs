use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;

pub fn builtin_to_ascii(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "to_ascii() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            let ascii_codes: Vec<String> = s.chars().map(|c| (c as u32).to_string()).collect();
            Ok(Value::String(ascii_codes.join(" ")))
        }
        Value::Array(arr) => {
            let mut result = Vec::new();
            for val in arr {
                match val {
                    Value::String(s) => {
                        let ascii_codes: Vec<String> =
                            s.chars().map(|c| (c as u32).to_string()).collect();
                        result.push(Value::String(ascii_codes.join(" ")));
                    }
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "to_ascii() requires string elements in array",
                        ));
                    }
                }
            }
            Ok(Value::Array(result))
        }
        Value::DataFrame(df) => {
            // Apply to_ascii to string columns
            let mut new_df = df.clone();
            let col_names: Vec<String> = new_df
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            for col in col_names {
                if let Ok(series) = new_df.column(&col) {
                    if series.dtype() == &DataType::String {
                        let new_series = series
                            .str()
                            .unwrap()
                            .into_iter()
                            .map(|opt_s| {
                                opt_s.map(|s| {
                                    let ascii_codes: Vec<String> =
                                        s.chars().map(|c| (c as u32).to_string()).collect();
                                    ascii_codes.join(" ")
                                })
                            })
                            .collect::<StringChunked>()
                            .into_series();
                        let _ = new_df.replace(&col, new_series);
                    }
                }
            }
            Ok(Value::DataFrame(new_df))
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let new_series = series
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|opt_s| {
                        opt_s.map(|s| {
                            let ascii_codes: Vec<String> =
                                s.chars().map(|c| (c as u32).to_string()).collect();
                            ascii_codes.join(" ")
                        })
                    })
                    .collect::<StringChunked>()
                    .into_series();
                Ok(Value::Series(new_series))
            } else {
                Err(dsq_shared::error::operation_error(
                    "to_ascii() requires string Series",
                ))
            }
        }
        Value::LazyFrame(lf) => {
            // Collect the LazyFrame to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_to_ascii(&[Value::DataFrame(df)])
        }
        _ => Err(dsq_shared::error::operation_error(
            "to_ascii() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "to_ascii",
        func: builtin_to_ascii,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_to_ascii_string() {
        let result = builtin_to_ascii(&[Value::String("abc".to_string())]).unwrap();
        assert_eq!(result, Value::String("97 98 99".to_string()));
    }

    #[test]
    fn test_to_ascii_empty_string() {
        let result = builtin_to_ascii(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_to_ascii_unicode() {
        let result = builtin_to_ascii(&[Value::String("José".to_string())]).unwrap();
        assert_eq!(result, Value::String("74 111 115 233".to_string()));
    }

    #[test]
    fn test_to_ascii_array() {
        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ];
        let result = builtin_to_ascii(&[Value::Array(arr)]).unwrap();
        let expected = vec![
            Value::String("97".to_string()),
            Value::String("98".to_string()),
        ];
        assert_eq!(result, Value::Array(expected));
    }

    #[test]
    fn test_to_ascii_dataframe() {
        let df = DataFrame::new(vec![
            Series::new(
                PlSmallStr::from("text"),
                vec!["hello".to_string(), "world".to_string()],
            )
            .into(),
            Series::new(PlSmallStr::from("number"), vec![1, 2]).into(),
        ])
        .unwrap();
        let result = builtin_to_ascii(&[Value::DataFrame(df.clone())]).unwrap();
        if let Value::DataFrame(new_df) = result {
            let text_series = new_df.column("text").unwrap().str().unwrap();
            assert_eq!(text_series.get(0).unwrap(), "104 101 108 108 111");
            assert_eq!(text_series.get(1).unwrap(), "119 111 114 108 100");
            // number column should remain unchanged
            let number_series = new_df.column("number").unwrap();
            assert_eq!(number_series.dtype(), &DataType::Int32);
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_to_ascii_series() {
        let series = Series::new(PlSmallStr::from("test"), vec!["hi", "there"]);
        let result = builtin_to_ascii(&[Value::Series(series)]).unwrap();
        if let Value::Series(new_series) = result {
            let utf8_series = new_series.str().unwrap();
            assert_eq!(utf8_series.get(0).unwrap(), "104 105");
            assert_eq!(utf8_series.get(1).unwrap(), "116 104 101 114 101");
        } else {
            panic!("Expected Series");
        }
    }

    #[test]
    fn test_to_ascii_no_args() {
        let result = builtin_to_ascii(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_to_ascii_too_many_args() {
        let result = builtin_to_ascii(&[
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_to_ascii_invalid_array_element() {
        let arr = vec![Value::String("a".to_string()), Value::Int(1)];
        let result = builtin_to_ascii(&[Value::Array(arr)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string elements in array"));
    }

    #[test]
    fn test_to_ascii_invalid_type() {
        let result = builtin_to_ascii(&[Value::Int(42)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string, array, DataFrame, Series, or LazyFrame"));
    }

    #[test]
    fn test_to_ascii_non_string_series() {
        let series = Series::new("numbers".into(), &[1, 2, 3]);
        let result = builtin_to_ascii(&[Value::Series(series)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string Series"));
    }
}
