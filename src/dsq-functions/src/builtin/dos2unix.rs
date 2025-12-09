use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

inventory::submit! {
    FunctionRegistration {
        name: "dos2unix",
        func: builtin_dos2unix,
    }
}

pub fn builtin_dos2unix(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "dos2unix() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            let converted = s.replace("\r\n", "\n");
            Ok(Value::String(converted))
        }
        Value::Array(arr) => {
            let converted: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => Ok(Value::String(s.replace("\r\n", "\n"))),
                    _ => Err(dsq_shared::error::operation_error(
                        "dos2unix() requires string elements in array",
                    )),
                })
                .collect();
            Ok(Value::Array(converted?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::String {
                        let converted_series = series
                            .str()
                            .unwrap()
                            .apply(|s| s.map(|s| Cow::Owned(s.replace("\r\n", "\n"))))
                            .into_series();
                        let mut s = converted_series;
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
                    "dos2unix() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let converted_series = series
                    .str()
                    .unwrap()
                    .apply(|s| s.map(|s| Cow::Owned(s.replace("\r\n", "\n"))))
                    .into_series();
                Ok(Value::Series(converted_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "dos2unix() requires string, array, DataFrame, or Series",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_dos2unix_string() {
        let result = builtin_dos2unix(&[Value::String("hello\r\nworld".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("hello\nworld".to_string()));
    }

    #[test]
    fn test_dos2unix_no_crlf() {
        let result = builtin_dos2unix(&[Value::String("hello world".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("hello world".to_string()));
    }

    #[test]
    fn test_dos2unix_multiple_crlf() {
        let result = builtin_dos2unix(&[Value::String("line1\r\nline2\r\nline3".to_string())]);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Value::String("line1\nline2\nline3".to_string())
        );
    }

    #[test]
    fn test_dos2unix_empty_string() {
        let result = builtin_dos2unix(&[Value::String("".to_string())]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("".to_string()));
    }

    #[test]
    fn test_dos2unix_mixed_line_endings() {
        let result =
            builtin_dos2unix(&[Value::String("line1\r\nline2\nline3\r\nline4".to_string())]);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Value::String("line1\nline2\nline3\nline4".to_string())
        );
    }

    #[test]
    fn test_dos2unix_array() {
        let arr = vec![
            Value::String("hello\r\nworld".to_string()),
            Value::String("foo\r\nbar".to_string()),
        ];
        let result = builtin_dos2unix(&[Value::Array(arr)]);
        assert!(result.is_ok());
        if let Value::Array(result_arr) = result.unwrap() {
            assert_eq!(result_arr.len(), 2);
            assert_eq!(result_arr[0], Value::String("hello\nworld".to_string()));
            assert_eq!(result_arr[1], Value::String("foo\nbar".to_string()));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_dos2unix_array_non_string() {
        let arr = vec![Value::String("test".to_string()), Value::Int(42)];
        let result = builtin_dos2unix(&[Value::Array(arr)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string elements in array"));
    }

    #[test]
    fn test_dos2unix_no_args() {
        let result = builtin_dos2unix(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("dos2unix() expects 1 argument"));
    }

    #[test]
    fn test_dos2unix_too_many_args() {
        let result = builtin_dos2unix(&[
            Value::String("test".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("dos2unix() expects 1 argument"));
    }

    #[test]
    fn test_dos2unix_non_supported_type() {
        let result = builtin_dos2unix(&[Value::Int(42)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("dos2unix() requires string, array, DataFrame, or Series"));
    }

    #[test]
    fn test_dos2unix_registered_via_inventory() {
        // Test that the function is registered via inventory
        let mut found = false;
        for func in inventory::iter::<FunctionRegistration> {
            if func.name == "dos2unix" {
                found = true;
                // Test that calling the function works
                let result = (func.func)(&[Value::String("test\r\nline".to_string())]);
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), Value::String("test\nline".to_string()));
                break;
            }
        }
        assert!(found, "dos2unix function not found in inventory");
    }

    #[test]
    fn test_dos2unix_dataframe() {
        // Create a test DataFrame with string columns
        let names = Series::new(PlSmallStr::from("name"), &["Alice", "Bob", "Charlie"]).into();
        let descriptions = Series::new(
            PlSmallStr::from("description"),
            &["Line1\r\nLine2", "Single line", "Another\r\nline"],
        )
        .into();
        let df = DataFrame::new(vec![names, descriptions]).unwrap();

        let result = builtin_dos2unix(&[Value::DataFrame(df)]);
        assert!(result.is_ok());

        if let Value::DataFrame(result_df) = result.unwrap() {
            // Check that the DataFrame has the same structure
            assert_eq!(result_df.height(), 3);
            assert_eq!(result_df.width(), 2);

            // Check the converted description column
            let desc_series = result_df.column("description").unwrap();
            let desc_chunked = desc_series.str().unwrap();

            assert_eq!(desc_chunked.get(0).unwrap(), "Line1\nLine2");
            assert_eq!(desc_chunked.get(1).unwrap(), "Single line");
            assert_eq!(desc_chunked.get(2).unwrap(), "Another\nline");
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_dos2unix_series() {
        // Create a test Series with strings containing CRLF
        let series = Series::new(
            PlSmallStr::from("test"),
            &["line1\r\nline2", "no crlf", "another\r\nline"],
        );

        let result = builtin_dos2unix(&[Value::Series(series.clone())]);
        assert!(result.is_ok());

        if let Value::Series(result_series) = result.unwrap() {
            assert_eq!(result_series.dtype(), &DataType::String);

            let chunked = result_series.str().unwrap();
            assert_eq!(chunked.get(0).unwrap(), "line1\nline2");
            assert_eq!(chunked.get(1).unwrap(), "no crlf");
            assert_eq!(chunked.get(2).unwrap(), "another\nline");
        } else {
            panic!("Expected Series");
        }
    }

    #[test]
    fn test_dos2unix_series_non_utf8() {
        // Create a test Series with integers (should remain unchanged)
        let series = Series::new(PlSmallStr::from("numbers"), &[1, 2, 3]);

        let result = builtin_dos2unix(&[Value::Series(series.clone())]);
        assert!(result.is_ok());

        if let Value::Series(result_series) = result.unwrap() {
            assert_eq!(result_series, series);
        } else {
            panic!("Expected Series");
        }
    }
}
