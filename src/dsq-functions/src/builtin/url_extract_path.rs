use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_extract_path(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "url_extract_path() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => match Url::parse(s) {
            Ok(url) => Ok(Value::String(url.path().to_string())),
            Err(_) => Ok(Value::Null),
        },
        Value::Array(arr) => {
            let extracted: Vec<Value> =
                arr.iter()
                    .map(|v| match v {
                        Value::String(s) => builtin_url_extract_path(&[Value::String(s.clone())])
                            .unwrap_or(Value::Null),
                        _ => Value::Null,
                    })
                    .collect();
            Ok(Value::Array(extracted))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::String {
                        let extracted_series = series
                            .str()
                            .map_err(|e| {
                                dsq_shared::error::operation_error(format!(
                                    "url_extract_path() failed to cast series to utf8: {}",
                                    e
                                ))
                            })?
                            .apply(|s| {
                                s.map(|s| match Url::parse(s) {
                                    Ok(url) => Cow::Owned(url.path().to_string()),
                                    Err(_) => Cow::Owned("".to_string()),
                                })
                            })
                            .into_series();
                        let mut s = extracted_series;
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
                    "url_extract_path() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let extracted_series = series
                    .str()
                    .map_err(|e| {
                        dsq_shared::error::operation_error(format!(
                            "url_extract_path() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.map(|s| match Url::parse(s) {
                            Ok(url) => Cow::Owned(url.path().to_string()),
                            Err(_) => Cow::Owned("".to_string()),
                        })
                    })
                    .into_series();
                Ok(Value::Series(extracted_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_extract_path() requires string, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_extract_path",
        func: builtin_url_extract_path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_url_extract_path_basic() {
        let result = builtin_url_extract_path(&[Value::String(
            "https://example.com/path/to/resource".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("/path/to/resource".to_string()));
    }

    #[test]
    fn test_url_extract_path_root() {
        let result =
            builtin_url_extract_path(&[Value::String("https://example.com/".to_string())]).unwrap();
        assert_eq!(result, Value::String("/".to_string()));
    }

    #[test]
    fn test_url_extract_path_no_path() {
        let result =
            builtin_url_extract_path(&[Value::String("https://example.com".to_string())]).unwrap();
        assert_eq!(result, Value::String("/".to_string()));
    }

    #[test]
    fn test_url_extract_path_invalid_url() {
        let result = builtin_url_extract_path(&[Value::String("not a url".to_string())]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_url_extract_path_array() {
        let arr = vec![
            Value::String("https://example.com/path1".to_string()),
            Value::String("https://example.com/path2".to_string()),
            Value::String("invalid".to_string()),
        ];
        let result = builtin_url_extract_path(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(results) => {
                assert_eq!(results.len(), 3);
                assert_eq!(results[0], Value::String("/path1".to_string()));
                assert_eq!(results[1], Value::String("/path2".to_string()));
                assert_eq!(results[2], Value::Null);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_url_extract_path_dataframe() -> Result<()> {
        let urls = Series::new(
            "url".into(),
            &[
                "https://example.com/path1",
                "https://example.com/path2",
                "invalid",
            ],
        );
        let df = DataFrame::new(vec![urls.into()]).unwrap();
        let result = builtin_url_extract_path(&[Value::DataFrame(df)]).unwrap();
        match result {
            Value::DataFrame(result_df) => {
                let series = result_df.column("url").unwrap();
                let values: Vec<String> = series
                    .str()
                    .map_err(|e| {
                        dsq_shared::error::operation_error(format!(
                            "url_extract_path() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .into_iter()
                    .map(|s| s.unwrap_or("").to_string())
                    .collect();
                assert_eq!(values, vec!["/path1", "/path2", ""]);
            }
            _ => panic!("Expected DataFrame"),
        }
        Ok(())
    }

    #[test]
    fn test_url_extract_path_series() -> Result<()> {
        let series = Series::new(
            "urls".into(),
            &["https://example.com/path1", "https://example.com/path2"],
        );
        let result = builtin_url_extract_path(&[Value::Series(series)]).unwrap();
        match result {
            Value::Series(result_series) => {
                let values: Vec<String> = result_series
                    .str()
                    .map_err(|e| {
                        dsq_shared::error::operation_error(format!(
                            "url_extract_path() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .into_iter()
                    .map(|s| s.unwrap_or("").to_string())
                    .collect();
                assert_eq!(values, vec!["/path1", "/path2"]);
            }
            _ => panic!("Expected Series"),
        }
        Ok(())
    }

    #[test]
    fn test_url_extract_path_wrong_args() {
        let result = builtin_url_extract_path(&[]);
        assert!(result.is_err());
        let result = builtin_url_extract_path(&[
            Value::String("test".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_url_extract_path_wrong_type() {
        let result = builtin_url_extract_path(&[Value::Int(42)]);
        assert!(result.is_err());
    }
}
