use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_extract_protocol(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "url_extract_protocol() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => match Url::parse(s) {
            Ok(url) => Ok(Value::String(url.scheme().to_string())),
            Err(_) => Ok(Value::Null),
        },
        Value::Array(arr) => {
            let extracted: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => builtin_url_extract_protocol(&[Value::String(s.clone())])
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
                                    "url_extract_protocol() failed to cast series to utf8: {}",
                                    e
                                ))
                            })?
                            .apply(|s| {
                                s.and_then(|s| match Url::parse(s) {
                                    Ok(url) => Some(Cow::Owned(url.scheme().to_string())),
                                    Err(_) => None,
                                })
                            })
                            .into_series();
                        let mut s = extracted_series;
                        s.rename(col_name.clone());
                        new_series.push(s.into());
                    } else {
                        let mut s = series.clone();
                        s.rename(col_name.clone());
                        new_series.push(s.into());
                    }
                }
            }
            match DataFrame::new(new_series) {
                Ok(new_df) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "url_extract_protocol() failed on DataFrame: {}",
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
                            "url_extract_protocol() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.and_then(|s| match Url::parse(s) {
                            Ok(url) => Some(Cow::Owned(url.scheme().to_string())),
                            Err(_) => None,
                        })
                    })
                    .into_series();
                Ok(Value::Series(extracted_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_extract_protocol() requires string, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_extract_protocol",
        func: builtin_url_extract_protocol,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_url_extract_protocol_basic() {
        let result =
            builtin_url_extract_protocol(&[Value::String("https://example.com/path".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("https".to_string()));
    }

    #[test]
    fn test_url_extract_protocol_http() {
        let result =
            builtin_url_extract_protocol(&[Value::String("http://example.com/path".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("http".to_string()));
    }

    #[test]
    fn test_url_extract_protocol_ftp() {
        let result =
            builtin_url_extract_protocol(&[Value::String("ftp://example.com/path".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("ftp".to_string()));
    }

    #[test]
    fn test_url_extract_protocol_with_port() {
        let result = builtin_url_extract_protocol(&[Value::String(
            "https://example.com:8080/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("https".to_string()));
    }

    #[test]
    fn test_url_extract_protocol_with_query() {
        let result = builtin_url_extract_protocol(&[Value::String(
            "https://example.com/path?key=value".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("https".to_string()));
    }

    #[test]
    fn test_url_extract_protocol_with_fragment() {
        let result = builtin_url_extract_protocol(&[Value::String(
            "https://example.com/path#fragment".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("https".to_string()));
    }

    #[test]
    fn test_url_extract_protocol_invalid_url() {
        let result =
            builtin_url_extract_protocol(&[Value::String("not-a-url".to_string())]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_url_extract_protocol_no_protocol() {
        let result =
            builtin_url_extract_protocol(&[Value::String("example.com/path".to_string())]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_url_extract_protocol_array() {
        let arr = vec![
            Value::String("https://example.com/path".to_string()),
            Value::String("http://test.com/path".to_string()),
            Value::String("not-a-url".to_string()),
        ];
        let result = builtin_url_extract_protocol(&[Value::Array(arr)]).unwrap();
        let expected = Value::Array(vec![
            Value::String("https".to_string()),
            Value::String("http".to_string()),
            Value::Null,
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_url_extract_protocol_array_mixed_types() {
        let arr = vec![
            Value::String("https://example.com/path".to_string()),
            Value::Int(123),
            Value::String("ftp://example.com/path".to_string()),
        ];
        let result = builtin_url_extract_protocol(&[Value::Array(arr)]).unwrap();
        let expected = Value::Array(vec![
            Value::String("https".to_string()),
            Value::Null,
            Value::String("ftp".to_string()),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_url_extract_protocol_no_args() {
        let result = builtin_url_extract_protocol(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_url_extract_protocol_too_many_args() {
        let result = builtin_url_extract_protocol(&[
            Value::String("url".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_url_extract_protocol_invalid_type() {
        let result = builtin_url_extract_protocol(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string, array, DataFrame, or Series"));
    }

    #[test]
    fn test_url_extract_protocol_empty_array() {
        let result = builtin_url_extract_protocol(&[Value::Array(vec![])]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Array(vec![]));
    }

    #[test]
    fn test_url_extract_protocol_custom_protocol() {
        let result =
            builtin_url_extract_protocol(&[Value::String("custom://example.com/path".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("custom".to_string()));
    }

    #[test]
    fn test_url_extract_protocol_file_protocol() {
        let result =
            builtin_url_extract_protocol(&[Value::String("file:///path/to/file".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("file".to_string()));
    }

    #[test]
    fn test_url_extract_protocol_data_protocol() {
        let result = builtin_url_extract_protocol(&[Value::String(
            "data:text/plain;base64,SGVsbG8=".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("data".to_string()));
    }
}
