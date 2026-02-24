use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_set_protocol(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "url_set_protocol() expects 2 arguments",
        ));
    }

    let protocol = match &args[1] {
        Value::String(s) => s.clone(),
        _ => {
            return Err(dsq_shared::error::operation_error(
                "url_set_protocol() second argument must be a string",
            ));
        }
    };

    match &args[0] {
        Value::String(s) => {
            match Url::parse(s) {
                Ok(mut url) => {
                    url.set_scheme(&protocol)
                        .map_err(|_| dsq_shared::error::operation_error("Invalid protocol"))?;
                    Ok(Value::String(url.to_string()))
                }
                Err(_) => Ok(Value::String(s.clone())), // Return original if not a valid URL
            }
        }
        Value::Array(arr) => {
            let set: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => builtin_url_set_protocol(&[
                        Value::String(s.clone()),
                        Value::String(protocol.clone()),
                    ]),
                    _ => Err(dsq_shared::error::operation_error(
                        "url_set_protocol() requires string elements in array",
                    )),
                })
                .collect();
            Ok(Value::Array(set?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::String {
                        let set_series = series
                            .str()
                            .map_err(|e| {
                                dsq_shared::error::operation_error(format!(
                                    "url_set_protocol() failed to cast series to utf8: {}",
                                    e
                                ))
                            })?
                            .apply(|s| {
                                s.and_then(|s| match Url::parse(s) {
                                    Ok(mut url) => {
                                        url.set_scheme(&protocol).ok()?;
                                        Some(Cow::Owned(url.to_string()))
                                    }
                                    Err(_) => Some(Cow::Owned(s.to_string())),
                                })
                            })
                            .into_series();
                        let mut s = set_series;
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
                    "url_set_protocol() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let set_series = series
                    .str()
                    .map_err(|e| {
                        dsq_shared::error::operation_error(format!(
                            "url_set_protocol() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.and_then(|s| match Url::parse(s) {
                            Ok(mut url) => {
                                url.set_scheme(&protocol).ok()?;
                                Some(Cow::Owned(url.to_string()))
                            }
                            Err(_) => Some(Cow::Owned(s.to_string())),
                        })
                    })
                    .into_series();
                Ok(Value::Series(set_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        Value::LazyFrame(lf) => {
            // Collect the LazyFrame to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_url_set_protocol(&[Value::DataFrame(df), args[1].clone()])
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_set_protocol() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_set_protocol",
        func: builtin_url_set_protocol,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_url_set_protocol_basic_https_to_http() {
        let result = builtin_url_set_protocol(&[
            Value::String("https://example.com/path".to_string()),
            Value::String("http".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("http://example.com/path".to_string()));
    }

    #[test]
    fn test_url_set_protocol_basic_http_to_https() {
        let result = builtin_url_set_protocol(&[
            Value::String("http://example.com/path".to_string()),
            Value::String("https".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path".to_string())
        );
    }

    #[test]
    fn test_url_set_protocol_with_port() {
        let result = builtin_url_set_protocol(&[
            Value::String("https://example.com:8080/path".to_string()),
            Value::String("http".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::String("http://example.com:8080/path".to_string())
        );
    }

    #[test]
    fn test_url_set_protocol_with_query() {
        let result = builtin_url_set_protocol(&[
            Value::String("https://example.com/path?key=value".to_string()),
            Value::String("http".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::String("http://example.com/path?key=value".to_string())
        );
    }

    #[test]
    fn test_url_set_protocol_with_fragment() {
        let result = builtin_url_set_protocol(&[
            Value::String("https://example.com/path#fragment".to_string()),
            Value::String("http".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::String("http://example.com/path#fragment".to_string())
        );
    }

    #[test]
    fn test_url_set_protocol_invalid_url() {
        let result = builtin_url_set_protocol(&[
            Value::String("not-a-url".to_string()),
            Value::String("https".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("not-a-url".to_string()));
    }

    #[test]
    fn test_url_set_protocol_invalid_protocol() {
        let result = builtin_url_set_protocol(&[
            Value::String("https://example.com/path".to_string()),
            Value::String("invalid-protocol".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid protocol"));
    }

    #[test]
    fn test_url_set_protocol_array() {
        let arr = vec![
            Value::String("https://example.com/path".to_string()),
            Value::String("http://test.com/path".to_string()),
            Value::String("not-a-url".to_string()),
        ];
        let result =
            builtin_url_set_protocol(&[Value::Array(arr), Value::String("ftp".to_string())])
                .unwrap();
        let expected = Value::Array(vec![
            Value::String("ftp://example.com/path".to_string()),
            Value::String("ftp://test.com/path".to_string()),
            Value::String("not-a-url".to_string()),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_url_set_protocol_array_mixed_types() {
        let arr = vec![
            Value::String("https://example.com/path".to_string()),
            Value::Int(42),
        ];
        let result =
            builtin_url_set_protocol(&[Value::Array(arr), Value::String("http".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string elements in array"));
    }

    #[test]
    fn test_url_set_protocol_wrong_arg_count() {
        let result = builtin_url_set_protocol(&[Value::String("https://example.com".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));
    }

    #[test]
    fn test_url_set_protocol_second_arg_not_string() {
        let result = builtin_url_set_protocol(&[
            Value::String("https://example.com".to_string()),
            Value::Int(42),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("second argument must be a string"));
    }

    #[test]
    fn test_url_set_protocol_unsupported_type() {
        let result =
            builtin_url_set_protocol(&[Value::Int(42), Value::String("https".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string, array, DataFrame, Series, or LazyFrame"));
    }

    #[test]
    fn test_url_set_protocol_invalid_scheme() {
        let result = builtin_url_set_protocol(&[
            Value::String("https://example.com/path".to_string()),
            Value::String("invalid-scheme".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid protocol"));
    }
}
