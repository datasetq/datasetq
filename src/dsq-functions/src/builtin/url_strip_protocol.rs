use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_strip_protocol(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "url_strip_protocol() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            match Url::parse(s) {
                Ok(url) => {
                    let mut result = String::new();
                    if let Some(host) = url.host_str() {
                        result.push_str("//");
                        result.push_str(host);
                        if let Some(port) = url.port() {
                            result.push(':');
                            result.push_str(&port.to_string());
                        }
                    }
                    result.push_str(url.path());
                    if let Some(query) = url.query() {
                        result.push('?');
                        result.push_str(query);
                    }
                    if let Some(fragment) = url.fragment() {
                        result.push('#');
                        result.push_str(fragment);
                    }
                    Ok(Value::String(result))
                }
                Err(_) => Ok(Value::String(s.clone())), // Return original if not a valid URL
            }
        }
        Value::Array(arr) => {
            let stripped: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => builtin_url_strip_protocol(&[Value::String(s.clone())]),
                    _ => Err(dsq_shared::error::operation_error(
                        "url_strip_protocol() requires string elements in array",
                    )),
                })
                .collect();
            Ok(Value::Array(stripped?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::Utf8 {
                        let stripped_series = series
                            .utf8()
                            .map_err(|e| {
                                dsq_shared::error::operation_error(format!(
                                    "url_strip_protocol() failed to cast series to utf8: {}",
                                    e
                                ))
                            })?
                            .apply(|s| {
                                s.and_then(|s| match Url::parse(s) {
                                    Ok(url) => {
                                        let mut result = String::new();
                                        if let Some(host) = url.host_str() {
                                            result.push_str("//");
                                            result.push_str(host);
                                            if let Some(port) = url.port() {
                                                result.push(':');
                                                result.push_str(&port.to_string());
                                            }
                                        }
                                        result.push_str(url.path());
                                        if let Some(query) = url.query() {
                                            result.push('?');
                                            result.push_str(query);
                                        }
                                        if let Some(fragment) = url.fragment() {
                                            result.push('#');
                                            result.push_str(fragment);
                                        }
                                        Some(Cow::Owned(result))
                                    }
                                    Err(_) => Some(Cow::Owned(s.to_string())),
                                })
                            })
                            .into_series();
                        let mut s = stripped_series;
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
                    "url_strip_protocol() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::Utf8 {
                let stripped_series = series
                    .utf8()
                    .map_err(|e| {
                        dsq_shared::error::operation_error(format!(
                            "url_strip_protocol() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.and_then(|s| match Url::parse(s) {
                            Ok(url) => {
                                let mut result = String::new();
                                if let Some(host) = url.host_str() {
                                    result.push_str("//");
                                    result.push_str(host);
                                    if let Some(port) = url.port() {
                                        result.push(':');
                                        result.push_str(&port.to_string());
                                    }
                                }
                                result.push_str(url.path());
                                if let Some(query) = url.query() {
                                    result.push('?');
                                    result.push_str(query);
                                }
                                if let Some(fragment) = url.fragment() {
                                    result.push('#');
                                    result.push_str(fragment);
                                }
                                Some(Cow::Owned(result))
                            }
                            Err(_) => Some(Cow::Owned(s.to_string())),
                        })
                    })
                    .into_series();
                Ok(Value::Series(stripped_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_strip_protocol() requires string, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_strip_protocol",
        func: builtin_url_strip_protocol,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_url_strip_protocol_basic() {
        let result = builtin_url_strip_protocol(&[Value::String(
            "https://www.example.com/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("//www.example.com/path".to_string()));
    }

    #[test]
    fn test_url_strip_protocol_with_port() {
        let result = builtin_url_strip_protocol(&[Value::String(
            "https://example.com:8080/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("//example.com:8080/path".to_string()));
    }

    #[test]
    fn test_url_strip_protocol_with_query() {
        let result = builtin_url_strip_protocol(&[Value::String(
            "https://example.com/path?key=value".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("//example.com/path?key=value".to_string())
        );
    }

    #[test]
    fn test_url_strip_protocol_with_fragment() {
        let result = builtin_url_strip_protocol(&[Value::String(
            "https://example.com/path#fragment".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("//example.com/path#fragment".to_string())
        );
    }

    #[test]
    fn test_url_strip_protocol_with_all() {
        let result = builtin_url_strip_protocol(&[Value::String(
            "https://example.com:8080/path?key=value#fragment".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("//example.com:8080/path?key=value#fragment".to_string())
        );
    }

    #[test]
    fn test_url_strip_protocol_http() {
        let result =
            builtin_url_strip_protocol(&[Value::String("http://example.com/path".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("//example.com/path".to_string()));
    }

    #[test]
    fn test_url_strip_protocol_ftp() {
        let result =
            builtin_url_strip_protocol(&[Value::String("ftp://example.com/path".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("//example.com/path".to_string()));
    }

    #[test]
    fn test_url_strip_protocol_no_path() {
        let result =
            builtin_url_strip_protocol(&[Value::String("https://example.com".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("//example.com/".to_string()));
    }

    #[test]
    fn test_url_strip_protocol_root_path() {
        let result =
            builtin_url_strip_protocol(&[Value::String("https://example.com/".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("//example.com/".to_string()));
    }

    #[test]
    fn test_url_strip_protocol_invalid_url() {
        let result = builtin_url_strip_protocol(&[Value::String("not-a-url".to_string())]).unwrap();
        assert_eq!(result, Value::String("not-a-url".to_string()));
    }

    #[test]
    fn test_url_strip_protocol_no_protocol() {
        let result =
            builtin_url_strip_protocol(&[Value::String("www.example.com/path".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("www.example.com/path".to_string()));
    }

    #[test]
    fn test_url_strip_protocol_ip_address() {
        let result =
            builtin_url_strip_protocol(&[Value::String("https://192.168.1.1/path".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("//192.168.1.1/path".to_string()));
    }

    #[test]
    fn test_url_strip_protocol_localhost() {
        let result =
            builtin_url_strip_protocol(&[Value::String("http://localhost:3000/path".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("//localhost:3000/path".to_string()));
    }

    #[test]
    fn test_url_strip_protocol_array() {
        let arr = vec![
            Value::String("https://example.com/path".to_string()),
            Value::String("http://test.com/path".to_string()),
            Value::String("not-a-url".to_string()),
        ];
        let result = builtin_url_strip_protocol(&[Value::Array(arr)]).unwrap();
        let expected = Value::Array(vec![
            Value::String("//example.com/path".to_string()),
            Value::String("//test.com/path".to_string()),
            Value::String("not-a-url".to_string()),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_url_strip_protocol_array_mixed_types() {
        let arr = vec![
            Value::String("https://example.com/path".to_string()),
            Value::Int(123),
            Value::String("ftp://example.com/path".to_string()),
        ];
        let result = builtin_url_strip_protocol(&[Value::Array(arr)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string elements in array"));
    }

    #[test]
    fn test_url_strip_protocol_no_args() {
        let result = builtin_url_strip_protocol(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_url_strip_protocol_too_many_args() {
        let result = builtin_url_strip_protocol(&[
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
    fn test_url_strip_protocol_invalid_type() {
        let result = builtin_url_strip_protocol(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string, array, DataFrame, or Series"));
    }

    #[test]
    fn test_url_strip_protocol_empty_array() {
        let result = builtin_url_strip_protocol(&[Value::Array(vec![])]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Array(vec![]));
    }

    #[test]
    fn test_url_strip_protocol_custom_domain() {
        let result = builtin_url_strip_protocol(&[Value::String(
            "https://my.custom.domain.com/path".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("//my.custom.domain.com/path".to_string())
        );
    }

    #[test]
    fn test_url_strip_protocol_subdomain() {
        let result = builtin_url_strip_protocol(&[Value::String(
            "https://sub.example.com/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("//sub.example.com/path".to_string()));
    }

    #[test]
    fn test_url_strip_protocol_complex_path() {
        let result = builtin_url_strip_protocol(&[Value::String(
            "https://example.com/api/v1/users/123".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("//example.com/api/v1/users/123".to_string())
        );
    }

    #[test]
    fn test_url_strip_protocol_complex_query() {
        let result = builtin_url_strip_protocol(&[Value::String(
            "https://example.com/path?key1=value1&key2=value2".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("//example.com/path?key1=value1&key2=value2".to_string())
        );
    }

    #[test]
    fn test_url_strip_protocol_encoded_chars() {
        let result = builtin_url_strip_protocol(&[Value::String(
            "https://example.com/path%20with%20spaces".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("//example.com/path%20with%20spaces".to_string())
        );
    }
}
