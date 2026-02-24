use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_extract_domain_without_www(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "url_extract_domain_without_www() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => match Url::parse(s) {
            Ok(url) => {
                let domain = url.host_str().unwrap_or("").to_string();
                let domain = domain
                    .strip_prefix("www.")
                    .map(|s| s.to_string())
                    .unwrap_or(domain);
                Ok(Value::String(domain))
            }
            Err(_) => Ok(Value::String("".to_string())),
        },
        Value::Array(arr) => {
            let extracted: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    Value::String(_) => {
                        builtin_url_extract_domain_without_www(std::slice::from_ref(v))
                            .unwrap_or(Value::Null)
                    }
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
                        let domain_series = series
                            .str().map_err(|e| {
                            dsq_shared::error::operation_error(format!(
                                "url_extract_domain_without_www() failed to cast series to utf8: {}",
                                e
                            ))
                        })?
                            .apply(|s| {
                                s.map(|s| match Url::parse(s) {
                                    Ok(url) => {
                                        let domain = url.host_str().unwrap_or("").to_string();
                                        let domain = domain
                                            .strip_prefix("www.")
                                            .map(|s| s.to_string())
                                            .unwrap_or(domain);
                                        Cow::Owned(domain)
                                    }
                                    Err(_) => Cow::Owned("".to_string()),
                                })
                            })
                            .into_series();
                        let mut s = domain_series;
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
                    "url_extract_domain_without_www() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let domain_series = series
                    .str()
                    .map_err(|e| {
                        dsq_shared::error::operation_error(format!(
                            "url_extract_domain_without_www() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.map(|s| match Url::parse(s) {
                            Ok(url) => {
                                let domain = url.host_str().unwrap_or("").to_string();
                                let domain = domain
                                    .strip_prefix("www.")
                                    .map(|s| s.to_string())
                                    .unwrap_or(domain);
                                Cow::Owned(domain)
                            }
                            Err(_) => Cow::Owned("".to_string()),
                        })
                    })
                    .into_series();
                Ok(Value::Series(domain_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        Value::LazyFrame(lf) => {
            // Collect the LazyFrame to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_url_extract_domain_without_www(&[Value::DataFrame(df)])
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_extract_domain_without_www() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_extract_domain_without_www",
        func: builtin_url_extract_domain_without_www,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_url_extract_domain_without_www_basic() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "https://www.example.com/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("example.com".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_no_www() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "https://example.com/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("example.com".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_with_port() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "https://www.example.com:8080/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("example.com".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_with_query() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "https://www.example.com/path?key=value".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("example.com".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_with_fragment() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "https://www.example.com/path#fragment".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("example.com".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_invalid_url() {
        let result =
            builtin_url_extract_domain_without_www(&[Value::String("not-a-url".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_no_protocol() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "www.example.com/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_subdomain() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "https://sub.www.example.com/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("sub.www.example.com".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_ip_address() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "https://192.168.1.1/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("192.168.1.1".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_localhost() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "http://localhost:3000/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("localhost".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_array() {
        let arr = vec![
            Value::String("https://www.example.com/path".to_string()),
            Value::String("http://test.com/path".to_string()),
            Value::String("not-a-url".to_string()),
        ];
        let result = builtin_url_extract_domain_without_www(&[Value::Array(arr)]).unwrap();
        let expected = Value::Array(vec![
            Value::String("example.com".to_string()),
            Value::String("test.com".to_string()),
            Value::String("".to_string()),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_url_extract_domain_without_www_array_mixed_types() {
        let arr = vec![
            Value::String("https://www.example.com/path".to_string()),
            Value::Int(123),
            Value::String("ftp://example.com/path".to_string()),
        ];
        let result = builtin_url_extract_domain_without_www(&[Value::Array(arr)]).unwrap();
        let expected = Value::Array(vec![
            Value::String("example.com".to_string()),
            Value::Null,
            Value::String("example.com".to_string()),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_url_extract_domain_without_www_no_args() {
        let result = builtin_url_extract_domain_without_www(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_url_extract_domain_without_www_too_many_args() {
        let result = builtin_url_extract_domain_without_www(&[
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
    fn test_url_extract_domain_without_www_invalid_type() {
        let result = builtin_url_extract_domain_without_www(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string, array, DataFrame, Series, or LazyFrame"));
    }

    #[test]
    fn test_url_extract_domain_without_www_empty_array() {
        let result = builtin_url_extract_domain_without_www(&[Value::Array(vec![])]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Array(vec![]));
    }

    #[test]
    fn test_url_extract_domain_without_www_custom_domain() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "https://my.custom.domain.com/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("my.custom.domain.com".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_www_subdomain() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "https://www.sub.example.com/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("sub.example.com".to_string()));
    }

    #[test]
    fn test_url_extract_domain_without_www_multiple_www() {
        let result = builtin_url_extract_domain_without_www(&[Value::String(
            "https://www.www.example.com/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("www.example.com".to_string()));
    }
}
