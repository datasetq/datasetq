use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_extract_query_string(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "url_extract_query_string() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => match Url::parse(s) {
            Ok(url) => Ok(url
                .query()
                .map(|q| Value::String(q.to_string()))
                .unwrap_or(Value::Null)),
            Err(_) => Ok(Value::Null),
        },
        Value::Array(arr) => {
            let extracted: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => {
                        builtin_url_extract_query_string(&[Value::String(s.clone())])
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
                    if series.dtype() == &DataType::Utf8 {
                        let extracted_series = series
                            .utf8()
                            .map_err(|e| {
                                dsq_shared::error::operation_error(format!(
                                    "url_extract_query_string() failed to cast series to utf8: {}",
                                    e
                                ))
                            })?
                            .apply(|s| {
                                s.and_then(|s| match Url::parse(s) {
                                    Ok(url) => url.query().map(|q| Cow::Owned(q.to_string())),
                                    Err(_) => None,
                                })
                            })
                            .into_series();
                        let mut s = extracted_series;
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
                    "url_extract_query_string() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::Utf8 {
                let extracted_series = series
                    .utf8()
                    .map_err(|e| {
                        dsq_shared::error::operation_error(format!(
                            "url_extract_query_string() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.and_then(|s| match Url::parse(s) {
                            Ok(url) => url.query().map(|q| Cow::Owned(q.to_string())),
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
            "url_extract_query_string() requires string, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_extract_query_string",
        func: builtin_url_extract_query_string,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_url_extract_query_string_basic() {
        let result = builtin_url_extract_query_string(&[Value::String(
            "https://example.com/path?key=value&foo=bar".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("key=value&foo=bar".to_string()));
    }

    #[test]
    fn test_url_extract_query_string_no_query() {
        let result = builtin_url_extract_query_string(&[Value::String(
            "https://example.com/path".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_url_extract_query_string_empty_query() {
        let result = builtin_url_extract_query_string(&[Value::String(
            "https://example.com/path?".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_url_extract_query_string_invalid_url() {
        let result =
            builtin_url_extract_query_string(&[Value::String("not-a-url".to_string())]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_url_extract_query_string_with_fragment() {
        let result = builtin_url_extract_query_string(&[Value::String(
            "https://example.com/path?key=value#fragment".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("key=value".to_string()));
    }

    #[test]
    fn test_url_extract_query_string_special_chars() {
        let result = builtin_url_extract_query_string(&[Value::String(
            "https://example.com/path?q=hello%20world&special=%40%23%24".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("q=hello%20world&special=%40%23%24".to_string())
        );
    }

    #[test]
    fn test_url_extract_query_string_array() {
        let arr = vec![
            Value::String("https://example.com/path?key1=value1".to_string()),
            Value::String("https://example.com/path?key2=value2".to_string()),
            Value::String("not-a-url".to_string()),
        ];
        let result = builtin_url_extract_query_string(&[Value::Array(arr)]).unwrap();
        let expected = Value::Array(vec![
            Value::String("key1=value1".to_string()),
            Value::String("key2=value2".to_string()),
            Value::Null,
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_url_extract_query_string_array_mixed_types() {
        let arr = vec![
            Value::String("https://example.com/path?key=value".to_string()),
            Value::Int(123),
            Value::String("https://example.com/path?foo=bar".to_string()),
        ];
        let result = builtin_url_extract_query_string(&[Value::Array(arr)]).unwrap();
        let expected = Value::Array(vec![
            Value::String("key=value".to_string()),
            Value::Null,
            Value::String("foo=bar".to_string()),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_url_extract_query_string_no_args() {
        let result = builtin_url_extract_query_string(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_url_extract_query_string_too_many_args() {
        let result = builtin_url_extract_query_string(&[
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
    fn test_url_extract_query_string_invalid_type() {
        let result = builtin_url_extract_query_string(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string, array, DataFrame, or Series"));
    }

    #[test]
    fn test_url_extract_query_string_empty_array() {
        let result = builtin_url_extract_query_string(&[Value::Array(vec![])]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Array(vec![]));
    }

    #[test]
    fn test_url_extract_query_string_complex_query() {
        let result = builtin_url_extract_query_string(&[Value::String(
            "https://example.com/search?q=rust+programming&sort=date&filter=recent&page=1"
                .to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("q=rust+programming&sort=date&filter=recent&page=1".to_string())
        );
    }

    #[test]
    fn test_url_extract_query_string_url_with_port() {
        let result = builtin_url_extract_query_string(&[Value::String(
            "https://example.com:8080/path?key=value".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("key=value".to_string()));
    }

    #[test]
    fn test_url_extract_query_string_url_with_user_pass() {
        let result = builtin_url_extract_query_string(&[Value::String(
            "https://user:pass@example.com/path?key=value".to_string(),
        )])
        .unwrap();
        assert_eq!(result, Value::String("key=value".to_string()));
    }
}
