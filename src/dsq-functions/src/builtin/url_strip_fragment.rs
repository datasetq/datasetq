use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_strip_fragment(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "url_strip_fragment() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            match Url::parse(s) {
                Ok(mut url) => {
                    url.set_fragment(None);
                    Ok(Value::String(url.to_string()))
                }
                Err(_) => {
                    // If it's not a valid URL, try simple string manipulation
                    if let Some(pos) = s.find('#') {
                        Ok(Value::String(s[..pos].to_string()))
                    } else {
                        Ok(Value::String(s.clone()))
                    }
                }
            }
        }
        Value::Array(arr) => {
            let stripped: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => builtin_url_strip_fragment(&[Value::String(s.clone())]),
                    _ => Err(dsq_shared::error::operation_error(
                        "url_strip_fragment() requires string elements in array",
                    )),
                })
                .collect();
            Ok(Value::Array(stripped?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::String {
                        let stripped_series = series
                            .str()
                            .map_err(|e| {
                                dsq_shared::error::operation_error(format!(
                                    "url_strip_fragment() failed to cast series to utf8: {}",
                                    e
                                ))
                            })?
                            .apply(|s| {
                                s.map(|s| match Url::parse(s) {
                                    Ok(mut url) => {
                                        url.set_fragment(None);
                                        Cow::Owned(url.to_string())
                                    }
                                    Err(_) => {
                                        if let Some(pos) = s.find('#') {
                                            Cow::Owned(s[..pos].to_string())
                                        } else {
                                            Cow::Owned(s.to_string())
                                        }
                                    }
                                })
                            })
                            .into_series();
                        let mut s = stripped_series;
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
                    "url_strip_fragment() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let stripped_series = series
                    .str()
                    .map_err(|e| {
                        dsq_shared::error::operation_error(format!(
                            "url_strip_fragment() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.map(|s| match Url::parse(s) {
                            Ok(mut url) => {
                                url.set_fragment(None);
                                Cow::Owned(url.to_string())
                            }
                            Err(_) => {
                                if let Some(pos) = s.find('#') {
                                    Cow::Owned(s[..pos].to_string())
                                } else {
                                    Cow::Owned(s.to_string())
                                }
                            }
                        })
                    })
                    .into_series();
                Ok(Value::Series(stripped_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        Value::LazyFrame(lf) => {
            // Collect the LazyFrame to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_url_strip_fragment(&[Value::DataFrame(df)])
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_strip_fragment() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_strip_fragment",
        func: builtin_url_strip_fragment,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_url_strip_fragment_basic() {
        let result = builtin_url_strip_fragment(&[Value::String(
            "https://example.com/path#fragment".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path".to_string())
        );
    }

    #[test]
    fn test_url_strip_fragment_no_fragment() {
        let result =
            builtin_url_strip_fragment(&[Value::String("https://example.com/path".to_string())])
                .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path".to_string())
        );
    }

    #[test]
    fn test_url_strip_fragment_with_query_and_fragment() {
        let result = builtin_url_strip_fragment(&[Value::String(
            "https://example.com/path?key=value#fragment".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path?key=value".to_string())
        );
    }

    #[test]
    fn test_url_strip_fragment_only_fragment() {
        let result =
            builtin_url_strip_fragment(&[Value::String("https://example.com/path#".to_string())])
                .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path".to_string())
        );
    }

    #[test]
    fn test_url_strip_fragment_invalid_url_with_hash() {
        let result =
            builtin_url_strip_fragment(&[Value::String("not-a-url#fragment".to_string())]).unwrap();
        assert_eq!(result, Value::String("not-a-url".to_string()));
    }

    #[test]
    fn test_url_strip_fragment_invalid_url_no_hash() {
        let result = builtin_url_strip_fragment(&[Value::String("not-a-url".to_string())]).unwrap();
        assert_eq!(result, Value::String("not-a-url".to_string()));
    }

    #[test]
    fn test_url_strip_fragment_empty_string() {
        let result = builtin_url_strip_fragment(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_url_strip_fragment_hash_only() {
        let result = builtin_url_strip_fragment(&[Value::String("#fragment".to_string())]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_url_strip_fragment_multiple_hashes() {
        let result = builtin_url_strip_fragment(&[Value::String(
            "https://example.com/path#frag1#frag2".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path".to_string())
        );
    }

    #[test]
    fn test_url_strip_fragment_array() {
        let arr = vec![
            Value::String("https://example.com/path#fragment".to_string()),
            Value::String("http://test.com/page?query=1#section".to_string()),
            Value::String("not-a-url#frag".to_string()),
        ];
        let result = builtin_url_strip_fragment(&[Value::Array(arr)]).unwrap();
        let expected = Value::Array(vec![
            Value::String("https://example.com/path".to_string()),
            Value::String("http://test.com/page?query=1".to_string()),
            Value::String("not-a-url".to_string()),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_url_strip_fragment_array_mixed_types() {
        let arr = vec![
            Value::String("https://example.com/path#fragment".to_string()),
            Value::Int(123),
            Value::String("ftp://example.com/file#section".to_string()),
        ];
        let result = builtin_url_strip_fragment(&[Value::Array(arr)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string elements in array"));
    }

    #[test]
    fn test_url_strip_fragment_empty_array() {
        let result = builtin_url_strip_fragment(&[Value::Array(vec![])]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Array(vec![]));
    }

    #[test]
    fn test_url_strip_fragment_no_args() {
        let result = builtin_url_strip_fragment(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_url_strip_fragment_too_many_args() {
        let result = builtin_url_strip_fragment(&[
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
    fn test_url_strip_fragment_invalid_type() {
        let result = builtin_url_strip_fragment(&[Value::Int(123)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string, array, DataFrame, or Series"));
    }

    #[test]
    fn test_url_strip_fragment_special_characters() {
        let result = builtin_url_strip_fragment(&[Value::String(
            "https://example.com/path with spaces#fragment".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path%20with%20spaces".to_string())
        );
    }

    #[test]
    fn test_url_strip_fragment_encoded_fragment() {
        let result = builtin_url_strip_fragment(&[Value::String(
            "https://example.com/path#%C3%A9".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path".to_string())
        );
    }

    #[test]
    fn test_url_strip_fragment_fragment_with_query() {
        let result = builtin_url_strip_fragment(&[Value::String(
            "https://example.com/path?key=value#fragment?nested=1".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path?key=value".to_string())
        );
    }
}
