use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_set_query_string(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(dsq_shared::error::operation_error(
            "url_set_query_string() expects 3 arguments",
        ));
    }

    let key = match &args[1] {
        Value::String(s) => s.clone(),
        _ => {
            return Err(dsq_shared::error::operation_error(
                "url_set_query_string() second argument must be a string",
            ));
        }
    };

    let value = match &args[2] {
        Value::String(s) => s.clone(),
        _ => {
            return Err(dsq_shared::error::operation_error(
                "url_set_query_string() third argument must be a string",
            ));
        }
    };

    match &args[0] {
        Value::String(s) => {
            match Url::parse(s) {
                Ok(mut url) => {
                    let mut query_pairs = url
                        .query_pairs()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect::<Vec<_>>();
                    // Remove existing key if present
                    query_pairs.retain(|(k, _)| k != &key);
                    // Add new key-value pair
                    query_pairs.push((key, value));
                    let new_query = query_pairs
                        .into_iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect::<Vec<_>>()
                        .join("&");
                    url.set_query(Some(&new_query));
                    Ok(Value::String(url.to_string()))
                }
                Err(_) => Ok(Value::String(s.clone())), // Return original if not a valid URL
            }
        }
        Value::Array(arr) => {
            let set: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => builtin_url_set_query_string(&[
                        Value::String(s.clone()),
                        Value::String(key.clone()),
                        Value::String(value.clone()),
                    ]),
                    _ => Err(dsq_shared::error::operation_error(
                        "url_set_query_string() requires string elements in array",
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
                                    "url_set_query_string() failed to cast series to utf8: {}",
                                    e
                                ))
                            })?
                            .apply(|s| {
                                s.map(|s| {
                                    match Url::parse(s) {
                                        Ok(mut url) => {
                                            let mut query_pairs = url
                                                .query_pairs()
                                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                                .collect::<Vec<_>>();
                                            // Remove existing key if present
                                            query_pairs.retain(|(k, _)| k != &key);
                                            // Add new key-value pair
                                            query_pairs.push((key.clone(), value.clone()));
                                            let new_query = query_pairs
                                                .into_iter()
                                                .map(|(k, v)| format!("{}={}", k, v))
                                                .collect::<Vec<_>>()
                                                .join("&");
                                            url.set_query(Some(&new_query));
                                            Cow::Owned(url.to_string())
                                        }
                                        Err(_) => Cow::Owned(s.to_string()),
                                    }
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
                    "url_set_query_string() failed on DataFrame: {}",
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
                            "url_set_query_string() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.map(|s| {
                            match Url::parse(s) {
                                Ok(mut url) => {
                                    let mut query_pairs = url
                                        .query_pairs()
                                        .map(|(k, v)| (k.to_string(), v.to_string()))
                                        .collect::<Vec<_>>();
                                    // Remove existing key if present
                                    query_pairs.retain(|(k, _)| k != &key);
                                    // Add new key-value pair
                                    query_pairs.push((key.clone(), value.clone()));
                                    let new_query = query_pairs
                                        .into_iter()
                                        .map(|(k, v)| format!("{}={}", k, v))
                                        .collect::<Vec<_>>()
                                        .join("&");
                                    url.set_query(Some(&new_query));
                                    Cow::Owned(url.to_string())
                                }
                                Err(_) => Cow::Owned(s.to_string()),
                            }
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
            builtin_url_set_query_string(&[Value::DataFrame(df), args[1].clone(), args[2].clone()])
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_set_query_string() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_set_query_string",
        func: builtin_url_set_query_string,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_url_set_query_string_basic() {
        let result = builtin_url_set_query_string(&[
            Value::String("https://example.com/path".to_string()),
            Value::String("key".to_string()),
            Value::String("value".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path?key=value".to_string())
        );
    }

    #[test]
    fn test_url_set_query_string_replace_existing() {
        let result = builtin_url_set_query_string(&[
            Value::String("https://example.com/path?key=old".to_string()),
            Value::String("key".to_string()),
            Value::String("new".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path?key=new".to_string())
        );
    }

    #[test]
    fn test_url_set_query_string_add_to_existing() {
        let result = builtin_url_set_query_string(&[
            Value::String("https://example.com/path?existing=val".to_string()),
            Value::String("key".to_string()),
            Value::String("value".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path?existing=val&key=value".to_string())
        );
    }

    #[test]
    fn test_url_set_query_string_multiple_params() {
        let result = builtin_url_set_query_string(&[
            Value::String("https://example.com/path?a=1&b=2".to_string()),
            Value::String("c".to_string()),
            Value::String("3".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path?a=1&b=2&c=3".to_string())
        );
    }

    #[test]
    fn test_url_set_query_string_invalid_url() {
        let result = builtin_url_set_query_string(&[
            Value::String("not-a-url".to_string()),
            Value::String("key".to_string()),
            Value::String("value".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("not-a-url".to_string()));
    }

    #[test]
    fn test_url_set_query_string_array() {
        let arr = vec![
            Value::String("https://example.com/1".to_string()),
            Value::String("https://example.com/2".to_string()),
        ];
        let result = builtin_url_set_query_string(&[
            Value::Array(arr),
            Value::String("test".to_string()),
            Value::String("val".to_string()),
        ])
        .unwrap();
        let expected = Value::Array(vec![
            Value::String("https://example.com/1?test=val".to_string()),
            Value::String("https://example.com/2?test=val".to_string()),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_url_set_query_string_wrong_args() {
        let result = builtin_url_set_query_string(&[Value::String("url".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 3 arguments"));
    }

    #[test]
    fn test_url_set_query_string_non_string_key() {
        let result = builtin_url_set_query_string(&[
            Value::String("https://example.com".to_string()),
            Value::Int(123),
            Value::String("value".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("second argument must be a string"));
    }

    #[test]
    fn test_url_set_query_string_non_string_value() {
        let result = builtin_url_set_query_string(&[
            Value::String("https://example.com".to_string()),
            Value::String("key".to_string()),
            Value::Int(456),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("third argument must be a string"));
    }

    #[test]
    fn test_url_set_query_string_invalid_type() {
        let result = builtin_url_set_query_string(&[
            Value::Int(123),
            Value::String("key".to_string()),
            Value::String("value".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string, array, DataFrame, Series, or LazyFrame"));
    }

    #[test]
    fn test_url_set_query_string_empty_key() {
        let result = builtin_url_set_query_string(&[
            Value::String("https://example.com/path".to_string()),
            Value::String("".to_string()),
            Value::String("value".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path?=value".to_string())
        );
    }

    #[test]
    fn test_url_set_query_string_empty_value() {
        let result = builtin_url_set_query_string(&[
            Value::String("https://example.com/path".to_string()),
            Value::String("key".to_string()),
            Value::String("".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path?key=".to_string())
        );
    }

    #[test]
    fn test_url_set_query_string_special_chars() {
        let result = builtin_url_set_query_string(&[
            Value::String("https://example.com/path".to_string()),
            Value::String("key with spaces".to_string()),
            Value::String("value with spaces".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::String(
                "https://example.com/path?key%20with%20spaces=value%20with%20spaces".to_string()
            )
        );
    }
}
