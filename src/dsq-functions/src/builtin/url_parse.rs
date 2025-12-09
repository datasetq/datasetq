use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use serde_json;
use std::borrow::Cow;
use std::collections::HashMap;
use url::Url;

pub fn builtin_url_parse(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "url_parse() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => match Url::parse(s) {
            Ok(url) => {
                let mut result = HashMap::new();
                result.insert(
                    "scheme".to_string(),
                    Value::String(url.scheme().to_string()),
                );
                result.insert(
                    "username".to_string(),
                    Value::String(url.username().to_string()),
                );
                result.insert(
                    "password".to_string(),
                    url.password()
                        .map(|p| Value::String(p.to_string()))
                        .unwrap_or(Value::Null),
                );
                result.insert(
                    "host".to_string(),
                    url.host_str()
                        .map(|h| Value::String(h.to_string()))
                        .unwrap_or(Value::Null),
                );
                result.insert(
                    "port".to_string(),
                    url.port()
                        .map(|p| Value::Int(p as i64))
                        .unwrap_or(Value::Null),
                );
                result.insert("path".to_string(), Value::String(url.path().to_string()));
                result.insert(
                    "query".to_string(),
                    url.query()
                        .map(|q| Value::String(q.to_string()))
                        .unwrap_or(Value::Null),
                );
                result.insert(
                    "fragment".to_string(),
                    url.fragment()
                        .map(|f| Value::String(f.to_string()))
                        .unwrap_or(Value::Null),
                );
                Ok(Value::Object(result))
            }
            Err(_) => Err(dsq_shared::error::operation_error(
                "url_parse() invalid URL",
            )),
        },
        Value::Array(arr) => {
            let parsed: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => builtin_url_parse(&[Value::String(s.clone())]),
                    _ => Err(dsq_shared::error::operation_error(
                        "url_parse() requires string elements in array",
                    )),
                })
                .collect();
            Ok(Value::Array(parsed?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::String {
                        let utf8_series = series.str().map_err(|e| {
                            dsq_shared::error::operation_error(format!(
                                "url_parse() failed to cast series to utf8: {}",
                                e
                            ))
                        })?;
                        let parsed_series = utf8_series
                            .apply(|s| {
                                s.and_then(|s| match Url::parse(s) {
                                    Ok(url) => {
                                        let mut result = HashMap::new();
                                        result.insert(
                                            "scheme".to_string(),
                                            Value::String(url.scheme().to_string()),
                                        );
                                        result.insert(
                                            "username".to_string(),
                                            Value::String(url.username().to_string()),
                                        );
                                        result.insert(
                                            "password".to_string(),
                                            url.password()
                                                .map(|p| Value::String(p.to_string()))
                                                .unwrap_or(Value::Null),
                                        );
                                        result.insert(
                                            "host".to_string(),
                                            url.host_str()
                                                .map(|h| Value::String(h.to_string()))
                                                .unwrap_or(Value::Null),
                                        );
                                        result.insert(
                                            "port".to_string(),
                                            url.port()
                                                .map(|p| Value::Int(p as i64))
                                                .unwrap_or(Value::Null),
                                        );
                                        result.insert(
                                            "path".to_string(),
                                            Value::String(url.path().to_string()),
                                        );
                                        result.insert(
                                            "query".to_string(),
                                            url.query()
                                                .map(|q| Value::String(q.to_string()))
                                                .unwrap_or(Value::Null),
                                        );
                                        result.insert(
                                            "fragment".to_string(),
                                            url.fragment()
                                                .map(|f| Value::String(f.to_string()))
                                                .unwrap_or(Value::Null),
                                        );
                                        Some(Cow::Owned(
                                            serde_json::to_string(&Value::Object(result))
                                                .unwrap_or("null".to_string()),
                                        ))
                                    }
                                    Err(_) => None,
                                })
                            })
                            .into_series();
                        let mut s = parsed_series;
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
                    "url_parse() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let utf8_series = series.str().map_err(|e| {
                    dsq_shared::error::operation_error(format!(
                        "url_parse() failed to cast series to utf8: {}",
                        e
                    ))
                })?;
                let parsed_series = utf8_series
                    .apply(|s| {
                        s.and_then(|s| match Url::parse(s) {
                            Ok(url) => {
                                let mut result = HashMap::new();
                                result.insert(
                                    "scheme".to_string(),
                                    Value::String(url.scheme().to_string()),
                                );
                                result.insert(
                                    "username".to_string(),
                                    Value::String(url.username().to_string()),
                                );
                                result.insert(
                                    "password".to_string(),
                                    url.password()
                                        .map(|p| Value::String(p.to_string()))
                                        .unwrap_or(Value::Null),
                                );
                                result.insert(
                                    "host".to_string(),
                                    url.host_str()
                                        .map(|h| Value::String(h.to_string()))
                                        .unwrap_or(Value::Null),
                                );
                                result.insert(
                                    "port".to_string(),
                                    url.port()
                                        .map(|p| Value::Int(p as i64))
                                        .unwrap_or(Value::Null),
                                );
                                result.insert(
                                    "path".to_string(),
                                    Value::String(url.path().to_string()),
                                );
                                result.insert(
                                    "query".to_string(),
                                    url.query()
                                        .map(|q| Value::String(q.to_string()))
                                        .unwrap_or(Value::Null),
                                );
                                result.insert(
                                    "fragment".to_string(),
                                    url.fragment()
                                        .map(|f| Value::String(f.to_string()))
                                        .unwrap_or(Value::Null),
                                );
                                Some(Cow::Owned(
                                    serde_json::to_string(&Value::Object(result))
                                        .unwrap_or("null".to_string()),
                                ))
                            }
                            Err(_) => None,
                        })
                    })
                    .into_series();
                Ok(Value::Series(parsed_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_parse() requires string, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_parse",
        func: builtin_url_parse,
    }
}
