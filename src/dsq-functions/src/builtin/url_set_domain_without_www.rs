use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_set_domain_without_www(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "url_set_domain_without_www() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            match Url::parse(s) {
                Ok(mut url) => {
                    if let Some(host) = url.host_str() {
                        let new_host = if host.starts_with("www.") {
                            host[4..].to_string()
                        } else {
                            host.to_string()
                        };
                        url.set_host(Some(&new_host))
                            .map_err(|_| dsq_shared::error::operation_error("Invalid domain"))?;
                        Ok(Value::String(url.to_string()))
                    } else {
                        Ok(Value::String(s.clone()))
                    }
                }
                Err(_) => Ok(Value::String(s.clone())), // Return original if not a valid URL
            }
        }
        Value::Array(arr) => {
            let set: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => {
                        builtin_url_set_domain_without_www(&[Value::String(s.clone())])
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "url_set_domain_without_www() requires string elements in array",
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
                                "url_set_domain_without_www() failed to cast series to utf8: {}",
                                e
                            ))
                            })?
                            .apply(|s| {
                                s.and_then(|s| match Url::parse(s) {
                                    Ok(mut url) => {
                                        if let Some(host) = url.host_str() {
                                            let new_host = if host.starts_with("www.") {
                                                host[4..].to_string()
                                            } else {
                                                host.to_string()
                                            };
                                            url.set_host(Some(&new_host)).ok()?;
                                            Some(Cow::Owned(url.to_string()))
                                        } else {
                                            Some(Cow::Owned(s.to_string()))
                                        }
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
                    "url_set_domain_without_www() failed on DataFrame: {}",
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
                            "url_set_domain_without_www() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.and_then(|s| match Url::parse(s) {
                            Ok(mut url) => {
                                if let Some(host) = url.host_str() {
                                    let new_host = if host.starts_with("www.") {
                                        host[4..].to_string()
                                    } else {
                                        host.to_string()
                                    };
                                    url.set_host(Some(&new_host)).ok()?;
                                    Some(Cow::Owned(url.to_string()))
                                } else {
                                    Some(Cow::Owned(s.to_string()))
                                }
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
        _ => Err(dsq_shared::error::operation_error(
            "url_set_domain_without_www() requires string, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_set_domain_without_www",
        func: builtin_url_set_domain_without_www,
    }
}
