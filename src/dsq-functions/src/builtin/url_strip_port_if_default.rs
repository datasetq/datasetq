use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_strip_port_if_default(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "url_strip_port_if_default() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            match Url::parse(s) {
                Ok(mut url) => {
                    if let Some(port) = url.port() {
                        let scheme = url.scheme();
                        let is_default = match scheme {
                            "http" => port == 80,
                            "https" => port == 443,
                            "ftp" => port == 21,
                            "ssh" => port == 22,
                            "telnet" => port == 23,
                            _ => false,
                        };
                        if is_default {
                            url.set_port(None).unwrap();
                        }
                    }
                    Ok(Value::String(url.to_string()))
                }
                Err(_) => Ok(Value::String(s.clone())), // Return original if not a valid URL
            }
        }
        Value::Array(arr) => {
            let stripped: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => {
                        builtin_url_strip_port_if_default(&[Value::String(s.clone())])
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "url_strip_port_if_default() requires string elements in array",
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
                                    "url_strip_port_if_default() failed to cast series to utf8: {}",
                                    e
                                ))
                            })?
                            .apply(|s| {
                                s.map(|s| match Url::parse(s) {
                                    Ok(mut url) => {
                                        if let Some(port) = url.port() {
                                            let scheme = url.scheme();
                                            let is_default = match scheme {
                                                "http" => port == 80,
                                                "https" => port == 443,
                                                "ftp" => port == 21,
                                                "ssh" => port == 22,
                                                "telnet" => port == 23,
                                                _ => false,
                                            };
                                            if is_default {
                                                url.set_port(None).unwrap();
                                            }
                                        }
                                        Cow::Owned(url.to_string())
                                    }
                                    Err(_) => Cow::Owned(s.to_string()),
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
                    "url_strip_port_if_default() failed on DataFrame: {}",
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
                            "url_strip_port_if_default() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.map(|s| match Url::parse(s) {
                            Ok(mut url) => {
                                if let Some(port) = url.port() {
                                    let scheme = url.scheme();
                                    let is_default = match scheme {
                                        "http" => port == 80,
                                        "https" => port == 443,
                                        "ftp" => port == 21,
                                        "ssh" => port == 22,
                                        "telnet" => port == 23,
                                        _ => false,
                                    };
                                    if is_default {
                                        url.set_port(None).unwrap();
                                    }
                                }
                                Cow::Owned(url.to_string())
                            }
                            Err(_) => Cow::Owned(s.to_string()),
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
            builtin_url_strip_port_if_default(&[Value::DataFrame(df)])
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_strip_port_if_default() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_strip_port_if_default",
        func: builtin_url_strip_port_if_default,
    }
}
