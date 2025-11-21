use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_set_port(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "url_set_port() expects 2 arguments",
        ));
    }

    let port = match &args[1] {
        Value::Int(i) => *i as u16,
        Value::String(s) => s
            .parse::<u16>()
            .map_err(|_| dsq_shared::error::operation_error("Invalid port number"))?,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "url_set_port() second argument must be an integer or string",
            ));
        }
    };

    match &args[0] {
        Value::String(s) => {
            match Url::parse(s) {
                Ok(mut url) => {
                    url.set_port(Some(port))
                        .map_err(|_| dsq_shared::error::operation_error("Invalid port"))?;
                    Ok(Value::String(url.to_string()))
                }
                Err(_) => Ok(Value::String(s.clone())), // Return original if not a valid URL
            }
        }
        Value::Array(arr) => {
            let set: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => {
                        builtin_url_set_port(&[Value::String(s.clone()), args[1].clone()])
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "url_set_port() requires string elements in array",
                    )),
                })
                .collect();
            Ok(Value::Array(set?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::Utf8 {
                        let set_series = series
                            .utf8()
                            .unwrap()
                            .apply(|s| {
                                s.and_then(|s| match Url::parse(s) {
                                    Ok(mut url) => {
                                        url.set_port(Some(port)).ok()?;
                                        Some(Cow::Owned(url.to_string()))
                                    }
                                    Err(_) => Some(Cow::Owned(s.to_string())),
                                })
                            })
                            .into_series();
                        let mut s = set_series;
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
                    "url_set_port() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::Utf8 {
                let set_series = series
                    .utf8()
                    .unwrap()
                    .apply(|s| {
                        s.and_then(|s| match Url::parse(s) {
                            Ok(mut url) => {
                                url.set_port(Some(port)).ok()?;
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
        _ => Err(dsq_shared::error::operation_error(
            "url_set_port() requires string, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_set_port",
        func: builtin_url_set_port,
    }
}
