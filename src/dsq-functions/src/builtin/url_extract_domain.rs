use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_extract_domain(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "url_extract_domain() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => match Url::parse(s) {
            Ok(url) => Ok(url
                .host_str()
                .map(|h| Value::String(h.to_string()))
                .unwrap_or(Value::Null)),
            Err(_) => Ok(Value::Null),
        },
        Value::Array(arr) => {
            let extracted: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => builtin_url_extract_domain(&[Value::String(s.clone())])
                        .unwrap_or(Value::Null),
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
                        let extracted_series = series
                            .str()
                            .map_err(|e| {
                                dsq_shared::error::operation_error(format!(
                                    "url_extract_domain() failed to cast series to utf8: {}",
                                    e
                                ))
                            })?
                            .apply(|s| {
                                s.and_then(|s| match Url::parse(s) {
                                    Ok(url) => url.host_str().map(|h| Cow::Owned(h.to_string())),
                                    Err(_) => None,
                                })
                            })
                            .into_series();
                        let mut s = extracted_series;
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
                    "url_extract_domain() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let extracted_series = series
                    .str()
                    .map_err(|e| {
                        dsq_shared::error::operation_error(format!(
                            "url_extract_domain() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.and_then(|s| match Url::parse(s) {
                            Ok(url) => url.host_str().map(|h| Cow::Owned(h.to_string())),
                            Err(_) => None,
                        })
                    })
                    .into_series();
                Ok(Value::Series(extracted_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        Value::LazyFrame(lf) => {
            // Collect the LazyFrame to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_url_extract_domain(&[Value::DataFrame(df)])
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_extract_domain() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_extract_domain",
        func: builtin_url_extract_domain,
    }
}
