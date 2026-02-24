use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_set_path(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "url_set_path() expects 2 arguments",
        ));
    }

    let new_path = match &args[1] {
        Value::String(s) => s.clone(),
        _ => {
            return Err(dsq_shared::error::operation_error(
                "url_set_path() second argument must be a string",
            ));
        }
    };

    match &args[0] {
        Value::String(s) => {
            match Url::parse(s) {
                Ok(mut url) => {
                    url.set_path(&new_path);
                    Ok(Value::String(url.to_string()))
                }
                Err(_) => Ok(Value::String(s.clone())), // Return original if not a valid URL
            }
        }
        Value::Array(arr) => {
            let set: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => builtin_url_set_path(&[
                        Value::String(s.clone()),
                        Value::String(new_path.clone()),
                    ]),
                    _ => Err(dsq_shared::error::operation_error(
                        "url_set_path() requires string elements in array",
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
                                    "url_set_path() failed to cast series to utf8: {}",
                                    e
                                ))
                            })?
                            .apply(|s| {
                                s.map(|s| match Url::parse(s) {
                                    Ok(mut url) => {
                                        url.set_path(&new_path);
                                        Cow::Owned(url.to_string())
                                    }
                                    Err(_) => Cow::Owned(s.to_string()),
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
                    "url_set_path() failed on DataFrame: {}",
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
                            "url_set_path() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.map(|s| match Url::parse(s) {
                            Ok(mut url) => {
                                url.set_path(&new_path);
                                Cow::Owned(url.to_string())
                            }
                            Err(_) => Cow::Owned(s.to_string()),
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
            builtin_url_set_path(&[Value::DataFrame(df), args[1].clone()])
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_set_path() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_set_path",
        func: builtin_url_set_path,
    }
}
