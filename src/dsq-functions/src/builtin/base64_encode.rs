use base64::{engine::general_purpose, Engine};
use dsq_shared::value::Value;
use dsq_shared::Result;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_base64_encode(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "base64_encode() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(
            general_purpose::STANDARD.encode(s.as_bytes()),
        )),
        Value::Array(arr) => {
            let encoded: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => Ok(Value::String(
                        general_purpose::STANDARD.encode(s.as_bytes()),
                    )),
                    _ => Ok(Value::String(
                        general_purpose::STANDARD.encode(v.to_string().as_bytes()),
                    )),
                })
                .collect();
            Ok(Value::Array(encoded?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::String {
                        let utf8_series = series.str().map_err(|e| {
                            dsq_shared::error::operation_error(format!(
                                "base64_encode() failed to cast series to utf8: {}",
                                e
                            ))
                        })?;
                        let encoded_series = utf8_series
                            .apply(|s| {
                                s.map(|s| {
                                    Cow::Owned(general_purpose::STANDARD.encode(s.as_bytes()))
                                })
                            })
                            .into_series();
                        let mut s = encoded_series;
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
                    "base64_encode() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let utf8_series = series.str().map_err(|e| {
                    dsq_shared::error::operation_error(format!(
                        "base64_encode() failed to cast series to utf8: {}",
                        e
                    ))
                })?;
                let encoded_series = utf8_series
                    .apply(|s| {
                        s.map(|s| Cow::Owned(general_purpose::STANDARD.encode(s.as_bytes())))
                    })
                    .into_series();
                Ok(Value::Series(encoded_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        Value::LazyFrame(lf) => {
            // Collect the LazyFrame to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_base64_encode(&[Value::DataFrame(df)])
        }
        _ => Err(dsq_shared::error::operation_error(
            "base64_encode() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "base64_encode",
        func: builtin_base64_encode,
    }
}
