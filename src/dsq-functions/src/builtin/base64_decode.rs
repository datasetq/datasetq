use base64::{engine::general_purpose, Engine};
use dsq_shared::value::Value;
use dsq_shared::Result;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_base64_decode(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "base64_decode() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => match general_purpose::STANDARD.decode(s) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(decoded) => Ok(Value::String(decoded)),
                Err(_) => Err(dsq_shared::error::operation_error(
                    "base64_decode() decoded bytes are not valid UTF-8",
                )),
            },
            Err(_) => Err(dsq_shared::error::operation_error(
                "base64_decode() invalid base64 string",
            )),
        },
        Value::Array(arr) => {
            let decoded: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => match general_purpose::STANDARD.decode(s) {
                        Ok(bytes) => match String::from_utf8(bytes) {
                            Ok(decoded) => Ok(Value::String(decoded)),
                            Err(_) => Err(dsq_shared::error::operation_error(
                                "base64_decode() decoded bytes are not valid UTF-8",
                            )),
                        },
                        Err(_) => Err(dsq_shared::error::operation_error(
                            "base64_decode() invalid base64 string",
                        )),
                    },
                    _ => Err(dsq_shared::error::operation_error(
                        "base64_decode() requires string elements in array",
                    )),
                })
                .collect();
            Ok(Value::Array(decoded?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::Utf8 {
                        let utf8_series = series.utf8().map_err(|e| {
                            dsq_shared::error::operation_error(format!(
                                "base64_decode() failed to cast series to utf8: {}",
                                e
                            ))
                        })?;
                        let decoded_series = utf8_series
                            .apply(|s| {
                                s.and_then(|s| match general_purpose::STANDARD.decode(s) {
                                    Ok(bytes) => match String::from_utf8(bytes) {
                                        Ok(decoded) => Some(Cow::Owned(decoded)),
                                        Err(_) => None,
                                    },
                                    Err(_) => None,
                                })
                            })
                            .into_series();
                        let mut s = decoded_series;
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
                    "base64_decode() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::Utf8 {
                let utf8_series = series.utf8().map_err(|e| {
                    dsq_shared::error::operation_error(format!(
                        "base64_decode() failed to cast series to utf8: {}",
                        e
                    ))
                })?;
                let decoded_series = utf8_series
                    .apply(|s| {
                        s.and_then(|s| match general_purpose::STANDARD.decode(s) {
                            Ok(bytes) => match String::from_utf8(bytes) {
                                Ok(decoded) => Some(Cow::Owned(decoded)),
                                Err(_) => None,
                            },
                            Err(_) => None,
                        })
                    })
                    .into_series();
                Ok(Value::Series(decoded_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "base64_decode() requires string, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "base64_decode",
        func: builtin_base64_decode,
    }
}
