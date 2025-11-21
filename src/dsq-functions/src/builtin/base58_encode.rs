use base58::ToBase58;
use dsq_shared::value::Value;
use dsq_shared::Result;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_base58_encode(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "base58_encode() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.as_bytes().to_base58())),
        Value::Array(arr) => {
            let encoded: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => Value::String(s.as_bytes().to_base58()),
                    _ => Value::String(v.to_string().as_bytes().to_base58()),
                })
                .collect();
            Ok(Value::Array(encoded))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::Utf8 {
                        let encoded_series = series
                            .utf8()
                            .unwrap()
                            .apply(|s| s.map(|s| Cow::Owned(s.as_bytes().to_base58())))
                            .into_series();
                        let mut s = encoded_series;
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
                    "base58_encode() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::Utf8 {
                let encoded_series = series
                    .utf8()
                    .unwrap()
                    .apply(|s| s.map(|s| Cow::Owned(s.as_bytes().to_base58())))
                    .into_series();
                Ok(Value::Series(encoded_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "base58_encode() requires string, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "base58_encode",
        func: builtin_base58_encode,
    }
}
