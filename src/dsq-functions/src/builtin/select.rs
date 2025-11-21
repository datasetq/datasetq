use dsq_shared::value::{is_truthy, Value};
use dsq_shared::Result;
use polars::prelude::*;

pub fn builtin_select(args: &[Value]) -> Result<Value> {
    match args.len() {
        0 => Err(dsq_shared::error::operation_error(
            "select() expects at least 1 argument",
        )),
        1 => {
            // Single argument: return it if truthy, else null
            if is_truthy(&args[0]) {
                Ok(args[0].clone())
            } else {
                Ok(Value::Null)
            }
        }
        2 => {
            // Two arguments: could be input and condition, or array and single index
            let input = &args[0];
            let condition = &args[1];

            // Check if this is array indexing (array and integer index)
            if let (Value::Array(arr), Value::Int(idx)) = (input, condition) {
                let idx = *idx as usize;
                if idx < arr.len() {
                    return Ok(arr[idx].clone());
                } else {
                    return Ok(Value::Null);
                }
            }

            // Otherwise, treat as condition filtering
            match condition {
                Value::Bool(b) => {
                    if *b {
                        Ok(input.clone())
                    } else {
                        Ok(Value::Null)
                    }
                }
                Value::Array(mask) => match input {
                    Value::Array(arr) => {
                        if arr.len() != mask.len() {
                            return Err(dsq_shared::error::operation_error(
                                "select() array and mask must have same length",
                            ));
                        }
                        let filtered: Vec<Value> = arr
                            .iter()
                            .zip(mask.iter())
                            .filter_map(|(item, m)| {
                                if is_truthy(m) {
                                    Some(item.clone())
                                } else {
                                    None
                                }
                            })
                            .collect();
                        Ok(Value::Array(filtered))
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "select() with array mask requires array input",
                    )),
                },
                Value::Series(mask_series) => match input {
                    Value::DataFrame(df) => {
                        if df.height() != mask_series.len() {
                            return Err(dsq_shared::error::operation_error(
                                "select() DataFrame and mask series must have same length",
                            ));
                        }
                        let mask_vec: Vec<bool> = (0..mask_series.len())
                            .filter_map(|i| {
                                mask_series.get(i).ok().and_then(|v| match v {
                                    AnyValue::Boolean(b) => Some(b),
                                    _ => None,
                                })
                            })
                            .collect();
                        if mask_vec.len() != df.height() {
                            return Err(dsq_shared::error::operation_error(
                                "select() mask series must contain booleans",
                            ));
                        }
                        let mask_chunked = BooleanChunked::from_slice("mask", &mask_vec);
                        match df.filter(&mask_chunked) {
                            Ok(filtered_df) => Ok(Value::DataFrame(filtered_df)),
                            Err(e) => Err(dsq_shared::error::operation_error(format!(
                                "select() failed to filter DataFrame: {}",
                                e
                            ))),
                        }
                    }
                    Value::Series(series) => {
                        if series.len() != mask_series.len() {
                            return Err(dsq_shared::error::operation_error(
                                "select() series and mask series must have same length",
                            ));
                        }
                        let mask_vec: Vec<bool> = (0..mask_series.len())
                            .filter_map(|i| {
                                mask_series.get(i).ok().and_then(|v| match v {
                                    AnyValue::Boolean(b) => Some(b),
                                    _ => None,
                                })
                            })
                            .collect();
                        if mask_vec.len() != series.len() {
                            return Err(dsq_shared::error::operation_error(
                                "select() mask series must contain booleans",
                            ));
                        }
                        let mask_chunked = BooleanChunked::from_slice("mask", &mask_vec);
                        match series.filter(&mask_chunked) {
                            Ok(filtered_series) => Ok(Value::Series(filtered_series)),
                            Err(e) => Err(dsq_shared::error::operation_error(format!(
                                "select() failed to filter series: {}",
                                e
                            ))),
                        }
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "select() with series mask requires DataFrame or Series input",
                    )),
                },
                _ => {
                    if is_truthy(condition) {
                        Ok(input.clone())
                    } else {
                        Ok(Value::Null)
                    }
                }
            }
        }
        _ => {
            // Multiple arguments: array and indices
            let input = &args[0];
            if let Value::Array(arr) = input {
                let mut selected = Vec::new();
                for arg in &args[1..] {
                    if let Value::Int(idx) = arg {
                        let idx = *idx as usize;
                        if idx < arr.len() {
                            selected.push(arr[idx].clone());
                        } else {
                            selected.push(Value::Null);
                        }
                    } else {
                        return Err(dsq_shared::error::operation_error(
                            "select() indices must be integers",
                        ));
                    }
                }
                Ok(Value::Array(selected))
            } else {
                Err(dsq_shared::error::operation_error(
                    "select() with multiple indices requires array input",
                ))
            }
        }
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "select",
        func: builtin_select,
    }
}
