use dsq_shared::value::{is_truthy, Value};
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::collections::HashMap;

pub fn builtin_avg_ifs(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(dsq_shared::error::operation_error(
            "avg_ifs() expects at least 3 arguments: values and at least 2 masks",
        ));
    }

    let values = &args[0];
    let masks = &args[1..];

    match values {
        Value::LazyFrame(lf) => {
            // Collect LazyFrame to DataFrame
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;

            // Recursively call with the collected DataFrame
            let mut new_args = vec![Value::DataFrame(df)];
            new_args.extend_from_slice(&args[1..]);
            builtin_avg_ifs(&new_args)
        }
        Value::Array(arr) => {
            // Check all masks are arrays of same length
            for mask in masks {
                if let Value::Array(mask_arr) = mask {
                    if mask_arr.len() != arr.len() {
                        return Err(dsq_shared::error::operation_error(
                            "avg_ifs() all masks must have same length as values",
                        ));
                    }
                } else {
                    return Err(dsq_shared::error::operation_error(
                        "avg_ifs() all masks must be arrays",
                    ));
                }
            }

            let mut sum = 0.0;
            let mut count = 0;
            for i in 0..arr.len() {
                let all_true = masks.iter().all(|mask| {
                    if let Value::Array(mask_arr) = mask {
                        is_truthy(&mask_arr[i])
                    } else {
                        false
                    }
                });
                if all_true {
                    match &arr[i] {
                        Value::Int(val) => {
                            sum += *val as f64;
                            count += 1;
                        }
                        Value::Float(val) => {
                            sum += *val;
                            count += 1;
                        }
                        _ => {}
                    }
                }
            }
            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Float(sum / count as f64))
            }
        }
        Value::DataFrame(df) => {
            // Check all masks are series of same length
            for mask in masks {
                if let Value::Series(mask_series) = mask {
                    if mask_series.len() != df.height() {
                        return Err(dsq_shared::error::operation_error(
                            "avg_ifs() all masks must have same length as DataFrame",
                        ));
                    }
                } else {
                    return Err(dsq_shared::error::operation_error(
                        "avg_ifs() all masks must be series",
                    ));
                }
            }

            let mut result = HashMap::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype().is_numeric() {
                        let mut sum = 0.0;
                        let mut count = 0;
                        for i in 0..series.len() {
                            let all_true = masks.iter().all(|mask| {
                                if let Value::Series(mask_series) = mask {
                                    mask_series
                                        .get(i)
                                        .ok()
                                        .map(|v| matches!(v, AnyValue::Boolean(true)))
                                        .unwrap_or(false)
                                } else {
                                    false
                                }
                            });
                            if all_true {
                                if let Ok(val) = series.get(i) {
                                    match val {
                                        AnyValue::Int8(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::Int16(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::Int32(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::Int64(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::UInt8(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::UInt16(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::UInt32(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::UInt64(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::Float32(n) => {
                                            sum += n as f64;
                                            count += 1;
                                        }
                                        AnyValue::Float64(n) => {
                                            sum += n;
                                            count += 1;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        if count > 0 {
                            result.insert(col_name.to_string(), Value::Float(sum / count as f64));
                        }
                    }
                }
            }
            Ok(Value::Object(result))
        }
        Value::Series(series) => {
            // Check all masks are series of same length
            for mask in masks {
                if let Value::Series(mask_series) = mask {
                    if mask_series.len() != series.len() {
                        return Err(dsq_shared::error::operation_error(
                            "avg_ifs() all masks must have same length as series",
                        ));
                    }
                } else {
                    return Err(dsq_shared::error::operation_error(
                        "avg_ifs() all masks must be series",
                    ));
                }
            }

            if series.dtype().is_numeric() {
                let mut sum = 0.0;
                let mut count = 0;
                for i in 0..series.len() {
                    let all_true = masks.iter().all(|mask| {
                        if let Value::Series(mask_series) = mask {
                            mask_series
                                .get(i)
                                .ok()
                                .map(|v| matches!(v, AnyValue::Boolean(true)))
                                .unwrap_or(false)
                        } else {
                            false
                        }
                    });
                    if all_true {
                        if let Ok(val) = series.get(i) {
                            match val {
                                AnyValue::Int8(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::Int16(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::Int32(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::Int64(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::UInt8(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::UInt16(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::UInt32(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::UInt64(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::Float32(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                AnyValue::Float64(n) => {
                                    sum += n;
                                    count += 1;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                if count == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float(sum / count as f64))
                }
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "avg_ifs() first argument must be array, DataFrame, LazyFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "avg_ifs",
        func: builtin_avg_ifs,
    }
}
