use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use serde_json;
use std::borrow::Cow;

pub fn builtin_split(args: &[Value]) -> Result<Value> {
    if args.len() < 1 || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "split() expects 1 or 2 arguments",
        ));
    }

    let separator = if args.len() == 2 {
        match &args[1] {
            Value::String(s) => s.clone(),
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "split() separator must be a string",
                ));
            }
        }
    } else {
        " ".to_string() // default separator is whitespace
    };

    match &args[0] {
        Value::String(s) => {
            let parts: Vec<Value> = if separator.is_empty() {
                s.chars().map(|c| Value::String(c.to_string())).collect()
            } else {
                s.split(&separator)
                    .map(|part| Value::String(part.to_string()))
                    .collect()
            };
            Ok(Value::Array(parts))
        }
        Value::Array(arr) => {
            let split_arrays: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => {
                        let parts: Vec<Value> = if separator.is_empty() {
                            s.chars().map(|c| Value::String(c.to_string())).collect()
                        } else {
                            s.split(&separator)
                                .map(|part| Value::String(part.to_string()))
                                .collect()
                        };
                        Ok(Value::Array(parts))
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "split() requires string elements in array",
                    )),
                })
                .collect();
            Ok(Value::Array(split_arrays?))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::Utf8 {
                        let split_series = series
                            .utf8()
                            .unwrap()
                            .apply(|s| {
                                s.and_then(|s| {
                                    let parts: Vec<String> = if separator.is_empty() {
                                        s.chars().map(|c| c.to_string()).collect()
                                    } else {
                                        s.split(&separator).map(|part| part.to_string()).collect()
                                    };
                                    Some(Cow::Owned(
                                        serde_json::to_string(&Value::Array(
                                            parts.into_iter().map(Value::String).collect(),
                                        ))
                                        .unwrap_or("null".to_string()),
                                    ))
                                })
                            })
                            .into_series();
                        let mut s = split_series;
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
                    "split() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::Utf8 {
                let split_series = series
                    .utf8()
                    .unwrap()
                    .apply(|s| {
                        s.and_then(|s| {
                            let parts: Vec<String> = if separator.is_empty() {
                                s.chars().map(|c| c.to_string()).collect()
                            } else {
                                s.split(&separator).map(|part| part.to_string()).collect()
                            };
                            Some(Cow::Owned(
                                serde_json::to_string(&Value::Array(
                                    parts.into_iter().map(Value::String).collect(),
                                ))
                                .unwrap_or("null".to_string()),
                            ))
                        })
                    })
                    .into_series();
                Ok(Value::Series(split_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "split() requires string, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "split",
        func: builtin_split,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_split_string_default_separator() {
        let result = builtin_split(&[Value::String("hello world".to_string())]).unwrap();
        match result {
            Value::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Value::String("hello".to_string()));
                assert_eq!(parts[1], Value::String("world".to_string()));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_split_string_custom_separator() {
        let result = builtin_split(&[
            Value::String("a,b,c".to_string()),
            Value::String(",".to_string()),
        ])
        .unwrap();
        match result {
            Value::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Value::String("a".to_string()));
                assert_eq!(parts[1], Value::String("b".to_string()));
                assert_eq!(parts[2], Value::String("c".to_string()));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_split_string_empty_separator() {
        let result = builtin_split(&[
            Value::String("abc".to_string()),
            Value::String("".to_string()),
        ])
        .unwrap();
        match result {
            Value::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Value::String("a".to_string()));
                assert_eq!(parts[1], Value::String("b".to_string()));
                assert_eq!(parts[2], Value::String("c".to_string()));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_split_array() {
        let arr = Value::Array(vec![
            Value::String("a b".to_string()),
            Value::String("c d".to_string()),
        ]);
        let result = builtin_split(&[arr]).unwrap();
        match result {
            Value::Array(arrays) => {
                assert_eq!(arrays.len(), 2);
                if let Value::Array(first) = &arrays[0] {
                    assert_eq!(first.len(), 2);
                    assert_eq!(first[0], Value::String("a".to_string()));
                    assert_eq!(first[1], Value::String("b".to_string()));
                } else {
                    panic!("Expected nested Array");
                }
                if let Value::Array(second) = &arrays[1] {
                    assert_eq!(second.len(), 2);
                    assert_eq!(second[0], Value::String("c".to_string()));
                    assert_eq!(second[1], Value::String("d".to_string()));
                } else {
                    panic!("Expected nested Array");
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_split_wrong_args() {
        let result = builtin_split(&[]);
        assert!(result.is_err());
        let result = builtin_split(&[Value::Int(1), Value::String(",".to_string()), Value::Int(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_non_string_in_array() {
        let arr = Value::Array(vec![Value::Int(1)]);
        let result = builtin_split(&[arr]);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_registered_via_inventory() {
        let mut found = false;
        for func in inventory::iter::<crate::FunctionRegistration> {
            if func.name == "split" {
                found = true;
                // Test that the function works
                let result = (func.func)(&[Value::String("test split".to_string())]).unwrap();
                match result {
                    Value::Array(parts) => {
                        assert_eq!(parts.len(), 2);
                        assert_eq!(parts[0], Value::String("test".to_string()));
                        assert_eq!(parts[1], Value::String("split".to_string()));
                    }
                    _ => panic!("Expected Array"),
                }
                break;
            }
        }
        assert!(found, "split function not found in inventory");
    }
}
