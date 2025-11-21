use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;
use url::Url;

pub fn builtin_url_extract_port(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "url_extract_port() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => match Url::parse(s) {
            Ok(url) => Ok(url
                .port()
                .map(|p| Value::Int(p as i64))
                .unwrap_or(Value::Null)),
            Err(_) => Ok(Value::Null),
        },
        Value::Array(arr) => {
            let extracted: Vec<Value> =
                arr.iter()
                    .map(|v| match v {
                        Value::String(s) => builtin_url_extract_port(&[Value::String(s.clone())])
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
                    if series.dtype() == &DataType::Utf8 {
                        let extracted_series = series
                            .utf8()
                            .map_err(|e| {
                                dsq_shared::error::operation_error(format!(
                                    "url_extract_port() failed to cast series to utf8: {}",
                                    e
                                ))
                            })?
                            .apply(|s| {
                                s.and_then(|s| match Url::parse(s) {
                                    Ok(url) => url.port().map(|p| Cow::Owned(p.to_string())),
                                    Err(_) => None,
                                })
                            })
                            .into_series();
                        let mut s = extracted_series;
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
                    "url_extract_port() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::Utf8 {
                let extracted_series = series
                    .utf8()
                    .map_err(|e| {
                        dsq_shared::error::operation_error(format!(
                            "url_extract_port() failed to cast series to utf8: {}",
                            e
                        ))
                    })?
                    .apply(|s| {
                        s.and_then(|s| match Url::parse(s) {
                            Ok(url) => url.port().map(|p| Cow::Owned(p.to_string())),
                            Err(_) => None,
                        })
                    })
                    .into_series();
                Ok(Value::Series(extracted_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "url_extract_port() requires string, array, DataFrame, or Series",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "url_extract_port",
        func: builtin_url_extract_port,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;

    #[test]
    fn test_url_extract_port_string() {
        let url = "https://example.com:8080/path";
        let result = builtin_url_extract_port(&[Value::String(url.to_string())]).unwrap();
        assert_eq!(result, Value::Int(8080));
    }

    #[test]
    fn test_url_extract_port_no_port() {
        let url = "https://example.com/path";
        let result = builtin_url_extract_port(&[Value::String(url.to_string())]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_url_extract_port_invalid_url() {
        let url = "not a url";
        let result = builtin_url_extract_port(&[Value::String(url.to_string())]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_url_extract_port_array() {
        let urls = vec![
            Value::String("https://example.com:8080".to_string()),
            Value::String("http://test.com".to_string()),
        ];
        let result = builtin_url_extract_port(&[Value::Array(urls)]).unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Value::Int(8080));
            assert_eq!(arr[1], Value::Null);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_url_extract_port_dataframe() {
        let urls = Series::new("urls", &["https://example.com:8080", "http://test.com"]);
        let df = DataFrame::new(vec![urls]).unwrap();
        let result = builtin_url_extract_port(&[Value::DataFrame(df)]).unwrap();
        if let Value::DataFrame(result_df) = result {
            let col = result_df.column("urls").unwrap();
            let values: Vec<Option<&str>> = col.utf8().unwrap().into_iter().collect();
            assert_eq!(values, vec![Some("8080"), None]);
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_url_extract_port_series() {
        let urls = Series::new("urls", &["https://example.com:8080", "http://test.com"]);
        let result = builtin_url_extract_port(&[Value::Series(urls)]).unwrap();
        if let Value::Series(result_series) = result {
            let values: Vec<Option<&str>> = result_series.utf8().unwrap().into_iter().collect();
            assert_eq!(values, vec![Some("8080"), None]);
        } else {
            panic!("Expected Series");
        }
    }

    #[test]
    fn test_url_extract_port_invalid_args() {
        let result = builtin_url_extract_port(&[]);
        assert!(result.is_err());

        let result = builtin_url_extract_port(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());

        let result = builtin_url_extract_port(&[Value::Int(1)]);
        assert!(result.is_err());
    }
}
