use dsq_shared::value::Value;
use dsq_shared::Result;
use heck::ToSnakeCase;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_snake_case(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "snake_case() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.to_snake_case())),
        Value::Array(arr) => {
            let converted: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => Value::String(s.to_snake_case()),
                    _ => Value::String(v.to_string().to_snake_case()),
                })
                .collect();
            Ok(Value::Array(converted))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::String {
                        let snake_series = series
                            .str()
                            .unwrap()
                            .apply(|s| s.map(|s| Cow::Owned(s.to_snake_case())))
                            .into_series();
                        let mut s = snake_series;
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
                    "snake_case() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let snake_series = series
                    .str()
                    .unwrap()
                    .apply(|s| s.map(|s| Cow::Owned(s.to_snake_case())))
                    .into_series();
                Ok(Value::Series(snake_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        Value::LazyFrame(lf) => {
            // Collect the LazyFrame to DataFrame and recursively call
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            builtin_snake_case(&[Value::DataFrame(df)])
        }
        _ => Err(dsq_shared::error::operation_error(
            "snake_case() requires string, array, DataFrame, Series, or LazyFrame",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "snake_case",
        func: builtin_snake_case,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_snake_case_string() {
        let result = builtin_snake_case(&[Value::String("CamelCase".to_string())]).unwrap();
        assert_eq!(result, Value::String("camel_case".to_string()));

        let result = builtin_snake_case(&[Value::String("XMLHttpRequest".to_string())]).unwrap();
        assert_eq!(result, Value::String("xml_http_request".to_string()));

        let result = builtin_snake_case(&[Value::String("already_snake".to_string())]).unwrap();
        assert_eq!(result, Value::String("already_snake".to_string()));
    }

    #[test]
    fn test_builtin_snake_case_array() {
        let arr = vec![
            Value::String("CamelCase".to_string()),
            Value::String("XMLHttpRequest".to_string()),
            Value::Int(123),
        ];
        let result = builtin_snake_case(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(result_arr) => {
                assert_eq!(result_arr.len(), 3);
                assert_eq!(result_arr[0], Value::String("camel_case".to_string()));
                assert_eq!(result_arr[1], Value::String("xml_http_request".to_string()));
                assert_eq!(result_arr[2], Value::String("123".to_string()));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_builtin_snake_case_dataframe() {
        let df = DataFrame::new(vec![
            Series::new(PlSmallStr::from("col1"), &["CamelCase", "XMLHttpRequest"]).into(),
            Series::new(PlSmallStr::from("col2"), &[1, 2]).into(),
        ])
        .unwrap();

        let result = builtin_snake_case(&[Value::DataFrame(df)]).unwrap();
        match result {
            Value::DataFrame(result_df) => {
                let col1 = result_df.column("col1").unwrap().str().unwrap();
                assert_eq!(col1.get(0).unwrap(), "camel_case");
                assert_eq!(col1.get(1).unwrap(), "xml_http_request");

                let col2 = result_df.column("col2").unwrap();
                assert_eq!(col2.get(0).unwrap(), AnyValue::Int32(1));
                assert_eq!(col2.get(1).unwrap(), AnyValue::Int32(2));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_builtin_snake_case_series() {
        let series = Series::new(PlSmallStr::from("test"), &["CamelCase", "XMLHttpRequest"]);

        let result = builtin_snake_case(&[Value::Series(series)]).unwrap();
        match result {
            Value::Series(result_series) => {
                let utf8_series = result_series.str().unwrap();
                assert_eq!(utf8_series.get(0).unwrap(), "camel_case");
                assert_eq!(utf8_series.get(1).unwrap(), "xml_http_request");
            }
            _ => panic!("Expected Series"),
        }
    }

    #[test]
    fn test_builtin_snake_case_invalid_args() {
        let result = builtin_snake_case(&[]);
        assert!(result.is_err());

        let result = builtin_snake_case(&[
            Value::String("test".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());

        let result = builtin_snake_case(&[Value::Int(123)]);
        assert!(result.is_err());
    }
}
