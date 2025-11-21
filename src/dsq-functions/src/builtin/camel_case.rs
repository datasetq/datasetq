use dsq_shared::value::Value;
use dsq_shared::Result;
use heck::ToLowerCamelCase;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_camel_case(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "camel_case() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => return Ok(Value::String(s.to_lower_camel_case())),
        Value::Array(arr) => {
            let converted: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => Value::String(s.to_lower_camel_case()),
                    _ => Value::String(v.to_string().to_lower_camel_case()),
                })
                .collect();
            return Ok(Value::Array(converted));
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::Utf8 {
                        let camel_series = series
                            .utf8()
                            .unwrap()
                            .apply(|s| s.map(|s| Cow::Owned(s.to_lower_camel_case())))
                            .into_series();
                        let mut s = camel_series;
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
                Ok(new_df) => return Ok(Value::DataFrame(new_df)),
                Err(e) => {
                    return Err(dsq_shared::error::operation_error(format!(
                        "camel_case() failed on DataFrame: {}",
                        e
                    )))
                }
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::Utf8 {
                let camel_series = series
                    .utf8()
                    .unwrap()
                    .apply(|s| s.map(|s| Cow::Owned(s.to_lower_camel_case())))
                    .into_series();
                return Ok(Value::Series(camel_series));
            } else {
                return Ok(Value::Series(series.clone()));
            }
        }
        _ => {
            return Err(dsq_shared::error::operation_error(
                "camel_case() requires string, array, DataFrame, or Series",
            ))
        }
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "camel_case",
        func: builtin_camel_case,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::*;

    #[test]
    fn test_builtin_camel_case_string() {
        let result = builtin_camel_case(&[Value::String("hello_world".to_string())]).unwrap();
        assert_eq!(result, Value::String("helloWorld".to_string()));

        let result =
            builtin_camel_case(&[Value::String("snake_case_example".to_string())]).unwrap();
        assert_eq!(result, Value::String("snakeCaseExample".to_string()));

        let result = builtin_camel_case(&[Value::String("already-camel".to_string())]).unwrap();
        assert_eq!(result, Value::String("alreadyCamel".to_string()));
    }

    #[test]
    fn test_builtin_camel_case_array() {
        let arr = vec![
            Value::String("hello_world".to_string()),
            Value::String("snake_case".to_string()),
            Value::Int(123),
        ];
        let result = builtin_camel_case(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(result_arr) => {
                assert_eq!(result_arr.len(), 3);
                assert_eq!(result_arr[0], Value::String("helloWorld".to_string()));
                assert_eq!(result_arr[1], Value::String("snakeCase".to_string()));
                assert_eq!(result_arr[2], Value::String("123".to_string()));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_builtin_camel_case_dataframe() {
        let mut df = DataFrame::new(vec![
            Series::new("col1", &["hello_world", "snake_case"]),
            Series::new("col2", &[1, 2]),
        ])
        .unwrap();

        let result = builtin_camel_case(&[Value::DataFrame(df)]).unwrap();
        match result {
            Value::DataFrame(result_df) => {
                let col1 = result_df.column("col1").unwrap().utf8().unwrap();
                assert_eq!(col1.get(0).unwrap(), "helloWorld");
                assert_eq!(col1.get(1).unwrap(), "snakeCase");

                let col2 = result_df.column("col2").unwrap();
                assert_eq!(col2.get(0).unwrap(), AnyValue::Int32(1));
                assert_eq!(col2.get(1).unwrap(), AnyValue::Int32(2));
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_builtin_camel_case_series() {
        let series = Series::new("test", &["hello_world", "snake_case"]);

        let result = builtin_camel_case(&[Value::Series(series)]).unwrap();
        match result {
            Value::Series(result_series) => {
                let utf8_series = result_series.utf8().unwrap();
                assert_eq!(utf8_series.get(0).unwrap(), "helloWorld");
                assert_eq!(utf8_series.get(1).unwrap(), "snakeCase");
            }
            _ => panic!("Expected Series"),
        }
    }

    #[test]
    fn test_builtin_camel_case_invalid_args() {
        let result = builtin_camel_case(&[]);
        assert!(result.is_err());

        let result = builtin_camel_case(&[
            Value::String("test".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());

        let result = builtin_camel_case(&[Value::Int(123)]);
        assert!(result.is_err());
    }
}
