use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_singular(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "singular() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(singularize_word(s))),
        Value::Array(arr) => {
            let singularized: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => Value::String(singularize_word(s)),
                    _ => v.clone(),
                })
                .collect();
            Ok(Value::Array(singularized))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::String {
                        let singularized_series = series
                            .str()
                            .unwrap()
                            .apply(|s| s.map(|s| Cow::Owned(singularize_word(s))))
                            .into_series();
                        let mut s = singularized_series;
                        s.rename(col_name.clone());
                        new_series.push(s.into());
                    } else {
                        let mut s = series.clone();
                        s.rename(col_name.clone());
                        new_series.push(s.into());
                    }
                }
            }
            match DataFrame::new(new_series) {
                Ok(new_df) => Ok(Value::DataFrame(new_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "singular() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let singularized_series = series
                    .str()
                    .unwrap()
                    .apply(|s| s.map(|s| Cow::Owned(singularize_word(s))))
                    .into_series();
                Ok(Value::Series(singularized_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Ok(args[0].clone()),
    }
}

fn singularize_word(word: &str) -> String {
    let word_lower = word.to_lowercase();
    // Special cases
    match word_lower.as_str() {
        "children" => return "child".to_string(),
        "men" => return "man".to_string(),
        "women" => return "woman".to_string(),
        "oxen" => return "ox".to_string(),
        "geese" => return "goose".to_string(),
        "teeth" => return "tooth".to_string(),
        "feet" => return "foot".to_string(),
        "mice" => return "mouse".to_string(),
        "houses" => return "house".to_string(),
        "knives" => return "knife".to_string(),
        _ => {}
    }

    // Regular rules
    if word_lower.ends_with("ies") && word.len() > 3 {
        let before_ies = &word[..word.len() - 3];
        format!("{}y", before_ies)
    } else if word_lower.ends_with("ves") && word.len() > 3 {
        let base_len = word.len() - 3;
        let before_ves = &word[..base_len];
        format!("{}f", before_ves)
    } else if word_lower.ends_with("es") && word.len() > 2 {
        let before_es = &word[..word.len() - 2];
        if before_es.ends_with("s")
            || before_es.ends_with("sh")
            || before_es.ends_with("ch")
            || before_es.ends_with("x")
            || before_es.ends_with("z")
        {
            before_es.to_string()
        } else {
            // For words like "houses", remove just "s" not "es"
            let before_s = &word[..word.len() - 1];
            before_s.to_string()
        }
    } else if word_lower.ends_with("s") && word.len() > 1 {
        let before_s = &word[..word.len() - 1];
        // Don't singularize if it's already singular or special
        if word_lower.ends_with("ss") || word_lower.ends_with("us") || word_lower == "bus" {
            word.to_string()
        } else {
            before_s.to_string()
        }
    } else {
        word.to_string()
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "singular",
        func: builtin_singular,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_singular_basic() {
        let result = builtin_singular(&[Value::String("cats".to_string())]).unwrap();
        assert_eq!(result, Value::String("cat".to_string()));
    }

    #[test]
    fn test_singular_special_cases() {
        let test_cases = vec![
            ("children", "child"),
            ("men", "man"),
            ("women", "woman"),
            ("oxen", "ox"),
            ("geese", "goose"),
            ("teeth", "tooth"),
            ("feet", "foot"),
            ("mice", "mouse"),
        ];

        for (input, expected) in test_cases {
            let result = builtin_singular(&[Value::String(input.to_string())]).unwrap();
            assert_eq!(
                result,
                Value::String(expected.to_string()),
                "Failed for input: {}",
                input
            );
        }
    }

    #[test]
    fn test_singular_regular_rules() {
        let test_cases = vec![
            ("dogs", "dog"),
            ("boxes", "box"),
            ("wishes", "wish"),
            ("churches", "church"),
            ("buses", "bus"),
            ("houses", "house"),
            ("babies", "baby"),
            ("leaves", "leaf"),
            ("wolves", "wolf"),
            ("knives", "knife"),
        ];

        for (input, expected) in test_cases {
            let result = builtin_singular(&[Value::String(input.to_string())]).unwrap();
            assert_eq!(
                result,
                Value::String(expected.to_string()),
                "Failed for input: {}",
                input
            );
        }
    }

    #[test]
    fn test_singular_already_singular() {
        let test_cases = vec!["cat", "dog", "house", "class", "glass", "bus", "access"];

        for input in test_cases {
            let result = builtin_singular(&[Value::String(input.to_string())]).unwrap();
            assert_eq!(
                result,
                Value::String(input.to_string()),
                "Failed for input: {}",
                input
            );
        }
    }

    #[test]
    fn test_singular_array() {
        let arr = vec![
            Value::String("cats".to_string()),
            Value::String("dogs".to_string()),
            Value::Int(42),
        ];
        let result = builtin_singular(&[Value::Array(arr)]).unwrap();
        match result {
            Value::Array(res_arr) => {
                assert_eq!(res_arr.len(), 3);
                assert_eq!(res_arr[0], Value::String("cat".to_string()));
                assert_eq!(res_arr[1], Value::String("dog".to_string()));
                assert_eq!(res_arr[2], Value::Int(42));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_singular_no_args() {
        let result = builtin_singular(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_singular_too_many_args() {
        let result = builtin_singular(&[
            Value::String("cats".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_singular_non_string() {
        let result = builtin_singular(&[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_singular_empty_string() {
        let result = builtin_singular(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_singular_single_char() {
        let result = builtin_singular(&[Value::String("a".to_string())]).unwrap();
        assert_eq!(result, Value::String("a".to_string()));
    }

    #[test]
    fn test_singular_single_s() {
        let result = builtin_singular(&[Value::String("s".to_string())]).unwrap();
        assert_eq!(result, Value::String("s".to_string()));
    }
}
