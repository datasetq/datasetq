use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_pluralize(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "pluralize() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(pluralize_word(s))),
        Value::Array(arr) => {
            let pluralized: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => Value::String(pluralize_word(s)),
                    _ => v.clone(),
                })
                .collect();
            Ok(Value::Array(pluralized))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::String {
                        let pluralized_series = series
                            .str()
                            .unwrap()
                            .apply(|s| s.map(|s| Cow::Owned(pluralize_word(s))))
                            .into_series();
                        let mut s = pluralized_series;
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
                    "pluralize() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::String {
                let pluralized_series = series
                    .str()
                    .unwrap()
                    .apply(|s| s.map(|s| Cow::Owned(pluralize_word(s))))
                    .into_series();
                Ok(Value::Series(pluralized_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Ok(args[0].clone()),
    }
}

fn pluralize_word(word: &str) -> String {
    let word_lower = word.to_lowercase();
    // Special cases
    match word_lower.as_str() {
        "child" => return "children".to_string(),
        "man" => return "men".to_string(),
        "woman" => return "women".to_string(),
        "ox" => return "oxen".to_string(),
        "goose" => return "geese".to_string(),
        "tooth" => return "teeth".to_string(),
        "foot" => return "feet".to_string(),
        _ => {}
    }

    // Regular rules
    if word_lower.ends_with("s")
        || word_lower.ends_with("sh")
        || word_lower.ends_with("ch")
        || word_lower.ends_with("x")
    {
        format!("{}es", word)
    } else if word_lower.ends_with("z") && word_lower.len() > 1 {
        let before_z = &word_lower[..word_lower.len() - 1];
        if is_vowel(before_z.chars().last().unwrap()) {
            format!("{}zzes", &word[..word.len() - 1])
        } else {
            format!("{}es", word)
        }
    } else if word_lower.ends_with("y") && word_lower.len() > 1 {
        let before_y = &word_lower[..word_lower.len() - 1];
        if !is_vowel(before_y.chars().last().unwrap()) {
            format!("{}ies", &word[..word.len() - 1])
        } else {
            format!("{}s", word)
        }
    } else if word_lower.ends_with("f") {
        format!("{}ves", &word[..word.len() - 1])
    } else if word_lower.ends_with("fe") {
        format!("{}ves", &word[..word.len() - 2])
    } else {
        format!("{}s", word)
    }
}

fn is_vowel(c: char) -> bool {
    matches!(c, 'a' | 'e' | 'i' | 'o' | 'u')
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "pluralize",
        func: builtin_pluralize,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_pluralize_string_basic() {
        let args = vec![Value::String("cat".to_string())];
        let result = builtin_pluralize(&args).unwrap();
        assert_eq!(result, Value::String("cats".to_string()));
    }

    #[test]
    fn test_pluralize_string_special_cases() {
        let test_cases = vec![
            ("child", "children"),
            ("man", "men"),
            ("woman", "women"),
            ("ox", "oxen"),
            ("goose", "geese"),
            ("tooth", "teeth"),
            ("foot", "feet"),
        ];

        for (input, expected) in test_cases {
            let args = vec![Value::String(input.to_string())];
            let result = builtin_pluralize(&args).unwrap();
            assert_eq!(
                result,
                Value::String(expected.to_string()),
                "Failed for {}",
                input
            );
        }
    }

    #[test]
    fn test_pluralize_string_regular_rules() {
        let test_cases = vec![
            ("dog", "dogs"),
            ("bush", "bushes"),
            ("church", "churches"),
            ("box", "boxes"),
            ("quiz", "quizzes"),
            ("lady", "ladies"),
            ("day", "days"),
            ("leaf", "leaves"),
            ("knife", "knives"),
            ("house", "houses"),
        ];

        for (input, expected) in test_cases {
            let args = vec![Value::String(input.to_string())];
            let result = builtin_pluralize(&args).unwrap();
            assert_eq!(
                result,
                Value::String(expected.to_string()),
                "Failed for {}",
                input
            );
        }
    }

    #[test]
    fn test_pluralize_array() {
        let arr = vec![
            Value::String("cat".to_string()),
            Value::String("dog".to_string()),
            Value::Int(42),
        ];
        let args = vec![Value::Array(arr)];
        let result = builtin_pluralize(&args).unwrap();

        if let Value::Array(result_arr) = result {
            assert_eq!(result_arr.len(), 3);
            assert_eq!(result_arr[0], Value::String("cats".to_string()));
            assert_eq!(result_arr[1], Value::String("dogs".to_string()));
            assert_eq!(result_arr[2], Value::Int(42));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_pluralize_non_string() {
        let args = vec![Value::Int(42)];
        let result = builtin_pluralize(&args).unwrap();
        assert_eq!(result, Value::Int(42));
    }

    #[test]
    fn test_pluralize_wrong_args() {
        let args = vec![];
        let result = builtin_pluralize(&args);
        assert!(result.is_err());

        let args = vec![
            Value::String("test".to_string()),
            Value::String("extra".to_string()),
        ];
        let result = builtin_pluralize(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_pluralize_empty_string() {
        let args = vec![Value::String("".to_string())];
        let result = builtin_pluralize(&args).unwrap();
        assert_eq!(result, Value::String("s".to_string()));
    }

    #[test]
    fn test_pluralize_single_char() {
        let args = vec![Value::String("a".to_string())];
        let result = builtin_pluralize(&args).unwrap();
        assert_eq!(result, Value::String("as".to_string()));
    }

    #[test]
    fn test_pluralize_y_edge_cases() {
        let args = vec![Value::String("y".to_string())];
        let result = builtin_pluralize(&args).unwrap();
        assert_eq!(result, Value::String("ys".to_string())); // y is vowel, so +s

        let args = vec![Value::String("my".to_string())];
        let result = builtin_pluralize(&args).unwrap();
        assert_eq!(result, Value::String("mies".to_string())); // m is consonant, so ies
    }

    #[test]
    fn test_pluralize_dataframe() {
        use polars::prelude::*;
        let s1 = Series::new("words".into().into(), &["cat", "dog", "child"]);
        let s2 = Series::new("numbers".into().into(), &[1, 2, 3]);
        let df = DataFrame::new(vec![s1, s2]).unwrap();

        let args = vec![Value::DataFrame(df)];
        let result = builtin_pluralize(&args).unwrap();

        if let Value::DataFrame(result_df) = result {
            let words_series = result_df.column("words").unwrap();
            let words: Vec<String> = words_series
                .str()
                .unwrap()
                .into_iter()
                .map(|s| s.unwrap().to_string())
                .collect();
            assert_eq!(words, vec!["cats", "dogs", "children"]);

            let numbers_series = result_df.column("numbers").unwrap();
            assert_eq!(
                numbers_series,
                &Series::new("numbers".into().into(), &[1, 2, 3])
            );
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_pluralize_series() {
        use polars::prelude::*;
        let series = Series::new("words".into().into(), &["cat", "dog", "child"]);

        let args = vec![Value::Series(series.clone())];
        let result = builtin_pluralize(&args).unwrap();

        if let Value::Series(result_series) = result {
            let words: Vec<String> = result_series
                .str()
                .unwrap()
                .into_iter()
                .map(|s| s.unwrap().to_string())
                .collect();
            assert_eq!(words, vec!["cats", "dogs", "children"]);
        } else {
            panic!("Expected Series");
        }
    }

    #[test]
    fn test_pluralize_series_non_string() {
        use polars::prelude::*;
        let series = Series::new("numbers".into().into(), &[1, 2, 3]);

        let args = vec![Value::Series(series.clone())];
        let result = builtin_pluralize(&args).unwrap();

        if let Value::Series(result_series) = result {
            assert_eq!(result_series, series);
        } else {
            panic!("Expected Series");
        }
    }
}
