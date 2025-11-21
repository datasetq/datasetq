use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::borrow::Cow;

pub fn builtin_transliterate(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(dsq_shared::error::operation_error(
            "transliterate() expects 3 arguments: text, from_script, to_script",
        ));
    }

    let from_script = match &args[1] {
        Value::String(s) => s.to_lowercase(),
        _ => {
            return Err(dsq_shared::error::operation_error(
                "transliterate() second argument must be a string",
            ))
        }
    };

    let to_script = match &args[2] {
        Value::String(s) => s.to_lowercase(),
        _ => {
            return Err(dsq_shared::error::operation_error(
                "transliterate() third argument must be a string",
            ))
        }
    };

    if from_script != "cyrillic" || to_script != "latin" {
        return Err(dsq_shared::error::operation_error(format!(
            "transliterate() from '{}' to '{}' not supported",
            from_script, to_script
        )));
    }

    match &args[0] {
        Value::String(s) => {
            let transliterated = cyrillic_to_latin(s);
            Ok(Value::String(transliterated))
        }
        Value::Array(arr) => {
            let transliterated: Vec<Value> = arr
                .iter()
                .map(|v| {
                    match v {
                        Value::String(s) => Value::String(cyrillic_to_latin(s)),
                        _ => v.clone(), // Leave non-string values unchanged
                    }
                })
                .collect();
            Ok(Value::Array(transliterated))
        }
        Value::DataFrame(df) => {
            let mut new_series = Vec::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    if series.dtype() == &DataType::Utf8 {
                        let transliterate_series = series
                            .utf8()
                            .unwrap()
                            .apply(|s| s.map(|s| Cow::Owned(cyrillic_to_latin(s))))
                            .into_series();
                        let mut s = transliterate_series;
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
                    "transliterate() failed on DataFrame: {}",
                    e
                ))),
            }
        }
        Value::Series(series) => {
            if series.dtype() == &DataType::Utf8 {
                let transliterate_series = series
                    .utf8()
                    .unwrap()
                    .apply(|s| s.map(|s| Cow::Owned(cyrillic_to_latin(s))))
                    .into_series();
                Ok(Value::Series(transliterate_series))
            } else {
                Ok(Value::Series(series.clone()))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "transliterate() first argument must be a string, array, DataFrame, or Series",
        )),
    }
}

fn cyrillic_to_latin(text: &str) -> String {
    let mapping = [
        ("А", "A"),
        ("а", "a"),
        ("Б", "B"),
        ("б", "b"),
        ("В", "V"),
        ("в", "v"),
        ("Г", "G"),
        ("г", "g"),
        ("Д", "D"),
        ("д", "d"),
        ("Е", "E"),
        ("е", "e"),
        ("Ё", "E"),
        ("ё", "e"),
        ("Ж", "Zh"),
        ("ж", "zh"),
        ("З", "Z"),
        ("з", "z"),
        ("И", "I"),
        ("и", "i"),
        ("Й", "Y"),
        ("й", "y"),
        ("К", "K"),
        ("к", "k"),
        ("Л", "L"),
        ("л", "l"),
        ("М", "M"),
        ("м", "m"),
        ("Н", "N"),
        ("н", "n"),
        ("О", "O"),
        ("о", "o"),
        ("П", "P"),
        ("п", "p"),
        ("Р", "R"),
        ("р", "r"),
        ("С", "S"),
        ("с", "s"),
        ("Т", "T"),
        ("т", "t"),
        ("У", "U"),
        ("у", "u"),
        ("Ф", "F"),
        ("ф", "f"),
        ("Х", "Kh"),
        ("х", "kh"),
        ("Ц", "Ts"),
        ("ц", "ts"),
        ("Ч", "Ch"),
        ("ч", "ch"),
        ("Ш", "Sh"),
        ("ш", "sh"),
        ("Щ", "Shch"),
        ("щ", "shch"),
        ("Ъ", "'"),
        ("ъ", "'"),
        ("Ы", "Y"),
        ("ы", "y"),
        ("Ь", "'"),
        ("ь", "'"),
        ("Э", "E"),
        ("э", "e"),
        ("Ю", "Yu"),
        ("ю", "yu"),
        ("Я", "Ya"),
        ("я", "ya"),
    ];

    let mut result = text.to_string();
    for (cyrillic, latin) in mapping.iter() {
        result = result.replace(cyrillic, latin);
    }
    result
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "transliterate",
        func: builtin_transliterate,
    }
}
