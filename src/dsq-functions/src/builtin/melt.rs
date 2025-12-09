use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use inventory;
use polars::datatypes::PlSmallStr;
use polars::prelude::{DataFrame, NamedFrom, Series};

pub fn builtin_melt(args: &[Value]) -> Result<Value> {
    if args.len() < 1 || args.len() > 3 {
        return Err(dsq_shared::error::operation_error(
            "melt() expects 1-3 arguments",
        ));
    }

    match &args[0] {
        Value::DataFrame(df) => {
            let id_vars = if args.len() >= 2 {
                match &args[1] {
                    Value::Array(arr) => {
                        let mut vars = Vec::new();
                        for v in arr {
                            if let Value::String(s) = v {
                                vars.push(s.clone());
                            }
                        }
                        vars
                    }
                    Value::String(s) => vec![s.clone()],
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "melt() id_vars must be string or array of strings",
                        ));
                    }
                }
            } else {
                // No id variables specified, default to first column
                df.get_column_names()
                    .first()
                    .map(|s| vec![s.to_string()])
                    .unwrap_or(vec![])
            };

            let value_vars = if args.len() >= 3 {
                match &args[2] {
                    Value::Array(arr) => {
                        let mut vars = Vec::new();
                        for v in arr {
                            if let Value::String(s) = v {
                                vars.push(s.clone());
                            }
                        }
                        vars
                    }
                    Value::String(s) => vec![s.clone()],
                    _ => {
                        return Err(dsq_shared::error::operation_error(
                            "melt() value_vars must be string or array of strings",
                        ));
                    }
                }
            } else {
                // All columns except id_vars
                df.get_column_names()
                    .iter()
                    .filter(|name| !id_vars.contains(&name.to_string()))
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            };

            let mut variable_values = Vec::new();
            let mut value_values = Vec::new();
            let mut id_columns_data: Vec<Vec<String>> = vec![vec![]; id_vars.len()];

            // Melt each value column
            for value_var in &value_vars {
                if let Ok(series) = df.column(value_var) {
                    for i in 0..df.height() {
                        // Add variable name
                        variable_values.push(value_var.clone());

                        // Add value as string
                        match series.get(i) {
                            Ok(val) => {
                                let value = value_from_any_value(val).unwrap_or(Value::Null);
                                value_values.push(format!("{}", value));
                            }
                            _ => {
                                value_values.push("null".to_string());
                            }
                        }

                        // Add id values
                        for (j, id_var) in id_vars.iter().enumerate() {
                            if let Ok(id_series) = df.column(id_var) {
                                match id_series.get(i) {
                                    Ok(id_val) => {
                                        let id_value =
                                            value_from_any_value(id_val).unwrap_or(Value::Null);
                                        id_columns_data[j].push(format!("{}", id_value));
                                    }
                                    _ => {
                                        id_columns_data[j].push("null".to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Create new DataFrame
            let mut new_series = Vec::new();

            // Add id columns
            for (i, id_var) in id_vars.iter().enumerate() {
                new_series.push(Series::new(id_var.clone().into(), &id_columns_data[i]).into());
            }

            // Add variable column
            new_series.push(Series::new(PlSmallStr::from("variable"), &variable_values).into());

            // Add value column
            new_series.push(Series::new(PlSmallStr::from("value"), &value_values).into());

            match DataFrame::new(new_series) {
                Ok(melted_df) => Ok(Value::DataFrame(melted_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "melt() failed: {}",
                    e
                ))),
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "melt() requires DataFrame argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "melt",
        func: builtin_melt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::{Column, DataFrame, NamedFrom, Series};

    #[test]
    fn test_builtin_melt_basic() {
        // Create a simple DataFrame
        let series1 = Series::new(PlSmallStr::from("id"), &[1i64, 2i64]);
        let series2 = Series::new(PlSmallStr::from("A"), &[10i64, 20i64]);
        let series3 = Series::new(PlSmallStr::from("B"), &[100i64, 200i64]);
        let df = DataFrame::new(vec![series1.into(), series2.into(), series3.into()]).unwrap();

        let result = builtin_melt(&[Value::DataFrame(df)]).unwrap();

        if let Value::DataFrame(melted_df) = result {
            // Should have 4 rows (2 original rows * 2 value columns)
            assert_eq!(melted_df.height(), 4);
            // Should have 3 columns: id, variable, value
            assert_eq!(melted_df.width(), 3);

            // Check column names
            let col_names = melted_df.get_column_names();
            assert!(col_names.contains(&&"id".into()));
            assert!(col_names.contains(&&"variable".into()));
            assert!(col_names.contains(&&"value".into()));
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_builtin_melt_with_id_vars() {
        // Create a DataFrame
        let series1 = Series::new("id".into().into(), vec![1i64, 2]);
        let series2 = Series::new("A".into().into(), vec![10i64, 20]);
        let series3 = Series::new("B".into().into(), vec![100i64, 200]);
        let df = DataFrame::new(vec![series1, series2, series3]).unwrap();

        let result =
            builtin_melt(&[Value::DataFrame(df), Value::String("id".to_string())]).unwrap();

        if let Value::DataFrame(melted_df) = result {
            assert_eq!(melted_df.height(), 4);
            assert_eq!(melted_df.width(), 3);
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_builtin_melt_with_value_vars() {
        // Create a DataFrame
        let series1 = Series::new("id".into().into(), vec![1i64, 2]);
        let series2 = Series::new("A".into().into(), vec![10i64, 20]);
        let series3 = Series::new("B".into().into(), vec![100i64, 200]);
        let series4 = Series::new("C".into().into(), vec![1000i64, 2000]);
        let df = DataFrame::new(vec![
            series1.into(),
            series2.into(),
            series3.into(),
            series4.into(),
        ])
        .unwrap();

        let result = builtin_melt(&[
            Value::DataFrame(df),
            Value::String("id".to_string()),
            Value::Array(vec![
                Value::String("A".to_string()),
                Value::String("B".to_string()),
            ]),
        ])
        .unwrap();

        if let Value::DataFrame(melted_df) = result {
            // Should have 4 rows (2 original rows * 2 value columns)
            assert_eq!(melted_df.height(), 4);
            assert_eq!(melted_df.width(), 3);
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_builtin_melt_invalid_args() {
        // No arguments
        let result = builtin_melt(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1-3 arguments"));

        // Too many arguments
        let df = DataFrame::new(vec![Series::new("col".into(), vec![1i64])]).unwrap();
        let result = builtin_melt(&[
            Value::DataFrame(df),
            Value::String("id".to_string()),
            Value::String("val".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1-3 arguments"));
    }

    #[test]
    fn test_builtin_melt_invalid_id_vars() {
        let df = DataFrame::new(vec![Series::new("col".into(), vec![1i64])]).unwrap();
        let result = builtin_melt(&[
            Value::DataFrame(df),
            Value::Int(123), // Invalid id_vars type
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("id_vars must be string or array of strings"));
    }

    #[test]
    fn test_builtin_melt_invalid_value_vars() {
        let df = DataFrame::new(vec![Series::new("col".into(), vec![1i64])]).unwrap();
        let result = builtin_melt(&[
            Value::DataFrame(df),
            Value::String("id".to_string()),
            Value::Int(123), // Invalid value_vars type
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("value_vars must be string or array of strings"));
    }

    #[test]
    fn test_builtin_melt_non_dataframe() {
        let result = builtin_melt(&[Value::Int(42)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires DataFrame argument"));
    }

    #[test]
    fn test_melt_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("melt"));
    }
}
