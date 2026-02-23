use dsq_shared::error::operation_error;
use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use inventory;
use polars::prelude::*;
use std::collections::HashMap;

use crate::compare_values_for_sorting;

fn dataframe_to_objects(df: &DataFrame) -> Vec<HashMap<String, Value>> {
    let mut objects = Vec::new();
    for i in 0..df.height() {
        let mut obj = HashMap::new();
        for col_name in df.get_column_names() {
            if let Ok(series) = df.column(col_name) {
                if let Ok(val) = series.get(i) {
                    let value = value_from_any_value(val).unwrap_or(Value::Null);
                    obj.insert(col_name.to_string(), value);
                }
            }
        }
        objects.push(obj);
    }
    objects
}

pub fn builtin_group_by(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(operation_error("group_by() expects 2 arguments"));
    }

    match (&args[0], &args[1]) {
        (Value::LazyFrame(lf), Value::String(column)) => {
            // For LazyFrame, collect to DataFrame and recursively call
            let df = lf
                .clone()
                .collect()
                .map_err(|e| operation_error(format!("Failed to collect LazyFrame: {}", e)))?;
            builtin_group_by(&[Value::DataFrame(df), Value::String(column.clone())])
        }
        (Value::Array(arr), Value::Array(keys)) if arr.len() == keys.len() => {
            // Group array by keys
            let mut groups: HashMap<String, Vec<Value>> = HashMap::new();
            for (item, key) in arr.iter().zip(keys.iter()) {
                let key_str = match key {
                    Value::String(s) => s.clone(),
                    Value::Int(i) => i.to_string(),
                    Value::Float(f) => f.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => "".to_string(),
                };
                groups.entry(key_str).or_default().push(item.clone());
            }
            let mut result: Vec<Value> = groups.into_values().map(Value::Array).collect();
            // Sort groups by key
            result.sort_by(|a, b| {
                if let (Value::Array(a_arr), Value::Array(b_arr)) = (a, b) {
                    if let (Some(a_item), Some(b_item)) = (a_arr.first(), b_arr.first()) {
                        // Find the key for a_item and b_item
                        // This is inefficient but works for now
                        let mut a_key = None;
                        let mut b_key = None;
                        for (item, key) in arr.iter().zip(keys.iter()) {
                            if item == a_item {
                                a_key = Some(match key {
                                    Value::String(s) => s.clone(),
                                    Value::Int(i) => i.to_string(),
                                    Value::Float(f) => f.to_string(),
                                    Value::Bool(b) => b.to_string(),
                                    _ => "".to_string(),
                                });
                            }
                            if item == b_item {
                                b_key = Some(match key {
                                    Value::String(s) => s.clone(),
                                    Value::Int(i) => i.to_string(),
                                    Value::Float(f) => f.to_string(),
                                    Value::Bool(b) => b.to_string(),
                                    _ => "".to_string(),
                                });
                            }
                        }
                        if let (Some(a_key), Some(b_key)) = (a_key, b_key) {
                            a_key.cmp(&b_key)
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    } else {
                        std::cmp::Ordering::Equal
                    }
                } else {
                    std::cmp::Ordering::Equal
                }
            });
            Ok(Value::Array(result))
        }
        (Value::Array(arr), Value::String(field)) => {
            // Group array of objects by field
            let mut groups: HashMap<String, Vec<Value>> = HashMap::new();
            for item in arr {
                if let Value::Object(obj) = item {
                    let key = if let Some(Value::String(s)) = obj.get(field) {
                        s.clone()
                    } else {
                        "".to_string()
                    };
                    groups.entry(key).or_default().push(item.clone());
                }
            }
            let mut result: Vec<Value> = groups.into_values().map(Value::Array).collect();
            // Sort groups by first element's field for consistency
            result.sort_by(|a, b| {
                if let (Value::Array(a_arr), Value::Array(b_arr)) = (a, b) {
                    if let (Some(Value::Object(a_obj)), Some(Value::Object(b_obj))) =
                        (a_arr.first(), b_arr.first())
                    {
                        if let (Some(Value::String(a_key)), Some(Value::String(b_key))) =
                            (a_obj.get(field), b_obj.get(field))
                        {
                            a_key.cmp(b_key)
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    } else {
                        std::cmp::Ordering::Equal
                    }
                } else {
                    std::cmp::Ordering::Equal
                }
            });
            Ok(Value::Array(result))
        }
        (Value::DataFrame(df), Value::String(column)) => {
            // Group DataFrame by column
            match df.group_by([column.as_str()]) {
                Ok(grouped) => {
                    match grouped.groups() {
                        Ok(groups_df) => {
                            // groups_df has columns for the grouping keys and a "groups" column with indices
                            let mut result_groups = Vec::new();
                            for i in 0..groups_df.height() {
                                if let Ok(group_indices) = groups_df.column("groups") {
                                    if let Ok(AnyValue::List(list)) = group_indices.get(i) {
                                        let indices: Vec<u32> = list
                                            .iter()
                                            .filter_map(|v| match v {
                                                AnyValue::UInt32(idx) => Some(idx),
                                                _ => None,
                                            })
                                            .collect();
                                        if !indices.is_empty() {
                                            let indices_ca =
                                                UInt32Chunked::from_vec("indices".into(), indices);
                                            match df.take(&indices_ca) {
                                                Ok(group_df) => {
                                                    let objects = dataframe_to_objects(&group_df);
                                                    result_groups.push(Value::Array(
                                                        objects
                                                            .into_iter()
                                                            .map(Value::Object)
                                                            .collect(),
                                                    ));
                                                }
                                                Err(e) => {
                                                    return Err(operation_error(format!(
                                                        "group_by() take failed: {}",
                                                        e
                                                    )));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // Sort groups by the grouping key for consistency
                            result_groups.sort_by(|a, b| {
                                if let (Value::Array(a_arr), Value::Array(b_arr)) = (a, b) {
                                    if let (
                                        Some(Value::Object(a_obj)),
                                        Some(Value::Object(b_obj)),
                                    ) = (a_arr.first(), b_arr.first())
                                    {
                                        if let (Some(a_val), Some(b_val)) =
                                            (a_obj.get(column.as_str()), b_obj.get(column.as_str()))
                                        {
                                            compare_values_for_sorting(a_val, b_val)
                                        } else {
                                            std::cmp::Ordering::Equal
                                        }
                                    } else {
                                        std::cmp::Ordering::Equal
                                    }
                                } else {
                                    std::cmp::Ordering::Equal
                                }
                            });
                            Ok(Value::Array(result_groups))
                        }
                        Err(e) => Err(operation_error(format!(
                            "group_by() groups() failed: {}",
                            e
                        ))),
                    }
                }
                Err(e) => Err(operation_error(format!("group_by() failed: {}", e))),
            }
        }
        (Value::DataFrame(df), Value::Series(series)) => {
            // Group DataFrame by series values
            // This is more complex - we need to add the series as a column, group by it, then remove it
            let temp_col_name = "__group_by_temp_col";
            let mut df_clone = df.clone();
            let mut temp_series = series.clone();
            temp_series.rename(temp_col_name.into());
            match df_clone.with_column(temp_series) {
                Ok(df_with_group) => {
                    match df_with_group.group_by([temp_col_name]) {
                        Ok(grouped) => {
                            match grouped.groups() {
                                Ok(groups_df) => {
                                    // groups_df has columns for the grouping keys and a "groups" column with indices
                                    let mut result_groups = Vec::new();
                                    let mut group_keys = Vec::new();
                                    for i in 0..groups_df.height() {
                                        if let Ok(group_indices) = groups_df.column("groups") {
                                            if let Ok(AnyValue::List(list)) = group_indices.get(i) {
                                                let indices: Vec<u32> = list
                                                    .iter()
                                                    .filter_map(|v| match v {
                                                        AnyValue::UInt32(idx) => Some(idx),
                                                        _ => None,
                                                    })
                                                    .collect();
                                                if !indices.is_empty() {
                                                    let indices_ca = UInt32Chunked::from_vec(
                                                        "indices".into(),
                                                        indices,
                                                    );
                                                    match df_with_group.take(&indices_ca) {
                                                        Ok(group_df) => {
                                                            // Get the key
                                                            let key = if let Ok(temp_series) =
                                                                group_df.column(temp_col_name)
                                                            {
                                                                temp_series
                                                                    .get(0)
                                                                    .map(|v| {
                                                                        value_from_any_value(v)
                                                                            .unwrap_or(Value::Null)
                                                                    })
                                                                    .unwrap_or(Value::Null)
                                                            } else {
                                                                Value::Null
                                                            };
                                                            group_keys.push(key);
                                                            // Remove the temp column
                                                            match group_df.drop(temp_col_name) {
                                                                Ok(df_clean) => {
                                                                    let objects =
                                                                        dataframe_to_objects(
                                                                            &df_clean,
                                                                        );
                                                                    result_groups.push(
                                                                        Value::Array(
                                                                            objects
                                                                                .into_iter()
                                                                                .map(Value::Object)
                                                                                .collect(),
                                                                        ),
                                                                    );
                                                                }
                                                                _ => {
                                                                    result_groups
                                                                        .push(Value::Array(vec![]));
                                                                }
                                                            }
                                                        }
                                                        Err(e) => {
                                                            group_keys.push(Value::Null);
                                                            result_groups
                                                                .push(Value::Array(vec![]));
                                                            return Err(operation_error(format!(
                                                                "group_by() take failed: {}",
                                                                e
                                                            )));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    // Sort groups by key
                                    let mut indices: Vec<usize> =
                                        (0..result_groups.len()).collect();
                                    indices.sort_by(|&i, &j| {
                                        compare_values_for_sorting(&group_keys[i], &group_keys[j])
                                    });
                                    let sorted_groups = indices
                                        .into_iter()
                                        .map(|i| result_groups[i].clone())
                                        .collect();
                                    Ok(Value::Array(sorted_groups))
                                }
                                Err(e) => Err(operation_error(format!(
                                    "group_by() groups() failed: {}",
                                    e
                                ))),
                            }
                        }
                        Err(e) => Err(operation_error(format!("group_by() failed: {}", e))),
                    }
                }
                Err(e) => Err(operation_error(format!(
                    "group_by() failed to add temp column: {}",
                    e
                ))),
            }
        }
        _ => Err(operation_error(
            "group_by() requires (array, string) or (dataframe/lazyframe, string/series)",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "group_by",
        func: builtin_group_by,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::{DataFrame, NamedFrom, Series};
    use std::collections::HashMap;

    #[test]
    fn test_builtin_group_by_with_dataframe() {
        let df = DataFrame::new(vec![
            Series::new(
                "department".into(),
                &["Engineering", "Sales", "Engineering"],
            )
            .into(),
            polars::prelude::Column::new(
                PlSmallStr::from("salary"),
                vec![75000i64, 82000i64, 95000i64],
            ),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);
        let column = Value::String("department".to_string());

        let result = builtin_group_by(&[df_value, column]).unwrap();
        match result {
            Value::Array(groups) => {
                assert_eq!(groups.len(), 2); // Two groups: Engineering and Sales
                                             // Check that groups are sorted by key
                for group in groups {
                    match group {
                        Value::Array(objects) => {
                            assert!(!objects.is_empty());
                        }
                        _ => panic!("Expected array of objects"),
                    }
                }
            }
            _ => panic!("Expected array of groups"),
        }
    }

    #[test]
    fn test_builtin_group_by() {
        // Test array grouping by keys
        let arr = Value::Array(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("a".to_string()),
        ]);
        let keys = Value::Array(vec![Value::Int(1), Value::Int(1), Value::Int(2)]);
        let result = builtin_group_by(&[arr, keys]).unwrap();
        match result {
            Value::Array(groups) => {
                assert_eq!(groups.len(), 2); // Two groups
                                             // Groups should be sorted by key
            }
            _ => panic!("Expected array"),
        }

        // Test array of objects by field
        let mut obj1 = HashMap::new();
        obj1.insert("name".to_string(), Value::String("Alice".to_string()));
        obj1.insert("group".to_string(), Value::String("A".to_string()));
        let mut obj2 = HashMap::new();
        obj2.insert("name".to_string(), Value::String("Bob".to_string()));
        obj2.insert("group".to_string(), Value::String("B".to_string()));
        let mut obj3 = HashMap::new();
        obj3.insert("name".to_string(), Value::String("Charlie".to_string()));
        obj3.insert("group".to_string(), Value::String("A".to_string()));
        let arr = Value::Array(vec![
            Value::Object(obj1),
            Value::Object(obj2),
            Value::Object(obj3),
        ]);
        let result = builtin_group_by(&[arr, Value::String("group".to_string())]).unwrap();
        match result {
            Value::Array(groups) => {
                assert_eq!(groups.len(), 2); // Two groups: A and B
            }
            _ => panic!("Expected array"),
        }

        // Test DataFrame grouping
        let df = DataFrame::new(vec![
            Series::new(PlSmallStr::from("name"), &["Alice", "Bob", "Charlie"]).into(),
            Series::new(PlSmallStr::from("group"), &["A", "B", "A"]).into(),
        ])
        .unwrap();
        let df_val = Value::DataFrame(df);
        let result = builtin_group_by(&[df_val, Value::String("group".to_string())]).unwrap();
        match result {
            Value::Array(groups) => {
                assert_eq!(groups.len(), 2);
            }
            _ => panic!("Expected array"),
        }

        // Test DataFrame grouping by series
        let df = DataFrame::new(vec![Series::new(
            "name".into(),
            &["Alice", "Bob", "Charlie"],
        )
        .into()])
        .unwrap();
        let df_val = Value::DataFrame(df);
        let series = Series::new("group".into(), &["A", "B", "A"]);
        let series_val = Value::Series(series);
        let result = builtin_group_by(&[df_val, series_val]).unwrap();
        match result {
            Value::Array(groups) => {
                assert_eq!(groups.len(), 2);
            }
            _ => panic!("Expected array"),
        }

        // Test error cases
        let result = builtin_group_by(&[Value::Int(1), Value::String("test".to_string())]);
        assert!(result.is_err());

        let result = builtin_group_by(&[Value::Array(vec![]), Value::Array(vec![Value::Int(1)])]);
        assert!(result.is_err()); // Mismatched lengths
    }

    #[test]
    fn test_group_by_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("group_by"));
    }
}
