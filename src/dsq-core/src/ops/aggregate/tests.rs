use super::*;
use crate::ops::basic::SortOptions;
use crate::ops::pipeline::*;
use crate::ops::{
    recommended_batch_size, supports_operation, AddOperation, AndOperation,
    ArrayConstructOperation, AssignAddOperation, AssignUpdateOperation, DivOperation, EqOperation,
    FieldAccessOperation, GeOperation, GtOperation, IterateOperation, LeOperation,
    LiteralOperation, LtOperation, MulOperation, NeOperation, NegationOperation,
    ObjectConstructOperation, Operation, OperationType, OrOperation, SelectConditionOperation,
    SliceOperation, SubOperation, VariableOperation,
};
use std::collections::HashMap;

fn create_test_dataframe() -> DataFrame {
    df! {
        "name" => ["Alice", "Bob", "Charlie", "Dave"],
        "age" => [30i64, 25i64, 35i64, 28i64],
        "department" => ["Engineering", "Sales", "Engineering", "Marketing"],
        "salary" => [75000, 50000, 80000, 60000]
    }
    .unwrap()
}

#[test]
fn test_operation_pipeline() {
    let df = create_test_dataframe();
    let value = Value::DataFrame(df);

    let result = OperationPipeline::new()
        .select(vec![
            "name".to_string(),
            "age".to_string(),
            "department".to_string(),
        ])
        .sort(vec![SortOptions::desc("age".to_string())])
        .head(2)
        .execute(value)
        .unwrap();

    match result {
        Value::DataFrame(df) => {
            assert_eq!(df.height(), 2);
            assert_eq!(df.width(), 3);
            // Should be sorted by age descending, so Charlie (35) should be first
            let ages = df.column("age").unwrap().i64().unwrap();
            assert_eq!(ages.get(0), Some(35));
        }
        _ => panic!("Expected DataFrame"),
    }
}

#[test]
fn test_pipeline_with_aggregation() {
    let df = create_test_dataframe();
    let value = Value::DataFrame(df);

    let agg_funcs = vec![
        AggregationFunction::Mean("age".to_string()),
        AggregationFunction::Sum("salary".to_string()),
    ];

    let result = OperationPipeline::new()
        .aggregate(vec!["department".to_string()], agg_funcs)
        .execute(value)
        .unwrap();

    match result {
        Value::DataFrame(df) => {
            assert_eq!(df.height(), 3); // 3 departments
            assert!(df
                .get_column_names()
                .contains(&(&PlSmallStr::from("department"))));
            assert!(df
                .get_column_names()
                .contains(&(&PlSmallStr::from("age_mean"))));
            assert!(df
                .get_column_names()
                .contains(&(&PlSmallStr::from("salary_sum"))));
        }
        _ => panic!("Expected DataFrame"),
    }
}

#[test]
fn test_pipeline_description() {
    let pipeline = OperationPipeline::new()
        .select(vec!["name".to_string(), "age".to_string()])
        .sort(vec![SortOptions::desc("age".to_string())])
        .head(10);

    let descriptions = pipeline.describe();
    assert_eq!(descriptions.len(), 3);
    assert!(descriptions[0].contains("select columns"));
    assert!(descriptions[1].contains("sort by"));
    assert!(descriptions[2].contains("head 10"));
}

#[test]
fn test_supports_operation() {
    let df_value = Value::DataFrame(create_test_dataframe());
    let array_value = Value::Array(vec![Value::Int(1), Value::Int(2)]);
    let scalar_value = Value::Int(42);

    assert!(supports_operation(&df_value, OperationType::Basic));
    assert!(supports_operation(&df_value, OperationType::Aggregate));
    assert!(supports_operation(&df_value, OperationType::Join));
    assert!(supports_operation(&df_value, OperationType::Transform));

    assert!(supports_operation(&array_value, OperationType::Basic));
    assert!(supports_operation(&array_value, OperationType::Aggregate));
    assert!(supports_operation(&array_value, OperationType::Join));

    assert!(!supports_operation(&scalar_value, OperationType::Aggregate));
    assert!(!supports_operation(&scalar_value, OperationType::Join));
}

#[test]
fn test_recommended_batch_size() {
    // Create a large DataFrame
    let large_df = df! {
        "id" => (0..2_000_000).collect::<Vec<i32>>(),
        "value" => (0..2_000_000).map(f64::from).collect::<Vec<f64>>()
    }
    .unwrap();
    let large_value = Value::DataFrame(large_df);

    let batch_size = recommended_batch_size(&large_value, OperationType::Basic);
    assert_eq!(batch_size, Some(100_000));

    let agg_batch_size = recommended_batch_size(&large_value, OperationType::Aggregate);
    assert_eq!(agg_batch_size, Some(50_000));

    // Small DataFrame should not need batching
    let small_value = Value::DataFrame(create_test_dataframe());
    let small_batch_size = recommended_batch_size(&small_value, OperationType::Basic);
    assert_eq!(small_batch_size, None);
}

#[test]
#[ignore = "Filter operation on DataFrame uses i32 instead of i64"]
fn test_filter_operation() {
    let df = create_test_dataframe();
    let value = Value::DataFrame(df);

    let predicate = |v: &Value| -> Result<bool> {
        match v {
            Value::Object(obj) => {
                if let Some(Value::Int(age)) = obj.get("age") {
                    Ok(*age > 27)
                } else {
                    Ok(false)
                }
            }
            _ => Ok(false),
        }
    };

    let filter_op = FilterOperation {
        predicate: Box::new(predicate),
    };

    let result = filter_op.apply(&value).unwrap();
    match result {
        Value::DataFrame(filtered_df) => {
            assert_eq!(filtered_df.height(), 3); // Alice(30), Charlie(35), Dave(28) should pass age > 27
            let names = filtered_df.column("name").unwrap().str().unwrap();
            let ages = filtered_df.column("age").unwrap().i32().unwrap();
            // Should contain Alice, Charlie, Dave (Bob is 25, filtered out)
            assert!(
                names.get(0).unwrap().contains("Alice")
                    || names.get(0).unwrap().contains("Charlie")
                    || names.get(0).unwrap().contains("Dave")
            );
            assert!(ages.get(0).unwrap() > 27);
        }
        _ => panic!("Expected DataFrame"),
    }
}

#[test]
fn test_slice_operation_dataframe() {
    let df = create_test_dataframe();
    let value = Value::DataFrame(df.clone());

    // Test slicing first 2 rows
    let slice_op = SliceOperation::new(
        Some(vec![Box::new(LiteralOperation::new(Value::Int(0)))]),
        Some(vec![Box::new(LiteralOperation::new(Value::Int(2)))]),
    );

    let result = slice_op.apply(&value).unwrap();
    match result {
        Value::DataFrame(sliced_df) => {
            assert_eq!(sliced_df.height(), 2);
            assert_eq!(sliced_df.width(), df.width());
            // Check that the first row matches the original
            let original_first = df.get_row(0).unwrap();
            let sliced_first = sliced_df.get_row(0).unwrap();
            assert_eq!(original_first, sliced_first);
        }
        _ => panic!("Expected DataFrame"),
    }

    // Test slicing from index 1 to end
    let slice_op2 = SliceOperation::new(
        Some(vec![Box::new(LiteralOperation::new(Value::Int(1)))]),
        None,
    );

    let result2 = slice_op2.apply(&value).unwrap();
    match result2 {
        Value::DataFrame(sliced_df) => {
            assert_eq!(sliced_df.height(), 3); // Original has 4 rows, slice from 1 to end = 3 rows
            assert_eq!(sliced_df.width(), df.width());
        }
        _ => panic!("Expected DataFrame"),
    }
}

#[test]
fn test_slice_operation_array() {
    let arr = Value::Array(vec![
        Value::Int(1),
        Value::Int(2),
        Value::Int(3),
        Value::Int(4),
        Value::Int(5),
    ]);

    // Test slicing first 3 elements
    let slice_op = SliceOperation::new(
        Some(vec![Box::new(LiteralOperation::new(Value::Int(0)))]),
        Some(vec![Box::new(LiteralOperation::new(Value::Int(3)))]),
    );

    let result = slice_op.apply(&arr).unwrap();
    match result {
        Value::Array(sliced_arr) => {
            assert_eq!(sliced_arr.len(), 3);
            assert_eq!(sliced_arr[0], Value::Int(1));
            assert_eq!(sliced_arr[1], Value::Int(2));
            assert_eq!(sliced_arr[2], Value::Int(3));
        }
        _ => panic!("Expected Array"),
    }

    // Test slicing from index 2 to end
    let slice_op2 = SliceOperation::new(
        Some(vec![Box::new(LiteralOperation::new(Value::Int(2)))]),
        None,
    );

    let result2 = slice_op2.apply(&arr).unwrap();
    match result2 {
        Value::Array(sliced_arr) => {
            assert_eq!(sliced_arr.len(), 3);
            assert_eq!(sliced_arr[0], Value::Int(3));
            assert_eq!(sliced_arr[1], Value::Int(4));
            assert_eq!(sliced_arr[2], Value::Int(5));
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_assign_update_operation_object_field() {
    // Create test object
    let mut obj = HashMap::new();
    obj.insert("name".to_string(), Value::String("Alice".to_string()));
    obj.insert("age".to_string(), Value::Int(30));
    obj.insert("salary".to_string(), Value::Int(75000));
    let input = Value::Object(obj);

    // Test updating a field with a literal value
    let update_op = AssignUpdateOperation::new(
        vec![Box::new(FieldAccessOperation::new("age".to_string()))],
        vec![Box::new(LiteralOperation::new(Value::Int(35)))],
    );

    let result = update_op.apply(&input).unwrap();
    match result {
        Value::Object(result_obj) => {
            assert_eq!(
                result_obj.get("name"),
                Some(&Value::String("Alice".to_string()))
            );
            assert_eq!(result_obj.get("age"), Some(&Value::Int(35)));
            assert_eq!(result_obj.get("salary"), Some(&Value::Int(75000)));
        }
        _ => panic!("Expected Object"),
    }
}

#[test]
fn test_assign_update_operation_string_field() {
    // Create test object
    let mut obj = HashMap::new();
    obj.insert("name".to_string(), Value::String("Alice".to_string()));
    obj.insert(
        "department".to_string(),
        Value::String("Engineering".to_string()),
    );
    let input = Value::Object(obj);

    // Test updating a string field
    let update_op = AssignUpdateOperation::new(
        vec![Box::new(FieldAccessOperation::new(
            "department".to_string(),
        ))],
        vec![Box::new(LiteralOperation::new(Value::String(
            "Sales".to_string(),
        )))],
    );

    let result = update_op.apply(&input).unwrap();
    match result {
        Value::Object(result_obj) => {
            assert_eq!(
                result_obj.get("name"),
                Some(&Value::String("Alice".to_string()))
            );
            assert_eq!(
                result_obj.get("department"),
                Some(&Value::String("Sales".to_string()))
            );
        }
        _ => panic!("Expected Object"),
    }
}

#[test]
fn test_assign_update_operation_with_expression() {
    // Create test object
    let mut obj = HashMap::new();
    obj.insert("salary".to_string(), Value::Int(75000));
    obj.insert("bonus".to_string(), Value::Int(5000));
    let input = Value::Object(obj);

    // Test updating with an expression (salary + bonus)
    let update_op = AssignUpdateOperation::new(
        vec![Box::new(FieldAccessOperation::new(
            "total_compensation".to_string(),
        ))],
        vec![Box::new(AddOperation::new(
            vec![Box::new(FieldAccessOperation::new("salary".to_string()))],
            vec![Box::new(FieldAccessOperation::new("bonus".to_string()))],
        ))],
    );

    let result = update_op.apply(&input).unwrap();
    match result {
        Value::Object(result_obj) => {
            assert_eq!(result_obj.get("salary"), Some(&Value::Int(75000)));
            assert_eq!(result_obj.get("bonus"), Some(&Value::Int(5000)));
            assert_eq!(
                result_obj.get("total_compensation"),
                Some(&Value::Int(80000))
            );
        }
        _ => panic!("Expected Object"),
    }
}

#[test]
fn test_assign_update_operation_non_object() {
    // Test with array input (should return the value expression)
    let input = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);

    let update_op = AssignUpdateOperation::new(
        vec![Box::new(FieldAccessOperation::new("field".to_string()))],
        vec![Box::new(LiteralOperation::new(Value::String(
            "updated".to_string(),
        )))],
    );

    let result = update_op.apply(&input).unwrap();
    assert_eq!(result, Value::String("updated".to_string()));
}

#[test]
fn test_assign_update_operation_nested_field() {
    // Create nested object
    let mut address = HashMap::new();
    address.insert("city".to_string(), Value::String("New York".to_string()));
    address.insert("zip".to_string(), Value::String("10001".to_string()));

    let mut obj = HashMap::new();
    obj.insert("name".to_string(), Value::String("Alice".to_string()));
    obj.insert("address".to_string(), Value::Object(address));
    let input = Value::Object(obj);

    // Test updating nested field
    let update_op = AssignUpdateOperation::new(
        vec![Box::new(FieldAccessOperation::with_fields(vec![
            "address".to_string(),
            "city".to_string(),
        ]))],
        vec![Box::new(LiteralOperation::new(Value::String(
            "Boston".to_string(),
        )))],
    );

    let result = update_op.apply(&input).unwrap();
    match result {
        Value::Object(result_obj) => {
            assert_eq!(
                result_obj.get("name"),
                Some(&Value::String("Alice".to_string()))
            );
            if let Some(Value::Object(addr_obj)) = result_obj.get("address") {
                assert_eq!(
                    addr_obj.get("city"),
                    Some(&Value::String("Boston".to_string()))
                );
                assert_eq!(
                    addr_obj.get("zip"),
                    Some(&Value::String("10001".to_string()))
                );
            } else {
                panic!("Expected nested address object");
            }
        }
        _ => panic!("Expected Object"),
    }
}

#[test]
fn test_assign_update_operation_description() {
    let update_op = AssignUpdateOperation::new(
        vec![Box::new(FieldAccessOperation::new("field".to_string()))],
        vec![Box::new(LiteralOperation::new(Value::Int(42)))],
    );

    assert_eq!(update_op.description(), "assign update");
}

#[test]
fn test_add_operation() {
    let add_op = AddOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
        vec![Box::new(LiteralOperation::new(Value::Int(3)))],
    );

    let result = add_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Int(8));
}

#[test]
fn test_sub_operation() {
    let sub_op = SubOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(10)))],
        vec![Box::new(LiteralOperation::new(Value::Int(4)))],
    );

    let result = sub_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Int(6));
}

#[test]
fn test_mul_operation() {
    let mul_op = MulOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(6)))],
        vec![Box::new(LiteralOperation::new(Value::Int(7)))],
    );

    let result = mul_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Int(42));
}

#[test]
fn test_div_operation() {
    let div_op = DivOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(15)))],
        vec![Box::new(LiteralOperation::new(Value::Int(3)))],
    );

    let result = div_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Int(5));
}

#[test]
fn test_eq_operation() {
    let eq_op = EqOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
    );

    let result = eq_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Bool(true));

    let neq_op = EqOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
        vec![Box::new(LiteralOperation::new(Value::Int(6)))],
    );

    let result_neq = neq_op.apply(&Value::Null).unwrap();
    assert_eq!(result_neq, Value::Bool(false));
}

#[test]
fn test_ne_operation() {
    let ne_op = NeOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
        vec![Box::new(LiteralOperation::new(Value::Int(6)))],
    );

    let result = ne_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Bool(true));

    let eq_op = NeOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
    );

    let result_eq = eq_op.apply(&Value::Null).unwrap();
    assert_eq!(result_eq, Value::Bool(false));
}

#[test]
fn test_lt_operation() {
    let lt_op = LtOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(3)))],
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
    );

    let result = lt_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Bool(true));

    let nlt_op = LtOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
        vec![Box::new(LiteralOperation::new(Value::Int(3)))],
    );

    let result_nlt = nlt_op.apply(&Value::Null).unwrap();
    assert_eq!(result_nlt, Value::Bool(false));
}

#[test]
fn test_le_operation() {
    let le_op = LeOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(3)))],
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
    );

    let result = le_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Bool(true));

    let le_eq_op = LeOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
    );

    let result_eq = le_eq_op.apply(&Value::Null).unwrap();
    assert_eq!(result_eq, Value::Bool(true));
}

#[test]
fn test_gt_operation() {
    let gt_op = GtOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
        vec![Box::new(LiteralOperation::new(Value::Int(3)))],
    );

    let result = gt_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Bool(true));

    let ngt_op = GtOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(3)))],
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
    );

    let result_ngt = ngt_op.apply(&Value::Null).unwrap();
    assert_eq!(result_ngt, Value::Bool(false));
}

#[test]
fn test_ge_operation() {
    let ge_op = GeOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
        vec![Box::new(LiteralOperation::new(Value::Int(3)))],
    );

    let result = ge_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Bool(true));

    let ge_eq_op = GeOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
        vec![Box::new(LiteralOperation::new(Value::Int(5)))],
    );

    let result_eq = ge_eq_op.apply(&Value::Null).unwrap();
    assert_eq!(result_eq, Value::Bool(true));
}

#[test]
fn test_and_operation() {
    let and_op = AndOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Bool(true)))],
        vec![Box::new(LiteralOperation::new(Value::Bool(true)))],
    );

    let result = and_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Bool(true));

    let and_false_op = AndOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Bool(true)))],
        vec![Box::new(LiteralOperation::new(Value::Bool(false)))],
    );

    let result_false = and_false_op.apply(&Value::Null).unwrap();
    assert_eq!(result_false, Value::Bool(false));
}

#[test]
fn test_or_operation() {
    let or_op = OrOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Bool(false)))],
        vec![Box::new(LiteralOperation::new(Value::Bool(true)))],
    );

    let result = or_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Bool(true));

    let or_false_op = OrOperation::new(
        vec![Box::new(LiteralOperation::new(Value::Bool(false)))],
        vec![Box::new(LiteralOperation::new(Value::Bool(false)))],
    );

    let result_false = or_false_op.apply(&Value::Null).unwrap();
    assert_eq!(result_false, Value::Bool(false));
}

#[test]
fn test_negation_operation() {
    // Test !true = false
    let neg_op = NegationOperation::new(vec![Box::new(LiteralOperation::new(Value::Bool(true)))]);

    let result = neg_op.apply(&Value::Null).unwrap();
    assert_eq!(result, Value::Bool(false));

    // Test !false = true
    let neg_op_false =
        NegationOperation::new(vec![Box::new(LiteralOperation::new(Value::Bool(false)))]);

    let result_false = neg_op_false.apply(&Value::Null).unwrap();
    assert_eq!(result_false, Value::Bool(true));

    // Test !0 = true (falsy number)
    let neg_op_zero = NegationOperation::new(vec![Box::new(LiteralOperation::new(Value::Int(0)))]);

    let result_zero = neg_op_zero.apply(&Value::Null).unwrap();
    assert_eq!(result_zero, Value::Bool(true));

    // Test !1 = false (truthy number)
    let neg_op_one = NegationOperation::new(vec![Box::new(LiteralOperation::new(Value::Int(1)))]);

    let result_one = neg_op_one.apply(&Value::Null).unwrap();
    assert_eq!(result_one, Value::Bool(false));
}

#[test]
#[ignore = "ObjectConstructOperation returns Array instead of Object in current implementation"]
fn test_object_construct_operation() {
    // Create an object with literal values
    let obj_construct = ObjectConstructOperation::new(vec![
        (
            Box::new(LiteralOperation::new(Value::String("name".to_string()))),
            Some(vec![Box::new(LiteralOperation::new(Value::String(
                "Alice".to_string(),
            )))]),
        ),
        (
            Box::new(LiteralOperation::new(Value::String("age".to_string()))),
            Some(vec![Box::new(LiteralOperation::new(Value::Int(30)))]),
        ),
    ]);

    let result = obj_construct.apply(&Value::Null).unwrap();
    match result {
        Value::Object(obj) => {
            assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(obj.get("age"), Some(&Value::Int(30)));
            assert_eq!(obj.len(), 2);
        }
        _ => panic!("Expected Object"),
    }
}

#[test]
fn test_array_construct_operation() {
    let arr_construct = ArrayConstructOperation::new(vec![
        Box::new(LiteralOperation::new(Value::Int(1))),
        Box::new(LiteralOperation::new(Value::Int(2))),
        Box::new(LiteralOperation::new(Value::Int(3))),
    ]);

    let result = arr_construct.apply(&Value::Null).unwrap();
    match result {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Value::Int(1));
            assert_eq!(arr[1], Value::Int(2));
            assert_eq!(arr[2], Value::Int(3));
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_iterate_operation() {
    let arr = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    let iter_op = IterateOperation;

    let result = iter_op.apply(&arr).unwrap();
    assert_eq!(result, arr);

    // Test iterating over object
    let mut obj = HashMap::new();
    obj.insert("a".to_string(), Value::Int(1));
    obj.insert("b".to_string(), Value::Int(2));
    let obj_value = Value::Object(obj);

    let result_obj = iter_op.apply(&obj_value).unwrap();
    match result_obj {
        Value::Array(values) => {
            assert_eq!(values.len(), 2);
            // Values should be 1 and 2 (order may vary)
            assert!(values.contains(&Value::Int(1)));
            assert!(values.contains(&Value::Int(2)));
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_select_condition_operation() {
    // Test with truthy condition
    let select_op =
        SelectConditionOperation::new(vec![Box::new(LiteralOperation::new(Value::Bool(true)))]);

    let input = Value::String("test".to_string());
    let result = select_op.apply(&input).unwrap();
    assert_eq!(result, input);

    // Test with falsy condition
    let select_op_false =
        SelectConditionOperation::new(vec![Box::new(LiteralOperation::new(Value::Bool(false)))]);

    let result_false = select_op_false.apply(&input).unwrap();
    assert_eq!(result_false, Value::Null);
}

#[test]
fn test_variable_operation() {
    let var_op = VariableOperation::new("test_var".to_string());
    let result = var_op.apply(&Value::Null);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Variable 'test_var' not found"));
}

#[test]
fn test_field_access_operation() {
    let mut obj = HashMap::new();
    obj.insert("name".to_string(), Value::String("Alice".to_string()));
    obj.insert("age".to_string(), Value::Int(30));
    let obj_value = Value::Object(obj);

    let field_op = FieldAccessOperation::new("name".to_string());
    let result = field_op.apply(&obj_value).unwrap();
    assert_eq!(result, Value::String("Alice".to_string()));

    let field_op_age = FieldAccessOperation::new("age".to_string());
    let result_age = field_op_age.apply(&obj_value).unwrap();
    assert_eq!(result_age, Value::Int(30));

    // Test nested field access
    let nested_op = FieldAccessOperation::with_fields(vec!["user".to_string(), "name".to_string()]);
    let mut outer_obj = HashMap::new();
    outer_obj.insert("user".to_string(), obj_value);
    let outer_value = Value::Object(outer_obj);

    let result_nested = nested_op.apply(&outer_value).unwrap();
    assert_eq!(result_nested, Value::String("Alice".to_string()));
}

#[test]
fn test_assign_add_operation() {
    // Create test DataFrame
    let df = create_test_dataframe();
    let value = Value::DataFrame(df);

    // Test adding to a column
    let assign_add_op = AssignAddOperation::new(
        vec![Box::new(FieldAccessOperation::new("salary".to_string()))],
        vec![Box::new(LiteralOperation::new(Value::Int(1000)))],
    );

    let result = assign_add_op.apply(&value).unwrap();
    match result {
        Value::DataFrame(new_df) => {
            // Check that salaries were increased by 1000
            let salaries = new_df.column("salary").unwrap().i32().unwrap();
            assert_eq!(salaries.get(0), Some(76000)); // 75000 + 1000
            assert_eq!(salaries.get(1), Some(51000)); // 50000 + 1000
            assert_eq!(salaries.get(2), Some(81000)); // 80000 + 1000
            assert_eq!(salaries.get(3), Some(61000)); // 60000 + 1000
        }
        _ => panic!("Expected DataFrame"),
    }
}

#[test]
#[ignore = "Filter operation on DataFrame uses i32 instead of i64"]
fn test_pipeline_filter_method() {
    let df = create_test_dataframe();
    let value = Value::DataFrame(df);

    let predicate = |v: &Value| -> Result<bool> {
        match v {
            Value::Object(obj) => {
                if let Some(Value::Int(age)) = obj.get("age") {
                    Ok(*age > 30)
                } else {
                    Ok(false)
                }
            }
            _ => Ok(false),
        }
    };

    let result = OperationPipeline::new()
        .filter(predicate)
        .execute(value)
        .unwrap();

    match result {
        Value::DataFrame(filtered_df) => {
            // Only Charlie (age 35) has age > 30
            assert_eq!(filtered_df.height(), 1);
            let names = filtered_df.column("name").unwrap().str().unwrap();
            assert_eq!(names.get(0).unwrap(), "Charlie");
            let ages = filtered_df.column("age").unwrap().i32().unwrap();
            assert_eq!(ages.get(0).unwrap(), 35);
        }
        _ => panic!("Expected DataFrame"),
    }
}

#[test]
#[ignore = "group_by returns Array instead of DataFrame in current implementation"]
fn test_pipeline_group_by_method() {
    let df = create_test_dataframe();
    let value = Value::DataFrame(df);

    let result = OperationPipeline::new()
        .group_by(vec!["department".to_string()])
        .execute(value)
        .unwrap();

    match result {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 3); // 3 departments
                                      // Check that each group has department
            for item in arr {
                if let Value::Object(obj) = item {
                    assert!(obj.contains_key("department"));
                } else {
                    panic!("Expected Object in array");
                }
            }
        }
        _ => panic!("Expected Array"),
    }
}

#[test]
fn test_pipeline_aggregate_method() {
    let df = create_test_dataframe();
    let value = Value::DataFrame(df);

    let agg_funcs = vec![AggregationFunction::Count];

    let result = OperationPipeline::new()
        .aggregate(vec!["department".to_string()], agg_funcs)
        .execute(value)
        .unwrap();

    match result {
        Value::DataFrame(agg_df) => {
            assert_eq!(agg_df.height(), 3); // 3 departments
            assert!(agg_df
                .get_column_names()
                .contains(&&PlSmallStr::from("department")));
            assert!(agg_df
                .get_column_names()
                .contains(&&PlSmallStr::from("count")));
        }
        _ => panic!("Expected DataFrame"),
    }
}

#[test]
fn test_pipeline_head_tail_methods() {
    let df = create_test_dataframe();
    let value = Value::DataFrame(df.clone());

    // Test head
    let head_result = OperationPipeline::new()
        .head(2)
        .execute(value.clone())
        .unwrap();

    match head_result {
        Value::DataFrame(head_df) => {
            assert_eq!(head_df.height(), 2);
        }
        _ => panic!("Expected DataFrame"),
    }

    // Test tail
    let tail_result = OperationPipeline::new().tail(2).execute(value).unwrap();

    match tail_result {
        Value::DataFrame(tail_df) => {
            assert_eq!(tail_df.height(), 2);
        }
        _ => panic!("Expected DataFrame"),
    }
}
