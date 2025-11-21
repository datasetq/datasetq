use dsq_core::ops::aggregate::AggregationFunction;
use dsq_core::ops::basic::SortOptions;
use dsq_core::ops::pipeline::*;
use dsq_core::Value;
use polars::prelude::*;

fn create_test_dataframe() -> DataFrame {
    df! {
        "name" => ["Alice", "Bob", "Charlie", "Dave"],
        "age" => [30, 25, 35, 28],
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
            let ages = df.column("age").unwrap().i32().unwrap();
            assert_eq!(ages.get(0), Some(35));
        }
        _ => panic!("Expected DataFrame"),
    }
}

#[test]
fn test_pipeline_with_aggregation() {
    let df = create_test_dataframe();
    let value = Value::DataFrame(df);

    let result = OperationPipeline::new()
        .aggregate(
            vec!["department".to_string()],
            vec![
                AggregationFunction::Count,
                AggregationFunction::Mean("age".to_string()),
                AggregationFunction::Sum("salary".to_string()),
            ],
        )
        .execute(value)
        .unwrap();

    match result {
        Value::DataFrame(df) => {
            assert_eq!(df.height(), 3); // 3 unique departments
            assert!(df.width() >= 4); // department + count + mean_age + sum_salary
        }
        _ => panic!("Expected DataFrame"),
    }
}
