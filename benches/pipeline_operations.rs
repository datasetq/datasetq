use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use dsq_core::ops::{basic::SortOptions, OperationPipeline};
use dsq_shared::value::Value;
use polars::datatypes::PlSmallStr;
use polars::prelude::*;

fn benchmark_pipeline_execution(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_execution");

    // Create test DataFrame
    let df = DataFrame::new(vec![
        Series::new(PlSmallStr::from("id"), (0..10_000).collect::<Vec<i64>>()).into(),
        Series::new(
            PlSmallStr::from("name"),
            (0..10_000)
                .map(|i| format!("Item {}", i))
                .collect::<Vec<String>>(),
        )
        .into(),
        Series::new(
            PlSmallStr::from("value"),
            (0..10_000).map(|i| i as f64 * 1.5).collect::<Vec<f64>>(),
        )
        .into(),
        Series::new(
            PlSmallStr::from("category"),
            (0..10_000)
                .map(|i| format!("Cat{}", i % 10))
                .collect::<Vec<String>>(),
        )
        .into(),
    ])
    .unwrap();
    let df_val = Value::dataframe(df);

    group.throughput(Throughput::Elements(10_000));

    // Simple pipeline: select + sort
    group.bench_function("select_and_sort", |b| {
        b.iter(|| {
            let pipeline = OperationPipeline::new()
                .select(vec![
                    "id".to_string(),
                    "name".to_string(),
                    "value".to_string(),
                ])
                .sort(vec![SortOptions::desc("value".to_string())]);
            std::hint::black_box(pipeline.execute(df_val.clone()).unwrap())
        })
    });

    // Complex pipeline: select + filter + sort + head
    group.bench_function("complex_pipeline", |b| {
        b.iter(|| {
            let pipeline = OperationPipeline::new()
                .select(vec![
                    "id".to_string(),
                    "name".to_string(),
                    "value".to_string(),
                ])
                .filter(|v: &Value| {
                    if let Value::Object(obj) = v {
                        if let Some(Value::Float(val)) = obj.get("value") {
                            return Ok(*val > 5000.0);
                        }
                    }
                    Ok(false)
                })
                .sort(vec![SortOptions::desc("value".to_string())])
                .head(100);
            std::hint::black_box(pipeline.execute(df_val.clone()).unwrap())
        })
    });

    // Group by pipeline
    group.bench_function("group_by", |b| {
        b.iter(|| {
            let pipeline = OperationPipeline::new().group_by(vec!["category".to_string()]);
            std::hint::black_box(pipeline.execute(df_val.clone()).unwrap())
        })
    });

    group.finish();
}

fn benchmark_pipeline_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_overhead");

    let df = DataFrame::new(vec![
        Series::new("id".into(), (0..1_000).collect::<Vec<i64>>()).into(),
        Series::new(
            "value".into(),
            (0..1_000).map(|i| i as f64).collect::<Vec<f64>>(),
        )
        .into(),
    ])
    .unwrap();
    let df_val = Value::dataframe(df);

    // Empty pipeline (identity)
    group.bench_function("empty_pipeline", |b| {
        b.iter(|| {
            let pipeline = OperationPipeline::new();
            std::hint::black_box(pipeline.execute(df_val.clone()).unwrap())
        })
    });

    // Single operation
    group.bench_function("single_operation", |b| {
        b.iter(|| {
            let pipeline = OperationPipeline::new().select(vec!["id".to_string()]);
            std::hint::black_box(pipeline.execute(df_val.clone()).unwrap())
        })
    });

    // Multiple identity operations (testing overhead)
    group.bench_function("multiple_operations", |b| {
        b.iter(|| {
            let pipeline = OperationPipeline::new()
                .select(vec!["id".to_string(), "value".to_string()])
                .select(vec!["id".to_string(), "value".to_string()])
                .select(vec!["id".to_string(), "value".to_string()]);
            std::hint::black_box(pipeline.execute(df_val.clone()).unwrap())
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_pipeline_execution,
    benchmark_pipeline_overhead
);
criterion_main!(benches);
