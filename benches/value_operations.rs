use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dsq_shared::value::Value;
use polars::datatypes::PlSmallStr;
use polars::prelude::*;
use std::collections::HashMap;

fn benchmark_json_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_conversion");

    // Small array (100 objects)
    let small_array: Vec<Value> = (0..100)
        .map(|i| {
            Value::object(HashMap::from([
                ("id".to_string(), Value::int(i)),
                ("name".to_string(), Value::string(format!("Item {}", i))),
                ("value".to_string(), Value::float(i as f64 * 1.5)),
            ]))
        })
        .collect();

    // Medium array (10,000 objects)
    let medium_array: Vec<Value> = (0..10_000)
        .map(|i| {
            Value::object(HashMap::from([
                ("id".to_string(), Value::int(i)),
                ("name".to_string(), Value::string(format!("Item {}", i))),
                ("value".to_string(), Value::float(i as f64 * 1.5)),
            ]))
        })
        .collect();

    group.throughput(Throughput::Elements(100));
    group.bench_function("to_json_small", |b| {
        b.iter(|| {
            let array = Value::array(small_array.clone());
            std::hint::black_box(array.to_json().unwrap())
        })
    });

    group.throughput(Throughput::Elements(10_000));
    group.bench_function("to_json_medium", |b| {
        b.iter(|| {
            let array = Value::array(medium_array.clone());
            std::hint::black_box(array.to_json().unwrap())
        })
    });

    group.finish();
}

fn benchmark_dataframe_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataframe_conversion");

    // Create array of objects for conversion
    let sizes = [100, 1_000, 10_000];

    for size in sizes.iter() {
        let array: Vec<Value> = (0..*size)
            .map(|i| {
                Value::object(HashMap::from([
                    ("id".to_string(), Value::int(i)),
                    ("name".to_string(), Value::string(format!("Item {}", i))),
                    ("value".to_string(), Value::float(i as f64 * 1.5)),
                    ("active".to_string(), Value::bool(i % 2 == 0)),
                ]))
            })
            .collect();

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _size| {
            b.iter(|| {
                let val = Value::array(array.clone());
                std::hint::black_box(val.to_dataframe().unwrap())
            })
        });
    }

    group.finish();
}

fn benchmark_value_indexing(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_indexing");

    // Create test array
    let array: Vec<Value> = (0..1000).map(Value::int).collect();
    let array_val = Value::array(array);

    group.bench_function("array_index", |b| {
        b.iter(|| {
            for i in 0..100 {
                std::hint::black_box(array_val.index(i).unwrap());
            }
        })
    });

    // Create test DataFrame
    let df = DataFrame::new(vec![
        Series::new(PlSmallStr::from("id"), (0..1000).collect::<Vec<i64>>()).into(),
        Series::new(
            PlSmallStr::from("value"),
            (0..1000).map(|i| i as f64).collect::<Vec<f64>>(),
        )
        .into(),
    ])
    .unwrap();
    let df_val = Value::dataframe(df);

    group.bench_function("dataframe_index", |b| {
        b.iter(|| {
            for i in 0..100 {
                std::hint::black_box(df_val.index(i).unwrap());
            }
        })
    });

    group.finish();
}

fn benchmark_field_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("field_access");

    // Create test object
    let obj = Value::object(HashMap::from([
        ("name".to_string(), Value::string("Test")),
        ("age".to_string(), Value::int(30)),
        ("active".to_string(), Value::bool(true)),
        (
            "nested".to_string(),
            Value::object(HashMap::from([(
                "inner".to_string(),
                Value::string("value"),
            )])),
        ),
    ]));

    group.bench_function("direct_field", |b| {
        b.iter(|| std::hint::black_box(obj.field("name").unwrap()))
    });

    group.bench_function("nested_field", |b| {
        b.iter(|| std::hint::black_box(obj.field_path(&["nested", "inner"]).unwrap()))
    });

    // Create array of objects for field access
    let array: Vec<Value> = (0..1000)
        .map(|i| {
            Value::object(HashMap::from([
                ("id".to_string(), Value::int(i)),
                ("name".to_string(), Value::string(format!("Item {}", i))),
            ]))
        })
        .collect();
    let array_val = Value::array(array);

    group.bench_function("array_field_map", |b| {
        b.iter(|| std::hint::black_box(array_val.field("name").unwrap()))
    });

    group.finish();
}

fn benchmark_value_cloning(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_cloning");

    // Create various value types
    let small_array = Value::array((0..100).map(Value::int).collect());
    let large_array = Value::array((0..10_000).map(Value::int).collect());

    let df = DataFrame::new(vec![
        Series::new(PlSmallStr::from("id"), (0..10_000).collect::<Vec<i64>>()).into(),
        Series::new(
            PlSmallStr::from("value"),
            (0..10_000).map(|i| i as f64).collect::<Vec<f64>>(),
        )
        .into(),
    ])
    .unwrap();
    let df_val = Value::dataframe(df);

    group.bench_function("clone_small_array", |b| {
        b.iter(|| std::hint::black_box(small_array.clone()))
    });

    group.bench_function("clone_large_array", |b| {
        b.iter(|| std::hint::black_box(large_array.clone()))
    });

    group.bench_function("clone_dataframe", |b| {
        b.iter(|| std::hint::black_box(df_val.clone()))
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_json_conversion,
    benchmark_dataframe_conversion,
    benchmark_value_indexing,
    benchmark_field_access,
    benchmark_value_cloning
);
criterion_main!(benches);
