use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dsq_core::io::{read_file_sync, ReadOptions};
use dsq_core::ops::OperationPipeline;
use dsq_core::Value;
use std::io::Write;
use tempfile::NamedTempFile;

// Generate test data
fn generate_csv_bytes(rows: usize) -> Vec<u8> {
    let mut data = String::from("id,name,value,category,score\n");
    for i in 0..rows {
        data.push_str(&format!(
            "{},Person{},{},{},{:.2}\n",
            i,
            i,
            i * 100,
            if i % 3 == 0 {
                "A"
            } else if i % 3 == 1 {
                "B"
            } else {
                "C"
            },
            (i as f64) * 3.14
        ));
    }
    data.into_bytes()
}

fn generate_json_bytes(rows: usize) -> Vec<u8> {
    let mut items = Vec::with_capacity(rows);
    for i in 0..rows {
        items.push(format!(
            r#"{{"id":{},"name":"Person{}","value":{},"category":"{}","score":{:.2}}}"#,
            i,
            i,
            i * 100,
            if i % 3 == 0 {
                "A"
            } else if i % 3 == 1 {
                "B"
            } else {
                "C"
            },
            (i as f64) * 3.14
        ));
    }
    format!("[{}]", items.join(",")).into_bytes()
}

// Benchmark CSV reading
fn bench_csv_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_read");

    for size in [100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        let csv_data = generate_csv_bytes(*size);
        let mut csv_file = NamedTempFile::new().unwrap();
        csv_file.write_all(&csv_data).unwrap();
        let csv_path = csv_file.path();

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let options = ReadOptions::default();
                let _value = read_file_sync(black_box(csv_path), &options).unwrap();
            });
        });
    }

    group.finish();
}

// Benchmark JSON reading
fn bench_json_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_read");

    for size in [100, 1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        let json_data = generate_json_bytes(*size);
        let mut json_file = NamedTempFile::new().unwrap();
        json_file.write_all(&json_data).unwrap();
        let json_path = json_file.path();

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let options = ReadOptions::default();
                let _value = read_file_sync(black_box(json_path), &options).unwrap();
            });
        });
    }

    group.finish();
}

// Benchmark Value operations (to measure clone overhead)
fn bench_value_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_operations");

    // Create a large dataframe
    use polars::prelude::*;
    let size = 10000;
    let id_series = Series::new("id", (0..size).collect::<Vec<i64>>());
    let name_series = Series::new(
        "name",
        (0..size)
            .map(|i| format!("Person{}", i))
            .collect::<Vec<String>>(),
    );
    let value_series = Series::new("value", (0..size).map(|i| i * 100).collect::<Vec<i64>>());

    let df = DataFrame::new(vec![id_series, name_series, value_series]).unwrap();
    let value = Value::DataFrame(df);

    group.bench_function("clone_dataframe", |b| {
        b.iter(|| {
            let _cloned = black_box(value.clone());
        });
    });

    group.bench_function("to_json_conversion", |b| {
        b.iter(|| {
            let _json = black_box(value.to_json_value());
        });
    });

    group.finish();
}

// Benchmark pipeline operations
fn bench_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_operations");

    let size = 10000;
    let csv_data = generate_csv_bytes(size);
    let mut csv_file = NamedTempFile::new().unwrap();
    csv_file.write_all(&csv_data).unwrap();
    let csv_path = csv_file.path();

    let options = ReadOptions::default();
    let value = read_file_sync(csv_path, &options).unwrap();

    group.bench_function("select_columns", |b| {
        b.iter(|| {
            let pipeline =
                OperationPipeline::new().select(vec!["id".to_string(), "name".to_string()]);
            let _result = pipeline.execute(black_box(&value).clone()).unwrap();
        });
    });

    group.bench_function("head_operation", |b| {
        b.iter(|| {
            let pipeline = OperationPipeline::new().head(100);
            let _result = pipeline.execute(black_box(&value).clone()).unwrap();
        });
    });

    group.finish();
}

// Benchmark memory allocation patterns
fn bench_allocations(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocations");

    group.bench_function("vec_with_capacity", |b| {
        b.iter(|| {
            let mut v: Vec<String> = Vec::with_capacity(10000);
            for i in 0..10000 {
                v.push(format!("Item{}", i));
            }
            black_box(v);
        });
    });

    group.bench_function("vec_without_capacity", |b| {
        b.iter(|| {
            let mut v: Vec<String> = Vec::new();
            for i in 0..10000 {
                v.push(format!("Item{}", i));
            }
            black_box(v);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_csv_read,
    bench_json_read,
    bench_value_operations,
    bench_pipeline,
    bench_allocations
);

criterion_main!(benches);
