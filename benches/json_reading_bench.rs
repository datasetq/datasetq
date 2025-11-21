use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polars::prelude::*;
use std::io::Cursor;

// Import the actual dsq conversion function
use dsq_formats::reader::json_utils::json_to_dataframe;
use dsq_formats::reader::options::ReadOptions;

fn generate_json_data(rows: usize) -> String {
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
    format!("[{}]", items.join(","))
}

fn bench_polars_native_json(c: &mut Criterion) {
    let json_1k = generate_json_data(1000);
    let json_10k = generate_json_data(10000);

    c.bench_function("polars_native_1k", |b| {
        b.iter(|| {
            let cursor = Cursor::new(json_1k.as_bytes());
            let df = JsonReader::new(cursor).finish().unwrap();
            black_box(df);
        });
    });

    c.bench_function("polars_native_10k", |b| {
        b.iter(|| {
            let cursor = Cursor::new(json_10k.as_bytes());
            let df = JsonReader::new(cursor).finish().unwrap();
            black_box(df);
        });
    });
}

fn bench_serde_json_parse(c: &mut Criterion) {
    let json_1k = generate_json_data(1000);
    let json_10k = generate_json_data(10000);

    c.bench_function("serde_json_1k", |b| {
        b.iter(|| {
            let value: serde_json::Value = serde_json::from_str(&json_1k).unwrap();
            black_box(value);
        });
    });

    c.bench_function("serde_json_10k", |b| {
        b.iter(|| {
            let value: serde_json::Value = serde_json::from_str(&json_10k).unwrap();
            black_box(value);
        });
    });
}

fn bench_current_full_flow(c: &mut Criterion) {
    let json_1k = generate_json_data(1000);
    let json_10k = generate_json_data(10000);

    c.bench_function("current_full_1k", |b| {
        b.iter(|| {
            let value: serde_json::Value = serde_json::from_str(&json_1k).unwrap();
            let df = json_to_dataframe(&value, &ReadOptions::default()).unwrap();
            black_box(df);
        });
    });

    c.bench_function("current_full_10k", |b| {
        b.iter(|| {
            let value: serde_json::Value = serde_json::from_str(&json_10k).unwrap();
            let df = json_to_dataframe(&value, &ReadOptions::default()).unwrap();
            black_box(df);
        });
    });
}

criterion_group!(
    benches,
    bench_polars_native_json,
    bench_serde_json_parse,
    bench_current_full_flow
);
criterion_main!(benches);
