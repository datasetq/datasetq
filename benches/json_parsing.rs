use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use dsq_formats::reader::{deserialize_json, FormatReadOptions, ReadOptions};
use std::io::Cursor;

fn benchmark_json_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");

    // Small JSON (100 objects)
    let small_json = serde_json::json!([(0..100)
        .map(|i| serde_json::json!({
            "id": i,
            "name": format!("Item {}", i),
            "value": i as f64 * 1.5,
            "active": i % 2 == 0
        }))
        .collect::<Vec<_>>()]);
    let small_json_str = serde_json::to_string(&small_json[0]).unwrap();

    // Medium JSON (1,000 objects)
    let medium_json = (0..1_000)
        .map(|i| {
            serde_json::json!({
                "id": i,
                "name": format!("Item {}", i),
                "value": i as f64 * 1.5,
                "active": i % 2 == 0
            })
        })
        .collect::<Vec<_>>();
    let medium_json_str = serde_json::to_string(&medium_json).unwrap();

    // Large JSON (10,000 objects)
    let large_json = (0..10_000)
        .map(|i| {
            serde_json::json!({
                "id": i,
                "name": format!("Item {}", i),
                "value": i as f64 * 1.5,
                "active": i % 2 == 0
            })
        })
        .collect::<Vec<_>>();
    let large_json_str = serde_json::to_string(&large_json).unwrap();

    group.throughput(Throughput::Bytes(small_json_str.len() as u64));
    group.bench_function("parse_small", |b| {
        b.iter(|| {
            let cursor = Cursor::new(small_json_str.as_bytes());
            let options = ReadOptions::default();
            let format_options = FormatReadOptions::default();
            std::hint::black_box(deserialize_json(cursor, &options, &format_options).unwrap())
        })
    });

    group.throughput(Throughput::Bytes(medium_json_str.len() as u64));
    group.bench_function("parse_medium", |b| {
        b.iter(|| {
            let cursor = Cursor::new(medium_json_str.as_bytes());
            let options = ReadOptions::default();
            let format_options = FormatReadOptions::default();
            std::hint::black_box(deserialize_json(cursor, &options, &format_options).unwrap())
        })
    });

    group.throughput(Throughput::Bytes(large_json_str.len() as u64));
    group.bench_function("parse_large", |b| {
        b.iter(|| {
            let cursor = Cursor::new(large_json_str.as_bytes());
            let options = ReadOptions::default();
            let format_options = FormatReadOptions::default();
            std::hint::black_box(deserialize_json(cursor, &options, &format_options).unwrap())
        })
    });

    group.finish();
}

criterion_group!(benches, benchmark_json_parsing);
criterion_main!(benches);
