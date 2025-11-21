use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::io::Write;
use std::process::Command;
use std::time::Duration;
use tempfile::NamedTempFile;

// Generate test data
fn generate_csv_data(rows: usize) -> String {
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
    data
}

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

fn generate_jsonl_data(rows: usize) -> String {
    let mut data = String::new();
    for i in 0..rows {
        data.push_str(&format!(
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
        data.push('\n');
    }
    data
}

// Benchmark: Simple field selection
fn bench_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_fields");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        // CSV data
        let csv_data = generate_csv_data(*size);
        let mut csv_file = NamedTempFile::new().unwrap();
        csv_file.write_all(csv_data.as_bytes()).unwrap();
        let csv_path = csv_file.path().to_str().unwrap();

        // JSON data
        let json_data = generate_json_data(*size);
        let mut json_file = NamedTempFile::new().unwrap();
        json_file.write_all(json_data.as_bytes()).unwrap();
        let json_path = json_file.path().to_str().unwrap();

        // JSONL data
        let jsonl_data = generate_jsonl_data(*size);
        let mut jsonl_file = NamedTempFile::new().unwrap();
        jsonl_file.write_all(jsonl_data.as_bytes()).unwrap();
        let jsonl_path = jsonl_file.path().to_str().unwrap();

        // DSQ - CSV
        group.bench_with_input(BenchmarkId::new("dsq_csv", size), size, |b, _| {
            b.iter(|| {
                let _output = Command::new("cargo")
                    .args(&["run", "--release", "--", csv_path, ".[] | {id, name}"])
                    .output()
                    .expect("Failed to execute dsq");
            });
        });

        // jq - JSON (jq doesn't handle CSV natively)
        group.bench_with_input(BenchmarkId::new("jq_json", size), size, |b, _| {
            b.iter(|| {
                let _output = Command::new("jq")
                    .args(&[".[] | {id, name}", json_path])
                    .output()
                    .ok(); // Use ok() since jq might not be installed
            });
        });

        // mlr - CSV
        group.bench_with_input(BenchmarkId::new("mlr_csv", size), size, |b, _| {
            b.iter(|| {
                let _output = Command::new("mlr")
                    .args(&["--csv", "cut", "-f", "id,name", json_path])
                    .output()
                    .ok(); // Use ok() since mlr might not be installed
            });
        });
    }

    group.finish();
}

// Benchmark: Filtering
fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_rows");

    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.sample_size(10); // Reduce sample size for large datasets

        let csv_data = generate_csv_data(*size);
        let mut csv_file = NamedTempFile::new().unwrap();
        csv_file.write_all(csv_data.as_bytes()).unwrap();
        let csv_path = csv_file.path().to_str().unwrap();

        let json_data = generate_json_data(*size);
        let mut json_file = NamedTempFile::new().unwrap();
        json_file.write_all(json_data.as_bytes()).unwrap();
        let json_path = json_file.path().to_str().unwrap();

        // DSQ
        group.bench_with_input(BenchmarkId::new("dsq", size), size, |b, _| {
            b.iter(|| {
                let _output = Command::new("cargo")
                    .args(&[
                        "run",
                        "--release",
                        "--",
                        csv_path,
                        ".[] | select(.value > 5000)",
                    ])
                    .output()
                    .expect("Failed to execute dsq");
            });
        });

        // jq
        group.bench_with_input(BenchmarkId::new("jq", size), size, |b, _| {
            b.iter(|| {
                let _output = Command::new("jq")
                    .args(&[".[] | select(.value > 5000)", json_path])
                    .output()
                    .ok();
            });
        });

        // mlr
        group.bench_with_input(BenchmarkId::new("mlr", size), size, |b, _| {
            b.iter(|| {
                let _output = Command::new("mlr")
                    .args(&["--csv", "filter", "$value > 5000", csv_path])
                    .output()
                    .ok();
            });
        });
    }

    group.finish();
}

// Benchmark: Aggregation (group by)
fn bench_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by_aggregation");
    group.sample_size(10);

    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        let csv_data = generate_csv_data(*size);
        let mut csv_file = NamedTempFile::new().unwrap();
        csv_file.write_all(csv_data.as_bytes()).unwrap();
        let csv_path = csv_file.path().to_str().unwrap();

        let json_data = generate_json_data(*size);
        let mut json_file = NamedTempFile::new().unwrap();
        json_file.write_all(json_data.as_bytes()).unwrap();
        let json_path = json_file.path().to_str().unwrap();

        // DSQ
        group.bench_with_input(
            BenchmarkId::new("dsq", size),
            size,
            |b, _| {
                b.iter(|| {
                    let _output = Command::new("cargo")
                        .args(&[
                            "run",
                            "--release",
                            "--",
                            csv_path,
                            "group_by(.category) | {category: .[0].category, avg: (map(.score) | add / length)}"
                        ])
                        .output()
                        .expect("Failed to execute dsq");
                });
            },
        );

        // jq
        group.bench_with_input(
            BenchmarkId::new("jq", size),
            size,
            |b, _| {
                b.iter(|| {
                    let _output = Command::new("jq")
                        .args(&[
                            "group_by(.category) | map({category: .[0].category, avg: (map(.score) | add / length)})",
                            json_path
                        ])
                        .output()
                        .ok();
                });
            },
        );

        // mlr
        group.bench_with_input(BenchmarkId::new("mlr", size), size, |b, _| {
            b.iter(|| {
                let _output = Command::new("mlr")
                    .args(&[
                        "--csv", "stats1", "-a", "mean", "-f", "score", "-g", "category", csv_path,
                    ])
                    .output()
                    .ok();
            });
        });
    }

    group.finish();
}

// Benchmark: Pure I/O (read and write)
fn bench_io(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_throughput");

    for size in [10000, 100000, 1000000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.sample_size(10);

        let csv_data = generate_csv_data(*size);
        let mut csv_file = NamedTempFile::new().unwrap();
        csv_file.write_all(csv_data.as_bytes()).unwrap();
        let csv_path = csv_file.path().to_str().unwrap();

        let json_data = generate_json_data(*size);
        let mut json_file = NamedTempFile::new().unwrap();
        json_file.write_all(json_data.as_bytes()).unwrap();
        let json_path = json_file.path().to_str().unwrap();

        // DSQ - CSV passthrough
        group.bench_with_input(
            BenchmarkId::new("dsq_csv_passthrough", size),
            size,
            |b, _| {
                b.iter(|| {
                    let _output = Command::new("cargo")
                        .args(&["run", "--release", "--", csv_path, "."])
                        .output()
                        .expect("Failed to execute dsq");
                });
            },
        );

        // jq - JSON passthrough
        group.bench_with_input(
            BenchmarkId::new("jq_json_passthrough", size),
            size,
            |b, _| {
                b.iter(|| {
                    let _output = Command::new("jq").args(&[".", json_path]).output().ok();
                });
            },
        );

        // mlr - CSV passthrough
        group.bench_with_input(
            BenchmarkId::new("mlr_csv_passthrough", size),
            size,
            |b, _| {
                b.iter(|| {
                    let _output = Command::new("mlr")
                        .args(&["--csv", "cat", csv_path])
                        .output()
                        .ok();
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(3));
    targets = bench_select, bench_filter, bench_aggregation, bench_io
}

criterion_main!(benches);
