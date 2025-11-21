# Format Conversion Example

This example demonstrates converting between different data formats using DSQ.

## Use Case

Convert data between CSV, JSON, JSON Lines, and Parquet formats while optionally transforming the data.

## Sample Data

`products.csv`:
```csv
id,name,price,category,in_stock
1,Laptop,999.99,Electronics,true
2,Mouse,29.99,Electronics,true
3,Desk,299.99,Furniture,false
4,Chair,149.99,Furniture,true
5,Monitor,399.99,Electronics,true
```

## Format Conversions

### 1. CSV to JSON

Convert CSV to JSON array:

```bash
dsq '.' products.csv -o products.json
```

**Output** (`products.json`):
```json
[
  {"id":1,"name":"Laptop","price":999.99,"category":"Electronics","in_stock":true},
  {"id":2,"name":"Mouse","price":29.99,"category":"Electronics","in_stock":true},
  {"id":3,"name":"Desk","price":299.99,"category":"Furniture","in_stock":false},
  {"id":4,"name":"Chair","price":149.99,"category":"Furniture","in_stock":true},
  {"id":5,"name":"Monitor","price":399.99,"category":"Electronics","in_stock":true}
]
```

### 2. CSV to JSON Lines

Convert to newline-delimited JSON:

```bash
dsq '.[]' products.csv -o products.jsonl
```

**Output** (`products.jsonl`):
```json
{"id":1,"name":"Laptop","price":999.99,"category":"Electronics","in_stock":true}
{"id":2,"name":"Mouse","price":29.99,"category":"Electronics","in_stock":true}
{"id":3,"name":"Desk","price":299.99,"category":"Furniture","in_stock":false}
{"id":4,"name":"Chair","price":149.99,"category":"Furniture","in_stock":true}
{"id":5,"name":"Monitor","price":399.99,"category":"Electronics","in_stock":true}
```

### 3. JSON to CSV

Convert JSON back to CSV:

```bash
dsq '.' products.json -o products_output.csv
```

### 4. Format Conversion with Transformation

Convert and transform in one step:

```bash
dsq '.[] | {id, name, price: (.price | round), available: .in_stock}' products.csv -o transformed.json
```

**Output:**
```json
[
  {"id":1,"name":"Laptop","price":1000,"available":true},
  {"id":2,"name":"Mouse","price":30,"available":true},
  {"id":3,"name":"Desk","price":300,"available":false},
  {"id":4,"name":"Chair","price":150,"available":true},
  {"id":5,"name":"Monitor","price":400,"available":true}
]
```

### 5. Filter During Conversion

Convert only in-stock items:

```bash
dsq '.[] | select(.in_stock == true)' products.csv -o in_stock.json
```

### 6. CSV to Parquet

Convert to Parquet for efficient storage:

```bash
dsq '.' products.csv -o products.parquet
```

### 7. Parquet to JSON

Read Parquet and convert to JSON:

```bash
dsq '.' products.parquet -o products_from_parquet.json
```

## Format-Specific Options

### CSV Options

```bash
# Custom delimiter
dsq '.' data.tsv -i tsv -o output.csv

# Skip header row
dsq '.' data.csv --skip-rows 1 -o output.json
```

### JSON Pretty Printing

```bash
# Pretty-print JSON output
dsq '.' products.csv -o output.json --pretty
```

### Compression

```bash
# Write compressed Parquet
dsq '.' large_data.csv -o compressed.parquet --compression snappy
```

## Library Usage

```rust
use dsq_formats::{read_csv_file, write_json_file};

fn main() {
    // Read CSV
    let df = read_csv_file("products.csv")
        .expect("Failed to read CSV");

    // Write as JSON
    write_json_file(&df, "output.json")
        .expect("Failed to write JSON");
}
```

### Format Detection

```rust
use dsq_formats::{detect_format, read_file, write_file};

fn main() {
    // Auto-detect format
    let format = detect_format("data.csv").unwrap();
    println!("Detected format: {:?}", format);

    // Read with auto-detection
    let df = read_file("data.csv").expect("Failed to read");

    // Write to different format
    write_file(&df, "output.json").expect("Failed to write");
}
```

## Performance Tips

### 1. Large Files

For large files, use streaming:

```bash
# Process large CSV in chunks
dsq '.[] | select(.category == "Electronics")' large.csv -o filtered.json
```

### 2. Format Choice

- **CSV**: Human-readable, widely compatible
- **JSON Lines**: Streaming-friendly, good for logs
- **Parquet**: Best compression, fastest queries, columnar storage
- **JSON**: Easy to work with, but larger files

### 3. Batch Processing

```bash
# Convert multiple files
for file in *.csv; do
    dsq '.' "$file" -o "${file%.csv}.json"
done
```

## Common Patterns

### Normalize Data During Conversion

```bash
dsq '.[] | {
    id,
    name: .name | lowercase,
    price: .price | round,
    category: .category | uppercase
}' products.csv -o normalized.json
```

### Aggregate During Conversion

```bash
dsq 'group_by(.category) | map({
    category: .[0].category,
    count: length,
    avg_price: (map(.price) | add / length)
})' products.csv -o summary.json
```

### Flatten Nested JSON to CSV

```bash
dsq '.[] | {
    id: .id,
    name: .name,
    city: .address.city,
    country: .address.country
}' nested.json -o flattened.csv
```

## What You'll Learn

- How to convert between formats
- Format-specific options
- Combining conversion with transformation
- Performance considerations for different formats
- Library usage for format conversion

## Next Steps

- Explore [advanced filtering](../advanced_filtering/) with format conversion
- Try [batch processing](../batch_processing/) multiple files
- Learn about [Parquet optimization](../parquet_optimization/)
