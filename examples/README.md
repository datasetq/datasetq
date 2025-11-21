# DSQ Examples

This directory contains examples demonstrating various DSQ features and use cases.

## Running Examples

### CLI Examples

All CLI examples can be run using:

```bash
dsq '<query>' <input-file>
```

### Library Examples

Rust library examples are in the `src/` subdirectories and can be run with:

```bash
cargo run --example <example_name>
```

## Example Categories

### Basic Operations

- **Field Selection**: Extract specific fields from data
- **Filtering**: Select rows based on conditions
- **Sorting**: Order data by one or more fields
- **Aggregation**: Compute statistics and summaries

### Format Conversion

- **CSV to JSON**: Convert CSV files to JSON format
- **JSON to CSV**: Flatten JSON to CSV
- **Parquet to CSV**: Extract data from Parquet files
- **Multi-format**: Working with multiple formats in one pipeline

### Data Transformation

- **Field Mapping**: Rename and transform fields
- **Type Conversion**: Convert between data types
- **String Operations**: Text manipulation and cleaning
- **Date/Time**: Parse and format dates

### Advanced Filtering

- **Complex Conditions**: Multiple filters with logical operators
- **Nested Data**: Working with nested JSON structures
- **Array Operations**: Filter and transform array elements
- **Joins**: Combine data from multiple sources

### Functions

See the `functions/` directory for examples of built-in functions:

- String functions
- Math functions
- Date/time functions
- Array functions
- Object functions
- Encoding functions

## Common Use Cases

### 1. Extract Fields from CSV

```bash
dsq '.[] | {name, email, age}' users.csv
```

### 2. Filter and Sort

```bash
dsq '.[] | select(.age > 18) | sort_by(.name)' users.csv
```

### 3. Format Conversion

```bash
dsq '.' data.csv -o output.json
```

### 4. Aggregate Data

```bash
dsq 'group_by(.category) | map({category: .[0].category, count: length})' sales.csv
```

### 5. String Transformation

```bash
dsq '.[] | {name: .name | uppercase, email: .email | lowercase}' users.csv
```

### 6. Date Operations

```bash
dsq '.[] | select(.date | parse_date > "2024-01-01")' events.csv
```

### 7. Nested Data Access

```bash
dsq '.[] | {id, user_name: .user.name, city: .user.address.city}' data.json
```

### 8. Array Operations

```bash
dsq '.[] | {name, tag_count: .tags | length}' items.json
```

## Sample Data Files

Example data files are provided in the `data/` subdirectories:

- `users.csv` - Sample user data
- `sales.json` - Sales transaction data
- `events.csv` - Event log data
- `products.json` - Product catalog

## Creating Your Own Examples

To add a new example:

1. Create a directory: `examples/your-example/`
2. Add sample data: `examples/your-example/data.csv`
3. Add a README: `examples/your-example/README.md` explaining the use case
4. Include the query: Show the DSQ command or Rust code
5. Show expected output: Include sample output

## Library Usage Examples

### Basic Library Usage

```rust
use dsq_filter::execute_filter;
use dsq_shared::value::Value;

fn main() {
    let data = r#"{"name": "Alice", "age": 30}"#;
    let value = Value::from_json(serde_json::from_str(data).unwrap());

    let result = execute_filter(".name", &value).expect("Filter failed");
    println!("{:?}", result);
}
```

### DataFrame Processing

```rust
use dsq_filter::execute_dataframe_filter;
use polars::prelude::*;

fn main() {
    let df = df! {
        "name" => ["Alice", "Bob"],
        "age" => [30, 25],
    }.unwrap();

    let result = execute_dataframe_filter(
        ".[] | select(.age > 26)",
        df
    ).expect("Filter failed");

    println!("{:?}", result);
}
```

## Performance Tips

- Use streaming for large files
- Enable lazy evaluation for DataFrames
- Use format-specific readers for better performance
- Consider Parquet for large datasets

## Troubleshooting

### Common Issues

1. **Parse errors**: Check your query syntax against jq documentation
2. **Type mismatches**: Ensure field types match expected operations
3. **Memory issues**: Use streaming for very large files
4. **Performance**: Profile queries and consider format conversion

### Getting Help

- Check the main [README.md](../README.md)
- Read the [documentation](../docs/)
- Open an issue on GitHub
- Review existing examples for similar use cases

## Contributing Examples

We welcome example contributions! Please:

- Use realistic, representative data
- Include clear explanations
- Show both input and expected output
- Test examples before submitting
- Follow the directory structure

See [CONTRIBUTING.md](../CONTRIBUTING.md) for more details.
