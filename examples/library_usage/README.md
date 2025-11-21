# Library Usage Example

This example demonstrates how to use DSQ as a Rust library in your applications.

## Use Case

Integrate DSQ's data processing capabilities into a Rust application for:
- Parsing and filtering structured data
- Building data pipelines
- Processing API responses
- ETL operations

## Basic Setup

Add DSQ to your `Cargo.toml`:

```toml
[dependencies]
dsq-filter = "0.1"
dsq-shared = "0.1"
dsq-parser = "0.1"
dsq-formats = "0.1"
serde_json = "1.0"
polars = { version = "0.35", features = ["lazy", "csv", "json"] }
```

## Example 1: Simple Filtering

```rust
use dsq_filter::execute_filter;
use dsq_shared::value::Value;
use serde_json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Sample JSON data
    let json_data = r#"
    {
        "users": [
            {"name": "Alice", "age": 30, "active": true},
            {"name": "Bob", "age": 25, "active": false},
            {"name": "Charlie", "age": 35, "active": true}
        ]
    }
    "#;

    // Parse JSON to Value
    let data: serde_json::Value = serde_json::from_str(json_data)?;
    let value = Value::from_json(data);

    // Execute DSQ query
    let query = ".users[] | select(.active == true and .age > 28)";
    let result = execute_filter(query, &value)?;

    println!("Active users over 28: {:?}", result);
    Ok(())
}
```

## Example 2: DataFrame Processing

```rust
use dsq_filter::execute_dataframe_filter;
use polars::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a DataFrame
    let df = df! {
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor"],
        "price" => [999.99, 29.99, 79.99, 399.99],
        "stock" => [5, 50, 30, 10],
    }?;

    println!("Original data:");
    println!("{:?}", df);

    // Filter using DSQ syntax
    let query = ".[] | select(.stock > 10)";
    let filtered = execute_dataframe_filter(query, df)?;

    println!("\nFiltered data (stock > 10):");
    println!("{:?}", filtered);

    Ok(())
}
```

## Example 3: Reading and Processing Files

```rust
use dsq_formats::csv::read_csv_file;
use dsq_filter::execute_dataframe_filter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read CSV file
    let df = read_csv_file("data/sales.csv")?;

    // Apply filters and transformations
    let query = r#"
        .[] |
        select(.amount > 100) |
        {
            customer: .customer_name,
            total: .amount,
            date: .purchase_date
        }
    "#;

    let result = execute_dataframe_filter(query, df)?;

    println!("Large purchases:");
    println!("{:?}", result);

    Ok(())
}
```

## Example 4: Building a Data Pipeline

```rust
use dsq_formats::{read_csv_file, write_json_file};
use dsq_filter::execute_dataframe_filter;

fn process_sales_data(input: &str, output: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Read CSV
    let df = read_csv_file(input)?;

    // Step 2: Filter and transform
    let query = r#"
        group_by(.category) |
        map({
            category: .[0].category,
            total_sales: (map(.amount) | add),
            count: length,
            avg_sale: (map(.amount) | add / length)
        })
    "#;
    let processed = execute_dataframe_filter(query, df)?;

    // Step 3: Write JSON
    write_json_file(&processed, output)?;

    println!("Processed {} -> {}", input, output);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    process_sales_data("sales.csv", "summary.json")?;
    Ok(())
}
```

## Example 5: API Response Processing

```rust
use dsq_filter::execute_filter;
use dsq_shared::value::Value;
use reqwest;
use serde_json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Fetch data from API
    let response = reqwest::get("https://api.example.com/users")
        .await?
        .text()
        .await?;

    // Parse response
    let data: serde_json::Value = serde_json::from_str(&response)?;
    let value = Value::from_json(data);

    // Extract and filter
    let query = r#"
        .data[] |
        select(.status == "active") |
        {
            id,
            name: .full_name,
            email,
            joined: .created_at
        }
    "#;

    let result = execute_filter(query, &value)?;

    println!("Active users: {:?}", result);
    Ok(())
}
```

## Example 6: Custom Function Integration

```rust
use dsq_functions::{FunctionRegistry, Function};
use dsq_shared::value::Value;
use dsq_filter::execute_filter_with_registry;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create registry with custom function
    let mut registry = FunctionRegistry::default();

    registry.register("tax", Function {
        name: "tax",
        description: "Calculate tax amount",
        handler: |args| {
            let amount = args[0].as_f64()?;
            let rate = args.get(1)
                .and_then(|v| v.as_f64().ok())
                .unwrap_or(0.1);
            Ok(Value::Number((amount * rate).into()))
        },
    });

    // Use custom function in query
    let data = r#"{"prices": [100, 200, 300]}"#;
    let value = Value::from_json(serde_json::from_str(data)?);

    let query = ".prices[] | {price: ., tax: tax(., 0.15), total: . + tax(., 0.15)}";
    let result = execute_filter_with_registry(query, &value, &registry)?;

    println!("Prices with tax: {:?}", result);
    Ok(())
}
```

## Example 7: Error Handling

```rust
use dsq_filter::{execute_filter, FilterError};
use dsq_shared::value::Value;

fn process_data(query: &str, data: &str) -> Result<Value, String> {
    let json: serde_json::Value = serde_json::from_str(data)
        .map_err(|e| format!("JSON parse error: {}", e))?;

    let value = Value::from_json(json);

    execute_filter(query, &value)
        .map_err(|e| match e {
            FilterError::ParseError(msg) => format!("Query syntax error: {}", msg),
            FilterError::TypeError(msg) => format!("Type mismatch: {}", msg),
            FilterError::RuntimeError(msg) => format!("Execution error: {}", msg),
            _ => format!("Unknown error: {}", e),
        })
}

fn main() {
    let data = r#"{"name": "Alice", "age": 30}"#;
    let query = ".name";

    match process_data(query, data) {
        Ok(result) => println!("Result: {:?}", result),
        Err(e) => eprintln!("Error: {}", e),
    }
}
```

## Example 8: Streaming Large Files

```rust
use dsq_io::{StreamReader, StreamWriter};
use dsq_filter::execute_filter;
use dsq_shared::value::Value;
use tokio::io::AsyncBufReadExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = StreamReader::from_file("large_data.jsonl").await?;
    let mut writer = StreamWriter::to_file("filtered_output.jsonl").await?;

    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        // Parse line
        let json: serde_json::Value = serde_json::from_str(&line)?;
        let value = Value::from_json(json);

        // Filter
        let query = "select(.score > 90)";
        if let Ok(result) = execute_filter(query, &value) {
            // Write result
            let output = serde_json::to_string(&result.to_json()?)?;
            writer.write_line(&output).await?;
        }
    }

    writer.flush().await?;
    Ok(())
}
```

## Testing Your Integration

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_filter() {
        let data = r#"{"values": [1, 2, 3, 4, 5]}"#;
        let json: serde_json::Value = serde_json::from_str(data).unwrap();
        let value = Value::from_json(json);

        let result = execute_filter(".values[] | select(. > 3)", &value).unwrap();

        assert_eq!(result.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_transformation() {
        let data = r#"{"name": "alice"}"#;
        let json: serde_json::Value = serde_json::from_str(data).unwrap();
        let value = Value::from_json(json);

        let result = execute_filter(".name | uppercase", &value).unwrap();

        assert_eq!(result.as_string().unwrap(), "ALICE");
    }
}
```

## Best Practices

1. **Error Handling**: Always handle potential errors appropriately
2. **Type Safety**: Validate data types before operations
3. **Performance**: Use DataFrames for large datasets
4. **Memory**: Stream large files instead of loading entirely
5. **Testing**: Write unit tests for your queries
6. **Documentation**: Document query syntax used in your code

## Common Pitfalls

1. **Type Mismatches**: Ensure operations match data types
2. **Null Values**: Handle null/missing values explicitly
3. **Memory Usage**: Be careful with large in-memory operations
4. **Query Complexity**: Break complex queries into steps for clarity

## API Documentation

For detailed API documentation:
- [dsq-filter docs](https://docs.rs/dsq-filter)
- [dsq-shared docs](https://docs.rs/dsq-shared)
- [dsq-formats docs](https://docs.rs/dsq-formats)

## What You'll Learn

- How to integrate DSQ into Rust applications
- Different ways to process data (Values vs DataFrames)
- Error handling patterns
- Streaming large files
- Custom function registration
- Building data pipelines

## Next Steps

- Explore [advanced filtering](../advanced_filtering/)
- Learn about [performance optimization](../performance/)
- See [real-world applications](../applications/)
