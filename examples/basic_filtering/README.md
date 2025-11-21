# Basic Filtering Example

This example demonstrates basic filtering operations in DSQ.

## Use Case

Filter a CSV file of users to find all users:
- Over age 18
- From a specific city
- With verified email addresses

## Sample Data

`users.csv`:
```csv
name,age,city,email_verified
Alice,25,NYC,true
Bob,17,LA,true
Charlie,30,NYC,false
Diana,22,NYC,true
Eve,19,LA,true
```

## DSQ Queries

### 1. Filter by Age

Find all users over 18:

```bash
dsq '.[] | select(.age > 18)' users.csv
```

**Output:**
```json
{"name":"Alice","age":25,"city":"NYC","email_verified":true}
{"name":"Charlie","age":30,"city":"NYC","email_verified":false}
{"name":"Diana","age":22,"city":"NYC","email_verified":true}
{"name":"Eve","age":19,"city":"LA","email_verified":true}
```

### 2. Filter by Multiple Conditions

Find users over 18 AND from NYC:

```bash
dsq '.[] | select(.age > 18 and .city == "NYC")' users.csv
```

**Output:**
```json
{"name":"Alice","age":25,"city":"NYC","email_verified":true}
{"name":"Charlie","age":30,"city":"NYC","email_verified":false}
{"name":"Diana","age":22,"city":"NYC","email_verified":true}
```

### 3. Filter with Field Selection

Get only name and age for users with verified emails:

```bash
dsq '.[] | select(.email_verified == true) | {name, age}' users.csv
```

**Output:**
```json
{"name":"Alice","age":25}
{"name":"Bob","age":17}
{"name":"Diana","age":22}
{"name":"Eve","age":19}
```

### 4. Complex Filter

Users over 18, from NYC, with verified email:

```bash
dsq '.[] | select(.age > 18 and .city == "NYC" and .email_verified == true)' users.csv
```

**Output:**
```json
{"name":"Alice","age":25,"city":"NYC","email_verified":true}
{"name":"Diana","age":22,"city":"NYC","email_verified":true}
```

## Library Usage

```rust
use dsq_filter::execute_filter;
use dsq_shared::value::Value;

fn main() {
    let csv_data = r#"name,age,city,email_verified
Alice,25,NYC,true
Bob,17,LA,true
Charlie,30,NYC,false"#;

    // Parse CSV to Value (simplified for example)
    let value = parse_csv_to_value(csv_data);

    // Filter users over 18 from NYC
    let query = ".[] | select(.age > 18 and .city == \"NYC\")";
    let result = execute_filter(query, &value)
        .expect("Filter failed");

    println!("{:?}", result);
}
```

## What You'll Learn

- Basic `select()` filter usage
- Comparison operators (`>`, `==`)
- Logical operators (`and`, `or`)
- Field selection with `{name, age}`
- Combining multiple filters

## Next Steps

- Try modifying the conditions
- Experiment with `or` instead of `and`
- Add more fields to the selection
- See [sorting example](../sorting/) for ordering results
