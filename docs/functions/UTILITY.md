# Utility Functions

General-purpose utility functions for data processing.

## Basic Utilities

### `length(value)`
Returns the length of arrays, strings, objects, or DataFrame height.

```bash
dsq '[1, 2, 3] | length'
# Output: 3

dsq '"hello" | length'
# Output: 5

dsq '. | length' data.csv
# Number of rows
```

### `keys(value)`
Returns array indices, object keys, or DataFrame column names.

```bash
dsq '{name: "Alice", age: 30} | keys'
# Output: ["name", "age"]

dsq '. | keys' data.csv
# Column names
```

### `values(value)`
Returns array elements or object values.

```bash
dsq '{name: "Alice", age: 30} | values'
# Output: ["Alice", 30]
```

### `has(container, key)`
Checks if an object contains a key.

```bash
dsq '{name: "Alice"} | has("name")'
# Output: true

dsq 'map(select(has("email")))' data.csv
# Filter rows with email field
```

### `type(value)`
Returns the type name of a value.

```bash
dsq '123 | type'
# Output: "number"

dsq '"hello" | type'
# Output: "string"

dsq 'map({field: .value, type: (.value | type)})' data.csv
```

### `empty(value)`
Checks if a value is empty (null, empty array/string/object).

```bash
dsq '[] | empty'
# Output: true

dsq '"" | empty'
# Output: true

dsq 'map(select(.description | empty | not))' products.csv
# Filter non-empty descriptions
```

## Control Flow

### `iif(condition, true_value, false_value)`
Conditional expression (inline if).

```bash
dsq 'map({status: iif(.age >= 18, "adult", "minor")})' people.csv

dsq 'map({price: iif(.on_sale, .sale_price, .regular_price)})' products.csv
```

### `iferror(value, fallback)`
Returns fallback if value causes an error.

```bash
dsq 'map({value: iferror(.field | tonumber, 0)})' data.csv
# Convert to number, use 0 if fails

dsq '.config | iferror(fromjson, {})' data.csv
# Parse JSON, use empty object if fails
```

### `coalesce(values...)`
Returns the first non-null value.

```bash
dsq 'coalesce(.email, .backup_email, "no-email@example.com")'
# Use first available email

dsq 'map({value: coalesce(.value1, .value2, .value3, 0)})' data.csv
```

### `error(message)`
Throws an error with a message.

```bash
dsq 'if .age < 0 then error("Invalid age") else . end' data.csv
```

## Data Selection

### `select(value)`
Filters truthy values (keeps non-null, non-false).

```bash
dsq 'map(select(.active))' users.csv
# Keep only active users

dsq 'map(select(.score > 80))' students.csv
# Keep high scorers
```

### `map(array, field|template)`
Transforms array elements.

```bash
dsq 'map(.name)' people.csv
# Extract names

dsq 'map({name, age})' data.csv
# Keep specific fields

dsq 'map(. * 2)' numbers.csv
# Double all values
```

### `filter(array, condition)`
Filters array elements by condition.

```bash
dsq 'filter(.age > 18)' people.csv

dsq 'filter(.status == "active")' users.csv
```

## Object Operations

### `del(object, key)`
Removes a key from an object.

```bash
dsq 'map(del("password"))' users.json
# Remove password field

dsq 'del("temp_field")' data.json
```

### `transform_keys(object, function)`
Transforms all object keys.

```bash
dsq 'transform_keys(toupper)' data.json
# Uppercase all keys

dsq 'transform_keys(snake_case)' camelCase.json
# Convert keys to snake_case
```

## Data Generation

### `range(start, end, step?)`
Generates a number sequence.

```bash
dsq 'range(1, 10)'
# Output: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

dsq 'range(0, 100, 10)'
# Output: [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

dsq 'range(1, 5) | map({id: .})'
# Generate rows with IDs
```

### `generate_sequence(start, end, step?)`
Alias for range.

```bash
dsq 'generate_sequence(1, 5)'
# Output: [1, 2, 3, 4, 5]
```

### `time_series_range(start, end, interval)`
Generates time series sequences.

```bash
dsq 'time_series_range("2024-01-01", "2024-01-07", "1 day")'
# Daily sequence

dsq 'time_series_range("2024-01-01 00:00", "2024-01-01 23:00", "1 hour")'
# Hourly sequence
```

### `generate_uuidv4()`, `generate_uuidv7()`
Generates UUIDs.

```bash
dsq 'map({id: generate_uuidv4()})' data.csv
# Add UUID v4

dsq 'map({id: generate_uuidv7()})' data.csv
# Add UUID v7 (time-ordered)
```

## Array Processing

### `unnest(array)`
Unnests/flattens nested structures.

```bash
dsq 'map(.items) | unnest' orders.json
# Unnest order items

dsq '.nested_data | unnest' data.json
```

### `group_concat(array, separator?)`
Concatenates values with optional separator.

```bash
dsq 'group_by(.category) | map({
  category: .[0].category,
  names: (map(.name) | group_concat(", "))
})' products.csv
```

## Text Processing

### `transliterate(text, from_script, to_script)`
Transliterates between scripts (e.g., Cyrillic to Latin).

```bash
dsq '.name | transliterate("cyrillic", "latin")' russian_names.csv

dsq 'map({
  original: .name,
  transliterated: .name | transliterate("cyrillic", "latin")
})' data.csv
```

## Examples

### Null Handling
```bash
# Provide defaults for nulls
dsq 'map({
  name: coalesce(.name, "Unknown"),
  age: coalesce(.age, 0),
  email: coalesce(.email, .backup_email, "none")
})' people.csv

# Filter out nulls
dsq 'map(select(.value | empty | not))' data.csv

# Replace nulls
dsq 'map(iif(.score == null, {score: 0}, .))' students.csv
```

### Conditional Logic
```bash
# Categorize values
dsq 'map({
  category: iif(.value < 10, "low",
           iif(.value < 50, "medium", "high"))
})' data.csv

# Apply different transformations
dsq 'map({
  price: iif(.currency == "USD", .amount, .amount * 1.1)
})' transactions.csv

# Handle errors gracefully
dsq 'map({
  parsed: iferror(.json_field | fromjson, null)
})' data.csv
```

### Data Cleaning
```bash
# Remove unwanted fields
dsq 'map(del("temp") | del("debug"))' data.json

# Transform all keys
dsq 'transform_keys(snake_case)' camelCaseData.json

# Normalize field names
dsq 'transform_keys(tolower | trim)' messyData.csv
```

### Sequence Generation
```bash
# Generate IDs
dsq 'range(1, 100) | map({id: ., status: "pending"})' | dsq -o tasks.json

# Create test data
dsq 'range(1, 10) | map({
  id: .,
  name: ("User " + (. | tostring)),
  created: now()
})'

# Time series scaffolding
dsq 'time_series_range("2024-01-01", "2024-12-31", "1 day") | map({
  date: .,
  value: 0
})'
```

### Type Checking and Conversion
```bash
# Check types
dsq 'map({field: .value, type: (.value | type)})' data.csv

# Filter by type
dsq 'map(select(.value | type == "number"))' mixed_types.json

# Safe conversion
dsq 'map({
  number: iferror(.text | tonumber, null),
  is_number: (.text | tonumber | type == "number")
})' data.csv
```

### Object Manipulation
```bash
# Check for required fields
dsq 'map(select(has("email") and has("name")))' users.csv

# Get all unique keys
dsq 'map(keys) | flatten | unique' varied_objects.json

# Extract values only
dsq 'map(values)' key_value_data.json
```

### UUID Generation
```bash
# Add UUIDs to existing data
dsq 'map(. + {id: generate_uuidv4()})' data.csv

# Generate UUID lookup table
dsq 'map({
  old_id: .id,
  new_id: generate_uuidv7()
})' legacy_data.csv
```

## Performance Tips

- Use `select` early in pipelines to filter data
- Use `iif` instead of complex if-then-else when possible
- Use `coalesce` for simple null handling
- Use `has` to check field existence before accessing
- Generate sequences outside of map when possible

## Type Support

Utility functions work with:
- All data types (strings, numbers, arrays, objects)
- DataFrame rows and columns
- Null and undefined values
