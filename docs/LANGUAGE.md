# The dsq Language

## Introduction

dsq (pronounced "disk") is a powerful data processing language and tool that extends the familiar jq syntax to work with structured data formats beyond JSON. Built on top of the jaq jq implementation and leveraging Polars DataFrames, dsq provides high-performance data manipulation across multiple file formats including CSV, Parquet, Avro, JSON Lines, and more.

## Core Philosophy

dsq maintains the core philosophy of jq - providing a concise, functional programming language for data transformation - while extending it to handle the complexities of tabular data and modern data formats. The language emphasizes:

- **Familiar Syntax**: jq-compatible filter expressions that data engineers already know
- **Format Flexibility**: Seamless processing of multiple structured data formats
- **Performance**: High-performance columnar operations using Polars
- **Type Safety**: Proper type handling and clear error messages
- **Composability**: Chainable operations that work across different data representations

## Language Overview

### Data Model

Unlike jq which operates on JSON values (objects, arrays, primitives), dsq works with a unified data model that bridges JSON-like structures and tabular data:

```rust
// Core data types in dsq
enum Value {
    // JSON-compatible types
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),

    // DataFrame types for tabular data
    DataFrame(polars::DataFrame),
    LazyFrame(polars::LazyFrame),
    Series(polars::Series),
}
```

This unified model allows dsq to seamlessly work with both traditional JSON data and modern columnar formats.

### Basic Syntax

dsq inherits jq's core syntax for navigation and transformation:

```bash
# Field access
.name
.["name"]
.name.field

# Array operations
.[0]          # first element
.[-1]         # last element
.[]           # iterate over array
.[0:10]       # slice first 10 elements

# Filtering
select(.age > 30)
map(.name)
map(select(.status == "active"))

# Arithmetic and logic
. + 1
. > 100
and(.active; .verified)
```

### Extended Operations for Tabular Data

While maintaining jq compatibility, dsq adds powerful operations specifically designed for tabular data:

#### Column Selection and Manipulation

```bash
# Select specific columns (DataFrame extension)
{name, age, department}

# Add computed columns
{name, age, salary, bonus: (.salary * 0.1)}

# Rename columns
{name: .employee_name, age, department}
```

#### Sorting and Ordering

```bash
# Sort by single column
sort_by(.age)

# Sort by multiple columns with direction
sort_by(.department; .age)

# Descending sort
sort_by(.salary) | reverse
```

#### Aggregation Operations

```bash
# Group and aggregate
group_by(.department) | map({
  dept: .[0].department,
  count: length,
  avg_salary: (map(.salary) | add / length),
  max_age: (map(.age) | max)
})

# Statistical aggregations
group_by(.category) | map({
  category: .[0].category,
  total: map(.amount) | add,
  average: map(.amount) | add / length,
  minimum: map(.amount) | min,
  maximum: map(.amount) | max
})
```

#### Join Operations

```bash
# Join datasets (DataFrame extension)
join(other_data.csv; .id == .user_id)

# Different join types
inner_join(other.csv; .id)
left_join(other.csv; .id)
outer_join(other.csv; .id)
```

#### Advanced Transformations

```bash
# Pivot operations
pivot(.key_col, .value_col, .agg_col)

# Window functions
rolling_avg(.value; 7)  # 7-day rolling average
cumulative_sum(.amount)

# String operations on columns
{name: (.name | ascii_upcase), email: (.email | ascii_downcase)}
```

## Key Differences from jq/jaq

While dsq maintains syntax compatibility with jq, there are fundamental differences driven by its DataFrame-based architecture:

### 1. Data Model and Type System

**jq/jaq:**
- Operates on JSON values: objects, arrays, strings, numbers, booleans, null
- Dynamic typing with runtime type checking
- No concept of schemas or columnar data

**dsq:**
- Unified value system bridging JSON and DataFrames
- Preserves data types from source formats (dates remain dates, not strings)
- Schema-aware operations with type safety
- Columnar processing for better performance

### 2. Performance Characteristics

**jq/jaq:**
- Interpreted execution
- Row-by-row processing
- Memory usage scales with data size
- Limited to JSON-compatible formats

**dsq:**
- JIT compilation of filter expressions
- Columnar operations via Polars
- Lazy evaluation for query optimization
- Memory-efficient processing of large datasets
- SIMD operations for numerical computations

### 3. Supported Operations

**jq/jaq:**
- JSON navigation and transformation
- Array/object manipulation
- String processing
- Mathematical operations
- Custom function definitions

**dsq:**
- All jq operations (maintained for compatibility)
- **Plus:** DataFrame-specific operations:
  - Column selection and projection
  - Multi-column sorting
  - Group-by aggregations
  - Join operations (inner, left, right, outer)
  - Pivot and unpivot transformations
  - Window functions
  - Date/time operations
  - Statistical aggregations

### 4. File Format Support

**jq/jaq:**
- JSON input/output only
- Text-based processing

**dsq:**
- Multiple structured formats:
  - CSV/TSV (with automatic type inference)
  - Parquet (columnar, compressed)
  - Avro (schema-aware)
  - JSON Lines/NDJSON
  - Arrow (in-memory columnar)
  - Excel (XLSX)
  - ORC (Optimized Row Columnar)

### 5. Execution Model

**jq/jaq:**
- Streaming processing for large JSON files
- Single-pass execution
- Limited optimization

**dsq:**
- **Lazy Evaluation**: Operations are optimized before execution
- **Query Planning**: Automatic optimization of operation pipelines
- **Parallel Execution**: Multi-threaded processing where beneficial
- **Memory Management**: Efficient handling of datasets larger than RAM

### 6. Error Handling and Debugging

**jq/jaq:**
- Basic error reporting
- Limited debugging capabilities

**dsq:**
- Detailed type error messages
- Schema validation errors
- Performance profiling
- Query execution plans
- Memory usage reporting

## Practical Examples

### Processing CSV Data

```bash
# Filter and transform CSV data
dsq 'map(select(.age > 30)) | map({name, age, department})' employees.csv

# Group and aggregate
dsq 'group_by(.department) | map({
  dept: .[0].department,
  count: length,
  avg_salary: (map(.salary) | add / length)
})' employees.csv
```

### Converting Between Formats

```bash
# CSV to Parquet conversion with transformation
dsq 'map(.salary += 5000) | map(select(.active))' employees.csv --output employees.parquet

# Join multiple datasets
dsq 'join(departments.csv; .dept_id == .id) | {name, salary, dept_name: .department_name}' employees.csv
```

### Performance-Optimized Queries

```bash
# Lazy evaluation for large datasets
dsq --lazy 'filter(.amount > 1000) | group_by(.category) | map({
  category: .[0].category,
  total: map(.amount) | add
})' transactions.parquet
```

## Language Extensions and Compatibility

### jq Compatibility Mode

dsq can operate in full jq compatibility mode when working with JSON data:

```bash
# Pure jq syntax works unchanged
dsq '.users[] | select(.age > 21) | {name, email}' data.json
```

### DataFrame Extensions

When working with tabular data, dsq provides additional syntax and functions:

```bash
# Column references (DataFrame extension)
.col("column_name")  # Explicit column access
.col("price") * 1.1  # Column arithmetic

# Date operations (DataFrame extension)
.col("date") | date("2023-01-01")  # Date comparisons
.col("timestamp") | strftime("%Y-%m")  # Date formatting
```

### Custom Functions

dsq supports jq-style function definitions with DataFrame extensions:

```bash
# Define reusable functions
def department_summary:
  group_by(.department) | map({
    dept: .[0].department,
    headcount: length,
    budget: map(.salary) | add
  });

department_summary | sort_by(.budget)
```

## Performance Considerations

### When to Use Lazy Evaluation

```bash
# Use --lazy flag for large datasets
dsq --lazy 'filter(.amount > 1000) | group_by(.category)' large_dataset.parquet
```

### Memory Management

- dsq automatically manages memory for different data sizes
- Large datasets use streaming where appropriate
- Columnar formats (Parquet, Arrow) are more memory-efficient than row-based formats

### Optimization Tips

1. **Filter Early**: Apply filters before aggregations to reduce data volume
2. **Use Appropriate Formats**: Parquet for analytical workloads, CSV for simple transformations
3. **Leverage Laziness**: Use lazy evaluation for complex query pipelines
4. **Column Selection**: Select only needed columns to reduce memory usage

## Future Directions

dsq continues to evolve with planned enhancements including:

- Additional file format support (Delta Lake, Iceberg)
- Advanced analytics functions (correlation, regression)
- Machine learning integrations
- Real-time streaming support
- Enhanced SQL interoperability

## Conclusion

dsq represents the next evolution of the jq philosophy - extending its elegant, composable syntax to the world of structured data processing. By maintaining backward compatibility while adding powerful DataFrame operations, dsq provides a familiar yet powerful tool for data engineers working with modern data formats and large-scale datasets.

The language bridges the gap between traditional JSON processing and high-performance tabular analytics, making complex data transformations accessible through a concise, functional programming model.