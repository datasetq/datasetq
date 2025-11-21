# DataFrame Functions

Functions for working with tabular data and DataFrames.

## DataFrame Inspection

### `columns(dataframe)`
Returns the column names of a DataFrame.

```bash
dsq 'columns' data.csv
# Output: ["id", "name", "age", "city"]

dsq '. | columns | length' data.csv
# Count columns
```

### `shape(dataframe)`
Returns the dimensions (rows, columns) of a DataFrame.

```bash
dsq 'shape' data.csv
# Output: [100, 4]  (100 rows, 4 columns)
```

### `dtypes(dataframe)`
Returns the data types of each column.

```bash
dsq 'dtypes' data.csv
# Output: {"id": "Int64", "name": "Utf8", "age": "Int64", "city": "Utf8"}
```

## Column Selection

### `cut(dataframe, columns)`
Selects specific columns from a DataFrame.

```bash
dsq 'cut(["name", "age"])' people.csv
# Select only name and age columns

dsq 'cut(["id", "email", "status"])' users.csv
```

## Row Selection

### `head(dataframe, n?)`
Returns the first n rows (default: 5).

```bash
dsq 'head' data.csv
# First 5 rows

dsq 'head(10)' data.csv
# First 10 rows

dsq 'head(1)' data.csv
# Just the first row
```

### `tail(dataframe, n?)`
Returns the last n rows (default: 5).

```bash
dsq 'tail' data.csv
# Last 5 rows

dsq 'tail(20)' data.csv
# Last 20 rows
```

### `sample(dataframe, n?)`
Returns a random sample of n rows.

```bash
dsq 'sample(10)' data.csv
# Random sample of 10 rows

dsq 'sample(100)' large_dataset.parquet
# Random sample for quick inspection
```

## Grouping and Aggregation

### `group_by(dataframe, column)`
Groups a DataFrame by one or more columns.

```bash
dsq 'group_by(.department)' employees.csv
# Group by department

dsq 'group_by(.department) | map({dept: .[0].department, count: length})' employees.csv
# Group and count
```

### `pivot(dataframe, index, columns, values)`
Creates a pivot table.

```bash
dsq 'pivot("date", "category", "amount")' sales.csv
# Pivot sales by date and category

dsq 'pivot("month", "product", "revenue")' monthly_sales.csv
```

### `melt(dataframe, id_vars, value_vars?)`
Unpivots a DataFrame (opposite of pivot).

```bash
dsq 'melt(["id", "name"], ["score1", "score2", "score3"])' scores.csv
# Convert wide to long format

dsq 'melt(["date"], null)' wide_data.csv
# Melt all columns except date
```

## Examples

### Data Inspection
```bash
# Quick overview
dsq 'shape' data.csv
dsq 'columns' data.csv
dsq 'dtypes' data.csv
dsq 'head(3)' data.csv

# Check data quality
dsq '. | {rows: shape[0], cols: shape[1], columns: columns}' data.csv
```

### Column Operations
```bash
# Select specific columns
dsq 'cut(["customer_id", "order_date", "total"])' orders.csv

# Reorder columns
dsq 'cut(["name", "email", "id"])' users.csv
```

### Sampling and Preview
```bash
# Quick preview
dsq 'head(5)' large_file.parquet

# Random sample for testing
dsq 'sample(1000)' huge_dataset.csv -o sample.csv

# Top and bottom comparison
dsq '{top: head(3), bottom: tail(3)}' data.csv
```

### Grouping and Aggregation
```bash
# Count by category
dsq 'group_by(.category) | map({cat: .[0].category, count: length})' products.csv

# Sum by group
dsq 'group_by(.region) | map({region: .[0].region, total: (map(.sales) | add)})' sales.csv

# Multiple aggregations
dsq 'group_by(.department) | map({
  dept: .[0].department,
  count: length,
  avg_salary: (map(.salary) | add / length),
  max_salary: (map(.salary) | max)
})' employees.csv
```

### Pivot Tables
```bash
# Sales by date and category
dsq 'pivot("date", "category", "amount")' sales.csv

# Cross-tabulation
dsq 'pivot("region", "product", "quantity")' inventory.csv
```

### Reshaping Data
```bash
# Wide to long format
dsq 'melt(["id"], ["Q1", "Q2", "Q3", "Q4"])' quarterly_data.csv

# Long to wide format
dsq 'group_by(.category) | ...' long_data.csv
```

### Combining Operations
```bash
# Sample and select
dsq 'sample(1000) | cut(["id", "name", "value"])' data.csv

# Group, aggregate, and sort
dsq 'group_by(.category) | map({cat: .[0].category, total: (map(.amount) | add)}) | sort_by(.total) | reverse' sales.csv

# Inspect specific groups
dsq 'group_by(.status) | map({status: .[0].status, count: length, sample: head(2)})' orders.csv
```

## Performance Tips

- Use `head()` or `sample()` for quick data exploration
- Use `cut()` to select only needed columns before processing
- Combine with `--lazy` flag for better performance on large datasets
- Use `group_by()` before aggregation operations

## Type Support

DataFrame functions work with:
- CSV files loaded as DataFrames
- Parquet files
- JSON Lines files
- Any tabular data format
