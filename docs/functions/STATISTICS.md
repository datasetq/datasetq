# Statistical Functions

Functions for statistical analysis and aggregation.

## Aggregation Functions

### `sum(array|dataframe)`
Calculates the sum of all values.

```bash
dsq '[1, 2, 3, 4] | sum'
# Output: 10

dsq 'map(.amount) | sum' transactions.csv
# Total amount

dsq '.sales | sum' data.csv
# Sum of sales column
```

### `count(array|dataframe)`
Counts non-null values.

```bash
dsq '[1, 2, null, 3] | count'
# Output: 3

dsq '. | count' data.csv
# Count total rows

dsq 'map(select(.status == "active")) | count' users.csv
# Count active users
```

### `mean(array|dataframe)`, `avg(array|dataframe)`
Calculates the arithmetic mean (average).

```bash
dsq '[1, 2, 3, 4, 5] | mean'
# Output: 3

dsq 'map(.score) | avg' students.csv
# Average score

dsq '.salary | mean' employees.csv
# Average salary
```

### `median(array|dataframe)`
Calculates the median (middle value).

```bash
dsq '[1, 2, 3, 4, 5] | median'
# Output: 3

dsq '[1, 2, 3, 4] | median'
# Output: 2.5

dsq '.response_time | median' requests.csv
# Median response time
```

### `min(array|dataframe)`, `max(array|dataframe)`
Finds minimum or maximum values.

```bash
dsq '[5, 2, 8, 1, 9] | min'
# Output: 1

dsq '[5, 2, 8, 1, 9] | max'
# Output: 9

dsq 'map(.price) | {min: min, max: max}' products.csv
```

## Distribution Functions

### `quartile(array|dataframe, percentile)`
Calculates quartile values (0.25, 0.5, 0.75).

```bash
dsq '.values | quartile(0.25)' data.csv
# First quartile (Q1)

dsq '.values | quartile(0.5)' data.csv
# Second quartile (median)

dsq '.values | quartile(0.75)' data.csv
# Third quartile (Q3)
```

### `percentile(array|dataframe, p)`
Calculates the p-th percentile.

```bash
dsq '.response_time | percentile(0.95)' requests.csv
# 95th percentile

dsq '.revenue | percentile(0.99)' sales.csv
# 99th percentile
```

### `histogram(array|dataframe, bins?)`
Creates a histogram of value distributions.

```bash
dsq '.age | histogram(10)' people.csv
# Age distribution in 10 bins

dsq '.price | histogram' products.csv
# Price distribution (auto bins)
```

## Variance and Standard Deviation

### `var(array|dataframe)`
Calculates variance.

```bash
dsq '[1, 2, 3, 4, 5] | var'
# Output: variance value

dsq '.measurements | var' experiments.csv
```

### `std(array|dataframe)`, `stdev_p(array|dataframe)`
Calculates population standard deviation.

```bash
dsq '[1, 2, 3, 4, 5] | std'
# Population std dev

dsq '.scores | stdev_p' tests.csv
```

### `stdev_s(array|dataframe)`
Calculates sample standard deviation.

```bash
dsq '.sample_values | stdev_s' data.csv
# Sample std dev
```

## Window Functions

### `rolling_std(column, window_size, min_periods?)`
Calculates rolling (moving) standard deviation over a window of rows.

```bash
# 3-period rolling standard deviation
dsq '.value | rolling_std(3)' timeseries.csv

# Rolling std with minimum periods
dsq '.price | rolling_std(7, 5)' stock_prices.csv
# 7-day window, requires at least 5 values

# Analyze volatility
dsq '{
  price: .price,
  volatility: (.price | rolling_std(30))
}' daily_data.csv
```

**Parameters:**
- `column`: Column name to calculate rolling std on
- `window_size`: Number of periods in the rolling window
- `min_periods` (optional): Minimum number of observations required to have a result (defaults to window_size)

**Returns:** DataFrame or Array with additional `{column}_rolling_std` field/column

## Correlation

### `correl(array1, array2)`
Calculates correlation coefficient between two arrays.

```bash
dsq 'correl(.x, .y)' data.csv
# Correlation between x and y

dsq 'correl(.temperature, .sales)' weather_sales.csv
# Temperature vs sales correlation
```

## Conditional Aggregation

### `avg_if(values, mask)`
Average of values where mask is true.

```bash
dsq 'avg_if(.sales, .region == "North")' data.csv
# Average sales in North region

dsq 'avg_if(.score, .passed)' students.csv
# Average score of passing students
```

### `count_if(values, mask)`
Count of values where mask is true.

```bash
dsq 'count_if(.amount, .amount > 1000)' transactions.csv
# Count high-value transactions

dsq 'count_if(.status, .status == "completed")' orders.csv
```

### `avg_ifs(values, mask1, mask2, ...)`
Average with multiple conditions.

```bash
dsq 'avg_ifs(.price, .category == "electronics", .in_stock)' products.csv
# Average price of in-stock electronics
```

## Frequency Analysis

### `least_frequent(array|dataframe)`
Finds the most common value (note: naming is historical).

```bash
dsq '.category | least_frequent' products.csv
# Most common category

dsq 'map(.status) | least_frequent' orders.csv
```

### `min_by(array, key)`, `max_by(array, key)`
Find minimum/maximum by a key function.

```bash
dsq 'min_by(.price)' products.csv
# Product with minimum price

dsq 'max_by(.score)' students.csv
# Student with highest score
```

## Examples

### Basic Statistics
```bash
# Five-number summary
dsq '.values | {
  min: min,
  q1: quartile(0.25),
  median: median,
  q3: quartile(0.75),
  max: max
}' data.csv

# Mean and standard deviation
dsq '.measurements | {
  mean: mean,
  std: std,
  var: var
}' experiments.csv
```

### Group Statistics
```bash
# Statistics by group
dsq 'group_by(.category) | map({
  category: .[0].category,
  count: length,
  avg: (map(.price) | mean),
  min: (map(.price) | min),
  max: (map(.price) | max)
})' products.csv

# Regional analysis
dsq 'group_by(.region) | map({
  region: .[0].region,
  total_sales: (map(.sales) | sum),
  avg_sales: (map(.sales) | mean),
  median_sales: (map(.sales) | median)
})' sales.csv
```

### Percentile Analysis
```bash
# Response time analysis
dsq '.response_time | {
  p50: percentile(0.50),
  p95: percentile(0.95),
  p99: percentile(0.99)
}' requests.csv

# Performance thresholds
dsq 'map(select(.score > percentile(.score, 0.75)))' students.csv
# Students above 75th percentile
```

### Distribution Analysis
```bash
# Check data distribution
dsq '.values | histogram(20)' data.csv

# Identify outliers (beyond 3 std devs)
dsq '. as $data | (.values | std) as $std | (.values | mean) as $mean |
  $data | map(select(.values > ($mean + 3 * $std) or .values < ($mean - 3 * $std)))' data.csv
```

### Conditional Statistics
```bash
# Average by condition
dsq '{
  avg_all: (.price | avg),
  avg_premium: avg_if(.price, .tier == "premium"),
  avg_basic: avg_if(.price, .tier == "basic")
}' products.csv

# Count by multiple conditions
dsq '{
  total: count,
  high_value: count_if(.amount, .amount > 1000),
  completed: count_if(.status, .status == "completed")
}' transactions.csv
```

### Correlation Analysis
```bash
# Find correlations
dsq '{
  temp_sales: correl(.temperature, .sales),
  price_volume: correl(.price, .volume),
  age_income: correl(.age, .income)
}' data.csv
```

## Performance Tips

- Use conditional aggregation functions instead of multiple filters
- Group data before running statistics on subsets
- Use `--lazy` flag for large datasets
- Consider sampling for exploratory analysis: `sample(10000) | ...`

## Type Support

Statistical functions work with:
- Numeric arrays
- DataFrame numeric columns
- Filtered and transformed data
