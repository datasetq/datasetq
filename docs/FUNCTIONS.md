# Built-in Functions

dsq provides an extensive collection of built-in functions through the `dsq-functions` crate, supporting jq-compatible operations plus DataFrame-specific functions. All functions work with strings, arrays, objects, DataFrames, and Series.

## Function Categories

- [Array Functions](functions/ARRAY.md) - Array manipulation and operations
- [String Functions](functions/STRING.md) - Text processing and transformation
- [Math Functions](functions/MATH.md) - Mathematical operations
- [DataFrame Functions](functions/DATAFRAME.md) - Tabular data operations
- [Statistical Functions](functions/STATISTICS.md) - Statistical analysis and aggregation
- [Date/Time Functions](functions/DATETIME.md) - Date and time operations
- [URL Functions](functions/URL.md) - URL parsing and manipulation
- [Utility Functions](functions/UTILITY.md) - General-purpose utilities

## Quick Reference

## Basic Operations

- `length(value)` - Returns the length of arrays, strings, objects, or DataFrame height
- `keys(value)` - Returns array indices, object keys, or DataFrame column names
- `has(container, key)` - Checks if an object contains a key
- `values(value)` - Returns array elements or object values
- `type(value)` - Returns the type name of a value
- `empty(value)` - Checks if a value is empty (null, empty array/string/object)
- `error(message)` - Throws an error with the given message

## Array Operations

### Basic Array Functions
- `reverse(array)` - Reverses the order of array elements
- `sort(array)` - Sorts array elements
- `sort_by(array, keys)` - Sorts array by key values
- `unique(array)` - Removes duplicate elements
- `flatten(array)` - Flattens nested arrays
- `add(array)` - Sums numeric array elements
- `min(array)`, `max(array)` - Find minimum/maximum values
- `first(array)`, `last(array)` - Get first/last elements

### Array Manipulation
- `array_unshift(array, value)` - Add element to start
- `array_shift(array)` - Remove element from start
- `array_push(array, value)` - Add element to end
- `array_pop(array)` - Remove element from end
- `repeat(value, count)` - Repeat a value n times
- `zip(arrays...)` - Combine multiple arrays element-wise
- `transpose(array)` - Transpose a 2D array

## String Operations

### Basic String Functions
- `tostring(value)` - Convert any value to string
- `tonumber(string)` - Parse string as number
- `split(string, separator)` - Split string by separator
- `join(array, separator)` - Join array elements with separator
- `concat(strings...)` - Concatenate strings
- `replace(string, old, new)` - Replace substrings
- `contains(string, substring)` - Check if string contains substring

### String Checks
- `startswith(string, prefix)` - Check if string starts with prefix
- `endswith(string, suffix)` - Check if string ends with suffix
- `is_valid_utf8(string)` - Check UTF-8 validity

### String Transformations
- `lstrip(string)`, `rstrip(string)`, `trim(string)` - Remove whitespace
- `tolower(string)`, `toupper(string)` - Case conversion
- `lowercase(string)`, `uppercase(string)` - Aliases for case conversion
- `titlecase(string)` - Convert to title case
- `snake_case(string)`, `camel_case(string)` - Case style conversion
- `pluralize(string)`, `singular(string)` - Plural/singular forms
- `to_ascii(string)` - Convert to ASCII (remove accents)
- `to_valid_utf8(string)` - Convert invalid UTF-8 to valid

### String Formatting
- `dos2unix(string)`, `unix2dos(string)` - Convert line endings
- `tabs_to_spaces(string)`, `spaces_to_tabs(string)` - Tab/space conversion
- `humanize(number)` - Format numbers for human readability

### Encoding/Decoding
- `base32_encode(string)`, `base32_decode(string)` - Base32 encoding/decoding
- `base58_encode(string)`, `base58_decode(string)` - Base58 encoding/decoding
- `base64_encode(string)`, `base64_decode(string)` - Base64 encoding/decoding

### Hash Functions
- `sha512(string)` - SHA-512 hash
- `sha256(string)` - SHA-256 hash
- `sha1(string)` - SHA-1 hash
- `md5(string)` - MD5 hash

### JSON Operations
- `tojson(value)` - Convert to JSON string
- `fromjson(string)` - Parse JSON string

## Math Operations

### Basic Math
- `abs(number)` - Absolute value
- `sqrt(number)` - Square root
- `pow(base, exponent)` - Power function
- `exp(number)` - Exponential function
- `log10(number)` - Base-10 logarithm

### Rounding
- `floor(number)`, `ceil(number)` - Floor and ceiling functions
- `round(number)` - Round to nearest integer
- `roundup(number)`, `rounddown(number)` - Round up/down
- `mround(number, multiple)` - Round to nearest multiple

### Trigonometry
- `sin(number)`, `cos(number)`, `tan(number)` - Trigonometric functions
- `asin(number)`, `acos(number)`, `atan(number)` - Inverse trigonometric functions

### Random Numbers
- `rand()` - Random number between 0 and 1
- `randarray(count)` - Array of random numbers
- `randbetween(min, max)` - Random integer in range

### Constants
- `pi()` - Pi constant (3.14159...)

## DataFrame Operations

### DataFrame Inspection
- `columns(dataframe)` - Get column names
- `shape(dataframe)` - Get (rows, columns) dimensions
- `dtypes(dataframe)` - Get column data types

### DataFrame Selection
- `cut(dataframe, columns)` - Select specific columns
- `head(dataframe, n?)` - Get first n rows (default 5)
- `tail(dataframe, n?)` - Get last n rows (default 5)
- `sample(dataframe, n?)` - Random sample of n rows

### DataFrame Transformation
- `group_by(dataframe, column)` - Group DataFrame by column
- `pivot(dataframe, index, columns, values)` - Pivot table
- `melt(dataframe, id_vars, value_vars?)` - Unpivot DataFrame

## Statistical Operations

### Aggregation
- `sum(array|dataframe)` - Sum of values
- `count(array|dataframe)` - Count of non-null values
- `mean(array|dataframe)`, `avg(array|dataframe)` - Arithmetic mean
- `median(array|dataframe)` - Median value
- `min(array|dataframe)`, `max(array|dataframe)` - Minimum/maximum values

### Distribution
- `quartile(array|dataframe, percentile)` - Percentile (0.25, 0.5, 0.75)
- `percentile(array|dataframe, p)` - p-th percentile
- `histogram(array|dataframe, bins?)` - Value distribution

### Variance
- `std(array|dataframe)`, `stdev_p(array|dataframe)` - Population standard deviation
- `stdev_s(array|dataframe)` - Sample standard deviation
- `var(array|dataframe)` - Variance

### Correlation
- `correl(array1, array2)` - Correlation coefficient

### Conditional Aggregation
- `avg_if(values, mask)` - Average with condition
- `count_if(values, mask)` - Count with condition
- `avg_ifs(values, mask1, mask2, ...)` - Average with multiple conditions

### Frequency
- `least_frequent(array|dataframe)` - Most common value
- `min_by(array, key)`, `max_by(array, key)` - Min/max by key function

## Date/Time Operations

### Date Components
- `year(date)` - Extract year
- `month(date)` - Extract month
- `day(date)` - Extract day
- `hour(date)` - Extract hour
- `minute(date)` - Extract minute
- `second(date)` - Extract second

### Date Construction
- `mktime(year, month, day, hour?, min?, sec?)` - Create timestamp
- `today()` - Current date
- `now()` - Current timestamp

### System Time
- `systime()` - System time in seconds
- `systime_ns()` - System time in nanoseconds
- `systime_int()` - System time as integer

### Time Formatting
- `strftime(timestamp, format)` - Format timestamp
- `strflocaltime(timestamp, format)` - Format local timestamp
- `strptime(string, format)` - Parse timestamp string

### Time Conversion
- `localtime(timestamp)` - Local time
- `gmtime(timestamp)` - GMT time

### Date Arithmetic
- `date_diff(date1, date2, unit?)` - Date difference
- `truncate_date(date, unit)` - Truncate date to unit
- `truncate_time(time, unit)` - Truncate time to unit

### Date Ranges
- `start_of_month(date)`, `end_of_month(date)` - Month boundaries
- `start_of_week(date)`, `end_of_week(date)` - Week boundaries

## Utility Functions

### Control Flow
- `iif(condition, true_value, false_value)` - Conditional expression
- `iferror(value, fallback)` - Error handling
- `coalesce(values...)` - Return first non-null value

### Data Generation
- `range(start, end, step?)` - Generate number sequence
- `generate_sequence(start, end, step?)` - Generate sequences
- `time_series_range(start, end, interval)` - Time series sequences
- `generate_uuidv4()`, `generate_uuidv7()` - Generate UUIDs

### Data Processing
- `select(value)` - Filter truthy values
- `map(array, field|template)` - Transform array elements
- `filter(array, condition)` - Filter array elements
- `unnest(array)` - Unnest nested structures
- `group_concat(array, separator?)` - Concatenate with grouping

### Object Operations
- `del(object, key)` - Remove object key
- `transform_keys(object, function)` - Transform object keys

## URL Operations

### URL Parsing
- `url_parse(url)` - Parse URL into components
- `url_extract_domain(url)` - Extract domain from URL
- `url_extract_path(url)` - Extract path from URL
- `url_extract_query_string(url)` - Extract query string
- `url_extract_protocol(url)` - Extract protocol
- `url_extract_port(url)` - Extract port number

### URL Modification
- `url_set_protocol(url, protocol)` - Change URL protocol
- `url_set_path(url, path)` - Change URL path
- `url_set_domain(url, domain)` - Change URL domain
- `url_set_domain_without_www(url)` - Remove www from domain
- `url_set_query_string(url, key, value)` - Set query parameter
- `url_set_port(url, port)` - Change URL port

### URL Cleanup
- `url_strip_fragment(url)` - Remove URL fragment
- `url_strip_query_string(url)` - Remove query string
- `url_strip_port(url)` - Remove port (if default)
- `url_strip_port_if_default(url)` - Remove default ports
- `url_strip_protocol(url)` - Remove protocol

## Text Processing

- `transliterate(text, from_script, to_script)` - Script transliteration (e.g., Cyrillic to Latin)

## Function Usage Examples

### Array Operations
```bash
# Sort array by field
dsq 'sort_by(.age)' people.csv

# Get unique values
dsq '.[] | unique' data.json

# Flatten nested arrays
dsq 'flatten' nested.json
```

### String Operations
```bash
# Split and join
dsq '.name | split(" ") | join("_")' data.csv

# Case conversion
dsq 'map({name: .name | titlecase})' names.json

# URL encoding
dsq '.url | base64_encode' urls.csv
```

### Statistical Operations
```bash
# Calculate mean
dsq 'map(.salary) | mean' employees.csv

# Get percentiles
dsq '.values | percentile(0.95)' metrics.json

# Correlation
dsq 'correl(.x, .y)' data.csv
```

### Date Operations
```bash
# Extract date components
dsq 'map({y: .date | year, m: .date | month})' events.csv

# Date formatting
dsq '.timestamp | strftime("%Y-%m-%d")' logs.json

# Date difference
dsq 'date_diff(.end_date, .start_date, "days")' periods.csv
```

### DataFrame Operations
```bash
# Select columns
dsq 'cut(["name", "age"])' people.csv

# Group and aggregate
dsq 'group_by(.department)' employees.csv

# Pivot table
dsq 'pivot("date", "category", "amount")' sales.csv
```

## Type Polymorphism

Many functions work across different data types:

```bash
# length works on strings, arrays, DataFrames
dsq '.name | length'        # string length
dsq '.items | length'       # array length
dsq '. | length'            # DataFrame row count

# mean works on arrays and DataFrames
dsq '.scores | mean'        # array mean
dsq '.salary | mean'        # column mean
```

## Error Handling

Functions include comprehensive error handling:

```bash
# Use iferror for graceful fallback
dsq 'iferror(.value | tonumber, 0)' data.csv

# Use coalesce for null handling
dsq 'coalesce(.value1, .value2, 0)' data.csv
```
