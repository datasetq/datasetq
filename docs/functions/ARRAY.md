# Array Functions

Functions for working with arrays and lists.

## Basic Operations

### `reverse(array)`
Reverses the order of array elements.

```bash
dsq '[1, 2, 3] | reverse'
# Output: [3, 2, 1]
```

### `sort(array)`
Sorts array elements in ascending order.

```bash
dsq '[3, 1, 2] | sort'
# Output: [1, 2, 3]

dsq '["banana", "apple", "cherry"] | sort'
# Output: ["apple", "banana", "cherry"]
```

### `sort_by(array, keys)`
Sorts array by specified key values.

```bash
dsq 'sort_by(.age)' people.csv
# Sorts people by age field

dsq 'sort_by(.name, .age)' people.csv
# Sorts by name, then age
```

### `unique(array)`
Removes duplicate elements from an array.

```bash
dsq '[1, 2, 2, 3, 1] | unique'
# Output: [1, 2, 3]
```

### `flatten(array)`
Flattens nested arrays into a single-level array.

```bash
dsq '[[1, 2], [3, 4]] | flatten'
# Output: [1, 2, 3, 4]

dsq '[1, [2, [3, 4]]] | flatten'
# Output: [1, 2, 3, 4]
```

### `add(array)`
Sums all numeric elements in an array.

```bash
dsq '[1, 2, 3, 4] | add'
# Output: 10
```

### `min(array)`, `max(array)`
Find minimum or maximum values in an array.

```bash
dsq '[5, 2, 8, 1] | min'
# Output: 1

dsq '[5, 2, 8, 1] | max'
# Output: 8
```

### `first(array)`, `last(array)`
Get the first or last element of an array.

```bash
dsq '[1, 2, 3] | first'
# Output: 1

dsq '[1, 2, 3] | last'
# Output: 3
```

## Array Manipulation

### `array_unshift(array, value)`
Adds an element to the start of an array.

```bash
dsq '[2, 3] | array_unshift(1)'
# Output: [1, 2, 3]
```

### `array_shift(array)`
Removes and returns the first element of an array.

```bash
dsq '[1, 2, 3] | array_shift'
# Output: [2, 3]
```

### `array_push(array, value)`
Adds an element to the end of an array.

```bash
dsq '[1, 2] | array_push(3)'
# Output: [1, 2, 3]
```

### `array_pop(array)`
Removes and returns the last element of an array.

```bash
dsq '[1, 2, 3] | array_pop'
# Output: [1, 2]
```

### `repeat(value, count)`
Repeats a value n times, creating an array.

```bash
dsq 'repeat("x", 5)'
# Output: ["x", "x", "x", "x", "x"]

dsq 'repeat(0, 3)'
# Output: [0, 0, 0]
```

### `zip(arrays...)`
Combines multiple arrays element-wise.

```bash
dsq 'zip([1, 2, 3], ["a", "b", "c"])'
# Output: [[1, "a"], [2, "b"], [3, "c"]]
```

### `transpose(array)`
Transposes a 2D array (swaps rows and columns).

```bash
dsq '[[1, 2], [3, 4]] | transpose'
# Output: [[1, 3], [2, 4]]
```

## Examples with CSV Data

```bash
# Get unique categories
dsq '.[] | .category | unique' products.csv

# Sort products by price
dsq 'sort_by(.price)' products.csv

# Flatten nested order items
dsq '.orders | map(.items) | flatten' orders.json

# Get top 5 prices
dsq 'map(.price) | sort | reverse | .[0:5]' products.csv

# Combine first and last names
dsq 'zip(.first_names, .last_names) | map(join(" "))' names.csv
```

## Type Support

Array functions work with:
- Standard arrays (`[1, 2, 3]`)
- DataFrame columns (treated as arrays)
- Nested structures
