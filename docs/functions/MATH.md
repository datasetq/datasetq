# Math Functions

Mathematical operations and calculations.

## Basic Arithmetic

### `abs(number)`
Returns the absolute value of a number.

```bash
dsq '-5 | abs'
# Output: 5

dsq 'map({value: .difference | abs})' data.csv
```

### `sqrt(number)`
Calculates the square root.

```bash
dsq '16 | sqrt'
# Output: 4

dsq '2 | sqrt'
# Output: 1.414...
```

### `pow(base, exponent)`
Raises a number to a power.

```bash
dsq 'pow(2, 3)'
# Output: 8

dsq 'pow(10, 2)'
# Output: 100
```

### `exp(number)`
Returns e raised to the given power.

```bash
dsq '1 | exp'
# Output: 2.718... (e)
```

### `log10(number)`
Calculates the base-10 logarithm.

```bash
dsq '100 | log10'
# Output: 2

dsq '1000 | log10'
# Output: 3
```

## Rounding Functions

### `floor(number)`
Rounds down to the nearest integer.

```bash
dsq '3.7 | floor'
# Output: 3

dsq '-2.3 | floor'
# Output: -3
```

### `ceil(number)`
Rounds up to the nearest integer.

```bash
dsq '3.2 | ceil'
# Output: 4

dsq '-2.7 | ceil'
# Output: -2
```

### `round(number)`
Rounds to the nearest integer.

```bash
dsq '3.5 | round'
# Output: 4

dsq '3.4 | round'
# Output: 3
```

### `roundup(number)`, `rounddown(number)`
Explicitly rounds up or down.

```bash
dsq '3.1 | roundup'
# Output: 4

dsq '3.9 | rounddown'
# Output: 3
```

### `mround(number, multiple)`
Rounds to the nearest multiple.

```bash
dsq '17 | mround(5)'
# Output: 15

dsq '23 | mround(10)'
# Output: 20
```

## Trigonometric Functions

### `sin(number)`, `cos(number)`, `tan(number)`
Basic trigonometric functions (input in radians).

```bash
dsq 'pi / 2 | sin'
# Output: 1

dsq '0 | cos'
# Output: 1

dsq 'pi / 4 | tan'
# Output: 1
```

### `asin(number)`, `acos(number)`, `atan(number)`
Inverse trigonometric functions (output in radians).

```bash
dsq '1 | asin'
# Output: π/2

dsq '0 | acos'
# Output: π/2

dsq '1 | atan'
# Output: π/4
```

## Random Numbers

### `rand()`
Generates a random number between 0 and 1.

```bash
dsq 'rand()'
# Output: 0.xxx (random)
```

### `randarray(count)`
Generates an array of random numbers.

```bash
dsq 'randarray(5)'
# Output: [0.xxx, 0.xxx, 0.xxx, 0.xxx, 0.xxx]
```

### `randbetween(min, max)`
Generates a random integer in a range.

```bash
dsq 'randbetween(1, 10)'
# Output: random integer from 1 to 10

dsq 'randbetween(100, 200)'
# Output: random integer from 100 to 200
```

## Constants

### `pi()`
Returns the value of π (pi).

```bash
dsq 'pi()'
# Output: 3.14159265...
```

## Examples with Data

```bash
# Calculate distances
dsq 'map({x, y, distance: sqrt(pow(.x, 2) + pow(.y, 2))})' points.csv

# Round prices to nearest 5
dsq 'map({price: .price | mround(5)})' products.csv

# Calculate percentage changes
dsq 'map({pct: ((.new - .old) / .old | abs * 100 | round)})' changes.csv

# Generate random sample IDs
dsq 'map({id: randbetween(1000, 9999)})' data.csv

# Normalize values
dsq 'map({normalized: .value / (sqrt(pow(.x, 2) + pow(.y, 2)))})' vectors.csv

# Calculate angles
dsq 'map({angle: atan(.y / .x)})' coordinates.csv

# Round to 2 decimal places
dsq 'map({value: (.value * 100 | round) / 100})' data.csv
```

## Type Support

Math functions work with:
- Numeric values (integers, floats)
- DataFrame numeric columns
- Results of numeric expressions
