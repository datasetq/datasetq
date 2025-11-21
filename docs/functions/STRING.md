# String Functions

Functions for working with text and strings.

## Basic String Operations

### `tostring(value)`
Converts any value to a string representation.

```bash
dsq '123 | tostring'
# Output: "123"

dsq 'true | tostring'
# Output: "true"
```

### `tonumber(string)`
Parses a string as a number.

```bash
dsq '"123" | tonumber'
# Output: 123

dsq '"3.14" | tonumber'
# Output: 3.14
```

### `split(string, separator)`
Splits a string by a separator into an array.

```bash
dsq '"a,b,c" | split(",")'
# Output: ["a", "b", "c"]

dsq '.full_name | split(" ")' people.csv
# Split names into parts
```

### `join(array, separator)`
Joins array elements into a string with a separator.

```bash
dsq '["a", "b", "c"] | join(",")'
# Output: "a,b,c"

dsq '.tags | join(", ")' articles.json
```

### `concat(strings...)`
Concatenates multiple strings.

```bash
dsq 'concat("Hello", " ", "World")'
# Output: "Hello World"
```

### `replace(string, old, new)`
Replaces all occurrences of a substring.

```bash
dsq '"hello world" | replace("world", "there")'
# Output: "hello there"

dsq '.description | replace("  ", " ")' data.csv
# Replace double spaces with single
```

### `contains(string, substring)`
Checks if a string contains a substring.

```bash
dsq '"hello world" | contains("world")'
# Output: true

dsq 'map(select(.email | contains("@example.com")))' users.csv
```

## String Checks

### `startswith(string, prefix)`
Checks if a string starts with a prefix.

```bash
dsq '"/api/users" | startswith("/api")'
# Output: true
```

### `endswith(string, suffix)`
Checks if a string ends with a suffix.

```bash
dsq '"file.txt" | endswith(".txt")'
# Output: true
```

### `is_valid_utf8(string)`
Checks if a string is valid UTF-8.

```bash
dsq '.text | is_valid_utf8' data.csv
```

## Whitespace Operations

### `trim(string)`, `lstrip(string)`, `rstrip(string)`
Removes whitespace from strings.

```bash
dsq '"  hello  " | trim'
# Output: "hello"

dsq '"  hello  " | lstrip'
# Output: "hello  "

dsq '"  hello  " | rstrip'
# Output: "  hello"
```

## Case Conversion

### `tolower(string)`, `lowercase(string)`
Converts string to lowercase.

```bash
dsq '"HELLO" | tolower'
# Output: "hello"
```

### `toupper(string)`, `uppercase(string)`
Converts string to uppercase.

```bash
dsq '"hello" | toupper'
# Output: "HELLO"
```

### `titlecase(string)`
Converts to title case (first letter of each word capitalized).

```bash
dsq '"hello world" | titlecase'
# Output: "Hello World"
```

### `snake_case(string)`, `camel_case(string)`
Converts between naming conventions.

```bash
dsq '"HelloWorld" | snake_case'
# Output: "hello_world"

dsq '"hello_world" | camel_case'
# Output: "helloWorld"
```

## String Transformations

### `pluralize(string)`, `singular(string)`
Converts between plural and singular forms.

```bash
dsq '"user" | pluralize'
# Output: "users"

dsq '"categories" | singular'
# Output: "category"
```

### `to_ascii(string)`
Converts to ASCII, removing accents.

```bash
dsq '"café" | to_ascii'
# Output: "cafe"
```

### `to_valid_utf8(string)`
Converts invalid UTF-8 sequences to valid UTF-8.

```bash
dsq '.text | to_valid_utf8' data.csv
```

### `humanize(number)`
Formats numbers for human readability.

```bash
dsq '1234567 | humanize'
# Output: "1,234,567"
```

## Line Ending Conversion

### `dos2unix(string)`, `unix2dos(string)`
Converts line endings between Windows and Unix formats.

```bash
dsq '.content | dos2unix' windows_file.txt
dsq '.content | unix2dos' unix_file.txt
```

### `tabs_to_spaces(string)`, `spaces_to_tabs(string)`
Converts between tabs and spaces.

```bash
dsq '.code | tabs_to_spaces' source.txt
```

## Encoding/Decoding

### Base64
```bash
dsq '"Hello" | base64_encode'
# Output: "SGVsbG8="

dsq '"SGVsbG8=" | base64_decode'
# Output: "Hello"
```

### Base58
```bash
dsq '"Hello" | base58_encode'
dsq '<encoded> | base58_decode'
```

### Base32
```bash
dsq '"Hello" | base32_encode'
dsq '<encoded> | base32_decode'
```

## Hash Functions

### `sha256(string)`, `sha512(string)`, `sha1(string)`, `md5(string)`
Generates cryptographic hashes.

```bash
dsq '"password" | sha256'
# Output: hash string

dsq '.password | sha512' users.csv
```

## JSON Operations

### `tojson(value)`
Converts a value to JSON string.

```bash
dsq '{name: "Alice", age: 30} | tojson'
# Output: '{"name":"Alice","age":30}'
```

### `fromjson(string)`
Parses a JSON string.

```bash
dsq '"{\"name\":\"Alice\"}" | fromjson'
# Output: {name: "Alice"}
```

## Examples with Data Files

```bash
# Clean up names
dsq 'map({name: .name | trim | titlecase})' people.csv

# Extract domains from emails
dsq '.email | split("@") | .[1]' users.csv

# Normalize text
dsq '.description | tolower | trim' products.csv

# Create slugs
dsq '.title | tolower | replace(" ", "-")' articles.json

# Encode sensitive data
dsq '.api_key | sha256' config.json
```

## Type Support

String functions work with:
- String values
- DataFrame string columns
- Converted values via `tostring`
