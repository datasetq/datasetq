# Supported Formats

dsq leverages the `dsq-formats` crate to provide comprehensive support for reading and writing various structured data formats. Format detection is automatic based on file extensions and content analysis.

## Input/Output Formats

### CSV - Comma-Separated Values (`.csv`)

Delimited text format with customizable options.

**Features:**
- Customizable field separators, quote characters
- Comment line support
- Null value handling
- Lazy reading and streaming
- Schema inference
- Whitespace trimming

**Example:**
```bash
dsq '.' data.csv
dsq --csv-separator ';' data.csv
dsq 'map(select(.age > 30))' people.csv -o filtered.csv
```

**Options:**
- `separator` - Field delimiter (default: `,`)
- `has_header` - First row contains headers (default: `true`)
- `quote_char` - Quote character (default: `"`)
- `comment_char` - Comment line prefix (default: `#`)
- `null_values` - Strings to treat as null (e.g., `["NULL", ""]`)
- `encoding` - Character encoding (default: UTF-8)
- `trim_whitespace` - Trim whitespace from fields (default: `false`)
- `infer_schema_length` - Number of rows to scan for schema (default: 1000)

### TSV - Tab-Separated Values (`.tsv`)

Tab-delimited text format.

**Features:**
- Tab as default separator
- Same options as CSV
- Streaming support

**Example:**
```bash
dsq '.' data.tsv
dsq 'head(10)' large.tsv
```

### Parquet (`.parquet`)

Columnar storage format with high performance and compression.

**Features:**
- Columnar storage for efficient queries
- Multiple compression algorithms (Snappy, Gzip, LZO, Brotli, LZ4, Zstd)
- Statistics and metadata
- Lazy reading with predicate pushdown
- Parallel processing
- Memory limits

**Example:**
```bash
dsq '.' data.parquet
dsq --lazy 'filter(.amount > 1000)' transactions.parquet
dsq 'cut(["id", "name"])' users.parquet -o subset.parquet
```

**Read Options:**
- `n_rows` - Limit number of rows
- `columns` - Select specific columns
- `parallel` - Enable parallel reading (default: `true`)
- `memory_map` - Use memory mapping (default: `false`)

**Write Options:**
- `compression` - Compression algorithm (Snappy, Gzip, Zstd, etc.)
- `statistics` - Generate column statistics (default: `true`)
- `row_group_size` - Rows per group (default: 50000)
- `use_dictionary` - Dictionary encoding (default: `true`)

**Detection:** Magic bytes "PAR1" at start and end of file

### JSON - JavaScript Object Notation (`.json`)

Standard JSON format supporting arrays of objects or single objects.

**Features:**
- Pretty printing or compact output
- Schema inference from structure
- Support for nested data

**Example:**
```bash
dsq '.' data.json
dsq -c '.' data.json  # Compact output
dsq 'map({id, name})' users.json -o subset.json
```

**Write Options:**
- `pretty` - Pretty print (default: `true`)
- `indent` - Indentation spaces (default: 2)
- `date_format` - Date formatting string

**Detection:** Valid JSON parsing

### JSON Lines / NDJSON (`.jsonl`, `.ndjson`)

Newline-delimited JSON, one object per line.

**Features:**
- Streaming processing
- Efficient for large datasets
- One JSON object per line
- Lazy reading support

**Example:**
```bash
dsq '.' data.jsonl
dsq 'map(select(.status == "active"))' users.jsonl -o active.jsonl
```

**Detection:** Multiple lines where each line is valid JSON

### Arrow (`.arrow`)

Apache Arrow IPC format for efficient data interchange.

**Features:**
- Columnar in-memory format
- Memory mapping support
- Zero-copy operations
- Fast loading
- Type preservation

**Example:**
```bash
dsq '.' data.arrow
dsq 'head(1000)' large.arrow -o sample.arrow
```

**Detection:** Magic bytes "ARROW1\x00\x00"

### Avro (`.avro`)

Row-based serialization format with schema support.

**Features:**
- Schema-aware serialization
- Compression support (Snappy)
- Type preservation
- Schema evolution

**Example:**
```bash
dsq '.' data.avro
dsq 'map({id, name, email})' users.avro -o subset.avro
```

**Write Options:**
- `compression` - Compression algorithm (Snappy, Deflate)
- `schema` - Explicit schema definition

**Detection:** Magic bytes "Obj\x01"

### ADT - ASCII Delimited Text (`.adt`)

Uses ASCII control characters (28-31) to avoid delimiter conflicts.

**Features:**
- Control characters as delimiters
- Robust handling of text with special characters
- Lazy reading and streaming

**Example:**
```bash
dsq '.' data.adt
dsq 'map(select(.description | contains(",")))' complex.adt
```

**Options:**
- Field separator: ASCII 31 (Unit Separator)
- Record separator: ASCII 30 (Record Separator)

**Detection:** File extension `.adt`

### JSON5 (`.json5`)

Extended JSON with comments and relaxed syntax.

**Features:**
- Single and multi-line comments
- Trailing commas
- Unquoted keys
- More human-readable

**Example:**
```bash
dsq '.' config.json5
```

**Detection:** JSON parsing with comment detection

## Output-Only Formats

### Excel (`.xlsx`)

Microsoft Excel format with multi-sheet support.

**Features:**
- Native Excel compatibility
- Multiple sheets support
- Formatting options

**Example:**
```bash
dsq '.' data.csv -o output.xlsx
dsq 'map({id, name, total})' sales.csv -o report.xlsx
```

**Write Options:**
- `sheet_name` - Name of the sheet (default: "Sheet1")
- `include_header` - Include column headers (default: `true`)

### ORC (`.orc`)

Optimized Row Columnar format for Hive.

**Features:**
- Columnar storage
- High compression
- Predicate pushdown
- Indexing

**Example:**
```bash
dsq '.' data.csv -o data.orc
```

**Write Options:**
- `compression` - Compression algorithm
- `stripe_size` - Size of stripes
- `create_index` - Create column indexes (default: `true`)

**Detection:** Magic bytes "ORC"

## Format Detection

dsq automatically detects formats using:

1. **File Extension** - Primary method (`.csv`, `.parquet`, `.json`, etc.)
2. **Magic Bytes** - Binary format detection (Parquet, Avro, Arrow, ORC)
3. **Content Analysis** - Text format detection by parsing sample content

### Override Detection

```bash
# Force input format
dsq --input-format csv '.' data.txt

# Force output format
dsq '.' data.csv --output-format parquet -o data.out

# Both
dsq -i csv --output-format json '.' input.txt -o output.json
```

## Format Conversion

Convert between any supported formats:

```bash
# CSV to Parquet
dsq '.' data.csv -o data.parquet

# JSON to CSV
dsq '.' data.json -o data.csv

# Parquet to JSON Lines
dsq '.' data.parquet -o data.jsonl

# Multiple conversions
dsq convert input.csv output.parquet
dsq convert data.json data.arrow
```

## Format-Specific Performance Tips

### CSV/TSV
- Use `--csv-separator` to specify delimiter
- Set `infer_schema_length` based on data variability
- Enable lazy reading for large files: `--lazy`

### Parquet
- Always use `--lazy` for large files
- Select only needed columns: `cut(["col1", "col2"])`
- Use Snappy compression for balanced speed/size
- Use Zstd for maximum compression

### JSON Lines
- Prefer over JSON for large datasets
- Enables streaming processing
- Better for append-only logs

### Arrow
- Best for in-memory processing
- Fast data interchange between tools
- Use for temporary files in pipelines

## Compression Support

### Reading Compressed Files

dsq automatically handles compressed inputs:

```bash
dsq '.' data.csv.gz      # Gzip
dsq '.' data.json.bz2    # Bzip2
dsq '.' data.csv.xz      # XZ
dsq '.' data.jsonl.zst   # Zstandard
```

### Writing Compressed Files

Compression is inferred from extension:

```bash
dsq '.' data.csv -o output.csv.gz
dsq '.' data.json -o output.json.zst
```

### Parquet Compression

Set compression algorithm for Parquet:

```bash
# Via config
dsq config set formats.parquet.compression "zstd"

# Via code (API)
ParquetWriteOptions { compression: Zstd, .. }
```

## Best Practices

### Format Selection

**Use CSV when:**
- Human readability is important
- Compatibility with other tools
- Simple tabular data

**Use Parquet when:**
- Large datasets (> 100MB)
- Column-based queries
- Long-term storage
- Need compression

**Use JSON Lines when:**
- Streaming data
- Append-only logs
- Event data
- Need line-by-line processing

**Use Arrow when:**
- Fast data interchange
- In-memory analytics
- Temporary pipeline data

**Use Avro when:**
- Schema evolution needed
- Cross-language compatibility
- Need row-based format with schema

### Conversion Workflow

```bash
# Archive CSV to Parquet
dsq '.' raw_data.csv -o archive/data.parquet

# Export sample as JSON for debugging
dsq 'sample(100)' large.parquet -o sample.json

# Convert logs to compressed JSON Lines
dsq '.' logs.json -o logs.jsonl.gz

# Prepare data for analysis
dsq 'cut(["id", "value", "date"]) | head(10000)' full.csv -o subset.parquet
```

## Configuration

Set default format options in config file:

```toml
[formats.csv]
separator = ","
has_header = true
infer_schema_length = 1000
trim_whitespace = true

[formats.parquet]
compression = "snappy"
statistics = true
row_group_size = 50000

[formats.json]
pretty = true
indent = 2
```

See [CONFIGURATION.md](CONFIGURATION.md) for details.
