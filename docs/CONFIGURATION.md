# Configuration

dsq can be configured via TOML or YAML configuration files to customize default behavior.

## Configuration File Locations

Configuration files are searched in the following order:

1. **Current directory:**
   - `dsq.toml`
   - `.dsq.toml`
   - `dsq.yaml`
   - `.dsq.yaml`

2. **Home directory:**
   - `~/.config/dsq/config.toml`
   - `~/.config/dsq/config.yaml`
   - `~/.dsq.toml`
   - `~/.dsq.yaml`

3. **System directory:**
   - `/etc/dsq/config.toml`
   - `/etc/dsq/config.yaml`

The first found file is used. You can also specify a custom config file with `--config`.

## Configuration File Format

### TOML Format (Recommended)

```toml
# dsq.toml

[filter]
lazy_evaluation = true
dataframe_optimizations = true
optimization_level = "advanced"  # none, basic, advanced
max_recursion_depth = 100
strict_mode = false

[performance]
batch_size = 10000
threads = 0  # 0 = auto-detect
parallel = true
memory_limit = "4GB"  # Optional: "1GB", "500MB", etc.

[formats.csv]
separator = ","
has_header = true
quote_char = "\""
comment_char = "#"
null_values = ["NULL", ""]
infer_schema_length = 1000
trim_whitespace = true
encoding = "utf8"

[formats.parquet]
compression = "snappy"  # snappy, gzip, lzo, brotli, lz4, zstd
statistics = true
row_group_size = 50000
use_dictionary = true
parallel_read = true

[formats.json]
pretty = true
indent = 2
date_format = "%Y-%m-%d"

[formats.jsonl]
buffer_size = 8192

[display]
color.enabled = true
color.auto_detect = true
compact = false
raw_output = false
sort_keys = false
indent = 2
max_display_rows = 100

[output]
default_format = "json"  # json, csv, parquet, etc.
overwrite = false
include_header = true
```

### YAML Format

```yaml
# dsq.yaml

filter:
  lazy_evaluation: true
  dataframe_optimizations: true
  optimization_level: advanced
  max_recursion_depth: 100
  strict_mode: false

performance:
  batch_size: 10000
  threads: 0
  parallel: true
  memory_limit: 4GB

formats:
  csv:
    separator: ","
    has_header: true
    quote_char: "\""
    comment_char: "#"
    null_values: ["NULL", ""]
    infer_schema_length: 1000
    trim_whitespace: true
    encoding: utf8

  parquet:
    compression: snappy
    statistics: true
    row_group_size: 50000
    use_dictionary: true
    parallel_read: true

  json:
    pretty: true
    indent: 2
    date_format: "%Y-%m-%d"

display:
  color:
    enabled: true
    auto_detect: true
  compact: false
  raw_output: false
  sort_keys: false
  indent: 2
  max_display_rows: 100

output:
  default_format: json
  overwrite: false
  include_header: true
```

## Configuration Sections

### Filter Settings

Controls filter compilation and execution behavior.

```toml
[filter]
lazy_evaluation = true           # Enable lazy evaluation
dataframe_optimizations = true   # Enable DataFrame-specific optimizations
optimization_level = "advanced"  # Optimization level: none, basic, advanced
max_recursion_depth = 100        # Maximum recursion depth
strict_mode = false              # Strict error handling
collect_stats = false            # Collect execution statistics
```

**Options:**
- `lazy_evaluation` - Defer execution until needed (default: `false`)
- `dataframe_optimizations` - Apply DataFrame optimizations (default: `true`)
- `optimization_level` - `"none"`, `"basic"`, `"advanced"` (default: `"basic"`)
- `max_recursion_depth` - Recursion limit (default: `100`)
- `strict_mode` - Fail on type errors vs. coerce (default: `false`)
- `collect_stats` - Gather execution statistics (default: `false`)

### Performance Settings

Controls execution performance and resource usage.

```toml
[performance]
batch_size = 10000        # Rows per batch
threads = 0               # Thread count (0 = auto)
parallel = true           # Enable parallel processing
memory_limit = "4GB"      # Optional memory limit
streaming_threshold = 1000000  # Switch to streaming above this
```

**Options:**
- `batch_size` - Rows to process in each batch (default: `10000`)
- `threads` - Number of threads, 0 for auto-detection (default: `0`)
- `parallel` - Enable parallel processing (default: `true`)
- `memory_limit` - Maximum memory usage (optional)
- `streaming_threshold` - Row count to trigger streaming (default: `1000000`)

### CSV Format Settings

```toml
[formats.csv]
separator = ","
has_header = true
quote_char = "\""
comment_char = "#"
null_values = ["NULL", "", "N/A"]
infer_schema_length = 1000
trim_whitespace = true
encoding = "utf8"
skip_rows = 0
skip_rows_after_header = 0
```

**Options:**
- `separator` - Field delimiter (default: `","`)
- `has_header` - First row is header (default: `true`)
- `quote_char` - Quote character (default: `"\""`)
- `comment_char` - Comment line prefix (default: `"#"`)
- `null_values` - Strings treated as null (default: `["NULL", ""]`)
- `infer_schema_length` - Rows to scan for schema (default: `1000`)
- `trim_whitespace` - Trim field whitespace (default: `false`)
- `encoding` - Character encoding (default: `"utf8"`)
- `skip_rows` - Skip N rows at start (default: `0`)
- `skip_rows_after_header` - Skip N rows after header (default: `0`)

### Parquet Format Settings

```toml
[formats.parquet]
compression = "snappy"
statistics = true
row_group_size = 50000
use_dictionary = true
parallel_read = true
memory_map = false
```

**Options:**
- `compression` - Compression algorithm: `"snappy"`, `"gzip"`, `"lzo"`, `"brotli"`, `"lz4"`, `"zstd"`, `"none"` (default: `"snappy"`)
- `statistics` - Generate column statistics (default: `true`)
- `row_group_size` - Rows per row group (default: `50000`)
- `use_dictionary` - Use dictionary encoding (default: `true`)
- `parallel_read` - Parallel reading (default: `true`)
- `memory_map` - Use memory mapping (default: `false`)

### JSON Format Settings

```toml
[formats.json]
pretty = true
indent = 2
date_format = "%Y-%m-%d"
```

**Options:**
- `pretty` - Pretty-print output (default: `true`)
- `indent` - Indentation spaces (default: `2`)
- `date_format` - Date formatting string (default: ISO 8601)

### Display Settings

Controls output appearance in terminal.

```toml
[display]
color.enabled = true
color.auto_detect = true
compact = false
raw_output = false
sort_keys = false
indent = 2
max_display_rows = 100
```

**Options:**
- `color.enabled` - Enable colored output (default: `true`)
- `color.auto_detect` - Auto-detect terminal color support (default: `true`)
- `compact` - Compact output without whitespace (default: `false`)
- `raw_output` - Output raw strings without quotes (default: `false`)
- `sort_keys` - Sort object keys (default: `false`)
- `indent` - Indentation spaces (default: `2`)
- `max_display_rows` - Maximum rows to display (default: `100`)

### Output Settings

Default output behavior.

```toml
[output]
default_format = "json"
overwrite = false
include_header = true
```

**Options:**
- `default_format` - Default output format (default: `"json"`)
- `overwrite` - Overwrite existing files (default: `false`)
- `include_header` - Include headers in output (default: `true`)

## Managing Configuration

### Using the CLI

```bash
# Show current configuration
dsq config show

# Get specific value
dsq config get filter.lazy_evaluation

# Set value
dsq config set filter.lazy_evaluation true
dsq config set formats.csv.separator ";"
dsq config set performance.threads 4

# Create default config file
dsq config init

# Create in specific location
dsq config init --path ~/.config/dsq/config.toml

# Force overwrite existing config
dsq config init --force
```

### Specify Config File

```bash
# Use specific config file
dsq --config my-config.toml '.' data.csv

# Override config settings
dsq --threads 8 --lazy '.' data.csv
```

## Environment Variables

Some settings can be overridden with environment variables:

```bash
# Override thread count
DSQ_THREADS=8 dsq '.' data.csv

# Disable color
DSQ_COLOR=false dsq '.' data.csv

# Set memory limit
DSQ_MEMORY_LIMIT=2GB dsq '.' large.parquet
```

## Example Configurations

### High Performance

For processing large datasets quickly:

```toml
[filter]
lazy_evaluation = true
dataframe_optimizations = true
optimization_level = "advanced"

[performance]
batch_size = 50000
threads = 0  # Use all cores
parallel = true
streaming_threshold = 500000

[formats.parquet]
compression = "lz4"  # Fast compression
parallel_read = true
```

### Maximum Compression

For minimizing output file sizes:

```toml
[formats.parquet]
compression = "zstd"
statistics = true
use_dictionary = true

[formats.csv]
# No compression for CSV, use gzip externally
```

### Development/Debugging

For interactive development:

```toml
[filter]
strict_mode = true
collect_stats = true

[display]
color.enabled = true
compact = false
indent = 2
max_display_rows = 50

[performance]
batch_size = 1000  # Smaller batches for responsiveness
```

### Production ETL

For production data pipelines:

```toml
[filter]
lazy_evaluation = true
dataframe_optimizations = true
optimization_level = "advanced"
strict_mode = true  # Fail fast on errors

[performance]
batch_size = 100000
threads = 0
parallel = true
memory_limit = "8GB"

[output]
overwrite = false  # Prevent accidental overwrites

[formats.parquet]
compression = "snappy"  # Good balance
statistics = true
row_group_size = 100000
```

## Command-Line Override Priority

Settings are applied in this order (later overrides earlier):

1. Default values
2. System config (`/etc/dsq/`)
3. Home directory config (`~/.config/dsq/`)
4. Current directory config (`./dsq.toml`)
5. Custom config (`--config`)
6. Environment variables
7. Command-line flags

Example:
```bash
# Config file sets threads=4
# Environment variable overrides to 8
DSQ_THREADS=8 dsq '.' data.csv

# Command-line flag overrides everything
dsq --threads 16 '.' data.csv
```

## Validating Configuration

Check configuration validity:

```bash
# Show resolved configuration
dsq config show

# Validate config file
dsq config validate dsq.toml

# Test with specific config
dsq --config test.toml --explain '.' data.csv
```

## Best Practices

1. **Use project-specific configs** - Keep `dsq.toml` in project directories
2. **Global defaults in home** - Set user preferences in `~/.config/dsq/config.toml`
3. **Version control configs** - Commit project configs to git
4. **Document custom settings** - Add comments to explain non-default values
5. **Test before deploying** - Validate configs with sample data first

## Migration

When upgrading dsq versions, check for configuration changes:

```bash
# Backup current config
cp ~/.config/dsq/config.toml ~/.config/dsq/config.toml.bak

# Generate new default config
dsq config init --force

# Merge custom settings back
```
