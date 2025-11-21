# TODO

## Known Test Issues (temporarily ignored)

### Filter Operations (i32 vs i64 dtype mismatch)
- [ ] `test_filter_operation` - Filter operation on DataFrame uses i32 instead of i64
- [ ] `test_pipeline_filter_method` - Same i32/i64 dtype issue
- Root cause: DataFrame columns created with `df!` macro use i32, but filter expects i64

### Group By Operations
- [ ] `test_group_by_multiple_columns` - group_by_agg returns different structure than expected
- [ ] `test_pipeline_group_by_method` - group_by returns Array instead of DataFrame

### Object Construction
- [ ] `test_object_construct_operation` - ObjectConstructOperation returns Array instead of Object

### Pivot Operations
- [ ] `test_pivot_current_behavior` - pivot returns different column names than expected

### Join Operations
- [ ] `test_right_join` - Right join not supported in current Polars version

### Explode Operations
- [ ] `test_explode` - explode operation not supported for binary dtype
- [ ] `test_explode_lazy` - Same binary dtype limitation

### Parser Improvements
- [ ] Bracket notation with spaces in field names not yet supported (e.g., `.["US City Name"]`)

## Upgrade Tasks
- [ ] Consider upgrading Polars for better right join support, fixed explode for list types, improved aggregation naming

## CLI
- [ ] Handle multiple input files (`src/dsq-cli/src/main.rs:222`, `src/dsq/main.rs:142`)

## Filter
- [ ] Check if we should yield control for async/streaming (`src/dsq-filter/src/executor.rs:292`)
- [ ] Compile AST and execute (`src/dsq-filter/src/compiler.rs:494`)

## Formats
- [ ] Convert array to List AnyValue in JSON5 (`src/dsq-formats/src/json5.rs:499`)
- [ ] Convert object to Struct AnyValue in JSON5 (`src/dsq-formats/src/json5.rs:503`)
- [ ] Handle nested arrays in JSON (`src/dsq-formats/src/reader/json_utils.rs:102`)
- [ ] Handle nested objects in JSON (`src/dsq-formats/src/reader/json_utils.rs:103`)

## IO
- [ ] Implement Avro writing (`src/dsq-io/src/formats/avro.rs:14`)
- [ ] Fix date/time format options in CSV (`src/dsq-io/src/formats/csv.rs:65`)

## Core
- [ ] Implement transpose (`src/dsq-core/src/ops/transform.rs:425`)
