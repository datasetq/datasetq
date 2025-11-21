# TODO

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
