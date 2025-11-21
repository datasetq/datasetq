# DatasetQ WebAssembly (WASM) Build

This document describes how to build DatasetQ for WebAssembly to enable client-side data processing in web browsers.

## Overview

DatasetQ can be compiled to WebAssembly for use in web applications. The WASM build provides full data processing capabilities using Polars, enabling powerful jq-compatible queries to run entirely in the browser without server-side processing.

## Prerequisites

- Rust toolchain with WASM target support
- `wasm-pack` for building and packaging WASM modules
- Node.js and npm for the demo application

## Building for WASM

### 1. Install wasm-pack

```bash
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
```

Or using cargo:

```bash
cargo install wasm-pack
```

### 2. Build the WASM module

From the datasetq directory:

```bash
wasm-pack build --target web --out-dir pkg --features wasm
```

**Note**: Do not use `devenv shell` for WASM builds, as the mold linker configuration conflicts with WASM linking requirements.

This will create a `pkg/` directory containing:
- `dsq.js` - JavaScript bindings
- `dsq_bg.wasm` - The compiled WebAssembly module
- `dsq.d.ts` - TypeScript definitions
- `dsq_bg.wasm.d.ts` - Additional TypeScript definitions

### 3. Copy files to demo application

```bash
cp pkg/* ../datasetq-wasm-demo/static/
```

Or use the automated script from the demo directory:

```bash
cd ../datasetq-wasm-demo
npm run build-wasm
```

## WASM API

The WASM module currently exposes the following functions:

### `greet(name: string): string`

A simple greeting function for testing.

```javascript
import init, { greet } from './dsq.js';

async function run() {
  await init();
  console.log(greet("World")); // "Hello, World!"
}
```

### `process_datasetq_query(query: string, data_json: string): Result<string, JsValue>`

Processes a DatasetQ query on JSON data using Polars for data processing. Supports the full DatasetQ query language including filtering, aggregation, and data transformations.

```javascript
import init, { process_datasetq_query } from './dsq.js';

async function run() {
  await init();
  try {
    const result = process_datasetq_query(".[] | select(.age > 25)", '{"data": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 20}]}');
    console.log(result); // Returns filtered JSON data
  } catch (error) {
    console.log("Query execution failed:", error);
  }
}
```

## Current Limitations

- Parquet file reading is not supported in the browser environment (due to file system access restrictions)
- Some advanced Polars features may have limited support in WASM

## Architecture

The WASM build is designed to be lightweight and focused on client-side data processing. Key architectural decisions:

### Polars Integration

Polars is fully supported in WASM builds, enabling powerful data processing capabilities in the browser. The WASM build includes:

- DataFrame and Series operations
- Lazy evaluation for efficient query processing
- Full DatasetQ query language support
- JSON data import/export

### Conditional Compilation

Polars features are selectively enabled for WASM vs native builds:

```toml
[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
polars = { version = "0.35", features = ["lazy", "csv", "json", "parquet", "strings", "temporal", "dtype-datetime", "dtype-date", "dtype-time", "describe", "rows", "chunked_ids"] }

[target.'cfg(target_arch = "wasm32")'.dependencies]
polars = { version = "0.35", features = ["lazy", "csv", "json", "strings", "temporal", "dtype-datetime", "dtype-date", "dtype-time", "describe", "rows", "chunked_ids"] }
```

### WASM Library

The WASM library (`src/lib.rs`) provides full DatasetQ query processing using the same dsq-filter engine as the native CLI, with Polars DataFrames for efficient data manipulation.

## Development

### Testing WASM Builds

```bash
# Build WASM
wasm-pack build --target web --out-dir pkg --features wasm

# Test in Node.js
node -e "
import('./pkg/dsq.js').then(async ({ default: init, greet }) => {
  await init();
  console.log(greet('Test'));
});
"
```

### Demo Application

The `datasetq-wasm-demo` directory contains a SvelteKit application that demonstrates WASM usage:

```bash
cd ../datasetq-wasm-demo
npm install
npm run build-wasm  # Builds WASM and copies files to static/
npm run build       # Builds the demo app
npm run preview     # Preview the application
```

The `build-wasm` script automatically builds the WASM module and copies the generated files to the `static/` directory for the web application.

## Future Enhancements

- Add support for Arrow data format processing in WASM
- Optimize bundle size and performance
- Add streaming data processing capabilities
- Implement Parquet reading in browser-compatible ways (e.g., via Web APIs)

## Troubleshooting

### Common Issues

1. **getrandom compilation errors**: Ensure you're using `wasm-pack` instead of `cargo build` directly for WASM targets.

2. **Mold linker errors**: WASM linking requires specific linkers. Use `wasm-pack` which handles this automatically. Avoid using `devenv shell` for WASM builds as it may enable mold linking which conflicts with WASM.

3. **Missing dependencies**: Make sure all conditional dependencies are properly gated for WASM vs native builds.

4. **Polars/arrow-arith compilation errors**: There may be compatibility issues between certain versions of Polars, Arrow, and Chrono crates. If you encounter `multiple applicable items in scope` errors for the `quarter` method, try:
   - Using Polars version 0.41 or earlier
   - Updating to the latest Polars version (which may have fixes)
   - Building with `--no-default-features` and only enabling specific formats you need

### Debug Builds

For debugging WASM builds:

```bash
wasm-pack build --target web --out-dir pkg --features wasm --dev
```

This includes debug symbols and disables optimizations.