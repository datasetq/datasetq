# dsq-sql

SQL query execution support for datasetq (dsq).

## Features

- Execute SQL queries against multiple database types (SQLite, PostgreSQL, MySQL)
- Convert query results to Polars DataFrames
- Automatic type inference from database columns
- Connection pooling for efficient query execution

## Usage

This crate is feature-gated and must be enabled explicitly:

```toml
[dependencies]
dsq-sql = { version = "0.1.0", path = "../dsq-sql" }
```

Or when building dsq:

```bash
# Build with SQL support
cargo build --features sql

# Build without SQL support (e.g., for WASM targets)
cargo build
```

## Examples

```rust
use dsq_sql::execute_query;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Query a SQLite database
    let result = execute_query(
        "SELECT * FROM users WHERE age > 30",
        Some("sqlite://data.db")
    ).await?;

    // Query using DATABASE_URL environment variable
    std::env::set_var("DATABASE_URL", "postgres://user:pass@localhost/mydb");
    let result = execute_query(
        "SELECT * FROM orders WHERE total > 100",
        None
    ).await?;

    Ok(())
}
```

## CLI Usage

When dsq is built with the `sql` feature, a `query` subcommand is available:

```bash
# Query SQLite database
dsq query 'SELECT * FROM users' sqlite://data.db

# Query PostgreSQL
dsq query 'SELECT * FROM orders WHERE total > 100' postgres://user:pass@localhost/mydb

# Pipe results to other dsq commands
dsq query 'SELECT * FROM users' sqlite://data.db | dsq 'select(.age > 30)'

# Use environment variable for connection
export DATABASE_URL=sqlite://data.db
dsq query 'SELECT * FROM users'
```

## Supported Databases

- SQLite (`sqlite://path/to/db.sqlite`)
- PostgreSQL (`postgres://user:pass@host:port/database`)
- MySQL (`mysql://user:pass@host:port/database`)

## Type Mapping

SQL types are automatically mapped to Polars DataFrame types:

| SQL Type | Polars Type |
|----------|-------------|
| TEXT, VARCHAR, CHAR, STRING | String |
| INTEGER, INT, BIGINT, INT8 | i64 |
| REAL, FLOAT, DOUBLE, NUMERIC | f64 |
| BOOLEAN, BOOL | bool |
| (others) | String (fallback) |

## Platform Support

This crate is **not** compatible with WebAssembly (WASM) builds due to sqlx's native dependencies. For WASM builds, dsq-sql should be excluded via feature flags.
