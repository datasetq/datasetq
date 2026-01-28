//! SQL query support for dsq
//!
//! This crate provides SQL query execution functionality for dsq,
//! allowing queries against various databases (SQLite, PostgreSQL, MySQL)
//! and converting results to Polars DataFrames.

use anyhow::Result;
use dsq_shared::value::Value;
use polars::prelude::*;
use sqlx::any::{AnyConnectOptions, AnyPoolOptions, AnyRow};
use sqlx::{Column, Row, TypeInfo};
use std::str::FromStr;

/// Execute a SQL query and return results as a Value (DataFrame)
///
/// # Arguments
///
/// * `query` - SQL query string to execute
/// * `connection` - Optional database connection string. If None, uses DATABASE_URL env var
///
/// # Example
///
/// ```no_run
/// use dsq_sql::execute_query;
///
/// # async fn example() -> anyhow::Result<()> {
/// let result = execute_query(
///     "SELECT * FROM users WHERE age > 30",
///     Some("sqlite://data.db")
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn execute_query(query: &str, connection: Option<&str>) -> Result<Value> {
    // Get connection string from argument or environment variable
    let db_url = std::env::var("DATABASE_URL").ok();
    let connection_str = connection.or_else(|| db_url.as_deref()).ok_or_else(|| {
        anyhow::anyhow!(
            "No database connection specified.\n\n\
                Provide a connection string:\n  \
                dsq query 'SELECT * FROM users' sqlite://data.db\n\n\
                Or set DATABASE_URL environment variable:\n  \
                export DATABASE_URL=sqlite://data.db\n  \
                dsq query 'SELECT * FROM users'"
        )
    })?;

    // Parse connection options
    let options = AnyConnectOptions::from_str(connection_str)
        .map_err(|e| anyhow::anyhow!("Invalid connection string: {}", e))?;

    // Create connection pool
    let pool = AnyPoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to database: {}", e))?;

    // Execute query
    let rows: Vec<AnyRow> = sqlx::query(query)
        .fetch_all(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to execute query: {}", e))?;

    if rows.is_empty() {
        // Return empty dataframe
        return Ok(Value::DataFrame(DataFrame::empty()));
    }

    // Get column information from first row
    let first_row = &rows[0];
    let columns = first_row.columns();
    let mut series_vec: Vec<Series> = Vec::new();

    // Process each column
    for col in columns {
        let col_name = col.name();
        let type_info = col.type_info();
        let type_name = type_info.name();

        // Build column data based on type
        match type_name {
            "TEXT" | "VARCHAR" | "CHAR" | "STRING" => {
                let mut values: Vec<Option<String>> = Vec::new();
                for row in &rows {
                    values.push(row.try_get::<Option<String>, _>(col_name).ok().flatten());
                }
                series_vec.push(Series::new(col_name.into(), values));
            }
            "INTEGER" | "INT" | "BIGINT" | "INT8" => {
                let mut values: Vec<Option<i64>> = Vec::new();
                for row in &rows {
                    values.push(row.try_get::<Option<i64>, _>(col_name).ok().flatten());
                }
                series_vec.push(Series::new(col_name.into(), values));
            }
            "REAL" | "FLOAT" | "DOUBLE" | "NUMERIC" => {
                let mut values: Vec<Option<f64>> = Vec::new();
                for row in &rows {
                    values.push(row.try_get::<Option<f64>, _>(col_name).ok().flatten());
                }
                series_vec.push(Series::new(col_name.into(), values));
            }
            "BOOLEAN" | "BOOL" => {
                let mut values: Vec<Option<bool>> = Vec::new();
                for row in &rows {
                    values.push(row.try_get::<Option<bool>, _>(col_name).ok().flatten());
                }
                series_vec.push(Series::new(col_name.into(), values));
            }
            _ => {
                // Default to string for unknown types
                let mut values: Vec<Option<String>> = Vec::new();
                for row in &rows {
                    values.push(row.try_get::<Option<String>, _>(col_name).ok().flatten());
                }
                series_vec.push(Series::new(col_name.into(), values));
            }
        }
    }

    // Create DataFrame from series (convert to columns)
    let columns: Vec<_> = series_vec.into_iter().map(|s| s.into_column()).collect();
    let df = DataFrame::new(columns)
        .map_err(|e| anyhow::anyhow!("Failed to create DataFrame: {}", e))?;

    Ok(Value::DataFrame(df))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires a database
    async fn test_execute_query_sqlite() {
        // This test requires a SQLite database to be set up
        // Run with: cargo test --features sql -- --ignored
        let result = execute_query("SELECT 1 as num", Some("sqlite::memory:")).await;
        assert!(result.is_ok());
    }
}
