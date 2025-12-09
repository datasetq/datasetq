use crate::{Error, Result};
use polars::prelude::*;
use std::fs::File;
use std::io::{BufWriter, Write};

use super::super::options::{FormatWriteOptions, WriteOptions};
use super::super::utils::series_value_to_json;

/// Write JSON file
pub fn write_json(
    df: &mut DataFrame,
    options: &WriteOptions,
    format_options: &FormatWriteOptions,
    path: &str,
) -> Result<()> {
    let json_opts = match format_options {
        FormatWriteOptions::Json {
            lines,
            pretty,
            maintain_order,
        } => (*lines, *pretty, *maintain_order),
        _ => (false, false, false),
    };

    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    if json_opts.0 {
        // JSON Lines format
        for row_idx in 0..df.height() {
            let mut row_obj = serde_json::Map::new();

            for col_name in df.get_column_names() {
                let series = df.column(col_name).map_err(Error::from)?;
                let value = series_value_to_json(series, row_idx)?;
                row_obj.insert(col_name.to_string(), value);
            }

            let json_obj = serde_json::Value::Object(row_obj);
            if json_opts.1 {
                serde_json::to_writer_pretty(&mut writer, &json_obj)
                    .map_err(|e| Error::Other(format!("JSON write error: {}", e)))?;
            } else {
                serde_json::to_writer(&mut writer, &json_obj)
                    .map_err(|e| Error::Other(format!("JSON write error: {}", e)))?;
            }
            writeln!(writer)?;
        }
    } else {
        // Regular JSON array format
        let mut rows = Vec::new();

        for row_idx in 0..df.height() {
            let mut row_obj = serde_json::Map::new();

            for col_name in df.get_column_names() {
                let series = df.column(col_name).map_err(Error::from)?;
                let value = series_value_to_json(series, row_idx)?;
                row_obj.insert(col_name.to_string(), value);
            }

            rows.push(serde_json::Value::Object(row_obj));
        }

        let json_array = serde_json::Value::Array(rows);
        if json_opts.1 {
            serde_json::to_writer_pretty(&mut writer, &json_array)
                .map_err(|e| Error::operation(format!("JSON write error: {}", e)))?;
        } else {
            serde_json::to_writer(&mut writer, &json_array)
                .map_err(|e| Error::operation(format!("JSON write error: {}", e)))?;
        }
    }

    writer.flush()?;
    Ok(())
}

