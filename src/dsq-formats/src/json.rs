use crate::error::{Error, FormatError, Result};

use dsq_shared::value::Value;
use polars::datatypes::AnyValue;
use polars::prelude::*;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// JSON-specific reading options
#[derive(Debug, Clone)]
pub struct JsonReadOptions {
    /// Whether to treat input as JSON Lines (newline-delimited JSON)
    pub lines: bool,
    /// Whether to ignore invalid JSON lines (only for JSON Lines)
    pub ignore_errors: bool,
    /// Maximum number of lines to read for schema inference
    pub infer_schema_length: Option<usize>,
    /// Whether to flatten nested objects
    pub flatten: bool,
    /// Separator for flattened field names
    pub flatten_separator: String,
    /// Maximum nesting depth to prevent stack overflow
    pub max_depth: usize,
    /// Buffer size for reading
    pub buffer_size: usize,
}

impl Default for JsonReadOptions {
    fn default() -> Self {
        Self {
            lines: false,
            ignore_errors: false,
            infer_schema_length: Some(1000),
            flatten: false,
            flatten_separator: ".".to_string(),
            max_depth: 64,
            buffer_size: 262144, // 256KB for better JSON parsing performance
        }
    }
}

/// JSON-specific writing options
#[derive(Debug, Clone)]
pub struct JsonWriteOptions {
    /// Whether to output as JSON Lines format
    pub lines: bool,
    /// Whether to pretty-print the JSON
    pub pretty: bool,
    /// Whether to maintain field order
    pub maintain_order: bool,
    /// String to use for null values
    pub null_value: Option<String>,
    /// Whether to escape non-ASCII characters
    pub escape_unicode: bool,
    /// Line terminator for JSON Lines
    pub line_terminator: String,
    /// Buffer size for writing
    pub buffer_size: usize,
}

impl Default for JsonWriteOptions {
    fn default() -> Self {
        Self {
            lines: false,
            pretty: false,
            maintain_order: false,
            null_value: None,
            escape_unicode: false,
            line_terminator: "\n".to_string(),
            buffer_size: 262144, // 256KB for faster JSON serialization
        }
    }
}

/// Process a JSON value with given options (standalone function)
fn process_json_value_with_options(
    json_val: JsonValue,
    depth: usize,
    options: &JsonReadOptions,
) -> Result<Value> {
    if depth > options.max_depth {
        return Err(Error::Format(FormatError::InvalidOption(format!(
            "JSON nesting depth exceeds maximum of {}",
            options.max_depth
        ))));
    }

    match json_val {
        JsonValue::Object(obj) => {
            if options.flatten {
                flatten_object_with_options(obj, "", depth, options)
            } else {
                let mut map = std::collections::HashMap::new();
                for (k, v) in obj {
                    let value = process_json_value_with_options(v, depth + 1, options)?;
                    map.insert(k, value);
                }
                Ok(Value::Object(map))
            }
        }
        _ => Ok(Value::from_json(json_val)),
    }
}

/// Flatten a JSON object into a flat structure with options
fn flatten_object_with_options(
    obj: JsonMap<String, JsonValue>,
    prefix: &str,
    depth: usize,
    options: &JsonReadOptions,
) -> Result<Value> {
    let mut flattened = std::collections::HashMap::new();

    for (key, value) in obj {
        let new_key = if prefix.is_empty() {
            key
        } else {
            format!("{}{}{}", prefix, options.flatten_separator, key)
        };

        match value {
            JsonValue::Object(nested_obj) => {
                if depth < options.max_depth {
                    let nested_flattened =
                        flatten_object_with_options(nested_obj, &new_key, depth + 1, options)?;
                    if let Value::Object(nested_map) = nested_flattened {
                        flattened.extend(nested_map);
                    }
                } else {
                    // Max depth reached, store as string representation
                    flattened.insert(
                        new_key,
                        Value::String(JsonValue::Object(nested_obj).to_string()),
                    );
                }
            }
            JsonValue::Array(arr) => {
                if should_flatten_array(&arr) {
                    for (i, item) in arr.into_iter().enumerate() {
                        let array_key = format!("{}[{}]", new_key, i);
                        let item_value = process_json_value_with_options(item, depth + 1, options)?;
                        if let Value::Object(item_map) = item_value {
                            for (nested_key, nested_value) in item_map {
                                let final_key = format!(
                                    "{}{}{}",
                                    array_key, options.flatten_separator, nested_key
                                );
                                flattened.insert(final_key, nested_value);
                            }
                        } else {
                            flattened.insert(array_key, item_value);
                        }
                    }
                } else {
                    flattened.insert(new_key, Value::from_json(JsonValue::Array(arr)));
                }
            }
            _ => {
                flattened.insert(new_key, Value::from_json(value));
            }
        }
    }

    Ok(Value::Object(flattened))
}

/// Determine if an array should be flattened
fn should_flatten_array(arr: &[JsonValue]) -> bool {
    // Only flatten arrays of objects or simple values, and only if they're not too large
    arr.len() <= 10
        && arr.iter().all(|v| {
            matches!(
                v,
                JsonValue::Object(_)
                    | JsonValue::String(_)
                    | JsonValue::Number(_)
                    | JsonValue::Bool(_)
                    | JsonValue::Null
            )
        })
}

/// JSON reader that handles both regular JSON and JSON Lines formats
pub struct JsonReader<R> {
    reader: R,
    options: JsonReadOptions,
    detected_format: Option<JsonFormat>,
}

/// Represents the detected format of JSON data
#[derive(Debug, Clone, Copy)]
pub enum JsonFormat {
    /// JSON array format
    Array,
    /// JSON object format
    Object,
    /// JSON Lines (NDJSON) format
    Lines,
}

impl<R: std::io::BufRead> JsonReader<R> {
    /// Create a new JSON reader with default options
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            options: JsonReadOptions::default(),
            detected_format: None,
        }
    }

    /// Create a JSON reader with custom options
    pub fn with_options(reader: R, options: JsonReadOptions) -> Self {
        Self {
            reader,
            options,
            detected_format: None,
        }
    }

    /// Set whether to treat input as JSON Lines
    pub fn lines(mut self, lines: bool) -> Self {
        self.options.lines = lines;
        self
    }

    /// Set whether to ignore errors in JSON Lines
    pub fn ignore_errors(mut self, ignore_errors: bool) -> Self {
        self.options.ignore_errors = ignore_errors;
        self
    }

    /// Set whether to flatten nested objects
    pub fn flatten(mut self, flatten: bool) -> Self {
        self.options.flatten = flatten;
        self
    }

    /// Auto-detect JSON format from the first few bytes
    pub fn detect_format(&mut self) -> Result<JsonFormat> {
        if let Some(format) = self.detected_format {
            return Ok(format);
        }

        if self.options.lines {
            self.detected_format = Some(JsonFormat::Lines);
            return Ok(JsonFormat::Lines);
        }

        // Peek at first few bytes to detect format
        let buffer = self.reader.fill_buf()?;
        if buffer.is_empty() {
            // Return empty DataFrame for empty input
            return Ok(JsonFormat::Array);
        }

        let text = String::from_utf8_lossy(buffer);
        let trimmed = text.trim_start();

        let format = if trimmed.starts_with('[') {
            JsonFormat::Array
        } else if trimmed.starts_with('{') {
            // Check if it's JSON Lines by looking for multiple objects
            let lines: Vec<&str> = trimmed.lines().take(3).collect();
            if lines.len() > 1
                && lines.iter().all(|line| {
                    let line = line.trim();
                    line.starts_with('{') && line.ends_with('}')
                })
            {
                JsonFormat::Lines
            } else {
                JsonFormat::Object
            }
        } else {
            return Err(Error::Format(FormatError::InvalidOption(
                "Invalid JSON format: must start with '[' or '{'".to_string(),
            )));
        };

        self.detected_format = Some(format);
        Ok(format)
    }

    /// Read JSON data into a DataFrame
    pub fn read_dataframe(&mut self) -> Result<DataFrame> {
        #[cfg(feature = "profiling")]
        coz::progress!("json_read_start");

        let format = self.detect_format()?;

        let result = match format {
            JsonFormat::Lines => self.read_json_lines(),
            JsonFormat::Array => self.read_json_array(),
            JsonFormat::Object => self.read_json_object(),
        };

        #[cfg(feature = "profiling")]
        coz::progress!("json_parsed");

        result
    }

    /// Read JSON Lines format
    fn read_json_lines(&mut self) -> Result<DataFrame> {
        // Pre-allocate with reasonable capacity for better performance
        let max_lines = self.options.infer_schema_length.unwrap_or(usize::MAX);
        let estimated_capacity = max_lines.min(10_000);
        let mut rows = Vec::with_capacity(estimated_capacity);
        let mut line = String::with_capacity(1024); // Typical line length
        let mut line_number = 0;

        loop {
            line.clear();
            let bytes_read = self.reader.read_line(&mut line)?;
            if bytes_read == 0 {
                break; // EOF
            }

            line_number += 1;
            let trimmed = line.trim();

            if trimmed.is_empty() {
                continue; // Skip empty lines
            }

            match serde_json::from_str::<JsonValue>(trimmed) {
                Ok(json_val) => {
                    let value = process_json_value_with_options(json_val, 0, &self.options)?;
                    rows.push(value);

                    if rows.len() >= max_lines {
                        break;
                    }
                }
                Err(e) => {
                    if self.options.ignore_errors {
                        eprintln!(
                            "Warning: Ignoring invalid JSON on line {}: {}",
                            line_number, e
                        );
                        continue;
                    } else {
                        return Err(Error::Format(FormatError::InvalidOption(format!(
                            "Invalid JSON on line {}: {}",
                            line_number, e
                        ))));
                    }
                }
            }
        }

        if rows.is_empty() {
            return Ok(DataFrame::empty());
        }

        self.values_to_dataframe(rows)
    }

    /// Read JSON array format
    fn read_json_array(&mut self) -> Result<DataFrame> {
        let mut content = String::new();
        self.reader
            .read_to_string(&mut content)
            .map_err(|e| Error::Format(FormatError::InvalidOption(format!("Read error: {}", e))))?;

        // Handle empty input
        if content.trim().is_empty() {
            return Ok(DataFrame::empty());
        }

        let json_val: JsonValue = serde_json::from_str(&content)
            .map_err(|e| Error::Format(FormatError::SerializationError(e.to_string())))?;

        match json_val {
            JsonValue::Array(arr) => {
                let max_items = self.options.infer_schema_length.unwrap_or(arr.len());
                // Pre-allocate with exact capacity
                let mut rows = Vec::with_capacity(max_items.min(arr.len()));

                for (i, item) in arr.into_iter().enumerate() {
                    if i >= max_items {
                        break;
                    }
                    let value = process_json_value_with_options(item, 0, &self.options)?;
                    rows.push(value);
                }

                if rows.is_empty() {
                    return Ok(DataFrame::empty());
                }

                self.values_to_dataframe(rows)
            }
            _ => Err(Error::Format(FormatError::InvalidOption(
                "Expected JSON array at root level".to_string(),
            ))),
        }
    }

    /// Read single JSON object format
    fn read_json_object(&mut self) -> Result<DataFrame> {
        let json_val: JsonValue = serde_json::from_reader(&mut self.reader).map_err(|e| {
            Error::Format(FormatError::InvalidOption(format!("Invalid JSON: {}", e)))
        })?;

        let value = process_json_value_with_options(json_val, 0, &self.options)?;
        let rows = vec![value];

        self.values_to_dataframe(rows)
    }

    /// Process a JSON value, applying flattening if configured
    #[allow(dead_code)]
    fn process_json_value(&self, json_val: JsonValue, depth: usize) -> Result<Value> {
        process_json_value_with_options(json_val, depth, &self.options)
    }

    /// Flatten a JSON object into a flat structure
    #[allow(dead_code)]
    fn flatten_object(
        &self,
        obj: JsonMap<String, JsonValue>,
        prefix: &str,
        depth: usize,
    ) -> Result<Value> {
        let mut flattened = std::collections::HashMap::new();

        for (key, value) in obj {
            let new_key = if prefix.is_empty() {
                key
            } else {
                format!("{}{}{}", prefix, self.options.flatten_separator, key)
            };

            let value_str = serde_json::to_string(&value)?;

            match &value {
                JsonValue::Object(ref nested_obj) => {
                    if depth < self.options.max_depth {
                        let nested_flattened =
                            self.flatten_object(nested_obj.clone(), &new_key, depth + 1)?;
                        if let Value::Object(nested_map) = nested_flattened {
                            flattened.extend(nested_map);
                        }
                    } else {
                        // Max depth reached, store as string representation
                        flattened.insert(new_key, Value::String(value_str));
                    }
                }
                JsonValue::Array(ref arr) => {
                    if self.should_flatten_array(arr) {
                        for (i, item) in arr.iter().enumerate() {
                            let array_key = format!("{}[{}]", new_key, i);
                            let item_value = self.process_json_value(item.clone(), depth + 1)?;
                            if let Value::Object(item_map) = item_value {
                                for (nested_key, nested_value) in item_map {
                                    let final_key = format!(
                                        "{}{}{}",
                                        array_key, self.options.flatten_separator, nested_key
                                    );
                                    flattened.insert(final_key, nested_value);
                                }
                            } else {
                                flattened.insert(array_key, item_value);
                            }
                        }
                    } else {
                        flattened.insert(new_key, Value::from_json(JsonValue::Array(arr.clone())));
                    }
                }
                _ => {
                    flattened.insert(new_key, Value::from_json(value.clone()));
                }
            }
        }

        Ok(Value::Object(flattened))
    }

    /// Determine if an array should be flattened
    #[allow(dead_code)]
    fn should_flatten_array(&self, arr: &[JsonValue]) -> bool {
        // Only flatten arrays of objects or simple values, and only if they're not too large
        arr.len() <= 10
            && arr.iter().all(|v| {
                matches!(
                    v,
                    JsonValue::Object(_)
                        | JsonValue::String(_)
                        | JsonValue::Number(_)
                        | JsonValue::Bool(_)
                        | JsonValue::Null
                )
            })
    }

    /// Convert a vector of Values to a DataFrame
    fn values_to_dataframe(&self, values: Vec<Value>) -> Result<DataFrame> {
        if values.is_empty() {
            return Ok(DataFrame::empty());
        }

        // Extract all unique column names from all objects
        let mut all_columns = std::collections::BTreeSet::new();
        for value in &values {
            if let Value::Object(obj) = value {
                for key in obj.keys() {
                    all_columns.insert(key.clone());
                }
            }
        }

        if all_columns.is_empty() {
            // No object structure found, try to create a simple DataFrame
            return self.create_simple_dataframe(values);
        }

        let columns: Vec<String> = all_columns.into_iter().collect();
        let mut series_data: std::collections::HashMap<String, Vec<AnyValue>> =
            std::collections::HashMap::new();

        // Initialize series vectors
        for col in &columns {
            series_data.insert(col.clone(), Vec::new());
        }

        // Process each row
        for value in values {
            match value {
                Value::Object(obj) => {
                    for col in &columns {
                        let val = obj.get(col).unwrap_or(&Value::Null);
                        let any_val = self.value_to_any_value(val)?;
                        series_data.get_mut(col).unwrap().push(any_val);
                    }
                }
                _ => {
                    // If we have a non-object value, put it in the first column
                    if let Some(first_col) = columns.first() {
                        let any_val = self.value_to_any_value(&value)?;
                        series_data.get_mut(first_col).unwrap().push(any_val);

                        // Fill other columns with nulls
                        for col in columns.iter().skip(1) {
                            series_data.get_mut(col).unwrap().push(AnyValue::Null);
                        }
                    }
                }
            }
        }

        // Create Series from vectors
        let mut series_vec = Vec::new();
        for col in columns {
            let values = series_data.remove(&col).unwrap();
            let series = Series::new(&col, values);
            series_vec.push(series);
        }

        DataFrame::new(series_vec).map_err(Error::from)
    }

    /// Create a simple DataFrame for non-object values
    fn create_simple_dataframe(&self, values: Vec<Value>) -> Result<DataFrame> {
        let any_values: Result<Vec<AnyValue>> =
            values.iter().map(|v| self.value_to_any_value(v)).collect();

        let series = Series::new("value", any_values?);
        DataFrame::new(vec![series]).map_err(Error::from)
    }

    /// Convert Value to AnyValue for Polars
    fn value_to_any_value(&self, value: &Value) -> Result<AnyValue<'static>> {
        match value {
            Value::Null => Ok(AnyValue::Null),
            Value::Bool(b) => Ok(AnyValue::Boolean(*b)),
            Value::Int(i) => Ok(AnyValue::Int64(*i)),
            Value::Float(f) => Ok(AnyValue::Float64(*f)),
            Value::String(s) => Ok(AnyValue::Utf8Owned(s.clone().into())),
            Value::Array(_) => Err(Error::Format(FormatError::UnsupportedFeature(
                "Cannot convert array to AnyValue".to_string(),
            ))),
            Value::Object(_) => Err(Error::Format(FormatError::UnsupportedFeature(
                "Cannot convert object to AnyValue".to_string(),
            ))),
            _ => Err(Error::Format(FormatError::UnsupportedFeature(format!(
                "Cannot convert {} to AnyValue",
                value.type_name()
            )))),
        }
    }

    /// Peek at the first few records without consuming the reader
    pub fn peek(&mut self, records: usize) -> Result<DataFrame> {
        let mut temp_options = self.options.clone();
        temp_options.infer_schema_length = Some(records);

        let mut temp_reader = JsonReader::with_options(&mut self.reader, temp_options);
        temp_reader.read_dataframe()
    }
}

/// JSON writer that handles both regular JSON and JSON Lines formats
pub struct JsonWriter<W: Write> {
    writer: BufWriter<W>,
    options: JsonWriteOptions,
    records_written: usize,
}

impl<W: Write> JsonWriter<W> {
    /// Create a new JSON writer with default options
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::with_capacity(8192, writer),
            options: JsonWriteOptions::default(),
            records_written: 0,
        }
    }

    /// Create a JSON writer with custom options
    pub fn with_options(writer: W, options: JsonWriteOptions) -> Self {
        Self {
            writer: BufWriter::with_capacity(options.buffer_size, writer),
            options,
            records_written: 0,
        }
    }

    /// Set whether to output as JSON Lines
    pub fn lines(mut self, lines: bool) -> Self {
        self.options.lines = lines;
        self
    }

    /// Set whether to pretty-print
    pub fn pretty(mut self, pretty: bool) -> Self {
        self.options.pretty = pretty;
        self
    }

    /// Write a DataFrame to JSON
    pub fn write_dataframe(&mut self, df: &DataFrame) -> Result<()> {
        if self.options.lines {
            self.write_json_lines(df)
        } else {
            self.write_json_array(df)
        }
    }

    /// Write DataFrame as JSON Lines
    fn write_json_lines(&mut self, df: &DataFrame) -> Result<()> {
        for row_idx in 0..df.height() {
            let row_obj = self.dataframe_row_to_json_object(df, row_idx)?;

            let json_str = if self.options.pretty {
                serde_json::to_string_pretty(&row_obj)
            } else {
                serde_json::to_string(&row_obj)
            }
            .map_err(|e| Error::operation(format!("JSON serialization error: {}", e)))?;

            self.writer.write_all(json_str.as_bytes())?;
            self.writer
                .write_all(self.options.line_terminator.as_bytes())?;

            self.records_written += 1;
        }

        Ok(())
    }

    /// Write DataFrame as JSON array
    fn write_json_array(&mut self, df: &DataFrame) -> Result<()> {
        // Write opening bracket
        self.writer.write_all(b"[")?;

        for row_idx in 0..df.height() {
            if row_idx > 0 {
                self.writer.write_all(b",")?;
            }

            if self.options.pretty {
                self.writer.write_all(b"\n  ")?;
            }

            let row_obj = self.dataframe_row_to_json_object(df, row_idx)?;

            let json_str = if self.options.pretty {
                serde_json::to_string_pretty(&row_obj)
                    .map_err(|e| Error::operation(format!("JSON serialization error: {}", e)))?
                    .replace('\n', "\n  ") // Indent nested content
            } else {
                serde_json::to_string(&row_obj)
                    .map_err(|e| Error::operation(format!("JSON serialization error: {}", e)))?
            };

            self.writer.write_all(json_str.as_bytes())?;
            self.records_written += 1;
        }

        if self.options.pretty && df.height() > 0 {
            self.writer.write_all(b"\n")?;
        }

        // Write closing bracket
        self.writer.write_all(b"]")?;

        Ok(())
    }

    /// Convert a DataFrame row to a JSON object
    fn dataframe_row_to_json_object(&self, df: &DataFrame, row_idx: usize) -> Result<JsonValue> {
        let mut obj = if self.options.maintain_order {
            serde_json::Map::new()
        } else {
            serde_json::Map::new()
        };

        for col_name in df.get_column_names() {
            let series = df.column(col_name).map_err(Error::from)?;
            let value = self.series_value_to_json(series, row_idx)?;
            obj.insert(col_name.to_string(), value);
        }

        Ok(JsonValue::Object(obj))
    }

    /// Convert a single Series value to JSON
    fn series_value_to_json(&self, series: &Series, index: usize) -> Result<JsonValue> {
        use polars::datatypes::*;

        if series.is_null().get(index).unwrap_or(false) {
            return if let Some(ref null_str) = self.options.null_value {
                Ok(JsonValue::String(null_str.clone()))
            } else {
                Ok(JsonValue::Null)
            };
        }

        match series.dtype() {
            DataType::Boolean => {
                let val = series.bool().map_err(Error::from)?.get(index);
                Ok(JsonValue::Bool(val.unwrap_or(false)))
            }
            DataType::Int8 => {
                let val = series.i8().map_err(Error::from)?.get(index);
                Ok(JsonValue::Number(serde_json::Number::from(
                    val.unwrap_or(0),
                )))
            }
            DataType::Int16 => {
                let val = series.i16().map_err(Error::from)?.get(index);
                Ok(JsonValue::Number(serde_json::Number::from(
                    val.unwrap_or(0),
                )))
            }
            DataType::Int32 => {
                let val = series.i32().map_err(Error::from)?.get(index);
                Ok(JsonValue::Number(serde_json::Number::from(
                    val.unwrap_or(0),
                )))
            }
            DataType::Int64 => {
                let val = series.i64().map_err(Error::from)?.get(index);
                Ok(JsonValue::Number(serde_json::Number::from(
                    val.unwrap_or(0),
                )))
            }
            DataType::UInt8 => {
                let val = series.u8().map_err(Error::from)?.get(index);
                Ok(JsonValue::Number(serde_json::Number::from(
                    val.unwrap_or(0),
                )))
            }
            DataType::UInt16 => {
                let val = series.u16().map_err(Error::from)?.get(index);
                Ok(JsonValue::Number(serde_json::Number::from(
                    val.unwrap_or(0),
                )))
            }
            DataType::UInt32 => {
                let val = series.u32().map_err(Error::from)?.get(index);
                Ok(JsonValue::Number(serde_json::Number::from(
                    val.unwrap_or(0),
                )))
            }
            DataType::UInt64 => {
                let val = series.u64().map_err(Error::from)?.get(index);
                Ok(JsonValue::Number(serde_json::Number::from(
                    val.unwrap_or(0),
                )))
            }
            DataType::Float32 | DataType::Float64 => {
                let val = series.f64().map_err(Error::from)?.get(index);
                serde_json::Number::from_f64(val.unwrap_or(0.0))
                    .map(JsonValue::Number)
                    .ok_or_else(|| Error::operation("Invalid float value"))
            }
            DataType::Utf8 => {
                let val = series.utf8().map_err(Error::from)?.get(index);
                let string_val = val.unwrap_or("").to_string();

                // Try to parse as JSON if it looks like JSON
                if (string_val.starts_with('{') && string_val.ends_with('}'))
                    || (string_val.starts_with('[') && string_val.ends_with(']'))
                {
                    match serde_json::from_str::<JsonValue>(&string_val) {
                        Ok(parsed) => Ok(parsed),
                        Err(_) => Ok(JsonValue::String(string_val)),
                    }
                } else {
                    Ok(JsonValue::String(string_val))
                }
            }
            DataType::Date => {
                let val = series.date().map_err(Error::from)?.get(index);
                if let Some(date) = val {
                    Ok(JsonValue::String(date.to_string()))
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::Datetime(_, _) => {
                let val = series.datetime().map_err(Error::from)?.get(index);
                if let Some(dt) = val {
                    Ok(JsonValue::String(dt.to_string()))
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::Time => {
                let val = series.time().map_err(Error::from)?.get(index);
                if let Some(time) = val {
                    Ok(JsonValue::String(time.to_string()))
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::List(_) => {
                // Handle list/array types
                let any_val = series.get(index).map_err(Error::from)?;
                match any_val {
                    AnyValue::List(series) => {
                        let mut arr = Vec::new();
                        for i in 0..series.len() {
                            let item_val = self.series_value_to_json(&series, i)?;
                            arr.push(item_val);
                        }
                        Ok(JsonValue::Array(arr))
                    }
                    _ => Ok(JsonValue::String(format!("{:?}", any_val))),
                }
            }
            DataType::Struct(_) => {
                // Handle struct types
                let any_val = series.get(index).map_err(Error::from)?;
                Ok(JsonValue::String(format!("{:?}", any_val)))
            }
            _ => {
                // For unsupported types, convert to string representation
                let any_val = series.get(index).map_err(Error::from)?;
                Ok(JsonValue::String(format!("{:?}", any_val)))
            }
        }
    }

    /// Get the number of records written
    pub fn records_written(&self) -> usize {
        self.records_written
    }

    /// Flush the writer
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(Error::from)
    }

    /// Finish writing and return the underlying writer
    pub fn finish(mut self) -> Result<W> {
        self.writer.flush()?;
        Ok(self
            .writer
            .into_inner()
            .map_err(|e| Error::operation(format!("Failed to finish JSON writer: {}", e)))?)
    }
}

/// Convenience function to read JSON from a file path
pub fn read_json_file<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let file = File::open(path)?;
    let mut reader = JsonReader::new(BufReader::new(file));
    reader.read_dataframe()
}

/// Convenience function to read JSON from a file path with options
pub fn read_json_file_with_options<P: AsRef<Path>>(
    path: P,
    options: JsonReadOptions,
) -> Result<DataFrame> {
    let file = File::open(path)?;
    let mut reader = JsonReader::with_options(BufReader::new(file), options);
    reader.read_dataframe()
}

/// Convenience function to read JSON Lines from a file path
pub fn read_jsonl_file<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let file = File::open(path)?;
    let options = JsonReadOptions {
        lines: true,
        ..Default::default()
    };
    let mut reader = JsonReader::with_options(BufReader::new(file), options);
    reader.read_dataframe()
}

/// Convenience function to write DataFrame to JSON file
pub fn write_json_file<P: AsRef<Path>>(df: &DataFrame, path: P) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = JsonWriter::new(file);
    writer.write_dataframe(df)
}

/// Convenience function to write DataFrame to JSON file with options
pub fn write_json_file_with_options<P: AsRef<Path>>(
    df: &DataFrame,
    path: P,
    options: JsonWriteOptions,
) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = JsonWriter::with_options(file, options);
    writer.write_dataframe(df)
}

/// Convenience function to write DataFrame to JSON Lines file
pub fn write_jsonl_file<P: AsRef<Path>>(df: &DataFrame, path: P) -> Result<()> {
    let file = File::create(path)?;
    let options = JsonWriteOptions {
        lines: true,
        ..Default::default()
    };
    let mut writer = JsonWriter::with_options(file, options);
    writer.write_dataframe(df)
}

/// Detect JSON format from sample data
pub fn detect_json_format<R: Read>(mut reader: R) -> Result<JsonFormat> {
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;

    let sample = String::from_utf8_lossy(&buffer[..std::cmp::min(buffer.len(), 4096)]);
    let trimmed = sample.trim_start();

    if trimmed.starts_with('[') {
        Ok(JsonFormat::Array)
    } else if trimmed.starts_with('{') {
        // Check if it's JSON Lines
        let lines: Vec<&str> = trimmed.lines().take(3).collect();
        if lines.len() > 1
            && lines.iter().all(|line| {
                let line = line.trim();
                line.starts_with('{') && line.ends_with('}')
            })
        {
            Ok(JsonFormat::Lines)
        } else {
            Ok(JsonFormat::Object)
        }
    } else {
        Err(Error::Format(FormatError::DetectionFailed(
            "Could not detect JSON format".to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_json_array_reader() {
        let json_data = r#"[
            {"name": "Alice", "age": 30, "active": true},
            {"name": "Bob", "age": 25, "active": false}
        ]"#;

        let cursor = BufReader::new(Cursor::new(json_data.as_bytes()));
        let mut reader = JsonReader::new(cursor);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 3);
        let columns = df.get_column_names();
        // Columns are collected in BTreeSet order, so they should be sorted
        assert_eq!(columns, vec!["active", "age", "name"]);
    }

    #[test]
    fn test_json_lines_reader() {
        let jsonl_data = r#"{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie", "age": 35}"#;

        let cursor = Cursor::new(jsonl_data.as_bytes());
        let options = JsonReadOptions {
            lines: true,
            ..Default::default()
        };
        let mut reader = JsonReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_json_writer() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25],
            "active" => [true, false]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = JsonWriter::new(&mut buffer);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        let json_val: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert!(json_val.is_array());
        let array = json_val.as_array().unwrap();
        assert_eq!(array.len(), 2);
    }

    #[test]
    fn test_json_lines_writer() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = JsonWriteOptions {
                lines: true,
                ..Default::default()
            };
            let mut writer = JsonWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        let lines: Vec<&str> = output.trim().split('\n').collect();

        assert_eq!(lines.len(), 2);
        for line in lines {
            let json_val: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(json_val.is_object());
        }
    }

    #[test]
    fn test_flattening() {
        let json_data = r#"[
            {"user": {"name": "Alice", "details": {"age": 30}}, "active": true},
            {"user": {"name": "Bob", "details": {"age": 25}}, "active": false}
        ]"#;

        let cursor = BufReader::new(Cursor::new(json_data.as_bytes()));
        let options = JsonReadOptions {
            flatten: true,
            ..Default::default()
        };
        let mut reader = JsonReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        // Should have flattened columns like "user.name", "user.details.age", "active"
        let columns = df.get_column_names();
        assert_eq!(columns.len(), 3);
        assert!(columns.contains(&"active"));
        assert!(columns.contains(&"user.name"));
        assert!(columns.contains(&"user.details.age"));
    }

    #[test]
    fn test_format_detection() {
        // Test JSON array detection
        let json_array = r#"[{"a": 1}, {"b": 2}]"#;
        let cursor = Cursor::new(json_array.as_bytes());
        let format = detect_json_format(cursor).unwrap();
        assert!(matches!(format, JsonFormat::Array));

        // Test JSON Lines detection
        let json_lines = r#"{"a": 1}
{"b": 2}"#;
        let cursor = Cursor::new(json_lines.as_bytes());
        let format = detect_json_format(cursor).unwrap();
        assert!(matches!(format, JsonFormat::Lines));

        // Test single object detection
        let json_object = r#"{"a": 1, "b": 2}"#;
        let cursor = Cursor::new(json_object.as_bytes());
        let format = detect_json_format(cursor).unwrap();
        assert!(matches!(format, JsonFormat::Object));
    }

    #[test]
    fn test_error_handling() {
        let invalid_json = r#"{"name": "Alice", "age": 30,}"#; // Trailing comma

        let cursor = Cursor::new(invalid_json.as_bytes());
        let mut reader = JsonReader::new(cursor);
        let result = reader.read_dataframe();

        assert!(result.is_err());
    }

    #[test]
    fn test_ignore_errors() {
        let mixed_json = r#"{"name": "Alice", "age": 30}
invalid json line
{"name": "Bob", "age": 25}"#;

        let cursor = Cursor::new(mixed_json.as_bytes());
        let options = JsonReadOptions {
            lines: true,
            ignore_errors: true,
            ..Default::default()
        };
        let mut reader = JsonReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2); // Should skip the invalid line
    }
}

// Public API functions for use by reader/writer modules

use crate::reader::{FormatReadOptions, ReadOptions};
use crate::writer::{FormatWriteOptions, WriteOptions};

/// Helper function to convert a Polars row to serde_json::Value
pub(crate) fn row_to_json_value(row: &[AnyValue], column_names: &[String]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (i, field) in row.iter().enumerate() {
        let key = column_names
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("col_{}", i));
        let value = match field {
            AnyValue::Null => serde_json::Value::Null,
            AnyValue::Boolean(b) => serde_json::Value::Bool(*b),
            AnyValue::Int8(i) => serde_json::Value::Number((*i).into()),
            AnyValue::Int16(i) => serde_json::Value::Number((*i).into()),
            AnyValue::Int32(i) => serde_json::Value::Number((*i).into()),
            AnyValue::Int64(i) => serde_json::Value::Number((*i).into()),
            AnyValue::UInt8(i) => serde_json::Value::Number((*i).into()),
            AnyValue::UInt16(i) => serde_json::Value::Number((*i).into()),
            AnyValue::UInt32(i) => serde_json::Value::Number((*i).into()),
            AnyValue::UInt64(i) => serde_json::Value::Number((*i).into()),
            AnyValue::Float32(f) => serde_json::Value::Number(
                serde_json::Number::from_f64((*f) as f64).unwrap_or(serde_json::Number::from(0)),
            ),
            AnyValue::Float64(f) => serde_json::Value::Number(
                serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)),
            ),
            AnyValue::Utf8(s) => serde_json::Value::String(s.to_string()),
            _ => serde_json::Value::Null,
        };
        map.insert(key.to_string(), value);
    }
    serde_json::Value::Object(map)
}

/// Deserialize JSON data from a reader
pub fn deserialize_json<R: Read>(
    mut reader: R,
    options: &ReadOptions,
    format_options: &FormatReadOptions,
) -> Result<Value> {
    use polars::prelude::SerReader;
    use std::io::Cursor;

    let (lines, ignore_errors) = match format_options {
        FormatReadOptions::Json {
            lines,
            ignore_errors,
        } => (*lines, *ignore_errors),
        _ => (false, false),
    };

    if lines {
        // JSON Lines format - use Polars native NDJSON reader
        // Note: ignore_errors is not directly supported by Polars JsonReader
        // Read into memory first since Polars requires MmapBytesReader
        let mut jsonl_str = String::new();
        reader.read_to_string(&mut jsonl_str).map_err(Error::from)?;

        // Handle empty input
        if jsonl_str.trim().is_empty() {
            return Ok(Value::DataFrame(DataFrame::empty()));
        }

        let cursor = Cursor::new(jsonl_str.as_bytes());
        let mut df = polars::io::json::JsonReader::new(cursor)
            .finish()
            .map_err(|e| {
                if ignore_errors {
                    // Return empty DataFrame on error if ignore_errors is true
                    return Error::Format(FormatError::SerializationError(format!(
                        "JSON parsing error: {}",
                        e
                    )));
                }
                Error::from(e)
            })?;

        // Apply max_rows by slicing the DataFrame
        if let Some(max_rows) = options.max_rows {
            if df.height() > max_rows {
                df = df.slice(0, max_rows);
            }
        }

        Ok(Value::DataFrame(df))
    } else {
        // Regular JSON - use Polars native JSON reader
        let mut json_str = String::new();
        reader.read_to_string(&mut json_str).map_err(Error::from)?;

        // Handle empty input
        if json_str.trim().is_empty() {
            return Ok(Value::DataFrame(DataFrame::empty()));
        }

        // Use Polars' native JsonReader for better performance
        let cursor = Cursor::new(json_str.as_bytes());
        let mut df = polars::io::json::JsonReader::new(cursor)
            .finish()
            .map_err(Error::from)?;

        // Apply max_rows by slicing the DataFrame
        if let Some(max_rows) = options.max_rows {
            if df.height() > max_rows {
                df = df.slice(0, max_rows);
            }
        }

        Ok(Value::DataFrame(df))
    }
}

/// Serialize JSON data to a writer
pub fn serialize_json<W: Write>(
    mut writer: W,
    value: &Value,
    _options: &WriteOptions,
    format_options: &FormatWriteOptions,
) -> Result<()> {
    let df = match value {
        Value::DataFrame(df) => df.clone(),
        Value::LazyFrame(lf) => (*lf).clone().collect().map_err(Error::from)?,
        _ => {
            return Err(Error::operation(
                "Expected DataFrame for JSON serialization",
            ))
        }
    };

    let json_opts = match format_options {
        FormatWriteOptions::Json { lines, pretty } => (*lines, *pretty),
        _ => (false, false),
    };

    let column_names = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    if json_opts.0 {
        // JSON Lines
        for i in 0..df.height() {
            let row = df.get_row(i).map_err(Error::from)?;
            let json_value = row_to_json_value(&row.0, &column_names);
            let json_str = if json_opts.1 {
                serde_json::to_string_pretty(&json_value)
            } else {
                serde_json::to_string(&json_value)
            }
            .map_err(|e| Error::Format(FormatError::SerializationError(e.to_string())))?;
            writer.write_all(json_str.as_bytes()).map_err(Error::from)?;
            writer.write_all(b"\n").map_err(Error::from)?;
        }
    } else {
        // Regular JSON array
        let mut rows = Vec::new();
        for i in 0..df.height() {
            let row = df.get_row(i).map_err(Error::from)?;
            rows.push(row_to_json_value(&row.0, &column_names));
        }
        let json_str = if json_opts.1 {
            serde_json::to_string_pretty(&rows)
        } else {
            serde_json::to_string(&rows)
        }
        .map_err(|e| Error::Format(FormatError::SerializationError(e.to_string())))?;
        writer.write_all(json_str.as_bytes()).map_err(Error::from)?;
    }

    Ok(())
}
