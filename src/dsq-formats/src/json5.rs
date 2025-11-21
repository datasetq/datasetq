use crate::error::{Error, FormatError, Result};
use crate::format::DataFormat;
use dsq_shared::value::Value;
use json5::{Deserializer, Location};
use polars::prelude::*;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;

/// JSON5-specific reading options
#[derive(Debug, Clone)]
pub struct Json5ReadOptions {
    /// Whether to treat input as JSON Lines (newline-delimited JSON5)
    pub lines: bool,
    /// Whether to ignore invalid JSON5 lines (only for JSON Lines)
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

impl Default for Json5ReadOptions {
    fn default() -> Self {
        Self {
            lines: false,
            ignore_errors: false,
            infer_schema_length: Some(1000),
            flatten: false,
            flatten_separator: ".".to_string(),
            max_depth: 64,
            buffer_size: 8192,
        }
    }
}

/// JSON5-specific writing options
#[derive(Debug, Clone)]
pub struct Json5WriteOptions {
    /// Whether to output as JSON Lines format
    pub lines: bool,
    /// Whether to pretty-print the JSON5
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

impl Default for Json5WriteOptions {
    fn default() -> Self {
        Self {
            lines: false,
            pretty: false,
            maintain_order: false,
            null_value: None,
            escape_unicode: false,
            line_terminator: "\n".to_string(),
            buffer_size: 8192,
        }
    }
}

/// JSON5 reader that handles both regular JSON5 and JSON Lines formats
pub struct Json5Reader<R> {
    reader: R,
    options: Json5ReadOptions,
    detected_format: Option<Json5Format>,
}

#[derive(Debug, Clone, Copy)]
enum Json5Format {
    Array,
    Object,
    Lines,
}

impl<R: BufRead> Json5Reader<R> {
    /// Create a new JSON5 reader with default options
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            options: Json5ReadOptions::default(),
            detected_format: None,
        }
    }

    /// Create a JSON5 reader with custom options
    pub fn with_options(reader: R, options: Json5ReadOptions) -> Self {
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

    /// Auto-detect JSON5 format from the first few bytes
    pub fn detect_format(&mut self) -> Result<Json5Format> {
        if let Some(format) = self.detected_format {
            return Ok(format);
        }

        if self.options.lines {
            self.detected_format = Some(Json5Format::Lines);
            return Ok(Json5Format::Lines);
        }

        // Peek at the first few bytes without consuming
        let buffer = self.reader.fill_buf()?;

        if buffer.is_empty() {
            // Return empty DataFrame for empty input
            return Ok(Json5Format::Array);
        }

        let text = String::from_utf8_lossy(buffer);
        let trimmed = text.trim_start();

        // Skip comments to find the actual JSON5 structure
        let content_start = trimmed.find(|c: char| c == '[' || c == '{');
        let content = if let Some(start) = content_start {
            &trimmed[start..]
        } else {
            trimmed
        };

        let format = if content.starts_with('[') {
            Json5Format::Array
        } else if content.starts_with('{') {
            // Check if it's JSON Lines by looking for multiple objects
            let lines: Vec<&str> = content.lines().take(3).collect();
            if lines.len() > 1
                && lines.iter().all(|line| {
                    let line = line.trim();
                    line.starts_with('{') && line.ends_with('}')
                })
            {
                Json5Format::Lines
            } else {
                Json5Format::Object
            }
        } else {
            return Err(Error::Format(FormatError::InvalidOption(
                "Invalid JSON5 format: must start with '[' or '{'".to_string(),
            )));
        };

        self.detected_format = Some(format);
        Ok(format)
    }

    /// Read JSON5 data into a DataFrame
    pub fn read_dataframe(&mut self) -> Result<DataFrame> {
        let format = self.detect_format()?;

        match format {
            Json5Format::Lines => self.read_json5_lines(),
            Json5Format::Array => self.read_json5_array(),
            Json5Format::Object => self.read_json5_object(),
        }
    }

    /// Read JSON5 Lines format
    fn read_json5_lines(&mut self) -> Result<DataFrame> {
        let mut buf_reader = BufReader::with_capacity(self.options.buffer_size, &mut self.reader);
        let mut json_vals = Vec::new();
        let mut line = String::new();
        let mut line_number = 0;

        let max_lines = self.options.infer_schema_length.unwrap_or(usize::MAX);

        loop {
            line.clear();
            let bytes_read = buf_reader.read_line(&mut line)?;
            if bytes_read == 0 {
                break; // EOF
            }

            line_number += 1;
            let trimmed = line.trim();

            if trimmed.is_empty() {
                continue; // Skip empty lines
            }

            match json5::from_str::<JsonValue>(trimmed) {
                Ok(json_val) => {
                    json_vals.push(json_val);

                    if json_vals.len() >= max_lines {
                        break;
                    }
                }
                Err(e) => {
                    if self.options.ignore_errors {
                        eprintln!(
                            "Warning: Ignoring invalid JSON5 on line {}: {}",
                            line_number, e
                        );
                        continue;
                    } else {
                        return Err(Error::Format(FormatError::InvalidOption(format!(
                            "Invalid JSON5 on line {}: {}",
                            line_number, e
                        ))));
                    }
                }
            }
        }

        if json_vals.is_empty() {
            return Ok(DataFrame::empty());
        }

        // Now process the JSON values
        let mut rows = Vec::new();
        for json_val in json_vals {
            let value = self.process_json_value(json_val, 0)?;
            rows.push(value);
        }

        self.values_to_dataframe(rows)
    }

    /// Read JSON5 array format
    fn read_json5_array(&mut self) -> Result<DataFrame> {
        let mut content = String::new();
        self.reader
            .read_to_string(&mut content)
            .map_err(|e| Error::Format(FormatError::InvalidOption(format!("Read error: {}", e))))?;

        // Handle empty input
        if content.trim().is_empty() {
            return Ok(DataFrame::empty());
        }

        let json_val: JsonValue = json5::from_str(&content)
            .map_err(|e| Error::Format(FormatError::SerializationError(e.to_string())))?;

        match json_val {
            JsonValue::Array(arr) => {
                let mut rows = Vec::new();
                let max_items = self.options.infer_schema_length.unwrap_or(arr.len());

                for (i, item) in arr.into_iter().enumerate() {
                    if i >= max_items {
                        break;
                    }
                    let value = self.process_json_value(item, 0)?;
                    rows.push(value);
                }

                if rows.is_empty() {
                    return Ok(DataFrame::empty());
                }

                self.values_to_dataframe(rows)
            }
            _ => Err(Error::Format(FormatError::InvalidOption(
                "Expected JSON5 array at root level".to_string(),
            ))),
        }
    }

    /// Read single JSON5 object format
    fn read_json5_object(&mut self) -> Result<DataFrame> {
        let mut content = String::new();
        self.reader
            .read_to_string(&mut content)
            .map_err(|e| Error::Format(FormatError::InvalidOption(format!("Read error: {}", e))))?;
        let json_val: JsonValue = json5::from_str(&content).map_err(|e| {
            Error::Format(FormatError::InvalidOption(format!("Invalid JSON5: {}", e)))
        })?;

        let value = self.process_json_value(json_val, 0)?;
        let rows = vec![value];

        self.values_to_dataframe(rows)
    }

    /// Process a JSON5 value, applying flattening if configured
    fn process_json_value(&self, json_val: JsonValue, depth: usize) -> Result<Value> {
        if depth > self.options.max_depth {
            return Err(Error::Format(FormatError::InvalidOption(format!(
                "JSON5 nesting depth exceeds maximum of {}",
                self.options.max_depth
            ))));
        }

        match json_val {
            JsonValue::Object(obj) => {
                if self.options.flatten {
                    self.flatten_object(obj, "", depth)
                } else {
                    let mut map = std::collections::HashMap::new();
                    for (k, v) in obj {
                        let value = self.process_json_value(v, depth + 1)?;
                        map.insert(k, value);
                    }
                    Ok(Value::Object(map))
                }
            }
            _ => Ok(Value::from_json(json_val)),
        }
    }

    /// Flatten a JSON5 object into a flat structure
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

            match &value {
                JsonValue::Object(nested_obj) => {
                    if depth < self.options.max_depth {
                        let nested_flattened =
                            self.flatten_object(nested_obj.clone(), &new_key, depth + 1)?;
                        if let Value::Object(nested_map) = nested_flattened {
                            flattened.extend(nested_map);
                        }
                    } else {
                        // Max depth reached, store as string representation
                        flattened.insert(new_key, Value::String(value.to_string()));
                    }
                }
                JsonValue::Array(arr) => {
                    if self.should_flatten_array(&arr) {
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
                    flattened.insert(new_key, Value::from_json(value));
                }
            }
        }

        Ok(Value::Object(flattened))
    }

    /// Determine if an array should be flattened
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
                        let val = obj.get(col).unwrap_or(&Value::Null).clone();
                        let any_val = self.value_to_any_value(val)?;
                        series_data
                            .get_mut(col)
                            .ok_or_else(|| Error::operation("Column not found in series data"))?
                            .push(any_val);
                    }
                }
                _ => {
                    // If we have a non-object value, put it in the first column
                    if let Some(first_col) = columns.first() {
                        let any_val = self.value_to_any_value(value.clone())?;
                        series_data
                            .get_mut(first_col)
                            .ok_or_else(|| Error::operation("Column not found in series data"))?
                            .push(any_val);

                        // Fill other columns with nulls
                        for col in columns.iter().skip(1) {
                            series_data
                                .get_mut(col)
                                .ok_or_else(|| Error::operation("Column not found in series data"))?
                                .push(AnyValue::Null);
                        }
                    }
                }
            }
        }

        // Create Series from vectors
        let mut series_vec = Vec::new();
        for col in columns {
            let values = series_data
                .remove(&col)
                .ok_or_else(|| Error::operation("Column not found in series data"))?;
            let series = Series::new(&col, values);
            series_vec.push(series);
        }

        DataFrame::new(series_vec).map_err(Error::from)
    }

    /// Create a simple DataFrame for non-object values
    fn create_simple_dataframe(&self, values: Vec<Value>) -> Result<DataFrame> {
        let any_values: Result<Vec<AnyValue>> = values
            .into_iter()
            .map(|v| self.value_to_any_value(v))
            .collect();

        let series = Series::new("value", any_values?);
        DataFrame::new(vec![series]).map_err(Error::from)
    }

    /// Convert Value to AnyValue for Polars
    fn value_to_any_value(&self, value: Value) -> Result<AnyValue<'static>> {
        match value {
            Value::Null => Ok(AnyValue::Null),
            Value::Bool(b) => Ok(AnyValue::Boolean(b)),
            Value::Int(i) => Ok(AnyValue::Int64(i)),
            Value::Float(f) => Ok(AnyValue::Float64(f)),
            Value::String(s) => Ok(AnyValue::Utf8(Box::leak(s.into_boxed_str()))),
            Value::Array(_) => {
                // TODO: Convert array to List AnyValue
                Ok(AnyValue::Utf8("array"))
            }
            Value::Object(_) => {
                // TODO: Convert object to Struct AnyValue
                Ok(AnyValue::Utf8("object"))
            }
            _ => Err(Error::Format(FormatError::UnsupportedFeature(format!(
                "Cannot convert {} to AnyValue",
                "unknown"
            )))),
        }
    }

    /// Peek at the first few records without consuming the reader
    pub fn peek(&mut self, records: usize) -> Result<DataFrame> {
        let mut temp_options = self.options.clone();
        temp_options.infer_schema_length = Some(records);

        let mut temp_reader = Json5Reader::with_options(&mut self.reader, temp_options);
        temp_reader.read_dataframe()
    }
}

/// JSON5 writer that handles both regular JSON5 and JSON Lines formats
pub struct Json5Writer<W: Write> {
    writer: BufWriter<W>,
    options: Json5WriteOptions,
    records_written: usize,
    first_record: bool,
}

impl<W: Write> Json5Writer<W> {
    /// Create a new JSON5 writer with default options
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::with_capacity(8192, writer),
            options: Json5WriteOptions::default(),
            records_written: 0,
            first_record: true,
        }
    }

    /// Create a JSON5 writer with custom options
    pub fn with_options(writer: W, options: Json5WriteOptions) -> Self {
        Self {
            writer: BufWriter::with_capacity(options.buffer_size, writer),
            options,
            records_written: 0,
            first_record: true,
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

    /// Write a DataFrame to JSON5
    pub fn write_dataframe(&mut self, df: &DataFrame) -> Result<()> {
        if self.options.lines {
            self.write_json5_lines(df)
        } else {
            self.write_json5_array(df)
        }
    }

    /// Write DataFrame as JSON5 Lines
    fn write_json5_lines(&mut self, df: &DataFrame) -> Result<()> {
        for row_idx in 0..df.height() {
            let row_obj = self.dataframe_row_to_json_object(df, row_idx)?;

            let json_str = if self.options.escape_unicode {
                let json_str = serde_json::to_string(&row_obj)
                    .map_err(|e| Error::operation(format!("JSON5 serialization error: {}", e)))?;
                // Manually escape unicode characters
                Self::escape_unicode_chars(&json_str)
            } else {
                json5::to_string(&row_obj)
                    .map_err(|e| Error::operation(format!("JSON5 serialization error: {}", e)))?
            };

            self.writer.write_all(json_str.as_bytes())?;
            self.writer
                .write_all(self.options.line_terminator.as_bytes())?;

            self.records_written += 1;
        }

        Ok(())
    }

    /// Write DataFrame as JSON5 array
    fn write_json5_array(&mut self, df: &DataFrame) -> Result<()> {
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

            let json_str = match self.options.escape_unicode {
                true => {
                    let s = serde_json::to_string(&row_obj).map_err(|e| {
                        Error::operation(format!("JSON5 serialization error: {}", e))
                    })?;
                    Self::escape_unicode_chars(&s)
                }
                false => json5::to_string(&row_obj)
                    .map_err(|e| Error::operation(format!("JSON5 serialization error: {}", e)))?,
            }
            .replace('\n', "\n  ") // Indent nested content for pretty printing
            .replace("{\n  ", "{\n    ") // Additional indentation for objects
            .replace("[\n  ", "[\n    ") // Additional indentation for arrays
            .replace("\n  }", "\n    }")
            .replace("\n  ]", "\n    ]");

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

    /// Convert a DataFrame row to a JSON5 object
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

    /// Convert a single Series value to JSON5
    fn series_value_to_json(&self, series: &Series, index: usize) -> Result<JsonValue> {
        use polars::datatypes::*;

        match series.dtype() {
            DataType::Boolean => {
                let val = series.bool().map_err(Error::from)?.get(index);
                match val {
                    Some(v) => Ok(JsonValue::Bool(v)),
                    None => {
                        if let Some(ref null_str) = self.options.null_value {
                            Ok(JsonValue::String(null_str.clone()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                }
            }
            DataType::Int8 => {
                let val = series.i8().map_err(Error::from)?.get(index);
                match val {
                    Some(v) => Ok(JsonValue::Number(serde_json::Number::from(v))),
                    None => {
                        if let Some(ref null_str) = self.options.null_value {
                            Ok(JsonValue::String(null_str.clone()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                }
            }
            DataType::Int16 => {
                let val = series.i16().map_err(Error::from)?.get(index);
                match val {
                    Some(v) => Ok(JsonValue::Number(serde_json::Number::from(v))),
                    None => {
                        if let Some(ref null_str) = self.options.null_value {
                            Ok(JsonValue::String(null_str.clone()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                }
            }
            DataType::Int32 => {
                let val = series.i32().map_err(Error::from)?.get(index);
                match val {
                    Some(v) => Ok(JsonValue::Number(serde_json::Number::from(v))),
                    None => {
                        if let Some(ref null_str) = self.options.null_value {
                            Ok(JsonValue::String(null_str.clone()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                }
            }
            DataType::Int64 => {
                let val = series.i64().map_err(Error::from)?.get(index);
                match val {
                    Some(v) => Ok(JsonValue::Number(serde_json::Number::from(v))),
                    None => {
                        if let Some(ref null_str) = self.options.null_value {
                            Ok(JsonValue::String(null_str.clone()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                }
            }
            DataType::UInt8 => {
                let val = series.u8().map_err(Error::from)?.get(index);
                match val {
                    Some(v) => Ok(JsonValue::Number(serde_json::Number::from(v))),
                    None => {
                        if let Some(ref null_str) = self.options.null_value {
                            Ok(JsonValue::String(null_str.clone()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                }
            }
            DataType::UInt16 => {
                let val = series.u16().map_err(Error::from)?.get(index);
                match val {
                    Some(v) => Ok(JsonValue::Number(serde_json::Number::from(v))),
                    None => {
                        if let Some(ref null_str) = self.options.null_value {
                            Ok(JsonValue::String(null_str.clone()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                }
            }
            DataType::UInt32 => {
                let val = series.u32().map_err(Error::from)?.get(index);
                match val {
                    Some(v) => Ok(JsonValue::Number(serde_json::Number::from(v))),
                    None => {
                        if let Some(ref null_str) = self.options.null_value {
                            Ok(JsonValue::String(null_str.clone()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                }
            }
            DataType::UInt64 => {
                let val = series.u64().map_err(Error::from)?.get(index);
                match val {
                    Some(v) => Ok(JsonValue::Number(serde_json::Number::from(v))),
                    None => {
                        if let Some(ref null_str) = self.options.null_value {
                            Ok(JsonValue::String(null_str.clone()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                }
            }
            DataType::Float32 | DataType::Float64 => {
                let val = series.f64().map_err(Error::from)?.get(index);
                match val {
                    Some(v) => serde_json::Number::from_f64(v)
                        .map(JsonValue::Number)
                        .ok_or_else(|| Error::operation("Invalid float value")),
                    None => {
                        if let Some(ref null_str) = self.options.null_value {
                            Ok(JsonValue::String(null_str.clone()))
                        } else {
                            Ok(JsonValue::Null)
                        }
                    }
                }
            }
            DataType::Utf8 => {
                let val = series.utf8().map_err(Error::from)?.get(index);
                let string_val = match val {
                    Some(v) => v.to_string(),
                    None => {
                        return if let Some(ref null_str) = self.options.null_value {
                            Ok(JsonValue::String(null_str.clone()))
                        } else {
                            Ok(JsonValue::Null)
                        };
                    }
                };

                // Try to parse as JSON5 if it looks like JSON5
                if (string_val.starts_with('{') && string_val.ends_with('}'))
                    || (string_val.starts_with('[') && string_val.ends_with(']'))
                {
                    match json5::from_str::<JsonValue>(&string_val) {
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
                } else if let Some(ref null_str) = self.options.null_value {
                    Ok(JsonValue::String(null_str.clone()))
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::Datetime(_, _) => {
                let val = series.datetime().map_err(Error::from)?.get(index);
                if let Some(dt) = val {
                    Ok(JsonValue::String(dt.to_string()))
                } else if let Some(ref null_str) = self.options.null_value {
                    Ok(JsonValue::String(null_str.clone()))
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

    /// Escape unicode characters in a JSON string
    fn escape_unicode_chars(s: &str) -> String {
        let mut result = String::new();
        let mut in_string = false;
        let mut chars = s.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '"' {
                result.push(ch);
                in_string = !in_string;
            } else if in_string && ch as u32 > 127 {
                // Escape non-ASCII characters inside strings
                result.push_str(&format!("\\u{:04x}", ch as u32));
            } else {
                result.push(ch);
            }
        }

        result
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
            .map_err(|e| Error::operation(format!("Failed to finish JSON5 writer: {}", e)))?)
    }
}

/// Convenience function to read JSON5 from a file path
pub fn read_json5_file<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let file = File::open(path)?;
    let mut reader = Json5Reader::new(BufReader::new(file));
    reader.read_dataframe()
}

/// Convenience function to read JSON5 from a file path with options
pub fn read_json5_file_with_options<P: AsRef<Path>>(
    path: P,
    options: Json5ReadOptions,
) -> Result<DataFrame> {
    let file = File::open(path)?;
    let mut reader = Json5Reader::with_options(BufReader::new(file), options);
    reader.read_dataframe()
}

/// Convenience function to read JSON5 Lines from a file path
pub fn read_json5l_file<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let file = File::open(path)?;
    let options = Json5ReadOptions {
        lines: true,
        ..Default::default()
    };
    let mut reader = Json5Reader::with_options(BufReader::new(file), options);
    reader.read_dataframe()
}

/// Convenience function to write DataFrame to JSON5 file
pub fn write_json5_file<P: AsRef<Path>>(df: &DataFrame, path: P) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = Json5Writer::new(file);
    writer.write_dataframe(df)
}

/// Convenience function to write DataFrame to JSON5 file with options
pub fn write_json5_file_with_options<P: AsRef<Path>>(
    df: &DataFrame,
    path: P,
    options: Json5WriteOptions,
) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = Json5Writer::with_options(file, options);
    writer.write_dataframe(df)
}

/// Convenience function to write DataFrame to JSON5 Lines file
pub fn write_json5l_file<P: AsRef<Path>>(df: &DataFrame, path: P) -> Result<()> {
    let file = File::create(path)?;
    let options = Json5WriteOptions {
        lines: true,
        ..Default::default()
    };
    let mut writer = Json5Writer::with_options(file, options);
    writer.write_dataframe(df)
}

/// Detect JSON5 format from sample data
pub fn detect_json5_format<R: BufRead>(mut reader: R) -> Result<Json5Format> {
    let buffer = reader.fill_buf()?;

    if buffer.is_empty() {
        return Err(Error::Format(FormatError::DetectionFailed(
            "Empty input".to_string(),
        )));
    }

    let sample = String::from_utf8_lossy(&buffer[..std::cmp::min(buffer.len(), 4096)]);
    let trimmed = sample.trim_start();

    if trimmed.starts_with('[') {
        Ok(Json5Format::Array)
    } else if trimmed.starts_with('{') {
        // Check if it's JSON Lines
        let lines: Vec<&str> = trimmed.lines().take(3).collect();
        if lines.len() > 1
            && lines.iter().all(|line| {
                let line = line.trim();
                line.starts_with('{') && line.ends_with('}')
            })
        {
            Ok(Json5Format::Lines)
        } else {
            Ok(Json5Format::Object)
        }
    } else {
        Err(Error::Format(FormatError::DetectionFailed(
            "Could not detect JSON5 format".to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_json5_array_reader() {
        let json5_data = r#"[
            {name: "Alice", age: 30, active: true},
            {name: "Bob", age: 25, active: false}
        ]"#;

        let cursor = BufReader::new(Cursor::new(json5_data.as_bytes()));
        let mut reader = Json5Reader::new(cursor);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 3);
        assert_eq!(df.get_column_names(), vec!["active", "age", "name"]);
    }

    #[test]
    fn test_json5_with_comments() {
        let json5_data = r#"// This is a comment
        [
            /* another comment */
            {name: "Alice", age: 30},
            {name: "Bob", age: 25}
        ]"#;

        let cursor = BufReader::new(Cursor::new(json5_data.as_bytes()));
        let mut reader = Json5Reader::new(cursor);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_json5_lines_reader() {
        let json5l_data = r#"{name: "Alice", age: 30}
{name: "Bob", age: 25}
{name: "Charlie", age: 35}"#;

        let cursor = BufReader::new(Cursor::new(json5l_data.as_bytes()));
        let options = Json5ReadOptions {
            lines: true,
            ..Default::default()
        };
        let mut reader = Json5Reader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_json5_writer() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25],
            "active" => [true, false]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = Json5Writer::new(&mut buffer);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        let json_val: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert!(json_val.is_array());
        let array = json_val.as_array().unwrap();
        assert_eq!(array.len(), 2);
    }

    #[test]
    fn test_json5_lines_writer() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = Json5WriteOptions {
                lines: true,
                ..Default::default()
            };
            let mut writer = Json5Writer::with_options(&mut buffer, options);
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
        let json5_data = r#"[
            {user: {name: "Alice", details: {age: 30}}, active: true},
            {user: {name: "Bob", details: {age: 25}}, active: false}
        ]"#;

        let cursor = BufReader::new(Cursor::new(json5_data.as_bytes()));
        let options = Json5ReadOptions {
            flatten: true,
            ..Default::default()
        };
        let mut reader = Json5Reader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        // Should have flattened columns like "user.name", "user.details.age", "active"
        let columns = df.get_column_names();
        assert!(columns.contains(&"active"));
        assert!(columns.contains(&"user.name"));
        assert!(columns.contains(&"user.details.age"));
    }

    #[test]
    fn test_format_detection() {
        // Test JSON5 array detection
        let json5_array = r#"[{a: 1}, {b: 2}]"#;
        let cursor = BufReader::new(Cursor::new(json5_array.as_bytes()));
        let format = detect_json5_format(cursor).unwrap();
        assert!(matches!(format, Json5Format::Array));

        // Test JSON5 Lines detection
        let json5_lines = r#"{a: 1}
{b: 2}"#;
        let cursor = BufReader::new(Cursor::new(json5_lines.as_bytes()));
        let format = detect_json5_format(cursor).unwrap();
        assert!(matches!(format, Json5Format::Lines));

        // Test single object detection
        let json5_object = r#"{a: 1, b: 2}"#;
        let cursor = BufReader::new(Cursor::new(json5_object.as_bytes()));
        let format = detect_json5_format(cursor).unwrap();
        assert!(matches!(format, Json5Format::Object));
    }

    #[test]
    fn test_error_handling() {
        let invalid_json5 = r#"{name: "Alice", age: 30,}"#; // Trailing comma

        let cursor = BufReader::new(Cursor::new(invalid_json5.as_bytes()));
        let mut reader = Json5Reader::new(cursor);
        let result = reader.read_dataframe();

        // JSON5 should handle trailing commas
        assert!(result.is_ok());
    }

    #[test]
    fn test_ignore_errors() {
        let mixed_json5 = r#"{name: "Alice", age: 30}
invalid json5 line
{name: "Bob", age: 25}"#;

        let cursor = BufReader::new(Cursor::new(mixed_json5.as_bytes()));
        let options = Json5ReadOptions {
            lines: true,
            ignore_errors: true,
            ..Default::default()
        };
        let mut reader = Json5Reader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2); // Should skip the invalid line
    }

    #[test]
    fn test_empty_input() {
        let empty = "";
        let cursor = BufReader::new(Cursor::new(empty.as_bytes()));
        let mut reader = Json5Reader::new(cursor);
        let result = reader.read_dataframe();
        assert!(result.is_ok()); // Should succeed on empty input
        let df = result.unwrap();
        assert!(df.is_empty()); // Should return empty DataFrame
    }

    #[test]
    fn test_single_object() {
        let json5_data = r#"{name: "Alice", age: 30, active: true}"#;

        let cursor = BufReader::new(Cursor::new(json5_data.as_bytes()));
        let mut reader = Json5Reader::new(cursor);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 3);
        assert_eq!(df.get_column_names(), vec!["active", "age", "name"]);
    }

    #[test]
    fn test_json5_with_trailing_commas() {
        let json5_data = r#"[
            {name: "Alice", age: 30,},
            {name: "Bob", age: 25,},
        ]"#;

        let cursor = BufReader::new(Cursor::new(json5_data.as_bytes()));
        let mut reader = Json5Reader::new(cursor);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_json5_unquoted_keys() {
        let json5_data = r#"[
            {name: "Alice", age: 30},
            {name: "Bob", age: 25}
        ]"#;

        let cursor = BufReader::new(Cursor::new(json5_data.as_bytes()));
        let mut reader = Json5Reader::new(cursor);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_max_depth() {
        let deep_json5 = r#"{"a": {"b": {"c": {"d": {"e": "deep"}}}}}"#;

        let cursor = BufReader::new(Cursor::new(deep_json5.as_bytes()));
        let options = Json5ReadOptions {
            max_depth: 3,
            ..Default::default()
        };
        let mut reader = Json5Reader::with_options(cursor, options);
        let result = reader.read_dataframe();
        assert!(result.is_err()); // Should error due to max depth
    }

    #[test]
    fn test_infer_schema_length() {
        let json5_lines = r#"{a: 1}
{b: 2}
{c: 3}
{d: 4}"#;

        let cursor = BufReader::new(Cursor::new(json5_lines.as_bytes()));
        let options = Json5ReadOptions {
            lines: true,
            infer_schema_length: Some(2),
            ..Default::default()
        };
        let mut reader = Json5Reader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2); // Should only read 2 lines
    }

    #[test]
    fn test_pretty_writer() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = Json5WriteOptions {
                pretty: true,
                ..Default::default()
            };
            let mut writer = Json5Writer::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains('\n')); // Should have newlines for pretty
        assert!(output.contains("  ")); // Should have indentation
    }

    #[test]
    fn test_null_value_writer() {
        let df = df! {
            "name" => [Some("Alice"), None],
            "age" => [Some(30), None]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = Json5WriteOptions {
                null_value: Some("N/A".to_string()),
                ..Default::default()
            };
            let mut writer = Json5Writer::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("N/A") || output.contains("N\\/A")); // Should use custom null value (may be escaped)
    }

    #[test]
    fn test_escape_unicode() {
        let df = df! {
            "text" => ["héllo", "wörld"]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = Json5WriteOptions {
                escape_unicode: true,
                ..Default::default()
            };
            let mut writer = Json5Writer::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        // With escape_unicode, non-ASCII should be escaped
        assert!(output.contains("\\u")); // Should escape unicode
    }

    #[test]
    fn test_convenience_functions() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        write_json5_file(&df, temp_file.path()).unwrap();

        let read_df = read_json5_file(temp_file.path()).unwrap();
        assert_eq!(read_df.height(), 2);
        assert_eq!(read_df.width(), 2);
    }

    #[test]
    fn test_detect_json5_format_function() {
        let array_data = r#"[{"a": 1}, {"b": 2}]"#;
        let cursor = BufReader::new(Cursor::new(array_data.as_bytes()));
        let format = detect_json5_format(cursor).unwrap();
        assert!(matches!(format, Json5Format::Array));

        let lines_data = r#"{a: 1}
{b: 2}"#;
        let cursor = BufReader::new(Cursor::new(lines_data.as_bytes()));
        let format = detect_json5_format(cursor).unwrap();
        assert!(matches!(format, Json5Format::Lines));

        let object_data = r#"{a: 1, b: 2}"#;
        let cursor = BufReader::new(Cursor::new(object_data.as_bytes()));
        let format = detect_json5_format(cursor).unwrap();
        assert!(matches!(format, Json5Format::Object));
    }
}
