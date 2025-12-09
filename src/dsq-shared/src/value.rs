//! Value types for DSQ data processing
//!
//! This module provides the core Value enum that represents all data types
//! that can be processed by DSQ, including JSON-like values and Polars DataFrames.

use chrono::{DateTime, Duration, NaiveDate};
use num_traits::identities::Zero;
use polars::prelude::*;
use serde::ser::{SerializeMap, SerializeSeq};
use serde_json::{Number as JsonNumber, Value as JsonValue};
use std::collections::HashMap;

use rayon::prelude::*;

/// A value type that bridges between jaq's JSON values and Polars `DataFrames`
#[derive(Clone)]
pub enum Value {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value (i64)
    Int(i64),
    /// Big integer value (arbitrary precision)
    BigInt(num_bigint::BigInt),
    /// Float value (f64)
    Float(f64),
    /// String value
    String(String),
    /// Array of values
    Array(Vec<Value>),
    /// Object (key-value pairs)
    Object(HashMap<String, Value>),
    /// Polars `DataFrame`
    DataFrame(polars::prelude::DataFrame),
    /// Polars `LazyFrame` (for lazy evaluation)
    LazyFrame(Box<polars::prelude::LazyFrame>),
    /// Polars Series (column)
    Series(polars::prelude::Series),
}

impl Value {
    /// Create a new null value
    #[must_use]
    pub fn null() -> Self {
        Value::Null
    }

    /// Create a new boolean value
    #[must_use]
    pub fn bool(b: bool) -> Self {
        Value::Bool(b)
    }

    /// Create a new integer value
    #[must_use]
    pub fn int(i: i64) -> Self {
        Value::Int(i)
    }

    /// Create a new big integer value
    #[must_use]
    pub fn bigint(i: num_bigint::BigInt) -> Self {
        Value::BigInt(i)
    }

    /// Create a new float value
    #[must_use]
    pub fn float(f: f64) -> Self {
        Value::Float(f)
    }

    /// Create a new string value
    pub fn string(s: impl Into<String>) -> Self {
        Value::String(s.into())
    }

    /// Create a new array value
    #[must_use]
    pub fn array(arr: Vec<Value>) -> Self {
        Value::Array(arr)
    }

    /// Create a new object value
    #[must_use]
    pub fn object(obj: HashMap<String, Value>) -> Self {
        Value::Object(obj)
    }

    /// Create a new `DataFrame` value
    #[must_use]
    pub fn dataframe(df: polars::prelude::DataFrame) -> Self {
        Value::DataFrame(df)
    }

    /// Create a new `LazyFrame` value
    #[must_use]
    pub fn lazy_frame(lf: polars::prelude::LazyFrame) -> Self {
        Value::LazyFrame(Box::new(lf))
    }

    /// Create a new Series value
    #[must_use]
    pub fn series(s: polars::prelude::Series) -> Self {
        Value::Series(s)
    }
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "Null"),
            Value::Bool(b) => f.debug_tuple("Bool").field(b).finish(),
            Value::Int(i) => f.debug_tuple("Int").field(i).finish(),
            Value::BigInt(bi) => f.debug_tuple("BigInt").field(bi).finish(),
            Value::Float(fl) => f.debug_tuple("Float").field(fl).finish(),
            Value::String(s) => f.debug_tuple("String").field(s).finish(),
            Value::Array(arr) => f.debug_tuple("Array").field(arr).finish(),
            Value::Object(obj) => f.debug_tuple("Object").field(obj).finish(),
            Value::DataFrame(df) => f
                .debug_tuple("DataFrame")
                .field(&format!("{}x{} DataFrame", df.height(), df.width()))
                .finish(),
            Value::LazyFrame(_) => f.debug_tuple("LazyFrame").field(&"<LazyFrame>").finish(),
            Value::Series(s) => f
                .debug_tuple("Series")
                .field(&format!("{} Series[{}]", s.len(), s.dtype()))
                .finish(),
        }
    }
}

impl Value {
    /// Check if value is null
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if value is a `DataFrame`
    #[must_use]
    pub fn is_dataframe(&self) -> bool {
        matches!(self, Value::DataFrame(_))
    }

    /// Check if value is a `LazyFrame`
    #[must_use]
    pub fn is_lazy_frame(&self) -> bool {
        matches!(self, Value::LazyFrame(_))
    }

    /// Check if value is a Series
    #[must_use]
    pub fn is_series(&self) -> bool {
        matches!(self, Value::Series(_))
    }

    /// Get the type name of this value
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Int(_) => "integer",
            Value::BigInt(_) => "biginteger",
            Value::Float(_) => "float",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
            Value::DataFrame(_) => "dataframe",
            Value::LazyFrame(_) => "lazyframe",
            Value::Series(_) => "series",
        }
    }

    /// Convert to JSON value (for jaq compatibility)
    pub fn to_json(&self) -> crate::Result<JsonValue> {
        match self {
            Value::Null => Ok(JsonValue::Null),
            Value::Bool(b) => Ok(JsonValue::Bool(*b)),
            Value::Int(i) => Ok(JsonValue::Number(JsonNumber::from(*i))),
            Value::BigInt(bi) => Ok(JsonValue::String(bi.to_string())),
            Value::Float(f) => JsonNumber::from_f64(*f)
                .map(JsonValue::Number)
                .ok_or_else(|| crate::error::operation_error(format!("Invalid float: {f}"))),
            Value::String(s) => Ok(JsonValue::String(s.clone())),
            Value::Array(arr) => {
                let json_arr: crate::Result<Vec<JsonValue>> =
                    arr.iter().map(Value::to_json).collect();
                Ok(JsonValue::Array(json_arr?))
            }
            Value::Object(obj) => {
                let json_obj: crate::Result<serde_json::Map<String, JsonValue>> = obj
                    .iter()
                    .map(|(k, v)| v.to_json().map(|json_v| (k.clone(), json_v)))
                    .collect();
                Ok(JsonValue::Object(json_obj?))
            }
            Value::DataFrame(df) => {
                // Convert DataFrame to array of objects
                Self::dataframe_to_json_array(df)
            }
            Value::LazyFrame(lf) => {
                // Collect LazyFrame first, then convert
                let df = lf.clone().collect().map_err(|e| {
                    crate::error::operation_error(format!("LazyFrame collect error: {e}"))
                })?;
                Self::dataframe_to_json_array(&df)
            }
            Value::Series(s) => {
                // Convert Series to array
                Self::series_to_json_array(s)
            }
        }
    }

    /// Helper to convert `DataFrame` to JSON array
    #[cfg(not(target_arch = "wasm32"))]
    fn dataframe_to_json_array(df: &polars::prelude::DataFrame) -> crate::Result<JsonValue> {
        let height = df.height();
        let columns = df.get_column_names();
        let num_cols = columns.len();

        // Cache column references to avoid repeated lookups
        let series_vec: crate::Result<Vec<_>> = columns
            .iter()
            .map(|col_name| {
                df.column(col_name)
                    .map_err(|e| crate::error::operation_error(format!("Column access error: {e}")))
            })
            .collect();
        let series_vec = series_vec?;

        // Use parallel processing for large datasets (>10k rows)
        let rows: crate::Result<Vec<_>> = if height > 10_000 {
            (0..height)
                .into_par_iter()
                .map(|row_idx| {
                    let mut row_obj = serde_json::Map::with_capacity(num_cols);

                    for (col_idx, col_name) in columns.iter().enumerate() {
                        let series = &series_vec[col_idx];
                        let value =
                            Self::series_value_to_json(series.as_materialized_series(), row_idx)?;
                        row_obj.insert((*col_name).to_string(), value);
                    }

                    Ok(JsonValue::Object(row_obj))
                })
                .collect()
        } else {
            // Sequential for smaller datasets to avoid overhead
            (0..height)
                .map(|row_idx| {
                    let mut row_obj = serde_json::Map::with_capacity(num_cols);

                    for (col_idx, col_name) in columns.iter().enumerate() {
                        let series = &series_vec[col_idx];
                        let value =
                            Self::series_value_to_json(series.as_materialized_series(), row_idx)?;
                        row_obj.insert((*col_name).to_string(), value);
                    }

                    Ok(JsonValue::Object(row_obj))
                })
                .collect()
        };

        Ok(JsonValue::Array(rows?))
    }

    /// Helper to convert `DataFrame` to JSON array (WASM fallback)
    #[cfg(target_arch = "wasm32")]
    fn dataframe_to_json_array(df: &polars::prelude::DataFrame) -> crate::Result<JsonValue> {
        let height = df.height();
        let columns = df.get_column_names();
        let num_cols = columns.len();

        // Cache column references to avoid repeated lookups
        let series_vec: crate::Result<Vec<_>> = columns
            .iter()
            .map(|col_name| {
                df.column(col_name)
                    .map_err(|e| crate::error::operation_error(format!("Column access error: {e}")))
            })
            .collect();
        let series_vec = series_vec?;

        let rows: crate::Result<Vec<_>> = (0..height)
            .map(|row_idx| {
                let mut row_obj = serde_json::Map::with_capacity(num_cols);

                for (col_idx, col_name) in columns.iter().enumerate() {
                    let series = &series_vec[col_idx];
                    let value =
                        Self::series_value_to_json(series.as_materialized_series(), row_idx)?;
                    row_obj.insert((*col_name).to_string(), value);
                }

                Ok(JsonValue::Object(row_obj))
            })
            .collect();

        Ok(JsonValue::Array(rows?))
    }

    /// Helper to convert Series to JSON array
    fn series_to_json_array(series: &polars::prelude::Series) -> crate::Result<JsonValue> {
        let len = series.len();
        let mut values = Vec::with_capacity(len);

        for i in 0..len {
            let value = Self::series_value_to_json(series, i)?;
            values.push(value);
        }

        Ok(JsonValue::Array(values))
    }

    /// Helper to convert a single Series value to JSON
    fn series_value_to_json(
        series: &polars::prelude::Series,
        index: usize,
    ) -> crate::Result<JsonValue> {
        use polars::prelude::DataType;

        if series.is_null().get(index).unwrap_or(false) {
            return Ok(JsonValue::Null);
        }

        match series.dtype() {
            DataType::Boolean => {
                let val = series
                    .bool()
                    .map_err(|e| {
                        crate::error::operation_error(format!("Boolean access error: {e}"))
                    })?
                    .get(index);
                Ok(JsonValue::Bool(val.unwrap_or(false)))
            }
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let val = series
                    .i64()
                    .map_err(|e| crate::error::operation_error(format!("Int access error: {e}")))?
                    .get(index);
                Ok(JsonValue::Number(JsonNumber::from(val.unwrap_or(0))))
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                let val = series
                    .u64()
                    .map_err(|e| crate::error::operation_error(format!("UInt access error: {e}")))?
                    .get(index);
                Ok(JsonValue::Number(JsonNumber::from(val.unwrap_or(0))))
            }
            DataType::Float32 | DataType::Float64 => {
                let val = series
                    .f64()
                    .map_err(|e| crate::error::operation_error(format!("Float access error: {e}")))?
                    .get(index);
                JsonNumber::from_f64(val.unwrap_or(0.0))
                    .map(JsonValue::Number)
                    .ok_or_else(|| crate::error::operation_error("Invalid float value"))
            }
            DataType::String => {
                let val = series
                    .str()
                    .map_err(|e| {
                        crate::error::operation_error(format!("String access error: {e}"))
                    })?
                    .get(index);
                Ok(JsonValue::String(val.unwrap_or("").to_string()))
            }
            DataType::Binary => {
                let val = series
                    .binary()
                    .map_err(|e| {
                        crate::error::operation_error(format!("Binary access error: {e}"))
                    })?
                    .get(index);
                // Convert binary to base64 string for JSON representation
                if let Some(bytes) = val {
                    use base64::{engine::general_purpose, Engine as _};
                    Ok(JsonValue::String(general_purpose::STANDARD.encode(bytes)))
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::Date => {
                let val = series
                    .date()
                    .map_err(|e| crate::error::operation_error(format!("Date access error: {e}")))?
                    .phys
                    .get(index);
                if let Some(days) = val {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let actual_date = epoch + chrono::Duration::days(i64::from(days));
                    Ok(JsonValue::String(
                        actual_date.format("%Y-%m-%d").to_string(),
                    ))
                } else {
                    Ok(JsonValue::Null)
                }
            }
            DataType::Datetime(_, _) => {
                let val = series
                    .datetime()
                    .map_err(|e| {
                        crate::error::operation_error(format!("Datetime access error: {e}"))
                    })?
                    .phys
                    .get(index);
                if let Some(ns) = val {
                    let secs = ns / 1_000_000_000;
                    #[allow(clippy::cast_sign_loss)]
                    let nsecs = (ns % 1_000_000_000) as u32;
                    let dt = chrono::DateTime::from_timestamp(secs, nsecs)
                        .unwrap()
                        .naive_utc();
                    Ok(JsonValue::String(
                        dt.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
                    ))
                } else {
                    Ok(JsonValue::Null)
                }
            }
            _ => Err(crate::error::operation_error(format!(
                "Unsupported series type: {:?}",
                series.dtype()
            ))),
        }
    }

    /// Convert from JSON value
    pub fn from_json(json: JsonValue) -> Self {
        match json {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(b) => Value::Bool(b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Null // Fallback for invalid numbers
                }
            }
            JsonValue::String(s) => {
                // Try to parse as BigInt if it's a large number
                if let Ok(bi) = s.parse::<num_bigint::BigInt>() {
                    return Value::BigInt(bi);
                }
                Value::String(s)
            }
            JsonValue::Array(arr) => {
                let values = arr.into_iter().map(Value::from_json).collect();
                Value::Array(values)
            }
            JsonValue::Object(obj) => {
                let map = obj
                    .into_iter()
                    .map(|(k, v)| (k, Value::from_json(v)))
                    .collect();
                Value::Object(map)
            }
        }
    }

    /// Convert to `DataFrame` if possible
    pub fn to_dataframe(&self) -> crate::Result<polars::prelude::DataFrame> {
        match self {
            Value::DataFrame(df) => Ok(df.clone()),
            Value::LazyFrame(lf) => lf.clone().collect().map_err(|e| {
                crate::error::operation_error(format!("LazyFrame collect error: {e}"))
            }),
            Value::Array(arr) => {
                // Try to convert array of objects to DataFrame
                if arr.is_empty() {
                    return Ok(DataFrame::empty());
                }

                // Check if all elements are objects with the same keys
                let Value::Object(first_obj) = &arr[0] else {
                    return Err(crate::error::operation_error(
                        "Cannot convert array to DataFrame: not all elements are objects",
                    ));
                };

                let columns: Vec<String> = first_obj.keys().cloned().collect();
                let num_rows = arr.len();
                let num_cols = columns.len();

                // Directly allocate column vectors without HashMap intermediate
                // This reduces memory allocations and improves cache locality
                let mut column_data: Vec<Vec<AnyValue>> = Vec::with_capacity(num_cols);
                for _ in 0..num_cols {
                    column_data.push(Vec::with_capacity(num_rows));
                }

                // Single-pass row processing - columnar collection
                for value in arr {
                    match value {
                        Value::Object(obj) => {
                            for (col_idx, col) in columns.iter().enumerate() {
                                let val = obj.get(col).unwrap_or(&Value::Null);
                                let any_val = Self::value_to_any_value(val)?;
                                // Direct indexing - we know col_idx is valid
                                column_data[col_idx].push(any_val);
                            }
                        }
                        _ => {
                            return Err(crate::error::operation_error(
                                "Cannot convert array to DataFrame: not all elements are objects",
                            ));
                        }
                    }
                }

                // Create Series directly from column vectors
                let mut series_vec = Vec::with_capacity(num_cols);
                for (col_idx, col_name) in columns.into_iter().enumerate() {
                    let values = std::mem::take(&mut column_data[col_idx]);
                    let series = Series::new(col_name.into(), values);
                    series_vec.push(series.into());
                }

                DataFrame::new(series_vec).map_err(|e| {
                    crate::error::operation_error(format!("DataFrame creation error: {e}"))
                })
            }
            _ => Err(crate::error::operation_error(format!(
                "Cannot convert {} to DataFrame",
                self.type_name()
            ))),
        }
    }

    /// Helper to convert Value to `AnyValue` for Polars
    fn value_to_any_value(value: &Value) -> crate::Result<polars::prelude::AnyValue<'_>> {
        match value {
            Value::Null => Ok(AnyValue::Null),
            Value::Bool(b) => Ok(AnyValue::Boolean(*b)),
            Value::Int(i) => Ok(AnyValue::Int64(*i)),
            Value::BigInt(_) => Err(crate::error::operation_error(
                "Cannot convert BigInt to AnyValue",
            )),
            Value::Float(f) => Ok(AnyValue::Float64(*f)),
            Value::String(s) => Ok(AnyValue::String(s)),
            _ => Err(crate::error::operation_error(format!(
                "Cannot convert {} to AnyValue",
                value.type_name()
            ))),
        }
    }

    /// Get length for array-like values
    #[must_use]
    pub fn len(&self) -> Option<usize> {
        match self {
            Value::Array(arr) => Some(arr.len()),
            Value::String(s) => Some(s.len()),
            Value::DataFrame(df) => Some(df.height()),
            Value::Series(s) => Some(s.len()),
            _ => None,
        }
    }

    /// Check if value is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == Some(0)
    }

    /// Index into array-like values
    pub fn index(&self, idx: i64) -> crate::Result<Value> {
        match self {
            Value::Array(arr) => {
                #[allow(clippy::cast_possible_wrap)]
                let len = arr.len() as i64;
                let index = if idx < 0 { len + idx } else { idx };

                if index >= 0 && index < len {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    Ok(arr[index as usize].clone())
                } else {
                    Ok(Value::Null)
                }
            }
            Value::String(s) => {
                // Optimize string indexing by avoiding full char collection when possible
                // For ASCII strings, we can use byte indexing directly
                let byte_len = s.len();

                // Fast path for empty strings
                if byte_len == 0 {
                    return Ok(Value::Null);
                }

                // Check if string is ASCII for fast path
                if s.is_ascii() {
                    let bytes = s.as_bytes();
                    #[allow(clippy::cast_possible_wrap)]
                    let len = bytes.len() as i64;
                    let index = if idx < 0 { len + idx } else { idx };

                    if index >= 0 && index < len {
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        let ch = bytes[index as usize] as char;
                        Ok(Value::String(ch.to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    // Slow path for non-ASCII: need proper UTF-8 char handling
                    // Use iterator to avoid collecting all chars if we just need one
                    #[allow(clippy::cast_possible_wrap)]
                    let char_count = s.chars().count() as i64;
                    let index = if idx < 0 { char_count + idx } else { idx };

                    if index >= 0 && index < char_count {
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        if let Some(ch) = s.chars().nth(index as usize) {
                            Ok(Value::String(ch.to_string()))
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
            }
            Value::DataFrame(df) => {
                #[allow(clippy::cast_possible_wrap)]
                let len = df.height() as i64;
                let index = if idx < 0 { len + idx } else { idx };

                if index >= 0 && index < len {
                    // Return a row as an object
                    let mut row_obj = HashMap::new();
                    for col_name in df.get_column_names() {
                        let series = df.column(col_name).map_err(|e| {
                            crate::error::operation_error(format!("Column access error: {e}"))
                        })?;
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        let value = Self::series_value_to_json(
                            series.as_materialized_series(),
                            index as usize,
                        )?;
                        row_obj.insert(col_name.to_string(), Value::from_json(value));
                    }
                    Ok(Value::Object(row_obj))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(crate::error::operation_error(format!(
                "Cannot index into {}",
                self.type_name()
            ))),
        }
    }

    /// Get field from object-like values
    pub fn field(&self, key: &str) -> crate::Result<Value> {
        match self {
            Value::Null => Ok(Value::Null),
            Value::Object(obj) => Ok(obj.get(key).cloned().unwrap_or(Value::Null)),
            Value::Array(arr) => {
                let mut result = Vec::new();
                for item in arr {
                    result.push(item.field(key)?);
                }
                Ok(Value::Array(result))
            }
            Value::DataFrame(df) => {
                // Return the column as a Series
                match df.column(key) {
                    Ok(series) => Ok(Value::Series(series.as_materialized_series().clone())),
                    Err(_) => Ok(Value::Null),
                }
            }
            _ => Err(crate::error::operation_error(format!(
                "Cannot access field '{}' on {}",
                key,
                self.type_name()
            ))),
        }
    }

    /// Get nested field path from object-like values
    pub fn field_path(&self, fields: &[&str]) -> crate::Result<Value> {
        let mut result = self.clone();
        for &field in fields {
            result = result.field(field)?;
        }
        Ok(result)
    }
}

impl PartialEq<&Value> for Value {
    fn eq(&self, other: &&Value) -> bool {
        self == *other
    }
}

impl PartialEq<Value> for &Value {
    fn eq(&self, other: &Value) -> bool {
        *self == other
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::BigInt(a), Value::BigInt(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Object(a), Value::Object(b)) => a == b,
            // For DataFrames, no comparison implemented
            (Value::DataFrame(_), Value::DataFrame(_)) => false,
            // Series comparison (content comparison not implemented)
            (Value::Series(_), Value::Series(_)) => false,
            // Cross-type numeric comparisons
            #[allow(clippy::cast_precision_loss)]
            (Value::Int(a), Value::Float(b)) => *a as f64 == *b,
            #[allow(clippy::cast_precision_loss)]
            (Value::Float(a), Value::Int(b)) => *a == *b as f64,
            #[cfg(not(target_arch = "wasm32"))]
            (Value::Int(a), Value::BigInt(b)) => num_bigint::BigInt::from(*a) == *b,
            #[cfg(not(target_arch = "wasm32"))]
            (Value::BigInt(a), Value::Int(b)) => *a == num_bigint::BigInt::from(*b),
            // Note: BigInt vs Float comparison is not implemented for precision reasons
            _ => false,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::BigInt(bi) => write!(f, "{bi}"),
            Value::Float(fl) => write!(f, "{fl}"),
            Value::String(s) => write!(f, "\"{s}\""),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, item) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            Value::Object(obj) => {
                write!(f, "{{")?;
                for (i, (key, value)) in obj.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "\"{key}\": {value}")?;
                }
                write!(f, "}}")
            }
            Value::DataFrame(df) => {
                write!(
                    f,
                    "DataFrame({} rows × {} columns)",
                    df.height(),
                    df.width()
                )
            }
            Value::LazyFrame(_) => write!(f, "LazyFrame"),
            Value::Series(s) => write!(f, "Series[{}]({} values)", s.dtype(), s.len()),
        }
    }
}

impl serde::Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Value::Null => serializer.serialize_none(),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::Int(i) => serializer.serialize_i64(*i),
            Value::BigInt(bi) => bi.serialize(serializer),
            Value::Float(f) => serializer.serialize_f64(*f),
            Value::String(s) => serializer.serialize_str(s),
            Value::Array(arr) => {
                let mut seq = serializer.serialize_seq(Some(arr.len()))?;
                for item in arr {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Value::Object(obj) => {
                let mut map = serializer.serialize_map(Some(obj.len()))?;
                for (key, value) in obj {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
            Value::DataFrame(df) => {
                // Serialize DataFrame as an object with metadata
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "DataFrame")?;
                map.serialize_entry("shape", &vec![df.height(), df.width()])?;
                map.serialize_entry("columns", &df.get_column_names())?;
                map.end()
            }
            Value::LazyFrame(_) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "LazyFrame")?;
                map.end()
            }
            Value::Series(s) => {
                // Serialize Series as an object with metadata
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("type", "Series")?;
                map.serialize_entry("name", s.name())?;
                map.serialize_entry("dtype", &s.dtype().to_string())?;
                map.end()
            }
        }
    }
}

/// Convert `AnyValue` to `Value`
#[must_use]
#[allow(clippy::needless_pass_by_value)]
pub fn value_from_any_value(av: polars::prelude::AnyValue<'_>) -> Option<Value> {
    match av {
        AnyValue::Null => Some(Value::Null),
        AnyValue::Boolean(b) => Some(Value::Bool(b)),
        AnyValue::String(s) => Some(Value::String(s.to_string())),
        AnyValue::Int8(i) => Some(Value::Int(i64::from(i))),
        AnyValue::Int16(i) => Some(Value::Int(i64::from(i))),
        AnyValue::Int32(i) => Some(Value::Int(i64::from(i))),
        AnyValue::Int64(i) => Some(Value::Int(i)),
        AnyValue::UInt8(i) => Some(Value::Int(i64::from(i))),
        AnyValue::UInt16(i) => Some(Value::Int(i64::from(i))),
        AnyValue::UInt32(i) => Some(Value::Int(i64::from(i))),
        #[allow(clippy::cast_possible_wrap)]
        AnyValue::UInt64(i) => Some(Value::Int(i as i64)),
        AnyValue::Float32(f) => Some(Value::Float(f64::from(f))),
        AnyValue::Float64(f) => Some(Value::Float(f)),
        _ => None,
    }
}

/// Convert a `DataFrame` row to a `Value::Object`
pub fn df_row_to_value(df: &polars::prelude::DataFrame, row_idx: usize) -> crate::Result<Value> {
    let mut obj = HashMap::new();

    for col_name in df.get_column_names() {
        let series = df
            .column(col_name)
            .map_err(|e| crate::error::operation_error(format!("Failed to get column: {e}")))?;
        let value = series_value_at(series.as_materialized_series(), row_idx)?;
        obj.insert(col_name.to_string(), value);
    }

    Ok(Value::Object(obj))
}

/// Helper to get value at index from Series
fn series_value_at(series: &polars::prelude::Series, idx: usize) -> crate::Result<Value> {
    if idx >= series.len() {
        return Ok(Value::Null);
    }

    if series.is_null().get(idx).unwrap_or(false) {
        return Ok(Value::Null);
    }

    match series.dtype() {
        DataType::Boolean => {
            let val = series
                .bool()
                .map_err(|e| crate::error::operation_error(format!("Boolean access error: {e}")))?
                .get(idx);
            Ok(Value::Bool(val.unwrap_or(false)))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let val = series
                .i64()
                .map_err(|e| crate::error::operation_error(format!("Int access error: {e}")))?
                .get(idx);
            Ok(Value::Int(val.unwrap_or(0)))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let val = series
                .u64()
                .map_err(|e| crate::error::operation_error(format!("UInt access error: {e}")))?
                .get(idx);
            #[allow(clippy::cast_possible_wrap)]
            Ok(Value::Int(val.unwrap_or(0) as i64))
        }
        DataType::Float32 | DataType::Float64 => {
            let val = series
                .f64()
                .map_err(|e| crate::error::operation_error(format!("Float access error: {e}")))?
                .get(idx);
            Ok(Value::Float(val.unwrap_or(0.0)))
        }
        DataType::String => {
            let val = series
                .str()
                .map_err(|e| crate::error::operation_error(format!("String access error: {e}")))?
                .get(idx);
            Ok(Value::String(val.unwrap_or("").to_string()))
        }
        DataType::Date => {
            let val = series
                .date()
                .map_err(|e| crate::error::operation_error(format!("Date access error: {e}")))?
                .phys
                .get(idx);
            if let Some(days) = val {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let actual_date = epoch + Duration::days(i64::from(days));
                Ok(Value::String(actual_date.format("%Y-%m-%d").to_string()))
            } else {
                Ok(Value::Null)
            }
        }
        DataType::Datetime(_, _) => {
            let val = series
                .datetime()
                .map_err(|e| crate::error::operation_error(format!("Datetime access error: {e}")))?
                .phys
                .get(idx);
            if let Some(ns) = val {
                let secs = ns / 1_000_000_000;
                #[allow(clippy::cast_sign_loss)]
                let nsecs = (ns % 1_000_000_000) as u32;
                let dt = DateTime::from_timestamp(secs, nsecs).unwrap().naive_utc();
                Ok(Value::String(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string()))
            } else {
                Ok(Value::Null)
            }
        }
        _ => Ok(Value::Null), // For unsupported types, return null
    }
}

/// Check if a value is truthy (non-null, non-empty, non-zero)
#[must_use]
pub fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Int(i) => *i != 0,
        Value::BigInt(bi) => !bi.is_zero(),
        Value::Float(f) => *f != 0.0 && !f.is_nan(),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
        Value::DataFrame(df) => df.height() > 0,
        Value::Series(s) => !s.is_empty(),
        Value::LazyFrame(_) => true, // Assume lazy frames are truthy if present
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;

    use serde_json::json;

    #[test]
    fn test_value_creation() {
        assert_eq!(Value::null(), Value::Null);
        assert_eq!(Value::bool(true), Value::Bool(true));
        assert_eq!(Value::bool(false), Value::Bool(false));
        assert_eq!(Value::int(42), Value::Int(42));
        assert_eq!(Value::int(-1), Value::Int(-1));
        assert_eq!(
            Value::bigint(BigInt::from(123456789012345678901234567890i128)),
            Value::BigInt(BigInt::from(123456789012345678901234567890i128))
        );
        assert_eq!(
            Value::float(std::f64::consts::PI),
            Value::Float(std::f64::consts::PI)
        );
        assert_eq!(Value::float(-2.5), Value::Float(-2.5));
        assert_eq!(Value::string("hello"), Value::String("hello".to_string()));
        assert_eq!(Value::string(""), Value::String("".to_string()));

        let arr = vec![Value::int(1), Value::int(2)];
        assert_eq!(Value::array(arr.clone()), Value::Array(arr));

        let obj = HashMap::from([("key".to_string(), Value::string("value"))]);
        assert_eq!(Value::object(obj.clone()), Value::Object(obj));

        let df = DataFrame::new(vec![Series::new("a".into(), vec![1, 2, 3]).into()]).unwrap();
        let df_val = Value::dataframe(df.clone());
        assert!(df_val.is_dataframe());

        let lf = df.clone().lazy();
        let lf_val = Value::lazy_frame(lf.clone());
        assert!(lf_val.is_lazy_frame());

        let series = Series::new("test".into(), vec![1, 2, 3]);
        let series_val = Value::series(series.clone());
        assert!(series_val.is_series());
    }

    #[test]
    fn test_value_is_methods() {
        let df = DataFrame::new(vec![Series::new("a".into(), vec![1, 2, 3]).into()]).unwrap();
        let lf = df.clone().lazy();
        let series = Series::new("test".into(), vec![1, 2, 3]);

        assert!(Value::Null.is_null());
        assert!(!Value::int(1).is_null());

        assert!(Value::dataframe(df.clone()).is_dataframe());
        assert!(!Value::int(1).is_dataframe());

        assert!(Value::lazy_frame(lf.clone()).is_lazy_frame());
        assert!(!Value::int(1).is_lazy_frame());

        assert!(Value::series(series.clone()).is_series());
        assert!(!Value::int(1).is_series());

        assert!(is_truthy(&Value::Bool(true)));
        assert!(!is_truthy(&Value::Bool(false)));
        assert!(is_truthy(&Value::int(1)));
        assert!(!is_truthy(&Value::int(0)));
        assert!(is_truthy(&Value::string("hello")));
        assert!(!is_truthy(&Value::string("")));
        assert!(is_truthy(&Value::array(vec![Value::int(1)])));
        assert!(!is_truthy(&Value::array(vec![])));
        assert!(is_truthy(&Value::object(HashMap::from([(
            "k".to_string(),
            Value::int(1)
        )]))));
        assert!(!is_truthy(&Value::object(HashMap::new())));
        assert!(is_truthy(&Value::dataframe(df)));
        assert!(is_truthy(&Value::series(series)));
        assert!(is_truthy(&Value::lazy_frame(lf)));
    }

    #[test]
    fn test_value_len_and_empty() {
        let df = DataFrame::new(vec![Series::new("a".into(), vec![1, 2, 3]).into()]).unwrap();
        let empty_df = DataFrame::empty();
        let series = Series::new("test".into(), vec![1, 2, 3]);
        let empty_series = Series::new("empty".into(), Vec::<i32>::new());
        let lf = df.clone().lazy();

        assert_eq!(Value::Null.len(), None);
        assert_eq!(Value::int(1).len(), None);
        assert_eq!(Value::string("hello").len(), Some(5));
        assert_eq!(Value::string("").len(), Some(0));
        assert_eq!(
            Value::array(vec![Value::int(1), Value::int(2)]).len(),
            Some(2)
        );
        assert_eq!(Value::array(vec![]).len(), Some(0));
        assert_eq!(Value::dataframe(df.clone()).len(), Some(3));
        assert_eq!(Value::dataframe(empty_df.clone()).len(), Some(0));
        assert_eq!(Value::series(series.clone()).len(), Some(3));
        assert_eq!(Value::series(empty_series.clone()).len(), Some(0));
        assert_eq!(Value::lazy_frame(lf.clone()).len(), None);

        assert!(Value::string("").is_empty());
        assert!(Value::array(vec![]).is_empty());
        assert!(Value::dataframe(empty_df).is_empty());
        assert!(Value::series(empty_series).is_empty());
        assert!(!Value::string("a").is_empty());
        assert!(!Value::array(vec![Value::int(1)]).is_empty());
        assert!(!Value::dataframe(df).is_empty());
        assert!(!Value::series(series).is_empty());
        assert!(!Value::lazy_frame(lf).is_empty());
        assert!(!Value::Null.is_empty()); // None len means not empty in this context?
    }

    #[test]
    fn test_json_conversion_primitives() {
        // Null
        let json = Value::Null.to_json().unwrap();
        assert_eq!(json, JsonValue::Null);
        assert_eq!(Value::from_json(json), Value::Null);

        // Bool
        let json = Value::bool(true).to_json().unwrap();
        assert_eq!(json, JsonValue::Bool(true));
        assert_eq!(Value::from_json(json), Value::bool(true));

        // Int
        let json = Value::int(42).to_json().unwrap();
        assert_eq!(json, JsonValue::Number(JsonNumber::from(42)));
        assert_eq!(Value::from_json(json), Value::int(42));

        // Float
        let json = Value::float(std::f64::consts::PI).to_json().unwrap();
        assert_eq!(
            json,
            JsonValue::Number(JsonNumber::from_f64(std::f64::consts::PI).unwrap())
        );
        assert_eq!(Value::from_json(json), Value::float(std::f64::consts::PI));

        // String
        let json = Value::string("hello").to_json().unwrap();
        assert_eq!(json, JsonValue::String("hello".to_string()));
        assert_eq!(Value::from_json(json), Value::string("hello"));

        // BigInt
        let big = BigInt::from(12345678901234567890u64);
        let json = Value::bigint(big.clone()).to_json().unwrap();
        assert_eq!(json, JsonValue::String(big.to_string()));
        assert_eq!(Value::from_json(json), Value::bigint(big));
    }

    #[test]
    fn test_json_conversion_complex() {
        // Array
        let arr = Value::array(vec![Value::int(1), Value::string("two"), Value::bool(true)]);
        let json = arr.to_json().unwrap();
        let expected = json!([1, "two", true]);
        assert_eq!(json, expected);
        assert_eq!(Value::from_json(json), arr);

        // Object
        let obj = Value::object(HashMap::from([
            ("name".to_string(), Value::string("Alice")),
            ("age".to_string(), Value::int(30)),
            ("active".to_string(), Value::bool(true)),
        ]));
        let json = obj.to_json().unwrap();
        let expected = json!({
            "name": "Alice",
            "age": 30,
            "active": true
        });
        assert_eq!(json, expected);
        assert_eq!(Value::from_json(json), obj);

        // Nested structures
        let nested = Value::object(HashMap::from([
            (
                "data".to_string(),
                Value::array(vec![Value::int(1), Value::int(2)]),
            ),
            (
                "meta".to_string(),
                Value::object(HashMap::from([("count".to_string(), Value::int(2))])),
            ),
        ]));
        let json = nested.to_json().unwrap();
        let expected = json!({
            "data": [1, 2],
            "meta": {"count": 2}
        });
        assert_eq!(json, expected);
        assert_eq!(Value::from_json(json), nested);
    }

    #[test]
    fn test_json_conversion_polars() {
        // DataFrame
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();
        let value = Value::dataframe(df.clone());
        let json = value.to_json().unwrap();
        let expected = json!([
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ]);
        assert_eq!(json, expected);

        // LazyFrame
        let lf = df.lazy();
        let value = Value::lazy_frame(lf);
        let json = value.to_json().unwrap();
        assert_eq!(json, expected); // Same as DataFrame after collect

        // Series
        let series = Series::new("ages".into(), vec![30i64, 25i64]);
        let value = Value::series(series);
        let json = value.to_json().unwrap();
        let expected_series = json!([30, 25]);
        assert_eq!(json, expected_series);
    }

    #[test]
    fn test_json_conversion_edge_cases() {
        // Invalid float (NaN)
        let nan_val = Value::float(f64::NAN);
        assert!(nan_val.to_json().is_err());

        // Very large number as string -> BigInt
        let json = JsonValue::String("999999999999999999999999999999".to_string());
        let value = Value::from_json(json);
        match value {
            Value::BigInt(_) => {}
            _ => panic!("Expected BigInt"),
        }

        // Invalid number string -> String
        let json = JsonValue::String("not_a_number".to_string());
        let value = Value::from_json(json);
        assert_eq!(value, Value::string("not_a_number"));
    }

    #[test]
    fn test_indexing_array() {
        let arr = Value::array(vec![Value::int(1), Value::int(2), Value::int(3)]);

        assert_eq!(arr.index(0).unwrap(), Value::int(1));
        assert_eq!(arr.index(1).unwrap(), Value::int(2));
        assert_eq!(arr.index(2).unwrap(), Value::int(3));
        assert_eq!(arr.index(-1).unwrap(), Value::int(3));
        assert_eq!(arr.index(-2).unwrap(), Value::int(2));
        assert_eq!(arr.index(-3).unwrap(), Value::int(1));
        assert_eq!(arr.index(10).unwrap(), Value::Null);
        assert_eq!(arr.index(-10).unwrap(), Value::Null);
    }

    #[test]
    fn test_indexing_string() {
        let s = Value::string("hello");

        assert_eq!(s.index(0).unwrap(), Value::string("h"));
        assert_eq!(s.index(1).unwrap(), Value::string("e"));
        assert_eq!(s.index(4).unwrap(), Value::string("o"));
        assert_eq!(s.index(-1).unwrap(), Value::string("o"));
        assert_eq!(s.index(-2).unwrap(), Value::string("l"));
        assert_eq!(s.index(10).unwrap(), Value::Null);
        assert_eq!(s.index(-10).unwrap(), Value::Null);
    }

    #[test]
    fn test_indexing_dataframe() {
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();
        let val = Value::dataframe(df);

        let row = val.index(0).unwrap();
        match row {
            Value::Object(obj) => {
                assert_eq!(obj.get("name"), Some(&Value::string("Alice")));
                assert_eq!(obj.get("age"), Some(&Value::int(30)));
            }
            _ => panic!("Expected object"),
        }

        let row1 = val.index(1).unwrap();
        match row1 {
            Value::Object(obj) => {
                assert_eq!(obj.get("name"), Some(&Value::string("Bob")));
                assert_eq!(obj.get("age"), Some(&Value::int(25)));
            }
            _ => panic!("Expected object"),
        }

        let null_row = val.index(10).unwrap();
        assert_eq!(null_row, Value::Null);
    }

    #[test]
    fn test_indexing_invalid() {
        let obj = Value::object(HashMap::new());
        assert!(obj.index(0).is_err());

        let null_val = Value::Null;
        assert!(null_val.index(0).is_err());
    }

    #[test]
    fn test_field_access() {
        let obj = Value::object(HashMap::from([
            ("name".to_string(), Value::string("Bob")),
            ("age".to_string(), Value::int(25)),
            (
                "nested".to_string(),
                Value::object(HashMap::from([("inner".to_string(), Value::bool(true))])),
            ),
        ]));

        assert_eq!(obj.field("name").unwrap(), Value::string("Bob"));
        assert_eq!(obj.field("age").unwrap(), Value::int(25));
        assert_eq!(obj.field("missing").unwrap(), Value::Null);

        // Nested field access
        assert_eq!(
            obj.field_path(&["nested", "inner"]).unwrap(),
            Value::bool(true)
        );
        assert_eq!(obj.field_path(&["nested", "missing"]).unwrap(), Value::Null);
        assert_eq!(obj.field_path(&["missing", "field"]).unwrap(), Value::Null);
    }

    #[test]
    fn test_field_access_array() {
        let arr = Value::array(vec![
            Value::object(HashMap::from([(
                "name".to_string(),
                Value::string("Alice"),
            )])),
            Value::object(HashMap::from([("name".to_string(), Value::string("Bob"))])),
        ]);

        let result = arr.field("name").unwrap();
        match result {
            Value::Array(names) => {
                assert_eq!(names.len(), 2);
                assert_eq!(names[0], Value::string("Alice"));
                assert_eq!(names[1], Value::string("Bob"));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_field_access_dataframe() {
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();
        let val = Value::dataframe(df);

        let name_series = val.field("name").unwrap();
        match name_series {
            Value::Series(s) => {
                assert_eq!(s.name(), "name");
            }
            _ => panic!("Expected series"),
        }

        let missing = val.field("missing").unwrap();
        assert_eq!(missing, Value::Null);
    }

    #[test]
    fn test_field_access_null() {
        let null_val = Value::Null;
        assert_eq!(null_val.field("any").unwrap(), Value::Null);
    }

    #[test]
    fn test_type_names() {
        assert_eq!(Value::Null.type_name(), "null");
        assert_eq!(Value::Bool(true).type_name(), "boolean");
        assert_eq!(Value::Int(42).type_name(), "integer");
        assert_eq!(Value::BigInt(BigInt::from(42)).type_name(), "biginteger");
        assert_eq!(Value::Float(std::f64::consts::PI).type_name(), "float");
        assert_eq!(Value::String("test".to_string()).type_name(), "string");
        assert_eq!(Value::Array(vec![]).type_name(), "array");
        assert_eq!(Value::Object(HashMap::new()).type_name(), "object");
    }

    #[test]
    fn test_equality() {
        // Same types
        assert_eq!(Value::int(42), Value::int(42));
        assert_eq!(
            Value::float(std::f64::consts::PI),
            Value::float(std::f64::consts::PI)
        );
        assert_eq!(
            Value::bigint(BigInt::from(42)),
            Value::bigint(BigInt::from(42))
        );
        assert_eq!(Value::string("hello"), Value::string("hello"));
        assert_eq!(Value::bool(true), Value::bool(true));
        assert_eq!(Value::Null, Value::Null);

        // Cross-type numeric
        assert_eq!(Value::int(42), Value::float(42.0));
        assert_eq!(Value::float(42.0), Value::int(42));
        assert_eq!(Value::int(42), Value::bigint(BigInt::from(42)));
        assert_eq!(Value::bigint(BigInt::from(42)), Value::int(42));

        // Arrays and objects
        let arr1 = Value::array(vec![Value::int(1), Value::int(2)]);
        let arr2 = Value::array(vec![Value::int(1), Value::int(2)]);
        assert_eq!(arr1, arr2);

        let obj1 = Value::object(HashMap::from([("a".to_string(), Value::int(1))]));
        let obj2 = Value::object(HashMap::from([("a".to_string(), Value::int(1))]));
        assert_eq!(obj1, obj2);

        // Inequalities
        assert_ne!(Value::int(1), Value::int(2));
        assert_ne!(Value::int(1), Value::float(1.1));
        assert_ne!(Value::string("a"), Value::string("b"));
        assert_ne!(Value::bool(true), Value::bool(false));

        // DataFrame/Series comparison (always false)
        let df = DataFrame::new(vec![Series::new("a".into(), vec![1, 2, 3]).into()]).unwrap();
        assert_ne!(Value::dataframe(df.clone()), Value::dataframe(df));
        let series = Series::new("test".into(), vec![1, 2, 3]);
        assert_ne!(Value::series(series.clone()), Value::series(series));

        // BigInt vs Float (not implemented, so false)
        assert_ne!(Value::bigint(BigInt::from(42)), Value::float(42.0));
    }

    #[test]
    fn test_serde() {
        // Primitive
        let val = Value::int(42);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, "42");

        // Array
        let arr = Value::array(vec![Value::int(1), Value::string("two")]);
        let json = serde_json::to_string(&arr).unwrap();
        assert_eq!(json, "[1,\"two\"]");

        // Object
        let obj = Value::object(HashMap::from([
            ("a".to_string(), Value::int(1)),
            ("b".to_string(), Value::string("x")),
        ]));
        let json = serde_json::to_string(&obj).unwrap();
        // Order may vary, so check contains
        assert!(json.contains("\"a\":1"));
        assert!(json.contains("\"b\":\"x\""));

        // DataFrame
        let df = DataFrame::new(vec![Series::new("name".into(), vec!["Alice"]).into()]).unwrap();
        let val = Value::dataframe(df);
        let json = serde_json::to_string(&val).unwrap();
        assert!(json.contains("\"type\":\"DataFrame\""));
        assert!(json.contains("\"shape\":[1,1]"));
        assert!(json.contains("\"columns\":[\"name\"]"));
    }

    #[test]
    fn test_debug() {
        assert!(format!("{:?}", Value::Null).contains("Null"));
        assert!(format!("{:?}", Value::int(42)).contains("Int(42)"));
        assert!(format!("{:?}", Value::string("hello")).contains("String(\"hello\")"));
        assert!(format!("{:?}", Value::array(vec![Value::int(1)])).contains("Array([Int(1)]"));
        assert!(format!("{:?}", Value::object(HashMap::new())).contains("Object({})"));
        assert!(format!("{:?}", Value::dataframe(DataFrame::empty())).contains("DataFrame"));
        assert!(
            format!("{:?}", Value::series(Series::new("test".into(), vec![1]))).contains("Series")
        );
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_display() {
        assert_eq!(format!("{}", Value::Null), "null");
        assert_eq!(format!("{}", Value::bool(true)), "true");
        assert_eq!(format!("{}", Value::int(42)), "42");
        assert_eq!(format!("{}", Value::bigint(BigInt::from(123))), "123");
        assert_eq!(format!("{}", Value::float(3.14)), "3.14");
        assert_eq!(format!("{}", Value::string("hello")), "\"hello\"");

        let arr = Value::array(vec![Value::int(1), Value::string("two")]);
        assert_eq!(format!("{}", arr), "[1, \"two\"]");

        let obj = Value::object(HashMap::from([
            ("a".to_string(), Value::int(1)),
            ("b".to_string(), Value::string("x")),
        ]));
        let display = format!("{}", obj);
        assert!(display.contains("\"a\": 1"));
        assert!(display.contains("\"b\": \"x\""));
    }

    #[test]
    fn test_to_dataframe() {
        // Array of objects
        let data = Value::array(vec![
            Value::object(HashMap::from([
                ("name".to_string(), Value::string("Alice")),
                ("age".to_string(), Value::int(30)),
            ])),
            Value::object(HashMap::from([
                ("name".to_string(), Value::string("Bob")),
                ("age".to_string(), Value::int(25)),
            ])),
        ]);

        let df = data.to_dataframe().unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
        assert!(df
            .get_column_names()
            .contains(&&polars::datatypes::PlSmallStr::from("name")));
        assert!(df
            .get_column_names()
            .contains(&&polars::datatypes::PlSmallStr::from("age")));

        // Empty array
        let empty = Value::array(vec![]);
        let df = empty.to_dataframe().unwrap();
        assert_eq!(df.height(), 0);

        // Invalid array (mixed types)
        let invalid = Value::array(vec![Value::int(1), Value::string("not object")]);
        assert!(invalid.to_dataframe().is_err());

        // Array with BigInt (unsupported in AnyValue)
        let data = Value::array(vec![Value::object(HashMap::from([
            ("name".to_string(), Value::string("Alice")),
            ("big".to_string(), Value::bigint(BigInt::from(123))),
        ]))]);
        assert!(data.to_dataframe().is_err());

        // DataFrame input
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();
        let val = Value::dataframe(df.clone());
        let df2 = val.to_dataframe().unwrap();
        assert_eq!(df2.height(), 2);
        assert_eq!(df2.width(), 2);

        // LazyFrame input
        let lf = df.lazy();
        let val = Value::lazy_frame(lf);
        let df3 = val.to_dataframe().unwrap();
        assert_eq!(df3.height(), 2);
        assert_eq!(df3.width(), 2);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_value_from_any_value() {
        use polars::datatypes::AnyValue;

        assert_eq!(value_from_any_value(AnyValue::Null), Some(Value::Null));
        assert_eq!(
            value_from_any_value(AnyValue::Boolean(true)),
            Some(Value::bool(true))
        );
        assert_eq!(
            value_from_any_value(AnyValue::Int64(42)),
            Some(Value::int(42))
        );
        assert_eq!(
            value_from_any_value(AnyValue::Float64(3.14)),
            Some(Value::float(3.14))
        );
        assert_eq!(
            value_from_any_value(AnyValue::String("hello")),
            Some(Value::string("hello"))
        );

        // Unsupported types return None
        assert_eq!(value_from_any_value(AnyValue::Date(0)), None);
    }

    #[test]
    fn test_df_row_to_value() {
        let df = DataFrame::new(vec![
            Series::new("name".into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();

        let row = df_row_to_value(&df, 0).unwrap();
        match row {
            Value::Object(obj) => {
                assert_eq!(obj.get("name"), Some(&Value::string("Alice")));
                assert_eq!(obj.get("age"), Some(&Value::int(30)));
            }
            _ => panic!("Expected object"),
        }

        // Out of bounds
        let out_of_bounds = df_row_to_value(&df, 10).unwrap();
        match out_of_bounds {
            Value::Object(obj) => {
                assert_eq!(obj.get("name"), Some(&Value::Null));
                assert_eq!(obj.get("age"), Some(&Value::Null));
            }
            _ => panic!("Expected object"),
        }
    }

    #[test]
    fn test_series_value_at() {
        let series = Series::new("test".into(), vec![Some(1i64), None, Some(3i64)]);

        assert_eq!(series_value_at(&series, 0).unwrap(), Value::int(1));
        assert_eq!(series_value_at(&series, 1).unwrap(), Value::Null);
        assert_eq!(series_value_at(&series, 2).unwrap(), Value::int(3));

        // Out of bounds
        assert_eq!(series_value_at(&series, 10).unwrap(), Value::Null);
    }
}
