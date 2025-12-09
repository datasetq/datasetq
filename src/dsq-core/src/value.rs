use crate::error::{Error, Result, TypeError};
use polars::prelude::*;
use serde_json::{Number as JsonNumber, Value as JsonValue};
use std::collections::HashMap;

impl Value {
    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if value is a DataFrame
    pub fn is_dataframe(&self) -> bool {
        matches!(self, Value::DataFrame(_))
    }

    /// Check if value is a LazyFrame
    pub fn is_lazy_frame(&self) -> bool {
        matches!(self, Value::LazyFrame(_))
    }

    /// Check if value is a Series
    pub fn is_series(&self) -> bool {
        matches!(self, Value::Series(_))
    }

    /// Get the type name of this value
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Int(_) => "integer",
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
    pub fn to_json(&self) -> Result<JsonValue> {
        match self {
            Value::Null => Ok(JsonValue::Null),
            Value::Bool(b) => Ok(JsonValue::Bool(*b)),
            Value::Int(i) => Ok(JsonValue::Number(JsonNumber::from(*i))),
            Value::Float(f) => JsonNumber::from_f64(*f)
                .map(JsonValue::Number)
                .ok_or_else(|| TypeError::OutOfRange(format!("Invalid float: {}", f)).into()),
            Value::String(s) => Ok(JsonValue::String(s.clone())),
            Value::Array(arr) => {
                let mut json_arr = Vec::with_capacity(arr.len());
                for v in arr {
                    json_arr.push(v.to_json()?);
                }
                Ok(JsonValue::Array(json_arr))
            }
            Value::Object(obj) => {
                let json_obj: Result<serde_json::Map<String, JsonValue>> = obj
                    .iter()
                    .map(|(k, v)| v.to_json().map(|json_v| (k.clone(), json_v)))
                    .collect();
                Ok(JsonValue::Object(json_obj?))
            }
            Value::DataFrame(df) => {
                // Convert DataFrame to array of objects
                self.dataframe_to_json_array(df)
            }
            Value::LazyFrame(lf) => {
                // Collect LazyFrame first, then convert
                let df = lf.clone().collect().map_err(Error::from)?;
                self.dataframe_to_json_array(&df)
            }
            Value::Series(s) => {
                // Convert Series to array
                self.series_to_json_array(s)
            }
        }
    }

    /// Helper to convert DataFrame to JSON array
    fn dataframe_to_json_array(&self, df: &DataFrame) -> Result<JsonValue> {
        let mut rows = Vec::new();
        let columns = df.get_column_names();

        // Cache column series to avoid repeated lookups
        let column_series: Vec<&Series> = columns
            .iter()
            .map(|col_name| df.column(col_name).map_err(Error::from))
            .collect::<Result<Vec<_>>>()?;

        for row_idx in 0..df.height() {
            let mut row_obj = serde_json::Map::with_capacity(columns.len());

            for (col_name, series) in columns.iter().zip(&column_series) {
                let value = self.series_value_to_json(series, row_idx)?;
                row_obj.insert(col_name.to_string(), value);
            }

            rows.push(JsonValue::Object(row_obj));
        }

        Ok(JsonValue::Array(rows))
    }

    /// Helper to convert Series to JSON array
    fn series_to_json_array(&self, series: &Series) -> Result<JsonValue> {
        let mut values = Vec::new();

        for i in 0..series.len() {
            let value = self.series_value_to_json(series, i)?;
            values.push(value);
        }

        Ok(JsonValue::Array(values))
    }

    /// Helper to convert a single Series value to JSON
    fn series_value_to_json(&self, series: &Series, index: usize) -> Result<JsonValue> {
        use polars::datatypes::*;

        if series.is_null().get(index).unwrap_or(false) {
            return Ok(JsonValue::Null);
        }

        match series.dtype() {
            DataType::Boolean => {
                let val = series.bool().map_err(Error::from)?.get(index);
                Ok(JsonValue::Bool(val.unwrap_or(false)))
            }
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let val = series.i64().map_err(Error::from)?.get(index);
                Ok(JsonValue::Number(JsonNumber::from(val.unwrap_or(0))))
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                let val = series.u64().map_err(Error::from)?.get(index);
                Ok(JsonValue::Number(JsonNumber::from(val.unwrap_or(0))))
            }
            DataType::Float32 | DataType::Float64 => {
                let val = series.f64().map_err(Error::from)?.get(index);
                JsonNumber::from_f64(val.unwrap_or(0.0))
                    .map(JsonValue::Number)
                    .ok_or_else(|| TypeError::OutOfRange("Invalid float value".to_string()).into())
            }
            DataType::String => {
                let val = series.str().map_err(Error::from)?.get(index);
                Ok(JsonValue::String(val.unwrap_or("").to_string()))
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
            _ => Err(TypeError::UnsupportedOperation {
                operation: "to_json".to_string(),
                typ: format!("{:?}", series.dtype()),
            }
            .into()),
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
            JsonValue::String(s) => Value::String(s),
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

    /// Convert to DataFrame if possible
    pub fn to_dataframe(&self) -> Result<DataFrame> {
        match self {
            Value::DataFrame(df) => Ok(df.clone()),
            Value::LazyFrame(lf) => lf.clone().collect().map_err(Error::from),
            Value::Array(arr) => {
                // Try to convert array of objects to DataFrame
                if arr.is_empty() {
                    return Ok(DataFrame::empty());
                }

                // Check if all elements are objects with the same keys
                let first_obj = match &arr[0] {
                    Value::Object(obj) => obj,
                    _ => {
                        return Err(TypeError::InvalidConversion {
                            from: "array".to_string(),
                            to: "dataframe".to_string(),
                        }
                        .into());
                    }
                };

                let columns: Vec<String> = first_obj.keys().cloned().collect();
                let mut series_map: HashMap<String, Vec<AnyValue>> = HashMap::new();

                // Initialize series vectors
                for col in &columns {
                    series_map.insert(col.clone(), Vec::new());
                }

                // Process each row
                for value in arr {
                    match value {
                        Value::Object(obj) => {
                            for col in &columns {
                                let val = obj.get(col).unwrap_or(&Value::Null);
                                let any_val = self.value_to_any_value(val)?;
                                series_map.get_mut(col).unwrap().push(any_val);
                            }
                        }
                        _ => {
                            return Err(TypeError::InvalidConversion {
                                from: "array".to_string(),
                                to: "dataframe".to_string(),
                            }
                            .into());
                        }
                    }
                }

                // Create Series from vectors
                let mut series_vec = Vec::new();
                for col in columns {
                    let values = series_map.remove(&col).unwrap();
                    let series = Series::new(col.as_str().into(), values);
                    series_vec.push(series);
                }

                let columns: Vec<_> = series_vec.into_iter().map(|s| s.into()).collect();
                DataFrame::new(columns).map_err(Error::from)
            }
            _ => Err(TypeError::InvalidConversion {
                from: self.type_name().to_string(),
                to: "dataframe".to_string(),
            }
            .into()),
        }
    }

    /// Helper to convert Value to AnyValue for Polars
    fn value_to_any_value<'a>(&self, value: &'a Value) -> Result<AnyValue<'a>> {
        match value {
            Value::Null => Ok(AnyValue::Null),
            Value::Bool(b) => Ok(AnyValue::Boolean(*b)),
            Value::Int(i) => Ok(AnyValue::Int64(*i)),
            Value::BigInt(_) => Err(TypeError::UnsupportedOperation {
                operation: "to_any_value".to_string(),
                typ: "biginteger".to_string(),
            }
            .into()),
            Value::Float(f) => Ok(AnyValue::Float64(*f)),
            Value::String(s) => Ok(AnyValue::String(s)),
            _ => Err(TypeError::UnsupportedOperation {
                operation: "to_any_value".to_string(),
                typ: value.type_name().to_string(),
            }
            .into()),
        }
    }

    /// Get length for array-like values
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
    pub fn is_empty(&self) -> bool {
        self.len().map_or(false, |len| len == 0)
    }

    /// Index into array-like values
    pub fn index(&self, idx: i64) -> Result<Value> {
        match self {
            Value::Array(arr) => {
                let len = arr.len() as i64;
                let index = if idx < 0 { len + idx } else { idx };

                if index >= 0 && index < len {
                    Ok(arr[index as usize].clone())
                } else {
                    Ok(Value::Null)
                }
            }
            Value::String(s) => {
                let chars: Vec<char> = s.chars().collect();
                let len = chars.len() as i64;
                let index = if idx < 0 { len + idx } else { idx };

                if index >= 0 && index < len {
                    Ok(Value::String(chars[index as usize].to_string()))
                } else {
                    Ok(Value::Null)
                }
            }
            Value::DataFrame(df) => {
                let len = df.height() as i64;
                let index = if idx < 0 { len + idx } else { idx };

                if index >= 0 && index < len {
                    // Return a row as an object
                    let mut row_obj = HashMap::new();
                    for col_name in df.get_column_names() {
                        let series = df.column(col_name).map_err(Error::from)?;
                        let value = self.series_value_to_json(series, index as usize)?;
                        row_obj.insert(col_name.to_string(), Value::from_json(value));
                    }
                    Ok(Value::Object(row_obj))
                } else {
                    Ok(Value::Null)
                }
            }
            Value::LazyFrame(lf) => {
                // Collect LazyFrame first, then index
                let df = lf.clone().collect().map_err(Error::from)?;
                let len = df.height() as i64;
                let index = if idx < 0 { len + idx } else { idx };

                if index >= 0 && index < len {
                    // Return a row as an object
                    let mut row_obj = HashMap::new();
                    for col_name in df.get_column_names() {
                        let series = df.column(col_name).map_err(Error::from)?;
                        let value = self.series_value_to_json(series, index as usize)?;
                        row_obj.insert(col_name.to_string(), Value::from_json(value));
                    }
                    Ok(Value::Object(row_obj))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(TypeError::UnsupportedOperation {
                operation: "index".to_string(),
                typ: self.type_name().to_string(),
            }
            .into()),
        }
    }

    /// Get field from object-like values
    pub fn field(&self, key: &str) -> Result<Value> {
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
                    Ok(series) => Ok(Value::Series(series.clone())),
                    Err(_) => Ok(Value::Null),
                }
            }
            _ => Err(TypeError::UnsupportedOperation {
                operation: "field".to_string(),
                typ: self.type_name().to_string(),
            }
            .into()),
        }
    }

    /// Get nested field path from object-like values
    pub fn field_path(&self, fields: &[&str]) -> Result<Value> {
        let mut result = self.clone();
        for &field in fields {
            result = result.field(field)?;
        }
        Ok(result)
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Object(a), Value::Object(b)) => a == b,
            // For DataFrames, compare shape and content
            (Value::DataFrame(a), Value::DataFrame(b)) => a.shape() == b.shape() && a == b,
            // Series comparison
            (Value::Series(a), Value::Series(b)) => a.len() == b.len() && a == b,
            // Cross-type numeric comparisons
            (Value::Int(a), Value::Float(b)) => *a as f64 == *b,
            (Value::Float(a), Value::Int(b)) => *a == *b as f64,
            _ => false,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, item) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            Value::Object(obj) => {
                write!(f, "{{")?;
                for (i, (key, value)) in obj.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "\"{}\": {}", key, value)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;
    use polars::prelude::*;
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
        assert_eq!(Value::float(3.14), Value::Float(3.14));
        assert_eq!(Value::float(-2.5), Value::Float(-2.5));
        assert_eq!(Value::string("hello"), Value::String("hello".to_string()));
        assert_eq!(Value::string(""), Value::String("".to_string()));

        let arr = vec![Value::int(1), Value::int(2)];
        assert_eq!(Value::array(arr.clone()), Value::Array(arr));

        let obj = HashMap::from([("key".to_string(), Value::string("value"))]);
        assert_eq!(Value::object(obj.clone()), Value::Object(obj));
    }

    #[test]
    fn test_is_methods() {
        assert!(Value::Null.is_null());
        assert!(!Value::int(1).is_null());

        assert!(Value::DataFrame(DataFrame::empty()).is_dataframe());
        assert!(!Value::int(1).is_dataframe());

        assert!(Value::LazyFrame(Box::new(LazyFrame::default())).is_lazy_frame());
        assert!(!Value::int(1).is_lazy_frame());

        assert!(Value::Series(Series::new_empty("test".into(), &DataType::Int64)).is_series());
        assert!(!Value::int(1).is_series());
    }

    #[test]
    fn test_len_and_empty() {
        assert_eq!(Value::Null.len(), None);
        assert_eq!(Value::int(1).len(), None);
        assert_eq!(Value::string("hello").len(), Some(5));
        assert_eq!(Value::string("").len(), Some(0));
        assert_eq!(
            Value::array(vec![Value::int(1), Value::int(2)]).len(),
            Some(2)
        );
        assert_eq!(Value::array(vec![]).len(), Some(0));

        let df = DataFrame::new(vec![Series::new("a".into().into(), vec![1, 2, 3]).into()]).unwrap();
        assert_eq!(Value::DataFrame(df).len(), Some(3));

        let series = Series::new("test".into().into(), vec![1, 2, 3]);
        assert_eq!(Value::Series(series).len(), Some(3));

        assert!(Value::string("").is_empty());
        assert!(Value::array(vec![]).is_empty());
        assert!(!Value::string("a").is_empty());
        assert!(!Value::array(vec![Value::int(1)]).is_empty());
        assert!(!Value::Null.is_empty()); // None len means not empty in this context?
    }

    #[test]
    fn test_type_names() {
        assert_eq!(Value::Null.type_name(), "null");
        assert_eq!(Value::Bool(true).type_name(), "boolean");
        assert_eq!(Value::Int(42).type_name(), "integer");
        assert_eq!(Value::BigInt(BigInt::from(42)).type_name(), "biginteger");
        assert_eq!(Value::Float(3.14).type_name(), "float");
        assert_eq!(Value::String("test".to_string()).type_name(), "string");
        assert_eq!(Value::Array(vec![]).type_name(), "array");
        assert_eq!(Value::Object(HashMap::new()).type_name(), "object");
        assert_eq!(
            Value::DataFrame(DataFrame::empty()).type_name(),
            "dataframe"
        );
        assert_eq!(
            Value::LazyFrame(Box::new(LazyFrame::default())).type_name(),
            "lazyframe"
        );
        assert_eq!(
            Value::Series(Series::new_empty("test".into(), &DataType::Int64)).type_name(),
            "series"
        );
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

        // BigInt
        let big = BigInt::from(12345678901234567890u64);
        let json = Value::bigint(big.clone()).to_json().unwrap();
        assert_eq!(json, JsonValue::String(big.to_string()));
        assert_eq!(Value::from_json(json), Value::bigint(big));

        // Float
        let json = Value::float(3.14).to_json().unwrap();
        assert_eq!(json, JsonValue::Number(JsonNumber::from_f64(3.14).unwrap()));
        assert_eq!(Value::from_json(json), Value::float(3.14));

        // String
        let json = Value::string("hello").to_json().unwrap();
        assert_eq!(json, JsonValue::String("hello".to_string()));
        assert_eq!(Value::from_json(json), Value::string("hello"));
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
    fn test_indexing_invalid() {
        let obj = Value::object(HashMap::new());
        assert!(obj.index(0).is_err());

        let null_val = Value::Null;
        assert!(null_val.index(0).is_err());
    }

    #[test]
    fn test_field_access_object() {
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
    fn test_field_access_null() {
        let null_val = Value::Null;
        assert_eq!(null_val.field("any").unwrap(), Value::Null);
    }

    #[test]
    fn test_equality() {
        // Same types
        assert_eq!(Value::int(42), Value::int(42));
        assert_eq!(Value::float(3.14), Value::float(3.14));
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
        let df = DataFrame::new(vec![Series::new("a".into().into(), vec![1, 2, 3]).into()]).unwrap();
        assert_ne!(Value::dataframe(df.clone()), Value::dataframe(df));
        let series = Series::new("test".into().into(), vec![1, 2, 3]);
        assert_ne!(Value::series(series.clone()), Value::series(series));
    }

    #[test]
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
        assert!(df.get_column_names().contains(&"name".into()));
        assert!(df.get_column_names().contains(&"age".into()));

        // Empty array
        let empty = Value::array(vec![]);
        let df = empty.to_dataframe().unwrap();
        assert_eq!(df.height(), 0);

        // Invalid array (mixed types)
        let invalid = Value::array(vec![Value::int(1), Value::string("not object")]);
        assert!(invalid.to_dataframe().is_err());
    }

    #[test]
    fn test_value_to_any_value() {
        // Test successful conversions
        assert_eq!(
            Value::Null.value_to_any_value(&Value::Null).unwrap(),
            AnyValue::Null
        );
        assert_eq!(
            Value::bool(true)
                .value_to_any_value(&Value::bool(true))
                .unwrap(),
            AnyValue::Boolean(true)
        );
        assert_eq!(
            Value::int(42).value_to_any_value(&Value::int(42)).unwrap(),
            AnyValue::Int64(42)
        );
        assert_eq!(
            Value::float(3.14)
                .value_to_any_value(&Value::float(3.14))
                .unwrap(),
            AnyValue::Float64(3.14)
        );
        assert_eq!(
            Value::string("hello")
                .value_to_any_value(&Value::string("hello"))
                .unwrap(),
            AnyValue::String("hello")
        );

        // Test BigInt error
        let bigint = Value::bigint(BigInt::from(42));
        assert!(bigint.value_to_any_value(&bigint).is_err());

        // Test unsupported types
        let arr = Value::array(vec![]);
        assert!(arr.value_to_any_value(&arr).is_err());
    }

    #[test]
    fn test_dataframe_to_json() {
        let df = DataFrame::new(vec![
            Series::new("name".into().into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into().into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();

        let value = Value::DataFrame(df);
        let json = value.to_json().unwrap();

        match json {
            JsonValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                match &arr[0] {
                    JsonValue::Object(obj) => {
                        assert_eq!(
                            obj.get("name"),
                            Some(&JsonValue::String("Alice".to_string()))
                        );
                        assert_eq!(
                            obj.get("age"),
                            Some(&JsonValue::Number(JsonNumber::from(30)))
                        );
                    }
                    _ => panic!("Expected object"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_series_to_json() {
        let series = Series::new("ages".into().into(), vec![30i64, 25i64, 35i64]);
        let value = Value::Series(series);
        let json = value.to_json().unwrap();

        match json {
            JsonValue::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], JsonValue::Number(JsonNumber::from(30)));
                assert_eq!(arr[1], JsonValue::Number(JsonNumber::from(25)));
                assert_eq!(arr[2], JsonValue::Number(JsonNumber::from(35)));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_lazyframe_to_json() {
        let df = DataFrame::new(vec![
            Series::new("name".into().into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into().into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();

        let lf = df.clone().lazy();
        let value = Value::LazyFrame(Box::new(lf));
        let json = value.to_json().unwrap();

        match json {
            JsonValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_indexing_dataframe() {
        let df = DataFrame::new(vec![
            Series::new("name".into().into(), vec!["Alice", "Bob", "Charlie"]).into(),
            Series::new("age".into().into(), vec![30i64, 25i64, 35i64]).into(),
        ])
        .unwrap();

        let value = Value::DataFrame(df);

        // Test valid indices
        let row0 = value.index(0).unwrap();
        match row0 {
            Value::Object(obj) => {
                assert_eq!(obj.get("name"), Some(&Value::string("Alice")));
                assert_eq!(obj.get("age"), Some(&Value::int(30)));
            }
            _ => panic!("Expected object"),
        }

        let row_neg1 = value.index(-1).unwrap();
        match row_neg1 {
            Value::Object(obj) => {
                assert_eq!(obj.get("name"), Some(&Value::string("Charlie")));
            }
            _ => panic!("Expected object"),
        }

        // Test out of bounds
        assert_eq!(value.index(10).unwrap(), Value::Null);
        assert_eq!(value.index(-10).unwrap(), Value::Null);
    }

    #[test]
    fn test_field_access_dataframe() {
        let df = DataFrame::new(vec![
            Series::new("name".into().into(), vec!["Alice", "Bob"]).into(),
            Series::new("age".into().into(), vec![30i64, 25i64]).into(),
        ])
        .unwrap();

        let value = Value::DataFrame(df);

        let name_series = value.field("name").unwrap();
        match name_series {
            Value::Series(s) => {
                assert_eq!(s.name(), "name");
                assert_eq!(s.len(), 2);
            }
            _ => panic!("Expected series"),
        }

        // Non-existent column
        assert_eq!(value.field("missing").unwrap(), Value::Null);
    }

    #[test]
    fn test_bigint_operations() {
        let big1 = Value::bigint(BigInt::from(100));
        let big2 = Value::bigint(BigInt::from(200));
        let int_val = Value::int(100);

        // Equality
        assert_eq!(big1, Value::bigint(BigInt::from(100)));
        assert_ne!(big1, big2);
        assert_eq!(big1, int_val); // Cross-type equality

        // JSON conversion
        let json = big1.to_json().unwrap();
        assert_eq!(json, JsonValue::String("100".to_string()));
        assert_eq!(Value::from_json(json), big1);

        // Display
        assert_eq!(format!("{}", big1), "100");
    }

    #[test]
    fn test_nested_field_path() {
        let nested = Value::object(HashMap::from([(
            "user".to_string(),
            Value::object(HashMap::from([
                ("name".to_string(), Value::string("Alice")),
                (
                    "profile".to_string(),
                    Value::object(HashMap::from([("age".to_string(), Value::int(30))])),
                ),
            ])),
        )]));

        assert_eq!(
            nested.field_path(&["user", "name"]).unwrap(),
            Value::string("Alice")
        );
        assert_eq!(
            nested.field_path(&["user", "profile", "age"]).unwrap(),
            Value::int(30)
        );
        assert_eq!(
            nested.field_path(&["user", "missing"]).unwrap(),
            Value::Null
        );
        assert_eq!(
            nested.field_path(&["missing", "field"]).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_array_field_access_complex() {
        let arr = Value::array(vec![
            Value::object(HashMap::from([
                ("name".to_string(), Value::string("Alice")),
                (
                    "scores".to_string(),
                    Value::array(vec![Value::int(85), Value::int(90)]),
                ),
            ])),
            Value::object(HashMap::from([
                ("name".to_string(), Value::string("Bob")),
                (
                    "scores".to_string(),
                    Value::array(vec![Value::int(75), Value::int(80)]),
                ),
            ])),
        ]);

        // Access names
        let names = arr.field("name").unwrap();
        match names {
            Value::Array(n) => {
                assert_eq!(n.len(), 2);
                assert_eq!(n[0], Value::string("Alice"));
                assert_eq!(n[1], Value::string("Bob"));
            }
            _ => panic!("Expected array"),
        }

        // Access scores
        let scores = arr.field("scores").unwrap();
        match scores {
            Value::Array(s) => {
                assert_eq!(s.len(), 2);
                match &s[0] {
                    Value::Array(alice_scores) => {
                        assert_eq!(alice_scores.len(), 2);
                        assert_eq!(alice_scores[0], Value::int(85));
                    }
                    _ => panic!("Expected array"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_error_cases() {
        // Test unsupported operations
        let arr = Value::array(vec![]);
        assert!(arr.field("invalid").is_err()); // Arrays don't support field access except for "field" on objects

        let string_val = Value::string("test");
        assert!(string_val.field("any").is_err());

        let int_val = Value::int(42);
        assert!(int_val.index(0).is_err());
        assert!(int_val.field("any").is_err());
    }
}
