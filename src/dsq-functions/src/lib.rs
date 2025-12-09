//! Built-in functions for dsq
//!
//! This crate provides all built-in functions available in dsq filters,
//! including jq-compatible functions and DataFrame-specific operations.

#![allow(
    clippy::manual_range_contains,
    clippy::match_result_ok,
    clippy::len_zero,
    clippy::needless_pass_by_value,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    clippy::module_name_repetitions,
    clippy::match_same_arms,
    clippy::redundant_else,
    clippy::redundant_closure,
    clippy::unnecessary_cast,
    clippy::redundant_pattern_matching,
    clippy::needless_return,
    clippy::uninlined_format_args,
    clippy::match_wildcard_for_single_variants,
    clippy::single_match_else,
    clippy::if_not_else,
    clippy::map_unwrap_or,
    clippy::from_over_into,
    clippy::or_fun_call,
    clippy::iter_over_hash_type,
    clippy::useless_conversion,
    clippy::option_map_or_none,
    clippy::needless_borrow,
    clippy::for_kv_map,
    clippy::needless_range_loop,
    clippy::manual_is_multiple_of,
    clippy::collapsible_else_if,
    clippy::manual_map,
    clippy::if_same_then_else,
    clippy::assign_op_pattern,
    clippy::manual_strip,
    clippy::option_map_unit_fn,
    clippy::bind_instead_of_map
)]

pub mod builtin;

// Re-export inventory for use by builtin modules
pub use inventory;

use dsq_shared::value::{value_from_any_value, Value};
use dsq_shared::Result;
use std::collections::HashMap;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};

/// Built-in function implementation
pub type BuiltinFunction = Arc<dyn Fn(&[Value]) -> Result<Value> + Send + Sync>;

inventory::collect!(FunctionRegistration);

pub struct FunctionRegistration {
    pub name: &'static str,
    pub func: fn(&[Value]) -> Result<Value>,
}

/// Registry of built-in functions
///
/// This struct manages all built-in functions and provides a unified interface
/// for calling them during filter execution.
pub struct BuiltinRegistry {
    functions: HashMap<String, BuiltinFunction>,
}

impl std::fmt::Debug for BuiltinRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BuiltinRegistry {{ functions: {} functions }}",
            self.functions.len()
        )
    }
}

impl BuiltinRegistry {
    /// Create a new builtin registry with standard functions
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };

        registry.register_standard_functions();
        registry
    }

    /// Register all standard built-in functions
    fn register_standard_functions(&mut self) {
        // Register functions from inventory
        for func in inventory::iter::<FunctionRegistration> {
            self.register(func.name, Arc::new(func.func));
        }
    }

    /// Register a built-in function
    pub fn register(&mut self, name: impl Into<String>, func: BuiltinFunction) {
        self.functions.insert(name.into(), func);
    }

    /// Check if a function exists
    pub fn has_function(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }

    /// Call a built-in function
    pub fn call_function(&self, name: &str, args: &[Value]) -> Result<Value> {
        if let Some(func) = self.functions.get(name) {
            func(args)
        } else {
            Err(dsq_shared::error::operation_error(format!(
                "built-in function '{}'",
                name
            )))
        }
    }

    /// Get the number of registered functions
    pub fn function_count(&self) -> usize {
        self.functions.len()
    }

    /// Get a built-in function by name
    pub fn get_function(&self, name: &str) -> Option<BuiltinFunction> {
        self.functions.get(name).cloned()
    }

    /// Get all function names
    pub fn function_names(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }
}

impl Default for BuiltinRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Helper function to extract DateTime from various input types
pub(crate) fn extract_timestamp(value: &Value) -> Result<DateTime<Utc>> {
    match value {
        Value::Int(i) => Utc
            .timestamp_opt(*i, 0)
            .single()
            .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp")),
        Value::Float(f) => {
            let secs = f.trunc() as i64;
            let nanos = (f.fract() * 1_000_000_000.0) as u32;
            Utc.timestamp_opt(secs, nanos)
                .single()
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp"))
        }
        Value::String(s) => {
            // Try parsing as RFC3339 first
            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                Ok(dt.with_timezone(&Utc))
            } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                Ok(dt.and_utc())
            } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                Ok(dt.and_utc())
            } else if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                let dt = date.and_hms_opt(0, 0, 0).unwrap();
                Ok(dt.and_utc())
            } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S") {
                Ok(dt.and_utc())
            } else if let Ok(date) = NaiveDate::parse_from_str(s, "%Y/%m/%d") {
                let dt = date.and_hms_opt(0, 0, 0).unwrap();
                Ok(dt.and_utc())
            } else {
                Err(dsq_shared::error::operation_error(
                    "Unable to parse date/time string",
                ))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "Unsupported type for date extraction",
        )),
    }
}

// Built-in function implementations

inventory::submit! {
    FunctionRegistration {
        name: "lstrip",
        func: builtin_lstrip,
    }
}

fn builtin_lstrip(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "lstrip() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => Ok(Value::String(s.trim_start().to_string())),
        _ => Err(dsq_shared::error::operation_error(
            "lstrip() requires string argument",
        )),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "sha256",
        func: builtin_sha256,
    }
}

fn builtin_sha256(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "sha256() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            let mut hasher = Sha256::new();
            hasher.update(s.as_bytes());
            let result = hasher.finalize();
            Ok(Value::String(format!("{:x}", result)))
        }
        _ => Err(dsq_shared::error::operation_error(
            "sha256() requires string argument",
        )),
    }
}

pub fn compare_values_for_sorting(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Bool(a_val), Value::Bool(b_val)) => a_val.cmp(b_val),
        (Value::Int(a_val), Value::Int(b_val)) => a_val.cmp(b_val),
        (Value::Float(a_val), Value::Float(b_val)) => a_val
            .partial_cmp(b_val)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(a_val), Value::String(b_val)) => a_val.cmp(b_val),
        (Value::Int(a_val), Value::Float(b_val)) => (*a_val as f64)
            .partial_cmp(b_val)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(a_val), Value::Int(b_val)) => a_val
            .partial_cmp(&(*b_val as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        _ => std::cmp::Ordering::Equal,
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "dtypes",
        func: builtin_dtypes,
    }
}

fn builtin_dtypes(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "dtypes() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::DataFrame(df) => {
            let mut dtypes_obj = HashMap::new();
            for col_name in df.get_column_names() {
                if let Ok(series) = df.column(col_name) {
                    dtypes_obj.insert(
                        col_name.to_string(),
                        Value::String(series.dtype().to_string()),
                    );
                }
            }
            Ok(Value::Object(dtypes_obj))
        }
        _ => Err(dsq_shared::error::operation_error(
            "dtypes() requires DataFrame argument",
        )),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "round",
        func: builtin_round,
    }
}

fn builtin_round(args: &[Value]) -> Result<Value> {
    if args.len() < 1 || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "round() expects 1 or 2 arguments",
        ));
    }

    let precision = if args.len() == 2 {
        match &args[1] {
            Value::Int(i) => *i as usize,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "round() precision must be an integer",
                ))
            }
        }
    } else {
        0
    };

    match &args[0] {
        Value::Int(i) => {
            if precision == 0 {
                Ok(Value::Int(*i))
            } else {
                Ok(Value::Float(*i as f64))
            }
        }
        Value::Float(f) => {
            let multiplier = 10f64.powi(precision as i32);
            let rounded = (f * multiplier).round() / multiplier;
            Ok(Value::Float(rounded))
        }
        Value::Array(arr) => {
            let rounded_arr: Result<Vec<Value>> = arr
                .iter()
                .map(|v| match v {
                    Value::Int(i) => {
                        if precision == 0 {
                            Ok(Value::Int(*i))
                        } else {
                            Ok(Value::Float(*i as f64))
                        }
                    }
                    Value::Float(f) => {
                        let multiplier = 10f64.powi(precision as i32);
                        let rounded = (f * multiplier).round() / multiplier;
                        Ok(Value::Float(rounded))
                    }
                    _ => Ok(v.clone()),
                })
                .collect();
            Ok(Value::Array(rounded_arr?))
        }
        Value::DataFrame(df) => {
            // For simplicity, return unchanged for now
            Ok(Value::DataFrame(df.clone()))
        }
        Value::Series(series) => {
            // For simplicity, return unchanged for now
            Ok(Value::Series(series.clone()))
        }
        _ => Ok(args[0].clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtin::filter::builtin_filter;
    use crate::builtin::histogram::builtin_histogram;
    use crate::builtin::select::builtin_select;
    use crate::builtin::transliterate::builtin_transliterate;
    use chrono::Datelike;
    use dsq_shared::value::Value;
    use polars::datatypes::PlSmallStr;
    use polars::prelude::*;
    use std::collections::HashMap;

    fn create_test_dataframe() -> DataFrame {
        let names = Series::new(PlSmallStr::from("name"), &["Alice", "Bob", "Charlie"]);
        let ages = Series::new(PlSmallStr::from("age"), &[25, 30, 35]);
        let scores = Series::new(PlSmallStr::from("score"), &[85.5, 92.0, 78.3]);
        DataFrame::new(vec![names.into(), ages.into(), scores.into()]).unwrap()
    }

    #[test]
    fn test_extract_timestamp() {
        // Test with integer timestamp
        let ts = Value::Int(1609459200); // 2021-01-01 00:00:00 UTC
        let dt = extract_timestamp(&ts).unwrap();
        assert_eq!(dt.year(), 2021);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);

        // Test with RFC3339 string
        let rfc3339 = Value::String("2021-01-01T00:00:00Z".to_string());
        let dt = extract_timestamp(&rfc3339).unwrap();
        assert_eq!(dt.year(), 2021);

        // Test with date string
        let date_str = Value::String("2021-01-01".to_string());
        let dt = extract_timestamp(&date_str).unwrap();
        assert_eq!(dt.year(), 2021);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
    }

    #[test]
    fn test_start_of_month() {
        let registry = BuiltinRegistry::new();

        // Test with timestamp in the middle of the month
        let ts = Value::Int(1612137600); // 2021-02-01 00:00:00 UTC (already start of month)
        let result = registry.call_function("start_of_month", &[ts]).unwrap();
        assert_eq!(result, Value::String("2021-02-01".to_string()));

        // Test with timestamp later in the month
        let ts = Value::Int(1614556800); // 2021-03-01 00:00:00 UTC
        let result = registry.call_function("start_of_month", &[ts]).unwrap();
        assert_eq!(result, Value::String("2021-03-01".to_string()));

        // Test with timestamp in the middle of February
        let ts = Value::Int(1613347200); // 2021-02-15 00:00:00 UTC
        let result = registry.call_function("start_of_month", &[ts]).unwrap();
        assert_eq!(result, Value::String("2021-02-01".to_string()));

        // Test with string date
        let date_str = Value::String("2021-06-15".to_string());
        let result = registry
            .call_function("start_of_month", &[date_str])
            .unwrap();
        assert_eq!(result, Value::String("2021-06-01".to_string()));
    }

    #[test]
    fn test_truncate_time_function() {
        let registry = BuiltinRegistry::new();

        // Test truncate to day
        let result = registry
            .call_function(
                "truncate_time",
                &[
                    Value::String("2021-06-15T14:30:45Z".to_string()),
                    Value::String("day".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("2021-06-15T00:00:00+00:00".to_string())
        );

        // Test truncate to hour
        let result = registry
            .call_function(
                "truncate_time",
                &[
                    Value::String("2021-06-15T14:30:45Z".to_string()),
                    Value::String("hour".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("2021-06-15T14:00:00+00:00".to_string())
        );

        // Test truncate to minute
        let result = registry
            .call_function(
                "truncate_time",
                &[
                    Value::String("2021-06-15T14:30:45Z".to_string()),
                    Value::String("minute".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("2021-06-15T14:30:00+00:00".to_string())
        );

        // Test truncate to month
        let result = registry
            .call_function(
                "truncate_time",
                &[
                    Value::String("2021-06-15T14:30:45Z".to_string()),
                    Value::String("month".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("2021-06-01T00:00:00+00:00".to_string())
        );

        // Test truncate to year
        let result = registry
            .call_function(
                "truncate_time",
                &[
                    Value::String("2021-06-15T14:30:45Z".to_string()),
                    Value::String("year".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("2021-01-01T00:00:00+00:00".to_string())
        );
    }

    #[test]
    fn test_year_function() {
        let registry = BuiltinRegistry::new();

        // Test with timestamp
        let result = registry
            .call_function("year", &[Value::Int(1609459200)])
            .unwrap();
        assert_eq!(result, Value::Int(2021));

        // Test with date string
        let result = registry
            .call_function("year", &[Value::String("2021-06-15".to_string())])
            .unwrap();
        assert_eq!(result, Value::Int(2021));

        // Test with RFC3339 string
        let result = registry
            .call_function("year", &[Value::String("2021-06-15T12:30:45Z".to_string())])
            .unwrap();
        assert_eq!(result, Value::Int(2021));
    }

    #[test]
    fn test_date_diff_function() {
        let registry = BuiltinRegistry::new();

        // Test with date strings
        let result = registry
            .call_function(
                "date_diff",
                &[
                    Value::String("2023-01-01".to_string()),
                    Value::String("2023-01-05".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(result, Value::Int(4));

        // Test with same date
        let result = registry
            .call_function(
                "date_diff",
                &[
                    Value::String("2023-01-01".to_string()),
                    Value::String("2023-01-01".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(result, Value::Int(0));

        // Test with RFC3339 strings
        let result = registry
            .call_function(
                "date_diff",
                &[
                    Value::String("2023-01-01T00:00:00Z".to_string()),
                    Value::String("2023-01-05T00:00:00Z".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(result, Value::Int(4));

        // Test with arrays
        let arr1 = Value::Array(vec![
            Value::String("2023-01-01".to_string()),
            Value::String("2023-02-01".to_string()),
        ]);
        let arr2 = Value::Array(vec![
            Value::String("2023-01-05".to_string()),
            Value::String("2023-02-05".to_string()),
        ]);
        let result = registry.call_function("date_diff", &[arr1, arr2]).unwrap();
        assert_eq!(result, Value::Array(vec![Value::Int(4), Value::Int(4)]));
    }

    #[test]
    fn test_gmtime_function() {
        let registry = BuiltinRegistry::new();

        // Test with timestamp 1609459200 (2021-01-01 00:00:00 UTC)
        let result = registry
            .call_function("gmtime", &[Value::Int(1609459200)])
            .unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.get("year"), Some(&Value::Int(2021)));
            assert_eq!(obj.get("month"), Some(&Value::Int(1)));
            assert_eq!(obj.get("day"), Some(&Value::Int(1)));
            assert_eq!(obj.get("hour"), Some(&Value::Int(0)));
            assert_eq!(obj.get("minute"), Some(&Value::Int(0)));
            assert_eq!(obj.get("second"), Some(&Value::Int(0)));
            assert_eq!(obj.get("weekday"), Some(&Value::Int(5))); // Friday
            assert_eq!(obj.get("yearday"), Some(&Value::Int(1)));
        } else {
            panic!("Expected object result");
        }

        // Test with date string
        let result = registry
            .call_function("gmtime", &[Value::String("2021-06-15".to_string())])
            .unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.get("year"), Some(&Value::Int(2021)));
            assert_eq!(obj.get("month"), Some(&Value::Int(6)));
            assert_eq!(obj.get("day"), Some(&Value::Int(15)));
            assert_eq!(obj.get("hour"), Some(&Value::Int(0)));
            assert_eq!(obj.get("minute"), Some(&Value::Int(0)));
            assert_eq!(obj.get("second"), Some(&Value::Int(0)));
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_month_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("month", &[Value::String("2021-06-15".to_string())])
            .unwrap();
        assert_eq!(result, Value::Int(6));
    }

    #[test]
    fn test_day_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("day", &[Value::String("2021-06-15".to_string())])
            .unwrap();
        assert_eq!(result, Value::Int(15));
    }

    #[test]
    fn test_hour_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("hour", &[Value::String("2021-06-15T14:30:45Z".to_string())])
            .unwrap();
        assert_eq!(result, Value::Int(14));
    }

    #[test]
    fn test_minute_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function(
                "minute",
                &[Value::String("2021-06-15T14:30:45Z".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::Int(30));
    }

    #[test]
    fn test_second_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function(
                "second",
                &[Value::String("2021-06-15T14:30:45Z".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::Int(45));
    }

    #[test]
    fn test_tostring_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("tostring", &[Value::Int(42)])
            .unwrap();
        assert_eq!(result, Value::String("42".to_string()));

        let result = registry
            .call_function("tostring", &[Value::Float(3.14)])
            .unwrap();
        assert_eq!(result, Value::String("3.14".to_string()));

        let result = registry
            .call_function("tostring", &[Value::Bool(true)])
            .unwrap();
        assert_eq!(result, Value::String("true".to_string()));
    }

    #[test]
    fn test_snake_case_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("snake_case", &[Value::String("CamelCase".to_string())])
            .unwrap();
        assert_eq!(result, Value::String("camel_case".to_string()));

        let result = registry
            .call_function("snake_case", &[Value::String("XMLHttpRequest".to_string())])
            .unwrap();
        assert_eq!(result, Value::String("xml_http_request".to_string()));
    }

    #[test]
    fn test_lowercase_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("lowercase", &[Value::String("HELLO WORLD".to_string())])
            .unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));

        let result = registry
            .call_function("lowercase", &[Value::String("HeLLo WoRlD".to_string())])
            .unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    #[test]
    fn test_camel_case_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("camel_case", &[Value::String("snake_case".to_string())])
            .unwrap();
        assert_eq!(result, Value::String("snakeCase".to_string()));

        let result = registry
            .call_function(
                "camel_case",
                &[Value::String("xml_http_request".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("xmlHttpRequest".to_string()));
    }

    #[test]
    fn test_is_valid_utf8_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("is_valid_utf8", &[Value::String("hello world".to_string())])
            .unwrap();
        assert_eq!(result, Value::Bool(true));

        let result = registry
            .call_function("is_valid_utf8", &[Value::String("hello".to_string())])
            .unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_to_valid_utf8_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("to_valid_utf8", &[Value::String("hello world".to_string())])
            .unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));

        let result = registry
            .call_function("to_valid_utf8", &[Value::String("café".to_string())])
            .unwrap();
        assert_eq!(result, Value::String("café".to_string()));

        // Test with array
        let arr = vec![
            Value::String("café".to_string()),
            Value::String("naïve".to_string()),
        ];
        let result = registry
            .call_function("to_valid_utf8", &[Value::Array(arr)])
            .unwrap();
        let expected = vec![
            Value::String("café".to_string()),
            Value::String("naïve".to_string()),
        ];
        assert_eq!(result, Value::Array(expected));
    }

    #[test]
    fn test_tabs_to_spaces_function() {
        let registry = BuiltinRegistry::new();

        // Test with default 4 spaces
        let result = registry
            .call_function(
                "tabs_to_spaces",
                &[Value::String("hello\tworld".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("hello    world".to_string()));

        // Test with custom 2 spaces
        let result = registry
            .call_function(
                "tabs_to_spaces",
                &[Value::String("a\tb\tc".to_string()), Value::Int(2)],
            )
            .unwrap();
        assert_eq!(result, Value::String("a  b  c".to_string()));

        // Test with leading tab
        let result = registry
            .call_function(
                "tabs_to_spaces",
                &[Value::String("\tindented text".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("    indented text".to_string()));

        // Test with multiple tabs
        let result = registry
            .call_function(
                "tabs_to_spaces",
                &[Value::String("Multiple\ttabs\tin\tone\tline".to_string())],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("Multiple    tabs    in    one    line".to_string())
        );
    }

    #[test]
    fn test_abs_function() {
        let registry = BuiltinRegistry::new();

        let result = registry.call_function("abs", &[Value::Int(-42)]).unwrap();
        assert_eq!(result, Value::Int(42));

        let result = registry
            .call_function("abs", &[Value::Float(-3.14)])
            .unwrap();
        assert_eq!(result, Value::Float(3.14));
    }

    #[test]
    fn test_floor_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("floor", &[Value::Float(3.7)])
            .unwrap();
        assert_eq!(result, Value::Float(3.0));

        let result = registry
            .call_function("floor", &[Value::Float(-3.7)])
            .unwrap();
        assert_eq!(result, Value::Float(-4.0));
    }

    #[test]
    fn test_ceil_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("ceil", &[Value::Float(3.1)])
            .unwrap();
        assert_eq!(result, Value::Float(4.0));

        let result = registry
            .call_function("ceil", &[Value::Float(-3.1)])
            .unwrap();
        assert_eq!(result, Value::Float(-3.0));
    }

    #[test]
    fn test_pow_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("pow", &[Value::Int(2), Value::Int(3)])
            .unwrap();
        assert_eq!(result, Value::Float(8.0));

        let result = registry
            .call_function("pow", &[Value::Float(2.0), Value::Float(0.5)])
            .unwrap();
        assert_eq!(result, Value::Float(1.4142135623730951));
    }

    #[test]
    fn test_log10_function() {
        let registry = BuiltinRegistry::new();

        let result = registry.call_function("log10", &[Value::Int(10)]).unwrap();
        assert_eq!(result, Value::Float(1.0));

        let result = registry.call_function("log10", &[Value::Int(100)]).unwrap();
        assert_eq!(result, Value::Float(2.0));

        let result = registry
            .call_function("log10", &[Value::Float(1000.0)])
            .unwrap();
        assert_eq!(result, Value::Float(3.0));

        // Test domain error
        let result = registry.call_function("log10", &[Value::Int(0)]);
        assert!(result.is_err());

        let result = registry.call_function("log10", &[Value::Float(-1.0)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_pi_function() {
        let registry = BuiltinRegistry::new();

        let result = registry.call_function("pi", &[]).unwrap();
        assert_eq!(result, Value::Float(std::f64::consts::PI));
    }

    #[test]
    fn test_sha512_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("sha512", &[Value::String("hello".to_string())])
            .unwrap();
        if let Value::String(hash) = result {
            assert_eq!(hash.len(), 128); // SHA512 produces 128 hex characters
            assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_sha256_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("sha256", &[Value::String("hello".to_string())])
            .unwrap();
        if let Value::String(hash) = result {
            assert_eq!(hash.len(), 64); // SHA256 produces 64 hex characters
            assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_sha1_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("sha1", &[Value::String("hello".to_string())])
            .unwrap();
        if let Value::String(hash) = result {
            assert_eq!(hash.len(), 40); // SHA1 produces 40 hex characters
            assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_md5_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function("md5", &[Value::String("hello".to_string())])
            .unwrap();
        if let Value::String(hash) = result {
            assert_eq!(hash.len(), 32); // MD5 produces 32 hex characters
            assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_base64_encode_decode() {
        let registry = BuiltinRegistry::new();

        let original = "hello world";
        let encoded = registry
            .call_function("base64_encode", &[Value::String(original.to_string())])
            .unwrap();
        if let Value::String(enc_str) = encoded {
            let decoded = registry
                .call_function("base64_decode", &[Value::String(enc_str)])
                .unwrap();
            assert_eq!(decoded, Value::String(original.to_string()));
        } else {
            panic!("Expected string result from base64_encode");
        }
    }

    #[test]
    fn test_base32_encode_decode() {
        let registry = BuiltinRegistry::new();

        let original = "hello world";
        let encoded = registry
            .call_function("base32_encode", &[Value::String(original.to_string())])
            .unwrap();
        if let Value::String(enc_str) = encoded {
            let decoded = registry
                .call_function("base32_decode", &[Value::String(enc_str)])
                .unwrap();
            assert_eq!(decoded, Value::String(original.to_string()));
        } else {
            panic!("Expected string result from base32_encode");
        }
    }

    #[test]
    fn test_base58_encode_decode() {
        let registry = BuiltinRegistry::new();

        let original = "hello world";
        let encoded = registry
            .call_function("base58_encode", &[Value::String(original.to_string())])
            .unwrap();
        if let Value::String(enc_str) = encoded {
            let decoded = registry
                .call_function("base58_decode", &[Value::String(enc_str)])
                .unwrap();
            assert_eq!(decoded, Value::String(original.to_string()));
        } else {
            panic!("Expected string result from base58_encode");
        }
    }

    #[test]
    fn test_columns_function() {
        let registry = BuiltinRegistry::new();
        let df = create_test_dataframe();

        let result = registry
            .call_function("columns", &[Value::DataFrame(df)])
            .unwrap();
        if let Value::Array(cols) = result {
            assert_eq!(cols.len(), 3);
            assert!(cols.contains(&Value::String("name".to_string())));
            assert!(cols.contains(&Value::String("age".to_string())));
            assert!(cols.contains(&Value::String("score".to_string())));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_shape_function() {
        let registry = BuiltinRegistry::new();
        let df = create_test_dataframe();

        let result = registry
            .call_function("shape", &[Value::DataFrame(df)])
            .unwrap();
        if let Value::Array(shape) = result {
            assert_eq!(shape.len(), 2);
            assert_eq!(shape[0], Value::Int(3)); // 3 rows
            assert_eq!(shape[1], Value::Int(3)); // 3 columns
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mean_function() {
        let registry = BuiltinRegistry::new();
        let df = create_test_dataframe();

        let result = registry
            .call_function("mean", &[Value::DataFrame(df)])
            .unwrap();
        if let Value::Object(means) = result {
            assert!(means.contains_key("age"));
            assert!(means.contains_key("score"));
            // age mean should be (25+30+35)/3 = 30
            if let Some(Value::Float(age_mean)) = means.get("age") {
                assert!((age_mean - 30.0).abs() < 0.001);
            } else {
                panic!("Expected float for age mean");
            }
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_median_function() {
        let registry = BuiltinRegistry::new();
        let df = create_test_dataframe();

        let result = registry
            .call_function("median", &[Value::DataFrame(df)])
            .unwrap();
        if let Value::Object(medians) = result {
            assert!(medians.contains_key("age"));
            assert!(medians.contains_key("score"));
            // age median should be 30 (sorted: 25, 30, 35)
            if let Some(Value::Float(age_median)) = medians.get("age") {
                assert!((age_median - 30.0).abs() < 0.001);
            } else {
                panic!("Expected float for age median");
            }
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_count_function() {
        let registry = BuiltinRegistry::new();
        let df = create_test_dataframe();

        // Test DataFrame
        let result = registry
            .call_function("count", &[Value::DataFrame(df.clone())])
            .unwrap();
        assert_eq!(result, Value::Int(3));

        // Test Array
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let result = registry.call_function("count", &[arr]).unwrap();
        assert_eq!(result, Value::Int(3));

        // Test empty Array
        let empty_arr = Value::Array(vec![]);
        let result = registry.call_function("count", &[empty_arr]).unwrap();
        assert_eq!(result, Value::Int(0));

        // Test Object
        let mut obj = std::collections::HashMap::new();
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("b".to_string(), Value::Int(2));
        let obj_val = Value::Object(obj);
        let result = registry.call_function("count", &[obj_val]).unwrap();
        assert_eq!(result, Value::Int(2));

        // Test Series
        let series = df.column("name").unwrap().clone();
        let result = registry
            .call_function("count", &[Value::Series(series)])
            .unwrap();
        assert_eq!(result, Value::Int(3));

        // Test String
        let string_val = Value::String("hello".to_string());
        let result = registry.call_function("count", &[string_val]).unwrap();
        assert_eq!(result, Value::Int(5));

        // Test String with unicode
        let unicode_string = Value::String("héllo".to_string());
        let result = registry.call_function("count", &[unicode_string]).unwrap();
        assert_eq!(result, Value::Int(5)); // "héllo" has 5 characters

        // Test other values (should return 1)
        let int_val = Value::Int(42);
        let result = registry.call_function("count", &[int_val]).unwrap();
        assert_eq!(result, Value::Int(1));

        let float_val = Value::Float(3.14);
        let result = registry.call_function("count", &[float_val]).unwrap();
        assert_eq!(result, Value::Int(1));

        let bool_val = Value::Bool(true);
        let result = registry.call_function("count", &[bool_val]).unwrap();
        assert_eq!(result, Value::Int(1));

        let null_val = Value::Null;
        let result = registry.call_function("count", &[null_val]).unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_url_parse_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function(
                "url_parse",
                &[Value::String(
                    "https://www.example.com:8080/path?query=value#fragment".to_string(),
                )],
            )
            .unwrap();
        if let Value::Object(parsed) = result {
            assert_eq!(
                parsed.get("scheme"),
                Some(&Value::String("https".to_string()))
            );
            assert_eq!(
                parsed.get("host"),
                Some(&Value::String("www.example.com".to_string()))
            );
            assert_eq!(parsed.get("port"), Some(&Value::Int(8080)));
            assert_eq!(
                parsed.get("path"),
                Some(&Value::String("/path".to_string()))
            );
            assert_eq!(
                parsed.get("query"),
                Some(&Value::String("query=value".to_string()))
            );
            assert_eq!(
                parsed.get("fragment"),
                Some(&Value::String("fragment".to_string()))
            );
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_url_extract_domain_function() {
        let registry = BuiltinRegistry::new();

        let result = registry
            .call_function(
                "url_extract_domain",
                &[Value::String("https://www.example.com/path".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("www.example.com".to_string()));
    }

    #[test]
    fn test_range_function() {
        let registry = BuiltinRegistry::new();

        let result = registry.call_function("range", &[Value::Int(5)]).unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 5);
            assert_eq!(arr[0], Value::Int(0));
            assert_eq!(arr[4], Value::Int(4));
        } else {
            panic!("Expected array result");
        }

        let result = registry
            .call_function("range", &[Value::Int(1), Value::Int(5)])
            .unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0], Value::Int(1));
            assert_eq!(arr[3], Value::Int(4));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_select_function() {
        let registry = BuiltinRegistry::new();

        let arr = Value::Array(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ]);
        let result = registry
            .call_function("select", &[arr, Value::Int(0), Value::Int(2)])
            .unwrap();
        if let Value::Array(selected) = result {
            assert_eq!(selected.len(), 2);
            assert_eq!(selected[0], Value::String("a".to_string()));
            assert_eq!(selected[1], Value::String("c".to_string()));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_del_function() {
        let registry = BuiltinRegistry::new();

        let mut obj = HashMap::new();
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("b".to_string(), Value::Int(2));
        obj.insert("c".to_string(), Value::Int(3));

        let result = registry
            .call_function("del", &[Value::Object(obj), Value::String("b".to_string())])
            .unwrap();
        if let Value::Object(del_obj) = result {
            assert!(!del_obj.contains_key("b"));
            assert_eq!(del_obj.get("a"), Some(&Value::Int(1)));
            assert_eq!(del_obj.get("c"), Some(&Value::Int(3)));
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_fromjson_function() {
        let registry = BuiltinRegistry::new();

        let json_str = r#"{"name": "Alice", "age": 30, "active": true}"#;
        let result = registry
            .call_function("fromjson", &[Value::String(json_str.to_string())])
            .unwrap();
        if let Value::Object(obj) = result {
            assert_eq!(obj.get("name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(obj.get("age"), Some(&Value::Int(30)));
            assert_eq!(obj.get("active"), Some(&Value::Bool(true)));
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_group_concat_function() {
        let registry = BuiltinRegistry::new();

        let arr = Value::Array(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ]);
        let result = registry
            .call_function("group_concat", &[arr.clone()])
            .unwrap();
        assert_eq!(result, Value::String("a,b,c".to_string()));

        let result = registry
            .call_function("group_concat", &[arr, Value::String(";".to_string())])
            .unwrap();
        assert_eq!(result, Value::String("a;b;c".to_string()));
    }

    #[test]
    fn test_registry_has_function() {
        let registry = BuiltinRegistry::new();

        assert!(registry.has_function("year"));
        assert!(registry.has_function("month"));
        assert!(registry.has_function("tostring"));
        assert!(registry.has_function("abs"));
        assert!(registry.has_function("sha512"));
        assert!(registry.has_function("has"));
        assert!(registry.has_function("rstrip"));
        assert!(registry.has_function("url_extract_path"));
        assert!(registry.has_function("uppercase"));
        assert!(registry.has_function("toupper"));
        assert!(registry.has_function("iif"));
        assert!(registry.has_function("time_series_range"));
        assert!(!registry.has_function("nonexistent"));
    }

    #[test]
    fn test_builtin_has() {
        let registry = BuiltinRegistry::new();

        let obj = Value::Object(
            vec![
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::Int(30)),
                ("city".to_string(), Value::String("New York".to_string())),
            ]
            .into_iter()
            .collect(),
        );

        // Test has with existing key
        let result = registry
            .call_function("has", &[obj.clone(), Value::String("city".to_string())])
            .unwrap();
        assert_eq!(result, Value::Bool(true));

        // Test has with non-existing key
        let result = registry
            .call_function("has", &[obj.clone(), Value::String("country".to_string())])
            .unwrap();
        assert_eq!(result, Value::Bool(false));

        // Test has with non-object
        let result = registry
            .call_function(
                "has",
                &[
                    Value::String("test".to_string()),
                    Value::String("key".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_registry_function_count() {
        let registry = BuiltinRegistry::new();

        // Should have many functions registered
        assert!(registry.function_count() > 50);
    }

    #[test]
    fn test_registry_function_names() {
        let registry = BuiltinRegistry::new();

        let names = registry.function_names();
        assert!(names.contains(&"year".to_string()));
        assert!(names.contains(&"month".to_string()));
        assert!(names.contains(&"tostring".to_string()));
        assert!(names.contains(&"strptime".to_string()));
        assert!(names.contains(&"now".to_string()));
        assert!(names.contains(&"url_extract_port".to_string()));
    }

    #[test]
    fn test_registry_strptime() {
        let registry = BuiltinRegistry::new();

        // Test strptime via registry
        let result = registry
            .call_function(
                "strptime",
                &[
                    Value::String("2021-01-01 00:00:00".to_string()),
                    Value::String("%Y-%m-%d %H:%M:%S".to_string()),
                ],
            )
            .unwrap();
        assert!(matches!(result, Value::Int(_)));
    }

    #[test]
    fn test_builtin_start_of_week() {
        let registry = BuiltinRegistry::new();

        // Test with string date
        let result = registry
            .call_function("start_of_week", &[Value::String("2023-10-02".to_string())])
            .unwrap();
        // 2023-10-02 is Monday, so start of week is 2023-10-02 00:00:00 UTC
        assert_eq!(result, Value::Int(1696204800));

        // Test with Tuesday
        let result = registry
            .call_function("start_of_week", &[Value::String("2023-10-03".to_string())])
            .unwrap();
        assert_eq!(result, Value::Int(1696204800)); // Still Monday

        // Test with Sunday
        let result = registry
            .call_function("start_of_week", &[Value::String("2023-10-08".to_string())])
            .unwrap();
        assert_eq!(result, Value::Int(1696204800)); // Monday

        // Test with sunday start day
        let result = registry
            .call_function(
                "start_of_week",
                &[
                    Value::String("2023-10-02".to_string()),
                    Value::String("sunday".to_string()),
                ],
            )
            .unwrap();
        // 2023-10-02 is Monday, start of week sunday is 2023-09-24? No.
        // 2023-10-02 Monday, previous Sunday is 2023-10-01
        // Timestamp for 2023-10-01 00:00:00 UTC
        assert_eq!(result, Value::Int(1696118400));

        // Test with array
        let arr = Value::Array(vec![
            Value::String("2023-10-02".to_string()),
            Value::String("2023-10-03".to_string()),
        ]);
        let result = registry.call_function("start_of_week", &[arr]).unwrap();
        match result {
            Value::Array(res) => {
                assert_eq!(res.len(), 2);
                assert_eq!(res[0], Value::Int(1696204800));
                assert_eq!(res[1], Value::Int(1696204800));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_builtin_length_with_dataframe() {
        let registry = BuiltinRegistry::new();
        let df = DataFrame::new(vec![
            Series::new(PlSmallStr::from("name"), &["Alice", "Bob", "Charlie"]),
            Series::new(PlSmallStr::from("age"), &[25, 30, 35]),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        let result = registry.call_function("length", &[df_value]).unwrap();
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn test_builtin_select_single_arg() {
        // Test with truthy value
        let result = builtin_select(&[Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::Bool(true));

        // Test with falsy value
        let result = builtin_select(&[Value::Bool(false)]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with null
        let result = builtin_select(&[Value::Null]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with string
        let result = builtin_select(&[Value::String("test".to_string())]).unwrap();
        assert_eq!(result, Value::String("test".to_string()));

        // Test with empty string
        let result = builtin_select(&[Value::String("".to_string())]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with int
        let result = builtin_select(&[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(42));

        // Test with zero
        let result = builtin_select(&[Value::Int(0)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_select_two_args() {
        // Test with truthy condition
        let result =
            builtin_select(&[Value::String("test".to_string()), Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::String("test".to_string()));

        // Test with falsy condition
        let result =
            builtin_select(&[Value::String("test".to_string()), Value::Bool(false)]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with null condition
        let result = builtin_select(&[Value::String("test".to_string()), Value::Null]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with array input
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let result = builtin_select(&[arr, Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::Array(vec![Value::Int(1), Value::Int(2)]));
    }

    #[test]
    fn test_builtin_select_array_with_mask() {
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let mask = vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)];
        let result = builtin_select(&[Value::Array(arr), Value::Array(mask)]).unwrap();
        match result {
            Value::Array(filtered) => {
                assert_eq!(filtered.len(), 2);
                assert_eq!(filtered[0], Value::Int(1));
                assert_eq!(filtered[1], Value::Int(3));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_select_dataframe_with_series() {
        let name = PlSmallStr::from("name");
        let age: PlSmallStr = "age".into();
        let mask: PlSmallStr = "mask".into();
        let df = DataFrame::new(vec![
            Series::new(
                name,
                vec![
                    "Alice".to_string(),
                    "Bob".to_string(),
                    "Charlie".to_string(),
                ],
            )
            .into(),
            Series::new(age, vec![25, 30, 35]).into(),
        ])
        .unwrap();
        let mask_series = Series::new(mask, vec![true, false, true]);
        let result =
            builtin_select(&[Value::DataFrame(df.clone()), Value::Series(mask_series)]).unwrap();
        match result {
            Value::DataFrame(filtered_df) => {
                assert_eq!(filtered_df.height(), 2);
                let names = filtered_df.column("name").unwrap();
                assert_eq!(names.len(), 2);
                if let Ok(AnyValue::String(name1)) = names.get(0) {
                    assert_eq!(name1, "Alice");
                }
                if let Ok(AnyValue::String(name2)) = names.get(1) {
                    assert_eq!(name2, "Charlie");
                }
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_builtin_select_series_with_series() {
        let series = Series::new(PlSmallStr::from("values"), vec![1, 2, 3, 4]);
        let mask_series = Series::new(PlSmallStr::from("mask"), vec![true, false, true, false]);
        let result = builtin_select(&[Value::Series(series), Value::Series(mask_series)]).unwrap();
        match result {
            Value::Series(filtered_series) => {
                assert_eq!(filtered_series.len(), 2);
                if let Ok(AnyValue::Int64(val1)) = filtered_series.get(0) {
                    assert_eq!(val1, 1);
                }
                if let Ok(AnyValue::Int64(val2)) = filtered_series.get(1) {
                    assert_eq!(val2, 3);
                }
            }
            _ => panic!("Expected Series"),
        }
    }

    #[test]
    fn test_builtin_select_single_arg_extended() {
        // Test with object
        let mut obj = std::collections::HashMap::new();
        obj.insert("key".to_string(), Value::String("value".to_string()));
        let result = builtin_select(&[Value::Object(obj.clone())]).unwrap();
        assert_eq!(result, Value::Object(obj));

        // Test with array
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let result = builtin_select(&[arr.clone()]).unwrap();
        assert_eq!(result, arr);

        // Test with float
        let result = builtin_select(&[Value::Float(3.14)]).unwrap();
        assert_eq!(result, Value::Float(3.14));

        // Test with negative int
        let result = builtin_select(&[Value::Int(-1)]).unwrap();
        assert_eq!(result, Value::Int(-1));
    }

    #[test]
    fn test_builtin_select_two_args_extended() {
        use polars::prelude::*;
        // Test with object input
        let mut obj = std::collections::HashMap::new();
        obj.insert("key".to_string(), Value::String("value".to_string()));
        let result = builtin_select(&[Value::Object(obj.clone()), Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::Object(obj.clone()));

        let result = builtin_select(&[Value::Object(obj), Value::Bool(false)]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with DataFrame input
        let df = DataFrame::new(vec![
            Series::new("name".into().into(), vec!["Alice"]).into(),
            Series::new("age".into().into(), vec![25]).into(),
        ])
        .unwrap();
        let result = builtin_select(&[Value::DataFrame(df.clone()), Value::Bool(true)]).unwrap();
        if let Value::DataFrame(result_df) = result {
            assert_eq!(result_df.height(), df.height());
            assert_eq!(result_df.width(), df.width());
        } else {
            panic!("Expected DataFrame result");
        }

        let result = builtin_select(&[Value::DataFrame(df), Value::Bool(false)]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with Series input
        let series = Series::new(PlSmallStr::from("values"), vec![1, 2, 3]);
        let result = builtin_select(&[Value::Series(series.clone()), Value::Bool(true)]).unwrap();
        if let Value::Series(result_series) = result {
            assert_eq!(result_series.name(), series.name());
            assert_eq!(result_series.len(), series.len());
        } else {
            panic!("Expected Series result");
        }

        let result = builtin_select(&[Value::Series(series), Value::Bool(false)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_select_mask_errors() {
        // Mismatched lengths for array mask
        let arr = Value::Array(vec![Value::Int(1)]);
        let mask = Value::Array(vec![Value::Bool(true), Value::Bool(false)]);
        let result = builtin_select(&[arr, mask]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("same length"));

        // Mismatched lengths for DataFrame mask series
        let df = DataFrame::new(vec![Series::new(PlSmallStr::from("a"), vec![1])]).unwrap();
        let mask_series = Series::new(PlSmallStr::from("mask"), vec![true, false]);
        let result = builtin_select(&[Value::DataFrame(df), Value::Series(mask_series)]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("same length"));

        // Mismatched lengths for Series mask series
        let series = Series::new(PlSmallStr::from("values"), vec![1]);
        let mask_series = Series::new(PlSmallStr::from("mask"), vec![true, false]);
        let result = builtin_select(&[Value::Series(series), Value::Series(mask_series)]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("same length"));

        // Mask series with non-booleans
        let series = Series::new(PlSmallStr::from("values"), vec![1, 2]);
        let mask_series = Series::new(
            PlSmallStr::from("mask"),
            vec![AnyValue::Int64(1), AnyValue::Int64(0)],
        ); // Non-booleans
        let result = builtin_select(&[Value::Series(series), Value::Series(mask_series)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must contain booleans"));
    }

    #[test]
    fn test_builtin_select_invalid_args() {
        // Empty args
        let result = builtin_select(&[]);
        assert!(result.is_err());

        // Too many args
        let result = builtin_select(&[Value::Int(1), Value::Int(2), Value::Int(3)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_builtin_filter_array() {
        let arr = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(1),
            Value::Int(3),
        ]);
        let filter_value = Value::Int(1);
        let result = builtin_filter(&[arr, filter_value]).unwrap();
        match result {
            Value::Array(filtered) => {
                assert_eq!(filtered.len(), 2);
                assert_eq!(filtered[0], Value::Int(1));
                assert_eq!(filtered[1], Value::Int(1));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_filter_dataframe() {
        let df = DataFrame::new(vec![
            Series::new(PlSmallStr::from("col1"), vec![1, 2, 1, 3]),
            Series::new(PlSmallStr::from("col2"), vec!["a", "b", "c", "d"]),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);
        let filter_value = Value::Int(1);
        let result = builtin_filter(&[df_value, filter_value]).unwrap();
        match result {
            Value::DataFrame(filtered_df) => {
                assert_eq!(filtered_df.height(), 2);
                let col1 = filtered_df.column("col1").unwrap();
                if let Ok(AnyValue::Int64(val1)) = col1.get(0) {
                    assert_eq!(val1, 1);
                }
                if let Ok(AnyValue::Int64(val2)) = col1.get(1) {
                    assert_eq!(val2, 1);
                }
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_builtin_filter_series() {
        let series = Series::new(
            <&str as Into<PlSmallStr>>::into("values"),
            vec![1i32, 2, 1, 3],
        );
        let series_value = Value::Series(series);
        let filter_value = Value::Int(1);
        let result = builtin_filter(&[series_value, filter_value]).unwrap();
        match result {
            Value::Series(filtered_series) => {
                assert_eq!(filtered_series.len(), 2);
                if let Ok(AnyValue::Int64(val1)) = filtered_series.get(0) {
                    assert_eq!(val1, 1);
                }
                if let Ok(AnyValue::Int64(val2)) = filtered_series.get(1) {
                    assert_eq!(val2, 1);
                }
            }
            _ => panic!("Expected Series"),
        }
    }

    #[test]
    fn test_builtin_filter_no_matches() {
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let filter_value = Value::Int(3);
        let result = builtin_filter(&[arr, filter_value]).unwrap();
        match result {
            Value::Array(filtered) => {
                assert_eq!(filtered.len(), 0);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_builtin_filter_invalid_args() {
        // Too few args
        let result = builtin_filter(&[Value::Array(vec![])]);
        assert!(result.is_err());

        // Too many args
        let result = builtin_filter(&[Value::Array(vec![]), Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_builtin_humanize_number() {
        let registry = BuiltinRegistry::new();

        // Test number formatting
        let result = registry
            .call_function(
                "humanize",
                &[Value::Int(1234567), Value::String("number".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("1,234,567".to_string()));

        // Test currency formatting
        let result = registry
            .call_function(
                "humanize",
                &[Value::Int(123456), Value::String("currency".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("$1,234.56".to_string()));

        // Test bytes formatting
        let result = registry
            .call_function(
                "humanize",
                &[Value::Int(1048576), Value::String("bytes".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("1.0 MB".to_string()));

        // Test percentage formatting
        let result = registry
            .call_function(
                "humanize",
                &[Value::Float(0.85), Value::String("percentage".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("85.0%".to_string()));

        // Test date formatting
        let result = registry
            .call_function(
                "humanize",
                &[
                    Value::String("2023-12-25".to_string()),
                    Value::String("date".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(result, Value::String("December 25, 2023".to_string()));
    }

    #[test]
    fn test_builtin_humanize_auto() {
        let registry = BuiltinRegistry::new();

        // Test auto number formatting
        let result = registry
            .call_function("humanize", &[Value::Int(1234567)])
            .unwrap();
        assert_eq!(result, Value::String("1,234,567".to_string()));

        // Test auto bytes detection
        let result = registry
            .call_function("humanize", &[Value::Int(1048576)])
            .unwrap();
        assert_eq!(result, Value::String("1.0 MB".to_string()));

        // Test auto date detection
        let result = registry
            .call_function("humanize", &[Value::String("2023-12-25".to_string())])
            .unwrap();
        assert_eq!(result, Value::String("December 25, 2023".to_string()));
    }

    #[test]
    fn test_builtin_humanize_invalid_args() {
        let registry = BuiltinRegistry::new();

        // Test wrong number of arguments
        let result = registry.call_function("humanize", &[]);
        assert!(result.is_err());

        let result = registry.call_function(
            "humanize",
            &[
                Value::Int(1),
                Value::String("number".to_string()),
                Value::Int(2),
            ],
        );
        assert!(result.is_err());

        // Test invalid format
        let result = registry.call_function(
            "humanize",
            &[Value::Int(1), Value::String("invalid".to_string())],
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown format"));
    }

    #[test]
    fn test_builtin_tan() {
        let registry = BuiltinRegistry::new();

        // Test tan(0) = 0
        let result = registry.call_function("tan", &[Value::Int(0)]).unwrap();
        if let Value::Float(val) = result {
            assert!((val - 0.0).abs() < 1e-10);
        } else {
            panic!("Expected float result");
        }

        // Test tan(π/4) ≈ 1.0 (45 degrees in radians)
        let pi_over_4 = std::f64::consts::PI / 4.0;
        let result = registry
            .call_function("tan", &[Value::Float(pi_over_4)])
            .unwrap();
        if let Value::Float(val) = result {
            assert!((val - 1.0).abs() < 1e-10);
        } else {
            panic!("Expected float result");
        }

        // Test tan(π/2) should be very large (approaches infinity)
        let pi_over_2 = std::f64::consts::PI / 2.0;
        let result = registry
            .call_function("tan", &[Value::Float(pi_over_2)])
            .unwrap();
        if let Value::Float(val) = result {
            assert!(val.abs() > 1e10); // Very large value (positive or negative)
        } else {
            panic!("Expected float result");
        }

        // Test tan with float (30 degrees in radians)
        let angle_radians = 30.0_f64.to_radians();
        let result = registry
            .call_function("tan", &[Value::Float(angle_radians)])
            .unwrap();
        let expected = angle_radians.tan();
        if let Value::Float(val) = result {
            assert!((val - expected).abs() < 1e-10);
        } else {
            panic!("Expected float result");
        }

        // Test tan with array
        let angle_radians = (45.0_f64).to_radians();
        let result = registry
            .call_function(
                "tan",
                &[Value::Array(vec![
                    Value::Int(0),
                    Value::Float(angle_radians),
                ])],
            )
            .unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            if let Value::Float(val0) = &arr[0] {
                assert!((val0 - 0.0).abs() < 1e-10);
            } else {
                panic!("Expected float in array");
            }
            if let Value::Float(val1) = &arr[1] {
                assert!((val1 - 1.0).abs() < 1e-10);
            } else {
                panic!("Expected float in array");
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_builtin_tan_invalid_args() {
        let registry = BuiltinRegistry::new();

        // Test wrong number of arguments
        let result = registry.call_function("tan", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        let result = registry.call_function("tan", &[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Test with non-numeric value
        let result = registry.call_function("tan", &[Value::String("not a number".to_string())]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires numeric"));

        // Test with array containing non-numeric value
        let result = registry.call_function(
            "tan",
            &[Value::Array(vec![
                Value::Int(1),
                Value::String("invalid".to_string()),
            ])],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires numeric values in array"));
    }

    #[test]
    fn test_builtin_quartile_invalid_args() {
        let registry = BuiltinRegistry::new();

        // Test wrong number of arguments
        let result = registry.call_function("quartile", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 or 2 arguments"));

        let result =
            registry.call_function("quartile", &[Value::Int(1), Value::Int(2), Value::Int(3)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 or 2 arguments"));

        // Test invalid quartile number
        let result = registry.call_function(
            "quartile",
            &[Value::Array(vec![Value::Int(1)]), Value::Int(0)],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("second argument must be 1, 2, or 3"));

        let result = registry.call_function(
            "quartile",
            &[Value::Array(vec![Value::Int(1)]), Value::Int(4)],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("second argument must be 1, 2, or 3"));

        let result = registry.call_function(
            "quartile",
            &[
                Value::Array(vec![Value::Int(1)]),
                Value::String("invalid".to_string()),
            ],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("second argument must be 1, 2, or 3"));

        // Test invalid input type
        let result = registry.call_function("quartile", &[Value::String("not array".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires array, DataFrame, or Series"));
    }

    #[test]
    fn test_builtin_array_pop_array() {
        let registry = BuiltinRegistry::new();

        // Test popping from array with elements
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let result = registry.call_function("array_pop", &[arr]).unwrap();
        assert_eq!(result, Value::Int(3));

        // Test popping from single-element array
        let arr = Value::Array(vec![Value::String("hello".to_string())]);
        let result = registry.call_function("array_pop", &[arr]).unwrap();
        assert_eq!(result, Value::String("hello".to_string()));

        // Test popping from empty array
        let arr = Value::Array(vec![]);
        let result = registry.call_function("array_pop", &[arr]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_array_pop_dataframe() {
        let registry = BuiltinRegistry::new();

        // Test popping from DataFrame with rows
        let df = DataFrame::new(vec![
            ChunkedArray::from_vec(
                PlSmallStr::from("name"),
                vec![
                    "Alice".to_string(),
                    "Bob".to_string(),
                    "Charlie".to_string(),
                ],
            )
            .into_column(),
            Series::new(PlSmallStr::from("age"), vec![25, 30, 35]).into(),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);
        let result = registry.call_function("array_pop", &[df_value]).unwrap();
        match result {
            Value::Object(obj) => {
                assert_eq!(obj.get("name"), Some(&Value::String("Charlie".to_string())));
                assert_eq!(obj.get("age"), Some(&Value::Int(35)));
            }
            _ => panic!("Expected object result"),
        }

        // Test popping from single-row DataFrame
        let df = DataFrame::new(vec![
            Series::new(PlSmallStr::from("id"), vec![42]),
            Series::new(PlSmallStr::from("value"), vec!["test".to_string()]),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);
        let result = registry.call_function("array_pop", &[df_value]).unwrap();
        match result {
            Value::Object(obj) => {
                assert_eq!(obj.get("id"), Some(&Value::Int(42)));
                assert_eq!(obj.get("value"), Some(&Value::String("test".to_string())));
            }
            _ => panic!("Expected object result"),
        }

        // Test popping from empty DataFrame
        let df = DataFrame::new(vec![Series::new(
            PlSmallStr::from("empty"),
            Vec::<String>::new(),
        )])
        .unwrap();
        let df_value = Value::DataFrame(df);
        let result = registry.call_function("array_pop", &[df_value]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_array_pop_series() {
        let registry = BuiltinRegistry::new();

        // Test popping from list series
        let list_series = Series::new(
            PlSmallStr::from("lists"),
            vec![Series::new(PlSmallStr::from(""), vec![1, 2, 3])],
        );
        let series_value = Value::Series(list_series);
        let result = registry
            .call_function("array_pop", &[series_value])
            .unwrap();
        assert_eq!(result, Value::Int(3));

        // Test popping from empty list series
        let list_series = Series::new(
            "empty_lists".into(),
            vec![Series::new(PlSmallStr::from(""), Vec::<i32>::new())],
        );
        let series_value = Value::Series(list_series);
        let result = registry
            .call_function("array_pop", &[series_value])
            .unwrap();
        assert_eq!(result, Value::Null);

        // Test with non-list series (should error)
        let int_series = Series::new(PlSmallStr::from("ints"), vec![1, 2, 3]);
        let series_value = Value::Series(int_series);
        let result = registry.call_function("array_pop", &[series_value]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires an array, DataFrame, or list series"));
    }

    #[test]
    fn test_builtin_array_pop_invalid_args() {
        let registry = BuiltinRegistry::new();

        // Test wrong number of arguments
        let result = registry.call_function("array_pop", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        let arr = Value::Array(vec![Value::Int(1)]);
        let result = registry.call_function("array_pop", &[arr, Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Test invalid input type
        let result = registry.call_function("array_pop", &[Value::String("not array".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires an array, DataFrame, or list series"));
    }

    #[test]
    fn test_builtin_mround() {
        // Test basic rounding to nearest multiple
        let result = builtin::mround::builtin_mround(&[Value::Int(17), Value::Int(5)]).unwrap();
        assert_eq!(result, Value::Float(15.0));

        let result = builtin::mround::builtin_mround(&[Value::Int(18), Value::Int(5)]).unwrap();
        assert_eq!(result, Value::Float(20.0));

        let result = builtin::mround::builtin_mround(&[Value::Int(15), Value::Int(5)]).unwrap();
        assert_eq!(result, Value::Float(15.0));

        // Test with floats
        let result = builtin::mround::builtin_mround(&[Value::Float(17.3), Value::Int(5)]).unwrap();
        assert_eq!(result, Value::Float(15.0));

        let result = builtin::mround::builtin_mround(&[Value::Float(18.7), Value::Int(5)]).unwrap();
        assert_eq!(result, Value::Float(20.0));

        // Test with string numbers
        let result =
            builtin::mround::builtin_mround(&[Value::String("17".to_string()), Value::Int(5)])
                .unwrap();
        assert_eq!(result, Value::Float(15.0));

        let result =
            builtin::mround::builtin_mround(&[Value::String("18.5".to_string()), Value::Int(5)])
                .unwrap();
        assert_eq!(result, Value::Float(20.0));

        // Test with string multiple
        let result =
            builtin::mround::builtin_mround(&[Value::Int(17), Value::String("5".to_string())])
                .unwrap();
        assert_eq!(result, Value::Float(15.0));

        // Test multiple 0
        let result = builtin::mround::builtin_mround(&[Value::Int(17), Value::Int(0)]).unwrap();
        assert_eq!(result, Value::Float(17.0));

        // Test invalid number
        let result = builtin::mround::builtin_mround(&[
            Value::String("not a number".to_string()),
            Value::Int(5),
        ]);
        assert!(result.is_err());

        // Test invalid multiple
        let result = builtin::mround::builtin_mround(&[
            Value::Int(17),
            Value::String("not a number".to_string()),
        ]);
        assert!(result.is_err());

        // Test wrong number of arguments
        let result = builtin::mround::builtin_mround(&[Value::Int(17)]);
        assert!(result.is_err());

        let result =
            builtin::mround::builtin_mround(&[Value::Int(17), Value::Int(5), Value::Int(1)]);
        assert!(result.is_err());
    }

    #[test]
    #[cfg(feature = "rand")]
    fn test_builtin_rand() {
        // Test rand with no arguments
        let result = builtin::rand::builtin_rand(&[]).unwrap();
        match result {
            Value::Float(f) => {
                assert!(f >= 0.0 && f < 1.0);
            }
            _ => panic!("Expected Float"),
        }

        // Test rand with one argument (should be ignored)
        let result = builtin::rand::builtin_rand(&[Value::Int(42)]).unwrap();
        match result {
            Value::Float(f) => {
                assert!(f >= 0.0 && f < 1.0);
            }
            _ => panic!("Expected Float"),
        }

        // Test rand with too many arguments
        let result = builtin::rand::builtin_rand(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_builtin_histogram_array() {
        let arr = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
            Value::Int(6),
            Value::Int(7),
            Value::Int(8),
            Value::Int(9),
            Value::Int(10),
        ]);
        let result = builtin_histogram(&[arr]).unwrap();
        match result {
            Value::Object(obj) => {
                assert!(obj.contains_key("counts"));
                assert!(obj.contains_key("bins"));
                if let Value::Array(counts) = &obj["counts"] {
                    assert_eq!(counts.len(), 10);
                    // Check some counts
                    assert_eq!(counts[0], Value::Int(1)); // bin for 1
                    assert_eq!(counts[9], Value::Int(1)); // bin for 10
                } else {
                    panic!("counts should be array");
                }
                if let Value::Array(bins) = &obj["bins"] {
                    assert_eq!(bins.len(), 11); // 10 bins + 1
                    assert_eq!(bins[0], Value::Float(1.0));
                    assert_eq!(bins[10], Value::Float(10.0));
                } else {
                    panic!("bins should be array");
                }
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_builtin_histogram_series() {
        let series = Series::new(
            PlSmallStr::from("values"),
            vec![1i64, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        );
        let series_value = Value::Series(series);
        let result = builtin_histogram(&[series_value]).unwrap();
        match result {
            Value::Object(obj) => {
                assert!(obj.contains_key("counts"));
                assert!(obj.contains_key("bins"));
                if let Value::Array(counts) = &obj["counts"] {
                    assert_eq!(counts.len(), 10);
                }
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_builtin_histogram_empty() {
        let arr = Value::Array(vec![]);
        let result = builtin_histogram(&[arr]).unwrap();
        match result {
            Value::Object(obj) => {
                if let Value::Array(counts) = &obj["counts"] {
                    assert_eq!(counts.len(), 0);
                }
                if let Value::Array(bins) = &obj["bins"] {
                    assert_eq!(bins.len(), 0);
                }
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_builtin_histogram_custom_bins() {
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let result = builtin_histogram(&[arr, Value::Int(5)]).unwrap();
        match result {
            Value::Object(obj) => {
                if let Value::Array(counts) = &obj["counts"] {
                    assert_eq!(counts.len(), 5);
                }
                if let Value::Array(bins) = &obj["bins"] {
                    assert_eq!(bins.len(), 6); // 5 + 1
                }
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_builtin_histogram_invalid_args() {
        // No args
        let result = builtin_histogram(&[]);
        assert!(result.is_err());

        // Too many args
        let arr = Value::Array(vec![Value::Int(1)]);
        let result = builtin_histogram(&[arr.clone(), Value::Int(5), Value::Int(6)]);
        assert!(result.is_err());

        // Invalid bins
        let result = builtin_histogram(&[arr.clone(), Value::Int(0)]);
        assert!(result.is_err());

        // Non-numeric bins
        let result = builtin_histogram(&[arr.clone(), Value::String("5".to_string())]);
        assert!(result.is_err());

        // Invalid input type
        let result = builtin_histogram(&[Value::Int(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_builtin_transliterate() {
        // Test basic cyrillic to latin transliteration
        let result = builtin_transliterate(&[
            Value::String("Привет".to_string()),
            Value::String("cyrillic".to_string()),
            Value::String("latin".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("Privet".to_string()));

        // Test another word
        let result = builtin_transliterate(&[
            Value::String("Москва".to_string()),
            Value::String("cyrillic".to_string()),
            Value::String("latin".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("Moskva".to_string()));

        // Test with mixed case
        let result = builtin_transliterate(&[
            Value::String("Александр".to_string()),
            Value::String("cyrillic".to_string()),
            Value::String("latin".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("Aleksandr".to_string()));

        // Test array input
        let result = builtin_transliterate(&[
            Value::Array(vec![
                Value::String("Привет".to_string()),
                Value::String("Москва".to_string()),
                Value::Int(123), // Non-string should be unchanged
            ]),
            Value::String("cyrillic".to_string()),
            Value::String("latin".to_string()),
        ])
        .unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::String("Privet".to_string()),
                Value::String("Moskva".to_string()),
                Value::Int(123)
            ])
        );

        // Test DataFrame input
        let text_series = Series::new(PlSmallStr::from("text"), &["Привет", "Москва", "тест"]);
        let df = DataFrame::new(vec![text_series]).unwrap();
        let result = builtin_transliterate(&[
            Value::DataFrame(df),
            Value::String("cyrillic".to_string()),
            Value::String("latin".to_string()),
        ])
        .unwrap();
        if let Value::DataFrame(result_df) = result {
            let text_col = result_df.column("text").unwrap();
            let values: Vec<String> = text_col
                .str()
                .unwrap()
                .into_iter()
                .map(|s| s.unwrap_or("").to_string())
                .collect();
            assert_eq!(values, vec!["Privet", "Moskva", "test"]);
        } else {
            panic!("Expected DataFrame");
        }

        // Test unsupported script conversion
        let result = builtin_transliterate(&[
            Value::String("hello".to_string()),
            Value::String("latin".to_string()),
            Value::String("cyrillic".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not supported"));

        // Test invalid arguments
        let result = builtin_transliterate(&[Value::String("test".to_string())]);
        assert!(result.is_err());

        let result = builtin_transliterate(&[
            Value::Int(123),
            Value::String("cyrillic".to_string()),
            Value::String("latin".to_string()),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_transliterate_registry() {
        let registry = BuiltinRegistry::new();
        assert!(registry.has_function("transliterate"));
        let result = registry
            .call_function(
                "transliterate",
                &[
                    Value::String("Привет".to_string()),
                    Value::String("cyrillic".to_string()),
                    Value::String("latin".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(result, Value::String("Privet".to_string()));
    }

    #[test]
    fn test_builtin_length() {
        let registry = BuiltinRegistry::new();

        // Test with arrays
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let result = registry.call_function("length", &[arr]).unwrap();
        assert_eq!(result, Value::Int(3));

        // Test with mixed type array
        let mixed_arr = Value::Array(vec![
            Value::Int(1),
            Value::String("hello".to_string()),
            Value::Bool(true),
        ]);
        let result = registry.call_function("length", &[mixed_arr]).unwrap();
        assert_eq!(result, Value::Int(3));

        // Test with empty array
        let empty_arr = Value::Array(vec![]);
        let result = registry.call_function("length", &[empty_arr]).unwrap();
        assert_eq!(result, Value::Int(0));

        // Test with strings
        let str_val = Value::String("hello".to_string());
        let result = registry.call_function("length", &[str_val]).unwrap();
        assert_eq!(result, Value::Int(5));

        // Test with Unicode string
        let unicode_str = Value::String("héllo wörld".to_string());
        let result = registry.call_function("length", &[unicode_str]).unwrap();
        assert_eq!(result, Value::Int(11)); // Character count, not byte count

        // Test with objects
        let mut obj = HashMap::new();
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("b".to_string(), Value::Int(2));
        let obj_val = Value::Object(obj);
        let result = registry.call_function("length", &[obj_val]).unwrap();
        assert_eq!(result, Value::Int(2));

        // Test with DataFrame
        let df = create_test_dataframe();
        let df_val = Value::DataFrame(df);
        let result = registry.call_function("length", &[df_val]).unwrap();
        assert_eq!(result, Value::Int(3)); // 3 rows

        // Test with Series
        let series = Series::new(PlSmallStr::from("test"), &[1, 2, 3, 4]);
        let series_val = Value::Series(series);
        let result = registry.call_function("length", &[series_val]).unwrap();
        assert_eq!(result, Value::Int(4));

        // Test with null
        let result = registry.call_function("length", &[Value::Null]).unwrap();
        assert_eq!(result, Value::Int(0));

        // Test with scalar values
        let result = registry.call_function("length", &[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Int(1));

        let result = registry
            .call_function("length", &[Value::Bool(true)])
            .unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    #[test]
    fn test_builtin_keys() {
        let registry = BuiltinRegistry::new();

        // Test object keys
        let mut obj = HashMap::new();
        obj.insert("b".to_string(), Value::Int(2));
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("c".to_string(), Value::Int(3));
        let obj_val = Value::Object(obj);
        let result = registry.call_function("keys", &[obj_val]).unwrap();
        if let Value::Array(keys) = result {
            assert_eq!(keys.len(), 3);
            // Keys should be sorted
            assert_eq!(keys[0], Value::String("a".to_string()));
            assert_eq!(keys[1], Value::String("b".to_string()));
            assert_eq!(keys[2], Value::String("c".to_string()));
        } else {
            panic!("Expected array of keys");
        }

        // Test DataFrame keys (column names)
        let df = create_test_dataframe();
        let df_val = Value::DataFrame(df);
        let result = registry.call_function("keys", &[df_val]).unwrap();
        if let Value::Array(keys) = result {
            assert_eq!(keys.len(), 3);
            assert!(keys.contains(&Value::String("name".to_string())));
            assert!(keys.contains(&Value::String("age".to_string())));
            assert!(keys.contains(&Value::String("score".to_string())));
        } else {
            panic!("Expected array of column names");
        }
    }

    #[test]
    fn test_builtin_values() {
        let registry = BuiltinRegistry::new();

        // Test object values
        let mut obj = HashMap::new();
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("b".to_string(), Value::Int(2));
        let obj_val = Value::Object(obj);
        let result = registry.call_function("values", &[obj_val]).unwrap();
        if let Value::Array(values) = result {
            assert_eq!(values.len(), 2);
            assert!(values.contains(&Value::Int(1)));
            assert!(values.contains(&Value::Int(2)));
        } else {
            panic!("Expected array of values");
        }
    }

    #[test]
    fn test_builtin_type() {
        let registry = BuiltinRegistry::new();

        // Test different types
        let result = registry.call_function("type", &[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::String("integer".to_string()));

        let result = registry
            .call_function("type", &[Value::Float(3.14)])
            .unwrap();
        assert_eq!(result, Value::String("float".to_string()));

        let result = registry
            .call_function("type", &[Value::String("hello".to_string())])
            .unwrap();
        assert_eq!(result, Value::String("string".to_string()));

        let result = registry
            .call_function("type", &[Value::Bool(true)])
            .unwrap();
        assert_eq!(result, Value::String("boolean".to_string()));

        let result = registry.call_function("type", &[Value::Null]).unwrap();
        assert_eq!(result, Value::String("null".to_string()));

        let arr = Value::Array(vec![Value::Int(1)]);
        let result = registry.call_function("type", &[arr]).unwrap();
        assert_eq!(result, Value::String("array".to_string()));

        let mut obj = HashMap::new();
        obj.insert("key".to_string(), Value::Int(1));
        let obj_val = Value::Object(obj);
        let result = registry.call_function("type", &[obj_val]).unwrap();
        assert_eq!(result, Value::String("object".to_string()));
    }

    #[test]
    fn test_builtin_empty() {
        let registry = BuiltinRegistry::new();

        // Test empty array
        let empty_arr = Value::Array(vec![]);
        let result = registry.call_function("empty", &[empty_arr]).unwrap();
        assert_eq!(result, Value::Bool(true));

        // Test non-empty array
        let arr = Value::Array(vec![Value::Int(1)]);
        let result = registry.call_function("empty", &[arr]).unwrap();
        assert_eq!(result, Value::Bool(false));

        // Test empty object
        let empty_obj = Value::Object(HashMap::new());
        let result = registry.call_function("empty", &[empty_obj]).unwrap();
        assert_eq!(result, Value::Bool(true));

        // Test non-empty object
        let mut obj = HashMap::new();
        obj.insert("key".to_string(), Value::Int(1));
        let obj_val = Value::Object(obj);
        let result = registry.call_function("empty", &[obj_val]).unwrap();
        assert_eq!(result, Value::Bool(false));

        // Test empty string
        let empty_str = Value::String("".to_string());
        let result = registry.call_function("empty", &[empty_str]).unwrap();
        assert_eq!(result, Value::Bool(true));

        // Test non-empty string
        let str_val = Value::String("hello".to_string());
        let result = registry.call_function("empty", &[str_val]).unwrap();
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_builtin_generate_uuidv4() {
        let registry = BuiltinRegistry::new();

        // Test no arguments
        let result1 = registry.call_function("generate_uuidv4", &[]).unwrap();
        let result2 = registry.call_function("generate_uuidv4", &[]).unwrap();

        match (&result1, &result2) {
            (Value::String(uuid1), Value::String(uuid2)) => {
                // Check they are different
                assert_ne!(uuid1, uuid2);

                // Check they are valid UUID v4 format
                assert_eq!(uuid1.len(), 36); // UUID v4 string length
                assert_eq!(uuid2.len(), 36);

                // Check version (4) and variant bits
                let uuid1_parsed = uuid::Uuid::parse_str(uuid1).unwrap();
                let uuid2_parsed = uuid::Uuid::parse_str(uuid2).unwrap();

                assert_eq!(uuid1_parsed.get_version_num(), 4);
                assert_eq!(uuid2_parsed.get_version_num(), 4);
            }
            _ => panic!("Expected strings"),
        }

        // Test with arguments should fail
        let result = registry.call_function("generate_uuidv4", &[Value::Int(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_builtin_avg_ifs_array() {
        let registry = BuiltinRegistry::new();

        // Test basic avg_ifs with arrays
        let values = Value::Array(vec![
            Value::Int(10),
            Value::Int(20),
            Value::Int(30),
            Value::Int(40),
        ]);
        let mask1 = Value::Array(vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(false),
        ]);
        let mask2 = Value::Array(vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
        ]);
        let result = registry
            .call_function("avg_ifs", &[values, mask1, mask2])
            .unwrap();
        assert_eq!(result, Value::Float(20.0)); // (10 + 30) / 2 = 20.0

        // Test with floats
        let values = Value::Array(vec![
            Value::Float(1.5),
            Value::Float(2.5),
            Value::Float(3.5),
        ]);
        let mask1 = Value::Array(vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
        ]);
        let mask2 = Value::Array(vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
        ]);
        let result = registry
            .call_function("avg_ifs", &[values, mask1, mask2])
            .unwrap();
        assert_eq!(result, Value::Float(2.5)); // (1.5 + 3.5) / 2

        // Test with no matches
        let values = Value::Array(vec![Value::Int(10), Value::Int(20)]);
        let mask1 = Value::Array(vec![Value::Bool(false), Value::Bool(false)]);
        let mask2 = Value::Array(vec![Value::Bool(true), Value::Bool(true)]);
        let result = registry
            .call_function("avg_ifs", &[values, mask1, mask2])
            .unwrap();
        assert_eq!(result, Value::Null);

        // Test with two masks (same as first test but different values)
        let values = Value::Array(vec![Value::Int(5), Value::Int(15), Value::Int(25)]);
        let mask1 = Value::Array(vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
        ]);
        let mask2 = Value::Array(vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
        ]);
        let result = registry
            .call_function("avg_ifs", &[values, mask1, mask2])
            .unwrap();
        assert_eq!(result, Value::Float(5.0)); // Only first value (5) satisfies both masks
    }

    #[test]
    fn test_builtin_avg_ifs_dataframe() {
        let registry = BuiltinRegistry::new();

        // Create test DataFrame
        let df = DataFrame::new(vec![
            Series::new(PlSmallStr::from("a"), vec![10i64, 20, 30]),
            Series::new(PlSmallStr::from("b"), vec![1i64, 2, 3]),
            Series::new(PlSmallStr::from("c"), vec![100.0f64, 200.0, 300.0]),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        // Create masks
        let mask1 = Series::new(PlSmallStr::from("mask1"), vec![true, false, true]);
        let mask1_value = Value::Series(mask1);
        let mask2 = Series::new(PlSmallStr::from("mask2"), vec![true, true, true]);
        let mask2_value = Value::Series(mask2);

        let result = registry
            .call_function("avg_ifs", &[df_value, mask1_value, mask2_value])
            .unwrap();
        match result {
            Value::Object(obj) => {
                // Should average columns where masks are true
                assert_eq!(obj.get("a"), Some(&Value::Float(20.0))); // (10 + 30) / 2
                assert_eq!(obj.get("b"), Some(&Value::Float(2.0))); // (1 + 3) / 2
                assert_eq!(obj.get("c"), Some(&Value::Float(200.0))); // (100 + 300) / 2
            }
            _ => panic!("Expected object"),
        }
    }

    #[test]
    fn test_builtin_avg_ifs_series() {
        let registry = BuiltinRegistry::new();

        // Test with numeric series
        let series = Series::new(PlSmallStr::from("values"), vec![10.0f64, 20.0, 30.0, 40.0]);
        let series_value = Value::Series(series);

        let mask1 = Series::new(PlSmallStr::from("mask1"), vec![true, false, true, false]);
        let mask1_value = Value::Series(mask1);
        let mask2 = Series::new(PlSmallStr::from("mask2"), vec![true, true, true, false]);
        let mask2_value = Value::Series(mask2);

        let result = registry
            .call_function("avg_ifs", &[series_value, mask1_value, mask2_value])
            .unwrap();
        assert_eq!(result, Value::Float(20.0)); // (10.0 + 30.0) / 2 = 20.0

        // Test with no matches
        let series = Series::new(PlSmallStr::from("values"), vec![10.0f64, 20.0]);
        let series_value = Value::Series(series);
        let mask1 = Series::new(PlSmallStr::from("mask1"), vec![false, false]);
        let mask1_value = Value::Series(mask1);
        let mask2 = Series::new(PlSmallStr::from("mask2"), vec![true, true]);
        let mask2_value = Value::Series(mask2);

        let result = registry
            .call_function("avg_ifs", &[series_value, mask1_value, mask2_value])
            .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_avg_ifs_invalid_args() {
        let registry = BuiltinRegistry::new();

        // Test wrong number of arguments
        let result = registry.call_function("avg_ifs", &[Value::Array(vec![Value::Int(1)])]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects at least 3 arguments"));

        // Test mismatched array lengths
        let values = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let mask1 = Value::Array(vec![Value::Bool(true)]); // Different length
        let mask2 = Value::Array(vec![Value::Bool(true), Value::Bool(false)]);
        let result = registry.call_function("avg_ifs", &[values, mask1, mask2]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("all masks must have same length as values"));

        // Test non-array mask for arrays
        let values = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let mask1 = Value::String("not array".to_string());
        let mask2 = Value::Array(vec![Value::Bool(true), Value::Bool(false)]);
        let result = registry.call_function("avg_ifs", &[values, mask1, mask2]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("all masks must be arrays"));

        // Test invalid input type
        let result = registry.call_function(
            "avg_ifs",
            &[
                Value::String("not array".to_string()),
                Value::Array(vec![Value::Bool(true)]),
                Value::Array(vec![Value::Bool(true)]),
            ],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("first argument must be array, DataFrame, or Series"));
    }

    #[test]
    fn test_builtin_count_if_array() {
        let registry = BuiltinRegistry::new();

        // Test basic count_if with arrays
        let values = Value::Array(vec![
            Value::Int(10),
            Value::Int(20),
            Value::Int(30),
            Value::Int(40),
        ]);
        let mask = Value::Array(vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(false),
        ]);
        let result = registry.call_function("count_if", &[values, mask]).unwrap();
        assert_eq!(result, Value::Int(2)); // Two true values

        // Test with all true
        let values = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let mask = Value::Array(vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
        ]);
        let result = registry.call_function("count_if", &[values, mask]).unwrap();
        assert_eq!(result, Value::Int(3));

        // Test with all false
        let values = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let mask = Value::Array(vec![Value::Bool(false), Value::Bool(false)]);
        let result = registry.call_function("count_if", &[values, mask]).unwrap();
        assert_eq!(result, Value::Int(0));

        // Test with truthy values
        let values = Value::Array(vec![
            Value::Int(1),
            Value::Int(0),
            Value::String("hello".to_string()),
            Value::String("".to_string()),
        ]);
        let mask = Value::Array(vec![
            Value::Int(1),
            Value::Int(0),
            Value::String("hello".to_string()),
            Value::String("".to_string()),
        ]);
        let result = registry.call_function("count_if", &[values, mask]).unwrap();
        assert_eq!(result, Value::Int(2)); // 1 and "hello" are truthy

        // Test with empty arrays
        let values = Value::Array(vec![]);
        let mask = Value::Array(vec![]);
        let result = registry.call_function("count_if", &[values, mask]).unwrap();
        assert_eq!(result, Value::Int(0));

        // Test with null values in mask
        let values = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let mask = Value::Array(vec![Value::Bool(true), Value::Null, Value::Bool(false)]);
        let result = registry.call_function("count_if", &[values, mask]).unwrap();
        assert_eq!(result, Value::Int(1)); // Only first is true, null is falsy
    }

    #[test]
    fn test_builtin_count_if_dataframe() {
        let registry = BuiltinRegistry::new();

        // Create test DataFrame
        let df = DataFrame::new(vec![
            Series::new(PlSmallStr::from("a"), vec![10i64, 20, 30]),
            Series::new(PlSmallStr::from("b"), vec![1i64, 2, 3]),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        // Create mask
        let mask = Series::new(PlSmallStr::from("mask"), vec![true, false, true]);
        let mask_value = Value::Series(mask);

        let result = registry
            .call_function("count_if", &[df_value, mask_value])
            .unwrap();
        assert_eq!(result, Value::Int(2)); // Two true values in mask
    }

    #[test]
    fn test_builtin_count_if_series() {
        let registry = BuiltinRegistry::new();

        // Test with numeric series
        let series = Series::new(PlSmallStr::from("values"), vec![10.0f64, 20.0, 30.0, 40.0]);
        let series_value = Value::Series(series);

        let mask = Series::new(PlSmallStr::from("mask"), vec![true, false, true, false]);
        let mask_value = Value::Series(mask);

        let result = registry
            .call_function("count_if", &[series_value, mask_value])
            .unwrap();
        assert_eq!(result, Value::Int(2)); // Two true values
    }

    #[test]
    fn test_builtin_count_if_invalid_args() {
        let registry = BuiltinRegistry::new();

        // Test wrong number of arguments
        let result = registry.call_function("count_if", &[Value::Array(vec![Value::Int(1)])]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));

        // Test mismatched array lengths
        let values = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let mask = Value::Array(vec![Value::Bool(true)]); // Different length
        let result = registry.call_function("count_if", &[values, mask]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("collection and mask arrays must have same length"));

        // Test invalid types
        let values = Value::String("not array".to_string());
        let mask = Value::Array(vec![Value::Bool(true)]);
        let result = registry.call_function("count_if", &[values, mask]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires (array, array) or (dataframe/series, series)"));
    }

    #[test]
    fn test_builtin_sort() {
        let registry = BuiltinRegistry::new();

        // Test sorting an array
        let arr = Value::Array(vec![Value::Int(3), Value::Int(1), Value::Int(2)]);
        let result = registry.call_function("sort", &[arr]).unwrap();
        if let Value::Array(sorted) = result {
            assert_eq!(sorted, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        } else {
            panic!("Expected sorted array");
        }

        // Test sorting strings
        let arr = Value::Array(vec![
            Value::String("c".to_string()),
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ]);
        let result = registry.call_function("sort", &[arr]).unwrap();
        if let Value::Array(sorted) = result {
            assert_eq!(
                sorted,
                vec![
                    Value::String("a".to_string()),
                    Value::String("b".to_string()),
                    Value::String("c".to_string())
                ]
            );
        } else {
            panic!("Expected sorted array");
        }

        // Test sorting mixed types (should not change order for unsupported comparisons)
        let arr = Value::Array(vec![Value::Int(1), Value::String("a".to_string())]);
        let result = registry.call_function("sort", &[arr.clone()]).unwrap();
        assert_eq!(result, arr); // Should remain unchanged

        // Test invalid arguments
        let result = registry.call_function("sort", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        let result = registry.call_function("sort", &[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_builtin_stdev_s_array() {
        let registry = BuiltinRegistry::new();

        // Test with array of integers
        let arr = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ]);
        let result = registry.call_function("stdev_s", &[arr]).unwrap();
        match result {
            Value::Float(stdev) => {
                // Sample standard deviation of [1,2,3,4,5] should be approximately 1.581
                assert!((stdev - 1.58113883).abs() < 0.0001);
            }
            _ => panic!("Expected float result"),
        }

        // Test with array of floats
        let arr = Value::Array(vec![
            Value::Float(1.0),
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(4.0),
            Value::Float(5.0),
        ]);
        let result = registry.call_function("stdev_s", &[arr]).unwrap();
        match result {
            Value::Float(stdev) => {
                assert!((stdev - 1.58113883).abs() < 0.0001);
            }
            _ => panic!("Expected float result"),
        }

        // Test with mixed int/float
        let arr = Value::Array(vec![Value::Int(1), Value::Float(2.0), Value::Int(3)]);
        let result = registry.call_function("stdev_s", &[arr]).unwrap();
        match result {
            Value::Float(stdev) => {
                // Sample std of [1,2,3] is 1.0
                assert!((stdev - 1.0).abs() < 0.0001);
            }
            _ => panic!("Expected float result"),
        }

        // Test with less than 2 values (should return null)
        let arr = Value::Array(vec![Value::Int(1)]);
        let result = registry.call_function("stdev_s", &[arr]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with empty array
        let arr = Value::Array(vec![]);
        let result = registry.call_function("stdev_s", &[arr]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with non-numeric values (should be ignored)
        let arr = Value::Array(vec![
            Value::Int(1),
            Value::String("test".to_string()),
            Value::Int(3),
        ]);
        let result = registry.call_function("stdev_s", &[arr]).unwrap();
        match result {
            Value::Float(stdev) => {
                // Only [1,3] should be considered, std is 1.414...
                assert!((stdev - 1.414213562).abs() < 0.0001);
            }
            _ => panic!("Expected float result"),
        }
    }

    #[test]
    fn test_builtin_stdev_s_invalid_args() {
        let registry = BuiltinRegistry::new();

        // Test with no arguments
        let result = registry.call_function("stdev_s", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Test with too many arguments
        let result = registry.call_function("stdev_s", &[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Test with invalid type
        let result = registry.call_function("stdev_s", &[Value::String("test".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires array, DataFrame, or Series"));
    }

    #[test]
    fn test_builtin_stdev_s_dataframe() {
        let registry = BuiltinRegistry::new();

        // Create a simple DataFrame
        let df = DataFrame::new(vec![
            Series::new(PlSmallStr::from("col1"), &[1.0, 2.0, 3.0]),
            Series::new(PlSmallStr::from("col2"), &[4.0, 5.0, 6.0]),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        let result = registry.call_function("stdev_s", &[df_value]).unwrap();
        match result {
            Value::Object(obj) => {
                // Should return object with null values (placeholder implementation)
                assert_eq!(obj.get("col1"), Some(&Value::Null));
                assert_eq!(obj.get("col2"), Some(&Value::Null));
            }
            _ => panic!("Expected object result"),
        }
    }

    #[test]
    fn test_builtin_stdev_s_series() {
        let registry = BuiltinRegistry::new();

        // Create a numeric series
        let series = Series::new(PlSmallStr::from("test"), &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let series_value = Value::Series(series);

        let result = registry.call_function("stdev_s", &[series_value]).unwrap();
        // Should return null (placeholder implementation)
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_std_array() {
        let registry = BuiltinRegistry::new();

        // Test with array of integers
        let arr = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ]);
        let result = registry.call_function("std", &[arr]).unwrap();
        match result {
            Value::Float(stdev) => {
                // Sample standard deviation of [1,2,3,4,5] should be approximately 1.581
                assert!((stdev - 1.58113883).abs() < 0.0001);
            }
            _ => panic!("Expected float result"),
        }

        // Test with array of floats
        let arr = Value::Array(vec![
            Value::Float(1.0),
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(4.0),
            Value::Float(5.0),
        ]);
        let result = registry.call_function("std", &[arr]).unwrap();
        match result {
            Value::Float(stdev) => {
                assert!((stdev - 1.58113883).abs() < 0.0001);
            }
            _ => panic!("Expected float result"),
        }

        // Test with mixed int/float
        let arr = Value::Array(vec![Value::Int(1), Value::Float(2.0), Value::Int(3)]);
        let result = registry.call_function("std", &[arr]).unwrap();
        match result {
            Value::Float(stdev) => {
                // Sample std of [1,2,3] is 1.0
                assert!((stdev - 1.0).abs() < 0.0001);
            }
            _ => panic!("Expected float result"),
        }

        // Test with less than 2 values (should return null)
        let arr = Value::Array(vec![Value::Int(1)]);
        let result = registry.call_function("std", &[arr]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with empty array
        let arr = Value::Array(vec![]);
        let result = registry.call_function("std", &[arr]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with non-numeric values (should be ignored)
        let arr = Value::Array(vec![
            Value::Int(1),
            Value::String("test".to_string()),
            Value::Int(3),
        ]);
        let result = registry.call_function("std", &[arr]).unwrap();
        match result {
            Value::Float(stdev) => {
                // Only [1,3] should be considered, std is 1.414...
                assert!((stdev - 1.414213562).abs() < 0.0001);
            }
            _ => panic!("Expected float result"),
        }
    }

    #[test]
    fn test_builtin_std_invalid_args() {
        let registry = BuiltinRegistry::new();

        // Test with no arguments
        let result = registry.call_function("std", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Test with too many arguments
        let result = registry.call_function("std", &[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Test with invalid type
        let result = registry.call_function("std", &[Value::String("test".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires array, DataFrame, or Series"));
    }

    #[test]
    fn test_builtin_std_dataframe() {
        let registry = BuiltinRegistry::new();

        // Create a simple DataFrame
        let df = DataFrame::new(vec![
            Series::new(PlSmallStr::from("col1"), &[1.0, 2.0, 3.0]),
            Series::new(PlSmallStr::from("col2"), &[4.0, 5.0, 6.0]),
        ])
        .unwrap();
        let df_value = Value::DataFrame(df);

        let result = registry.call_function("std", &[df_value]).unwrap();
        match result {
            Value::Object(obj) => {
                // Should return object with null values (placeholder implementation)
                assert_eq!(obj.get("col1"), Some(&Value::Null));
                assert_eq!(obj.get("col2"), Some(&Value::Null));
            }
            _ => panic!("Expected object result"),
        }
    }

    #[test]
    fn test_builtin_std_series() {
        let registry = BuiltinRegistry::new();

        // Create a numeric series
        let series = Series::new(PlSmallStr::from("test"), &[1.0, 2.0, 3.0, 4.0, 5.0]);
        let series_value = Value::Series(series);

        let result = registry.call_function("std", &[series_value]).unwrap();
        // Should return null (placeholder implementation)
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_today() {
        let registry = BuiltinRegistry::new();

        // Test that today() returns a string in YYYY-MM-DD format
        let result = registry.call_function("today", &[]).unwrap();
        match result {
            Value::String(date_str) => {
                // Should be in YYYY-MM-DD format
                assert_eq!(date_str.len(), 10);
                assert!(date_str.chars().nth(4) == Some('-'));
                assert!(date_str.chars().nth(7) == Some('-'));
                // Should be today's date
                let today = chrono::Utc::now().date_naive();
                let expected = today.format("%Y-%m-%d").to_string();
                assert_eq!(date_str, expected);
            }
            _ => panic!("Expected string result"),
        }

        // Test that today() with arguments fails
        let result = registry.call_function("today", &[Value::Int(1)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_builtin_repeat() {
        let registry = BuiltinRegistry::new();

        // Test basic repeat with integer count
        let value = Value::String("hello".to_string());
        let count = Value::Int(3);
        let result = registry.call_function("repeat", &[value, count]).unwrap();
        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Value::String("hello".to_string()));
                assert_eq!(arr[1], Value::String("hello".to_string()));
                assert_eq!(arr[2], Value::String("hello".to_string()));
            }
            _ => panic!("Expected array"),
        }

        // Test repeat with count 0
        let value = Value::Int(42);
        let count = Value::Int(0);
        let result = registry.call_function("repeat", &[value, count]).unwrap();
        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 0);
            }
            _ => panic!("Expected array"),
        }

        // Test repeat with count 1
        let value = Value::Bool(true);
        let count = Value::Int(1);
        let result = registry.call_function("repeat", &[value, count]).unwrap();
        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], Value::Bool(true));
            }
            _ => panic!("Expected array"),
        }

        // Test repeat with negative count (should error)
        let value = Value::String("test".to_string());
        let count = Value::Int(-1);
        let result = registry.call_function("repeat", &[value, count]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("non-negative integer"));

        // Test repeat with wrong number of arguments
        let result = registry.call_function("repeat", &[Value::String("test".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));

        let result = registry.call_function(
            "repeat",
            &[
                Value::String("test".to_string()),
                Value::Int(1),
                Value::Int(2),
            ],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));

        // Test repeat with invalid count type
        let value = Value::String("test".to_string());
        let count = Value::String("not a number".to_string());
        let result = registry.call_function("repeat", &[value, count]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("non-negative integer"));
    }

    #[test]
    fn test_builtin_avg() {
        let registry = BuiltinRegistry::new();

        // Test avg with array of integers
        let values = Value::Array(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        let result = registry.call_function("avg", &[values]).unwrap();
        assert_eq!(result, Value::Float(20.0));

        // Test avg with array of floats
        let values = Value::Array(vec![
            Value::Float(1.5),
            Value::Float(2.5),
            Value::Float(3.5),
        ]);
        let result = registry.call_function("avg", &[values]).unwrap();
        assert_eq!(result, Value::Float(2.5));

        // Test avg with mixed int and float
        let values = Value::Array(vec![Value::Int(10), Value::Float(20.0), Value::Int(30)]);
        let result = registry.call_function("avg", &[values]).unwrap();
        assert_eq!(result, Value::Float(20.0));

        // Test avg with empty array
        let values = Value::Array(vec![]);
        let result = registry.call_function("avg", &[values]).unwrap();
        assert_eq!(result, Value::Null);

        // Test avg with array containing non-numeric values (should ignore them)
        let values = Value::Array(vec![
            Value::Int(10),
            Value::String("ignore".to_string()),
            Value::Int(30),
        ]);
        let result = registry.call_function("avg", &[values]).unwrap();
        assert_eq!(result, Value::Float(20.0));

        // Test avg with no arguments (should error)
        let result = registry.call_function("avg", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Test avg with too many arguments (should error)
        let result = registry.call_function("avg", &[Value::Array(vec![]), Value::Array(vec![])]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_builtin_correl_arrays() {
        let registry = BuiltinRegistry::new();

        // Test perfect positive correlation
        let arr1 = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
        ]);
        let arr2 = Value::Array(vec![
            Value::Int(2),
            Value::Int(4),
            Value::Int(6),
            Value::Int(8),
        ]);
        let result = registry.call_function("correl", &[arr1, arr2]).unwrap();
        assert_eq!(result, Value::Float(1.0));

        // Test perfect negative correlation
        let arr1 = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
        ]);
        let arr2 = Value::Array(vec![
            Value::Int(8),
            Value::Int(6),
            Value::Int(4),
            Value::Int(2),
        ]);
        let result = registry.call_function("correl", &[arr1, arr2]).unwrap();
        assert_eq!(result, Value::Float(-1.0));

        // Test no correlation
        let arr1 = Value::Array(vec![
            Value::Int(1),
            Value::Int(1),
            Value::Int(1),
            Value::Int(1),
        ]);
        let arr2 = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
        ]);
        let result = registry.call_function("correl", &[arr1, arr2]).unwrap();
        assert_eq!(result, Value::Float(0.0));

        // Test with floats
        let arr1 = Value::Array(vec![
            Value::Float(1.0),
            Value::Float(2.0),
            Value::Float(3.0),
        ]);
        let arr2 = Value::Array(vec![
            Value::Float(1.0),
            Value::Float(2.0),
            Value::Float(3.0),
        ]);
        let result = registry.call_function("correl", &[arr1, arr2]).unwrap();
        assert_eq!(result, Value::Float(1.0));
    }

    #[test]
    fn test_builtin_correl_series() {
        let registry = BuiltinRegistry::new();

        // Test with Series (should return Null for now as placeholder)
        let series1 = Value::Series(Series::new(PlSmallStr::from("a"), &[1, 2, 3, 4]));
        let series2 = Value::Series(Series::new(PlSmallStr::from("b"), &[2, 4, 6, 8]));
        let result = registry
            .call_function("correl", &[series1, series2])
            .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_builtin_correl_errors() {
        let registry = BuiltinRegistry::new();

        // Test with wrong number of arguments (0 args)
        let result = registry.call_function("correl", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));

        // Test with wrong number of arguments (1 arg)
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let result = registry.call_function("correl", &[arr]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));

        // Test with wrong number of arguments (3 args)
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let result = registry.call_function("correl", &[arr.clone(), arr.clone(), arr]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 2 arguments"));

        // Test with mismatched array lengths
        let arr1 = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let arr2 = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let result = registry.call_function("correl", &[arr1, arr2]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires two arrays or two series"));

        // Test with non-array/non-series arguments
        let str1 = Value::String("hello".to_string());
        let str2 = Value::String("world".to_string());
        let result = registry.call_function("correl", &[str1, str2]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires two arrays or two series"));
    }

    #[test]
    fn test_builtin_correl_edge_cases() {
        let registry = BuiltinRegistry::new();

        // Test with empty arrays
        let arr1 = Value::Array(vec![]);
        let arr2 = Value::Array(vec![]);
        let result = registry.call_function("correl", &[arr1, arr2]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with single element arrays
        let arr1 = Value::Array(vec![Value::Int(1)]);
        let arr2 = Value::Array(vec![Value::Int(2)]);
        let result = registry.call_function("correl", &[arr1, arr2]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with arrays containing nulls (should be filtered out)
        let arr1 = Value::Array(vec![Value::Int(1), Value::Null, Value::Int(3)]);
        let arr2 = Value::Array(vec![Value::Int(2), Value::Null, Value::Int(6)]);
        let result = registry.call_function("correl", &[arr1, arr2]).unwrap();
        assert_eq!(result, Value::Float(1.0)); // 1,3 and 2,6 should correlate perfectly

        // Test with arrays containing non-numeric values (should be filtered out)
        let arr1 = Value::Array(vec![
            Value::Int(1),
            Value::String("ignore".to_string()),
            Value::Int(3),
        ]);
        let arr2 = Value::Array(vec![
            Value::Int(2),
            Value::String("ignore".to_string()),
            Value::Int(6),
        ]);
        let result = registry.call_function("correl", &[arr1, arr2]).unwrap();
        assert_eq!(result, Value::Float(1.0));

        // Test with constant arrays (zero variance)
        let arr1 = Value::Array(vec![Value::Int(5), Value::Int(5), Value::Int(5)]);
        let arr2 = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let result = registry.call_function("correl", &[arr1, arr2]).unwrap();
        assert_eq!(result, Value::Float(0.0));
    }

    #[test]
    fn test_url_set_domain_without_www() {
        let registry = BuiltinRegistry::new();

        // Test with URL that has www
        let url_with_www = Value::String("https://www.example.com/path".to_string());
        let result = registry
            .call_function("url_set_domain_without_www", &[url_with_www])
            .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path".to_string())
        );

        // Test with URL that doesn't have www
        let url_without_www = Value::String("https://example.com/path".to_string());
        let result = registry
            .call_function("url_set_domain_without_www", &[url_without_www])
            .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path".to_string())
        );

        // Test with subdomain that starts with www
        let subdomain = Value::String("https://www.sub.example.com/path".to_string());
        let result = registry
            .call_function("url_set_domain_without_www", &[subdomain])
            .unwrap();
        assert_eq!(
            result,
            Value::String("https://sub.example.com/path".to_string())
        );

        // Test with URL without path
        let no_path = Value::String("https://www.example.com".to_string());
        let result = registry
            .call_function("url_set_domain_without_www", &[no_path])
            .unwrap();
        assert_eq!(result, Value::String("https://example.com/".to_string()));

        // Test with invalid URL
        let invalid_url = Value::String("not-a-url".to_string());
        let result = registry
            .call_function("url_set_domain_without_www", &[invalid_url])
            .unwrap();
        assert_eq!(result, Value::String("not-a-url".to_string()));

        // Test with array
        let urls = Value::Array(vec![
            Value::String("https://www.example.com".to_string()),
            Value::String("https://test.com".to_string()),
        ]);
        let result = registry
            .call_function("url_set_domain_without_www", &[urls])
            .unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::String("https://example.com/".to_string()),
                Value::String("https://test.com/".to_string()),
            ])
        );
    }

    #[test]
    fn test_transform_values_registered() {
        let registry = BuiltinRegistry::new();
        assert!(registry.has_function("transform_values"));
        let names = registry.function_names();
        assert!(names.contains(&"transform_values".to_string()));
    }

    #[test]
    fn test_least_frequent() {
        let registry = BuiltinRegistry::new();

        // Test with array of strings
        let arr = Value::Array(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("a".to_string()),
            Value::String("c".to_string()),
            Value::String("b".to_string()),
            Value::String("a".to_string()),
        ]);
        let result = registry.call_function("least_frequent", &[arr]).unwrap();
        assert_eq!(result, Value::String("c".to_string())); // "c" appears once, others more

        // Test with array of numbers
        let arr = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(1),
            Value::Int(3),
            Value::Int(2),
            Value::Int(1),
        ]);
        let result = registry.call_function("least_frequent", &[arr]).unwrap();
        assert_eq!(result, Value::Int(3)); // 3 appears once

        // Test with empty array
        let arr = Value::Array(vec![]);
        let result = registry.call_function("least_frequent", &[arr]).unwrap();
        assert_eq!(result, Value::Null);

        // Test with single element
        let arr = Value::Array(vec![Value::String("single".to_string())]);
        let result = registry.call_function("least_frequent", &[arr]).unwrap();
        assert_eq!(result, Value::String("single".to_string()));

        // Test with all same frequency
        let arr = Value::Array(vec![
            Value::String("x".to_string()),
            Value::String("y".to_string()),
            Value::String("z".to_string()),
        ]);
        let result = registry.call_function("least_frequent", &[arr]).unwrap();
        // Should return one of them, arbitrary choice
        match result {
            Value::String(s) if s == "x" || s == "y" || s == "z" => {}
            _ => panic!("Expected one of x, y, z"),
        }

        // Test with DataFrame
        let names = Series::new(
            PlSmallStr::from("name"),
            &["Alice", "Bob", "Alice", "Charlie"],
        );
        let ages = Series::new(PlSmallStr::from("age"), &[25, 30, 25, 35]);
        let df = DataFrame::new(vec![names, ages]).unwrap();
        let df_val = Value::DataFrame(df);
        let result = registry.call_function("least_frequent", &[df_val]).unwrap();
        // Charlie appears once, others more
        assert_eq!(result, Value::String("Charlie".to_string()));

        // Test with Series
        let series = Series::new(PlSmallStr::from("values"), &[1, 2, 1, 3, 2, 1]);
        let series_val = Value::Series(series);
        let result = registry
            .call_function("least_frequent", &[series_val])
            .unwrap();
        assert_eq!(result, Value::Int(3));

        // Test error cases
        let result = registry.call_function("least_frequent", &[]);
        assert!(result.is_err());

        let result =
            registry.call_function("least_frequent", &[Value::String("not array".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_transpose_registered() {
        let registry = BuiltinRegistry::new();
        assert!(registry.has_function("transpose"));
    }

    #[test]
    fn test_iferror_via_registry() {
        let registry = BuiltinRegistry::new();

        // Test iferror is registered
        assert!(registry.has_function("iferror"));

        // Test with null first argument
        let result = registry
            .call_function(
                "iferror",
                &[Value::Null, Value::String("default".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("default".to_string()));

        // Test with non-null first argument
        let result = registry
            .call_function(
                "iferror",
                &[Value::Int(42), Value::String("default".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::Int(42));

        // Test error with wrong number of args
        let result = registry.call_function("iferror", &[Value::Null]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("iferror() expects 2 arguments"));
    }

    #[test]
    fn test_truncate_date_function() {
        let registry = BuiltinRegistry::new();

        // Test truncate to year
        let result = registry
            .call_function(
                "truncate_date",
                &[
                    Value::String("2021-06-15T14:30:45Z".to_string()),
                    Value::String("year".to_string()),
                ],
            )
            .unwrap();
        // 2021-01-01 00:00:00 UTC timestamp
        assert_eq!(result, Value::Int(1609459200));

        // Test truncate to month
        let result = registry
            .call_function(
                "truncate_date",
                &[
                    Value::String("2021-06-15T14:30:45Z".to_string()),
                    Value::String("month".to_string()),
                ],
            )
            .unwrap();
        // 2021-06-01 00:00:00 UTC timestamp
        assert_eq!(result, Value::Int(1622505600));

        // Test truncate to day
        let result = registry
            .call_function(
                "truncate_date",
                &[
                    Value::String("2021-06-15T14:30:45Z".to_string()),
                    Value::String("day".to_string()),
                ],
            )
            .unwrap();
        // 2021-06-15 00:00:00 UTC timestamp
        assert_eq!(result, Value::Int(1623715200));

        // Test truncate to hour
        let result = registry
            .call_function(
                "truncate_date",
                &[
                    Value::String("2021-06-15T14:30:45Z".to_string()),
                    Value::String("hour".to_string()),
                ],
            )
            .unwrap();
        // 2021-06-15 14:00:00 UTC timestamp
        assert_eq!(result, Value::Int(1623765600));

        // Test truncate to minute
        let result = registry
            .call_function(
                "truncate_date",
                &[
                    Value::String("2021-06-15T14:30:45Z".to_string()),
                    Value::String("minute".to_string()),
                ],
            )
            .unwrap();
        // 2021-06-15 14:30:00 UTC timestamp
        assert_eq!(result, Value::Int(1623767400));

        // Test with integer timestamp
        let result = registry
            .call_function(
                "truncate_date",
                &[Value::Int(1623767400), Value::String("day".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::Int(1623715200));

        // Test invalid unit
        let result = registry.call_function(
            "truncate_date",
            &[
                Value::String("2021-06-15T14:30:45Z".to_string()),
                Value::String("invalid".to_string()),
            ],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("truncate_date() invalid unit"));

        // Test wrong number of arguments
        let result = registry.call_function(
            "truncate_date",
            &[Value::String("2021-06-15T14:30:45Z".to_string())],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("truncate_date() expects 2 arguments"));

        // Test invalid date
        let result = registry.call_function(
            "truncate_date",
            &[
                Value::String("invalid-date".to_string()),
                Value::String("day".to_string()),
            ],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_url_strip_port_if_default() {
        let registry = BuiltinRegistry::new();

        // Test HTTP default port (80) - should be stripped
        let result = registry
            .call_function(
                "url_strip_port_if_default",
                &[Value::String("http://example.com:80/path".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("http://example.com/path".to_string()));

        // Test HTTPS default port (443) - should be stripped
        let result = registry
            .call_function(
                "url_strip_port_if_default",
                &[Value::String("https://example.com:443/path".to_string())],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path".to_string())
        );

        // Test FTP default port (21) - should be stripped
        let result = registry
            .call_function(
                "url_strip_port_if_default",
                &[Value::String("ftp://example.com:21/path".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("ftp://example.com/path".to_string()));

        // Test SSH default port (22) - should be stripped
        let result = registry
            .call_function(
                "url_strip_port_if_default",
                &[Value::String("ssh://example.com:22/path".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("ssh://example.com/path".to_string()));

        // Test Telnet default port (23) - should be stripped
        let result = registry
            .call_function(
                "url_strip_port_if_default",
                &[Value::String("telnet://example.com:23/".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("telnet://example.com/".to_string()));

        // Test non-default port - should NOT be stripped
        let result = registry
            .call_function(
                "url_strip_port_if_default",
                &[Value::String("http://example.com:8080/path".to_string())],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("http://example.com:8080/path".to_string())
        );

        // Test HTTPS with non-default port - should NOT be stripped
        let result = registry
            .call_function(
                "url_strip_port_if_default",
                &[Value::String("https://example.com:8443/path".to_string())],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com:8443/path".to_string())
        );

        // Test URL without port - should remain unchanged
        let result = registry
            .call_function(
                "url_strip_port_if_default",
                &[Value::String("http://example.com/path".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("http://example.com/path".to_string()));

        // Test invalid URL - should return original string
        let result = registry
            .call_function(
                "url_strip_port_if_default",
                &[Value::String("not-a-url".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("not-a-url".to_string()));

        // Test with array of URLs
        let urls = vec![
            Value::String("http://example.com:80/".to_string()),
            Value::String("https://example.com:443/".to_string()),
            Value::String("http://example.com:8080/".to_string()),
        ];
        let result = registry
            .call_function("url_strip_port_if_default", &[Value::Array(urls)])
            .unwrap();
        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Value::String("http://example.com/".to_string()));
                assert_eq!(arr[1], Value::String("https://example.com/".to_string()));
                assert_eq!(
                    arr[2],
                    Value::String("http://example.com:8080/".to_string())
                );
            }
            _ => panic!("Expected Array"),
        }

        // Test with invalid input type
        let result = registry.call_function("url_strip_port_if_default", &[Value::Int(42)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url_strip_port_if_default() requires string, array, DataFrame, or Series"));

        // Test with wrong number of arguments
        let result = registry.call_function("url_strip_port_if_default", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url_strip_port_if_default() expects 1 argument"));

        let result = registry.call_function(
            "url_strip_port_if_default",
            &[
                Value::String("test".to_string()),
                Value::String("extra".to_string()),
            ],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url_strip_port_if_default() expects 1 argument"));
    }

    #[test]
    fn test_url_strip_port() {
        let registry = BuiltinRegistry::new();

        // Test stripping port from URL with port
        let result = registry
            .call_function(
                "url_strip_port",
                &[Value::String("http://example.com:8080/path".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("http://example.com/path".to_string()));

        // Test stripping port from HTTPS URL
        let result = registry
            .call_function(
                "url_strip_port",
                &[Value::String("https://example.com:8443/path".to_string())],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com/path".to_string())
        );

        // Test URL without port - should remain unchanged
        let result = registry
            .call_function(
                "url_strip_port",
                &[Value::String("http://example.com/path".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("http://example.com/path".to_string()));

        // Test invalid URL - should return original string
        let result = registry
            .call_function("url_strip_port", &[Value::String("not-a-url".to_string())])
            .unwrap();
        assert_eq!(result, Value::String("not-a-url".to_string()));

        // Test with array of URLs
        let urls = vec![
            Value::String("http://example.com:8080/".to_string()),
            Value::String("https://example.com:8443/".to_string()),
            Value::String("http://example.com/".to_string()),
        ];
        let result = registry
            .call_function("url_strip_port", &[Value::Array(urls)])
            .unwrap();
        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Value::String("http://example.com/".to_string()));
                assert_eq!(arr[1], Value::String("https://example.com/".to_string()));
                assert_eq!(arr[2], Value::String("http://example.com/".to_string()));
            }
            _ => panic!("Expected Array"),
        }

        // Test with invalid input type
        let result = registry.call_function("url_strip_port", &[Value::Int(42)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url_strip_port() requires string, array, DataFrame, or Series"));

        // Test with wrong number of arguments
        let result = registry.call_function("url_strip_port", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url_strip_port() expects 1 argument"));

        let result = registry.call_function(
            "url_strip_port",
            &[
                Value::String("test".to_string()),
                Value::String("extra".to_string()),
            ],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url_strip_port() expects 1 argument"));
    }

    #[test]
    fn test_url_set_port() {
        let registry = BuiltinRegistry::new();

        // Test setting port on URL without port
        let result = registry
            .call_function(
                "url_set_port",
                &[
                    Value::String("http://example.com/path".to_string()),
                    Value::Int(8080),
                ],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("http://example.com:8080/path".to_string())
        );

        // Test setting port on URL with existing port
        let result = registry
            .call_function(
                "url_set_port",
                &[
                    Value::String("http://example.com:80/path".to_string()),
                    Value::Int(8080),
                ],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("http://example.com:8080/path".to_string())
        );

        // Test setting port with string port number
        let result = registry
            .call_function(
                "url_set_port",
                &[
                    Value::String("https://example.com/path".to_string()),
                    Value::String("8443".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("https://example.com:8443/path".to_string())
        );

        // Test setting port 0
        let result = registry
            .call_function(
                "url_set_port",
                &[
                    Value::String("http://example.com/path".to_string()),
                    Value::Int(0),
                ],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("http://example.com:0/path".to_string())
        );

        // Test setting maximum port
        let result = registry
            .call_function(
                "url_set_port",
                &[
                    Value::String("http://example.com/path".to_string()),
                    Value::Int(65535),
                ],
            )
            .unwrap();
        assert_eq!(
            result,
            Value::String("http://example.com:65535/path".to_string())
        );

        // Test invalid URL - should return original
        let result = registry
            .call_function(
                "url_set_port",
                &[Value::String("not-a-url".to_string()), Value::Int(8080)],
            )
            .unwrap();
        assert_eq!(result, Value::String("not-a-url".to_string()));

        // Test with array
        let urls = Value::Array(vec![
            Value::String("http://example.com/".to_string()),
            Value::String("https://test.com/path".to_string()),
        ]);
        let result = registry
            .call_function("url_set_port", &[urls, Value::Int(9000)])
            .unwrap();
        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(
                    arr[0],
                    Value::String("http://example.com:9000/".to_string())
                );
                assert_eq!(
                    arr[1],
                    Value::String("https://test.com:9000/path".to_string())
                );
            }
            _ => panic!("Expected Array"),
        }

        // Test invalid port number (string)
        let result = registry.call_function(
            "url_set_port",
            &[
                Value::String("http://example.com/".to_string()),
                Value::String("invalid".to_string()),
            ],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid port number"));

        // Test invalid second argument type
        let result = registry.call_function(
            "url_set_port",
            &[
                Value::String("http://example.com/".to_string()),
                Value::Float(8080.0),
            ],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url_set_port() second argument must be an integer or string"));

        // Test wrong number of arguments (0 args)
        let result = registry.call_function("url_set_port", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url_set_port() expects 2 arguments"));

        // Test wrong number of arguments (1 arg)
        let result = registry.call_function(
            "url_set_port",
            &[Value::String("http://example.com/".to_string())],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url_set_port() expects 2 arguments"));

        // Test wrong number of arguments (3 args)
        let result = registry.call_function(
            "url_set_port",
            &[
                Value::String("http://example.com/".to_string()),
                Value::Int(8080),
                Value::Int(1),
            ],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url_set_port() expects 2 arguments"));

        // Test with DataFrame
        let df = DataFrame::new(vec![Series::new(
            "urls",
            &["http://example.com/", "https://test.com/path"],
        )])
        .unwrap();
        let result = registry
            .call_function("url_set_port", &[Value::DataFrame(df), Value::Int(9000)])
            .unwrap();
        match result {
            Value::DataFrame(result_df) => {
                let urls_col = result_df.column("urls").unwrap().str().unwrap();
                assert_eq!(urls_col.get(0).unwrap(), "http://example.com:9000/");
                assert_eq!(urls_col.get(1).unwrap(), "https://test.com:9000/path");
            }
            _ => panic!("Expected DataFrame"),
        }

        // Test with Series
        let series = Series::new(
            "urls".into(),
            &["http://example.com/", "https://test.com/path"],
        );
        let result = registry
            .call_function("url_set_port", &[Value::Series(series), Value::Int(9000)])
            .unwrap();
        match result {
            Value::Series(result_series) => {
                let urls_col = result_series.str().unwrap();
                assert_eq!(urls_col.get(0).unwrap(), "http://example.com:9000/");
                assert_eq!(urls_col.get(1).unwrap(), "https://test.com:9000/path");
            }
            _ => panic!("Expected Series"),
        }

        // Test invalid input type
        let result = registry.call_function("url_set_port", &[Value::Int(42), Value::Int(8080)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url_set_port() requires string, array, DataFrame, or Series"));
    }

    #[test]
    fn test_time_series_range_function() {
        let registry = BuiltinRegistry::new();

        // Test basic time series range with seconds
        let start = Value::Int(1609459200); // 2021-01-01 00:00:00 UTC
        let end = Value::Int(1609459260); // 2021-01-01 00:01:00 UTC
        let interval = Value::String("10s".to_string());
        let result = registry
            .call_function("time_series_range", &[start.clone(), end, interval])
            .unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 7); // 0, 10, 20, 30, 40, 50, 60 seconds
            assert_eq!(arr[0], Value::Int(1609459200));
            assert_eq!(arr[6], Value::Int(1609459260));
        } else {
            panic!("Expected array result");
        }

        // Test with minutes
        let start = Value::Int(1609459200);
        let end = Value::Int(1609462800); // 1 hour later
        let interval = Value::String("15m".to_string());
        let result = registry
            .call_function("time_series_range", &[start, end, interval])
            .unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 5); // 0, 15, 30, 45, 60 minutes
        } else {
            panic!("Expected array result");
        }

        // Test with start >= end (should return empty array)
        let start = Value::Int(1609459260);
        let end = Value::Int(1609459200);
        let interval = Value::String("10s".to_string());
        let result = registry
            .call_function("time_series_range", &[start, end, interval])
            .unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 0);
        } else {
            panic!("Expected array result");
        }

        // Test with string timestamps
        let start = Value::String("2021-01-01T00:00:00Z".to_string());
        let end = Value::String("2021-01-01T00:00:30Z".to_string());
        let interval = Value::String("10s".to_string());
        let result = registry
            .call_function("time_series_range", &[start, end, interval])
            .unwrap();
        if let Value::Array(arr) = result {
            assert_eq!(arr.len(), 4); // 0, 10, 20, 30
        } else {
            panic!("Expected array result");
        }

        // Test invalid interval
        let start = Value::Int(1609459200);
        let end = Value::Int(1609459260);
        let interval = Value::String("10x".to_string());
        let result = registry.call_function("time_series_range", &[start.clone(), end, interval]);
        assert!(result.is_err());

        // Test wrong number of arguments
        let result = registry.call_function("time_series_range", &[start.clone()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_map_registered_via_inventory() {
        let registry = BuiltinRegistry::new();
        assert!(registry.has_function("map"));
        let function_names = registry.function_names();
        assert!(function_names.contains(&"map".to_string()));
    }
}
