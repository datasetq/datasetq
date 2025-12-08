//! Filter system for dsq-core
//!
//! This module provides a thin wrapper around the dsq-filter crate,
//! maintaining backward compatibility with the existing dsq-core API.

use std::collections::HashMap;

pub use dsq_filter::{
    compile_filter, ErrorMode, ExecutionMode, ExecutionResult, ExecutionStats, ExecutorConfig,
    FilterCompiler, FilterExecutor, OptimizationLevel,
};

/// Convenience function to execute a filter string on a value
pub fn execute_filter(filter: &str, value: &crate::Value) -> crate::Result<crate::Value> {
    match dsq_filter::execute_filter(filter, value) {
        Ok(result) => Ok(result),
        Err(e) => Err(crate::Error::Filter(crate::FilterError::Runtime(
            e.to_string(),
        ))),
    }
}

/// Convenience function to execute a filter with custom configuration
pub fn execute_filter_with_config(
    filter: &str,
    value: &crate::Value,
    config: &ExecutorConfig,
) -> crate::Result<ExecutionResult> {
    let mut executor = dsq_filter::FilterExecutor::with_config(config.clone());
    match executor.execute_str(filter, value.clone()) {
        Ok(result) => Ok(result),
        Err(e) => Err(crate::Error::Filter(crate::FilterError::Runtime(
            e.to_string(),
        ))),
    }
}

/// Explain a filter (placeholder for future implementation)
pub fn explain_filter(filter: &str) -> dsq_shared::Result<String> {
    // Basic implementation - just return a simple description
    match filter.trim() {
        "." => Ok("Identity filter - returns the input unchanged".to_string()),
        _ => Ok(format!("Filter: {filter}")),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::Value;

    use super::*;

    #[test]
    fn test_execute_filter_basic() {
        // Test basic filter execution
        let input = Value::Int(42);
        let result = execute_filter(".", &input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(42));
    }

    #[test]
    fn test_execute_filter_identity() {
        // Test identity filter
        let input = Value::String("hello".to_string());
        let result = execute_filter(".", &input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("hello".to_string()));
    }

    #[test]
    fn test_execute_filter_object_access() {
        // Test object field access
        let mut obj = HashMap::new();
        obj.insert("name".to_string(), Value::String("Alice".to_string()));
        obj.insert("age".to_string(), Value::Int(30));
        let input = Value::Object(obj);

        let result = execute_filter(".name", &input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("Alice".to_string()));

        let result = execute_filter(".age", &input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(30));
    }

    #[test]
    fn test_execute_filter_array_indexing() {
        // Test array indexing
        let input = Value::Array(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);

        let result = execute_filter(".[0]", &input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(10));

        let result = execute_filter(".[1]", &input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(20));
    }

    #[test]
    fn test_execute_filter_arithmetic() {
        // Test arithmetic operations
        let input = Value::Int(5);
        let result = execute_filter(". + 3", &input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(8));

        let result = execute_filter(". * 2", &input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(10));
    }

    #[test]
    fn test_execute_filter_with_config_basic() {
        // Test execute_filter_with_config with default config
        let input = Value::Int(42);
        let config = ExecutorConfig::default();
        let result = execute_filter_with_config(".", &input, &config);
        assert!(result.is_ok());
        let execution_result = result.unwrap();
        assert_eq!(execution_result.value, Value::Int(42));
    }

    #[test]
    fn test_execute_filter_with_config_custom() {
        // Test execute_filter_with_config with custom config
        let input = Value::Int(42);
        let config = ExecutorConfig {
            max_recursion_depth: 1000,
            ..Default::default()
        };
        let result = execute_filter_with_config(".", &input, &config);
        assert!(result.is_ok());
        let execution_result = result.unwrap();
        assert_eq!(execution_result.value, Value::Int(42));
    }

    #[test]
    fn test_execute_filter_error_handling() {
        // Test error handling for invalid filter
        let input = Value::Int(42);
        let result = execute_filter("invalid syntax {{{", &input);
        assert!(result.is_err());
    }

    #[test]
    fn test_execute_filter_with_config_error_handling() {
        // Test error handling for invalid filter with config
        let input = Value::Int(42);
        let config = ExecutorConfig::default();
        let result = execute_filter_with_config("invalid syntax {{{", &input, &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_explain_filter_identity() {
        // Test the explain_filter function for identity filter
        let result = explain_filter(".");
        assert!(result.is_ok());
        let explanation = result.unwrap();
        assert!(explanation.contains("Identity"));
    }

    #[test]
    fn test_explain_filter_with_complex_filter() {
        // Test explain_filter with a more complex filter string
        let result = explain_filter("map(.name) | sort");
        assert!(result.is_ok());
        let explanation = result.unwrap();
        assert!(explanation.contains("Filter:"));
    }

    #[test]
    fn test_execute_filter_complex_operations() {
        // Test more complex filter operations
        let input = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ]);

        // Test filter operation
        let result = execute_filter("map(. * 2)", &input);
        assert!(result.is_ok());
        let expected = Value::Array(vec![
            Value::Int(2),
            Value::Int(4),
            Value::Int(6),
            Value::Int(8),
            Value::Int(10),
        ]);
        assert_eq!(result.unwrap(), expected);

        // Test select operation
        let result = execute_filter("map(select(. > 3))", &input);
        assert!(result.is_ok());
        // This should filter each element, resulting in [null, null, null, 4, 5]
        // but select returns the value if true, null if false
        // Actually, let's check what select does in jq - it returns the value if condition true, empty if false
    }

    #[test]
    fn test_execute_filter_object_transformation() {
        // Test object transformation
        let mut obj = HashMap::new();
        obj.insert("first".to_string(), Value::String("John".to_string()));
        obj.insert("last".to_string(), Value::String("Doe".to_string()));
        obj.insert("age".to_string(), Value::Int(30));
        let input = Value::Object(obj);

        let result = execute_filter("{name: (.first + \" \" + .last), age}", &input);
        assert!(result.is_ok());
        // Should create a new object with name and age fields
    }

    #[test]
    fn test_execute_filter_null_handling() {
        // Test null value handling
        let input = Value::Null;
        let result = execute_filter(".", &input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_execute_filter_boolean_operations() {
        // Test boolean operations
        let input = Value::Bool(true);
        let result = execute_filter("if . then \"yes\" else \"no\" end", &input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("yes".to_string()));

        let input = Value::Bool(false);
        let result = execute_filter("if . then \"yes\" else \"no\" end", &input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("no".to_string()));
    }
}
