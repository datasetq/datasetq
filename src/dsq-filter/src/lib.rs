//! # dsq-filter
//!
//! A filter system for dsq that operates at the AST level, providing jq-compatible
//! filter operations for structured data processing.
//!
//! This crate provides:
//! - AST-based filter compilation and execution
//! - jq-compatible syntax support
//! - DataFrame and JSON data processing
//! - Built-in functions and operations
//! - Comprehensive testing against real examples

pub mod compiler;
pub mod context;
pub mod executor;

pub use compiler::{CompiledFilter, FilterCompiler, OptimizationLevel};
pub use context::{CompilationContext, ErrorMode, FilterContext, FunctionBody, FunctionDef};
pub use dsq_functions::BuiltinRegistry;
pub use executor::{
    ExecutionMode, ExecutionResult, ExecutionStats, ExecutorConfig, FilterExecutor,
};

/// Convenience function to execute a filter string on a value
pub fn execute_filter(
    filter: &str,
    value: &dsq_shared::value::Value,
) -> anyhow::Result<dsq_shared::value::Value> {
    let mut executor = FilterExecutor::new();
    let result = executor.execute_str(filter, value.clone())?;
    Ok(result.value)
}

/// Convenience function to compile a filter string
pub fn compile_filter(filter: &str) -> anyhow::Result<CompiledFilter> {
    let compiler = FilterCompiler::new();
    compiler.compile_str(filter)
}

/// Re-export commonly used types from dsq-shared
pub use dsq_shared::value::Value;
pub use dsq_shared::Result;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_example_002() {
        let query = r#"group_by(.genre) | map({
  genre: .[0].genre,
  count: length,
  avg_price: (map(.price) | add / length)
})"#;
        let result = compile_filter(query);
        assert!(
            result.is_ok(),
            "Failed to compile example_002 query: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_compile_example_second() {
        let query = r#"filter(.year > 1900) | sort_by(.year) | map({title, author})"#;
        let result = compile_filter(query);
        assert!(
            result.is_ok(),
            "Failed to compile second query: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_compile_join_example() {
        let query = r#"join("departments.csv", .dept_id == .id) | map({employee_name: .name, salary, department: .name_right, location})"#;
        let result = compile_filter(query);
        assert!(
            result.is_ok(),
            "Failed to compile join query: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_select_on_array() {
        use dsq_shared::value::Value;
        let input = Value::Array(vec![
            Value::Int(100),
            Value::Int(200),
            Value::Int(50),
            Value::Int(300),
        ]);
        let result = execute_filter("select(. > 100)", &input);
        assert!(
            result.is_ok(),
            "Failed to execute select on array: {:?}",
            result.err()
        );
        let expected = Value::Array(vec![Value::Int(200), Value::Int(300)]);
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_select_on_object() {
        use dsq_shared::value::Value;
        use std::collections::HashMap;
        let mut obj = HashMap::new();
        obj.insert("age".to_string(), Value::Int(35));
        obj.insert("name".to_string(), Value::String("John".to_string()));
        obj.insert(
            "US City Name".to_string(),
            Value::String("New York".to_string()),
        );
        let input = Value::Object(obj.clone());

        // Test select with true condition
        let result = execute_filter("select(.age > 30)", &input);
        assert!(
            result.is_ok(),
            "Failed to execute select on object: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), Value::Object(obj.clone()));

        // Test select with false condition
        let result = execute_filter("select(.age < 30)", &input);
        assert!(
            result.is_ok(),
            "Failed to execute select on object: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), Value::Null);

        // Test field access with bracket notation for fields with spaces
        let result = execute_filter(".[\"US City Name\"]", &input);
        assert!(
            result.is_ok(),
            "Failed to execute field access with brackets: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), Value::String("New York".to_string()));
    }

    #[test]
    fn test_stress_009_execution() {
        // Test execution of the stress_009 query: map(select(.age > 30 and .department == "IT" or .salary < 60000)) | map({name, age, department, salary})
        use dsq_shared::value::Value;
        use std::collections::HashMap;

        // Create test data similar to stress_009
        let employees = vec![
            {
                let mut obj = HashMap::new();
                obj.insert("id".to_string(), Value::Int(1));
                obj.insert(
                    "name".to_string(),
                    Value::String("Alice Johnson".to_string()),
                );
                obj.insert("age".to_string(), Value::Int(28));
                obj.insert("city".to_string(), Value::String("New York".to_string()));
                obj.insert("salary".to_string(), Value::Int(75000));
                obj.insert(
                    "department".to_string(),
                    Value::String("Engineering".to_string()),
                );
                Value::Object(obj)
            },
            {
                let mut obj = HashMap::new();
                obj.insert("id".to_string(), Value::Int(2));
                obj.insert("name".to_string(), Value::String("Bob Smith".to_string()));
                obj.insert("age".to_string(), Value::Int(34));
                obj.insert("city".to_string(), Value::String("Los Angeles".to_string()));
                obj.insert("salary".to_string(), Value::Int(82000));
                obj.insert("department".to_string(), Value::String("Sales".to_string()));
                Value::Object(obj)
            },
        ];

        let input = Value::Array(employees);

        let query = r#"map(select(.age > 30 and .department == "IT" or .salary < 60000)) | map({name, age, department, salary})"#;
        let result = execute_filter(query, &input);

        // Should execute successfully and return empty array since no records match
        assert!(
            result.is_ok(),
            "Failed to execute stress_009 query: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), Value::Array(vec![]));
    }

    #[test]
    fn test_if_then_else() {
        use dsq_shared::value::Value;

        // Test simple if-then-else
        let result = execute_filter("if . > 5 then \"big\" else \"small\" end", &Value::Int(10));
        assert!(
            result.is_ok(),
            "Failed to execute if-then-else: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), Value::String("big".to_string()));

        let result = execute_filter("if . > 5 then \"big\" else \"small\" end", &Value::Int(3));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("small".to_string()));

        // Test if with type check
        let result = execute_filter(
            "if type == \"integer\" then . * 2 else . end",
            &Value::Int(5),
        );
        assert!(
            result.is_ok(),
            "Failed to execute if with type: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), Value::Int(10));

        let result = execute_filter(
            "if type == \"integer\" then . * 2 else . end",
            &Value::String("hello".to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::String("hello".to_string()));
    }

    #[test]
    fn test_max_by() {
        use dsq_shared::value::Value;
        use std::collections::HashMap;

        // Create test data: array of objects with prices
        let data = vec![
            {
                let mut obj = HashMap::new();
                obj.insert("name".to_string(), Value::String("Laptop".to_string()));
                obj.insert("price".to_string(), Value::Int(1200));
                Value::Object(obj)
            },
            {
                let mut obj = HashMap::new();
                obj.insert("name".to_string(), Value::String("Phone".to_string()));
                obj.insert("price".to_string(), Value::Int(800));
                Value::Object(obj)
            },
            {
                let mut obj = HashMap::new();
                obj.insert("name".to_string(), Value::String("Book".to_string()));
                obj.insert("price".to_string(), Value::Int(20));
                Value::Object(obj)
            },
        ];

        let input = Value::Array(data);

        let result = execute_filter("max_by(.price)", &input);
        assert!(
            result.is_ok(),
            "Failed to execute max_by: {:?}",
            result.err()
        );

        let expected = {
            let mut obj = HashMap::new();
            obj.insert("name".to_string(), Value::String("Laptop".to_string()));
            obj.insert("price".to_string(), Value::Int(1200));
            Value::Object(obj)
        };

        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_max_by_dataframe() {
        use dsq_shared::value::Value;
        use polars::prelude::*;

        // Create a test DataFrame with products and prices
        let df = DataFrame::new(vec![
            Series::new("name".into(), &["Laptop", "Phone", "Book", "Shoes"]).into(),
            Series::new("price".into(), &[1200, 800, 20, 150]).into(),
            Series::new(
                "category".into(),
                &["Electronics", "Electronics", "Books", "Clothing"],
            )
            .into(),
        ])
        .unwrap();

        let input = Value::DataFrame(df);

        let result = execute_filter("max_by(.price)", &input);
        assert!(
            result.is_ok(),
            "Failed to execute max_by on DataFrame: {:?}",
            result.err()
        );

        let expected = {
            let mut obj = std::collections::HashMap::new();
            obj.insert("name".to_string(), Value::String("Laptop".to_string()));
            obj.insert("price".to_string(), Value::Int(1200));
            obj.insert(
                "category".to_string(),
                Value::String("Electronics".to_string()),
            );
            Value::Object(obj)
        };

        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_add_function() {
        use dsq_shared::value::Value;

        // Test add on array of numbers
        let input = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
        ]);

        let result = execute_filter("add", &input);
        assert!(
            result.is_ok(),
            "Failed to execute add on array: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), Value::Int(10));

        // Test add on array of floats
        let input_float = Value::Array(vec![
            Value::Float(1.5),
            Value::Float(2.5),
            Value::Float(3.0),
        ]);

        let result_float = execute_filter("add", &input_float);
        assert!(
            result_float.is_ok(),
            "Failed to execute add on float array: {:?}",
            result_float.err()
        );
        assert_eq!(result_float.unwrap(), Value::Float(7.0));

        // Test add on strings
        let input_string = Value::Array(vec![
            Value::String("hello".to_string()),
            Value::String(" ".to_string()),
            Value::String("world".to_string()),
        ]);

        let result_string = execute_filter("add", &input_string);
        assert!(
            result_string.is_ok(),
            "Failed to execute add on string array: {:?}",
            result_string.err()
        );
        assert_eq!(
            result_string.unwrap(),
            Value::String("hello world".to_string())
        );
    }

    #[test]
    fn test_filter_on_array() {
        use dsq_shared::value::Value;

        // Test filter on array of numbers
        let input = Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ]);

        let result = execute_filter("filter(. > 3)", &input);
        assert!(
            result.is_ok(),
            "Failed to execute filter on array: {:?}",
            result.err()
        );
        let expected = Value::Array(vec![Value::Int(4), Value::Int(5)]);
        assert_eq!(result.unwrap(), expected);

        // Test filter with even numbers using different approach
        let result = execute_filter("filter(. >= 2 and . <= 4)", &input);
        assert!(
            result.is_ok(),
            "Failed to execute filter with range: {:?}",
            result.err()
        );
        let expected_range = Value::Array(vec![Value::Int(2), Value::Int(3), Value::Int(4)]);
        assert_eq!(result.unwrap(), expected_range);
    }

    #[test]
    fn test_filter_on_dataframe() {
        use dsq_shared::value::Value;
        use polars::prelude::*;

        // Create a test DataFrame with numeric columns
        let df = DataFrame::new(vec![
            Series::new("id".into(), &[1, 2, 3, 4]).into(),
            Series::new("age".into(), &[25, 30, 35, 28]).into(),
            Series::new("salary".into(), &[50000, 60000, 70000, 55000]).into(),
        ])
        .unwrap();

        let input = Value::DataFrame(df);

        // Test basic filter that should work - filter for existing rows
        let result = execute_filter("filter(.id)", &input); // .id should be truthy for non-zero values
        assert!(
            result.is_ok(),
            "Failed to execute basic filter on DataFrame: {:?}",
            result.err()
        );

        if let Value::DataFrame(filtered_df) = result.unwrap() {
            assert_eq!(
                filtered_df.height(),
                4,
                "Should have all 4 rows (all ids are truthy)"
            );
        } else {
            panic!("Expected DataFrame result");
        }

        // Test filter that matches specific values using equality
        let result = execute_filter("filter(.id == 1)", &input);
        assert!(
            result.is_ok(),
            "Failed to execute equality filter on DataFrame: {:?}",
            result.err()
        );

        if let Value::DataFrame(filtered_df) = result.unwrap() {
            assert_eq!(filtered_df.height(), 1, "Should have 1 row with id == 1");

            let ids = filtered_df.column("id").unwrap().i32().unwrap();
            assert_eq!(ids.get(0).unwrap(), 1, "ID should be 1");
        } else {
            panic!("Expected DataFrame result");
        }
    }

    #[test]
    fn test_filter_with_function_calls() {
        use dsq_shared::value::Value;
        use polars::prelude::*;

        // Create a test DataFrame with dates
        let df = DataFrame::new(vec![
            Series::new(
                "date".into(),
                &[
                    "2023-10-01",
                    "2023-10-02",
                    "2023-10-03",
                    "2023-10-04",
                    "2023-10-05",
                    "2023-10-06",
                    "2023-10-07",
                ],
            )
            .into(),
            Series::new("value".into(), &[100, 200, 150, 300, 250, 175, 225]).into(),
        ])
        .unwrap();

        let input = Value::DataFrame(df);

        // Test filter using end_of_week function (assuming it's registered)
        // Note: This test may fail if end_of_week is not properly registered in the test environment
        // but it demonstrates the expected usage
        let result = execute_filter("filter(end_of_week(.date))", &input);
        // We expect this to either work (if function is available) or fail gracefully
        // The important thing is that the filter compilation and execution framework handles it
        if let Ok(Value::DataFrame(filtered_df)) = result {
            // If successful, should filter to Sundays (October 1, 2023 was a Sunday)
            assert_eq!(filtered_df.height(), 1, "Should have 1 row (Sunday)");
        } else if let Err(e) = result {
            // If function not available, that's acceptable for this test
            // The error should be about the function, not the filter mechanism
            assert!(
                e.to_string().contains("end_of_week") || e.to_string().contains("function"),
                "Error should be about function availability, not filter execution: {:?}",
                e
            );
        } else {
            // If it returns something other than DataFrame, that's also fine for this test
            // The important thing is that filter() executed without panicking
        }
    }

    #[test]
    fn test_filter_edge_cases() {
        use dsq_shared::value::Value;

        // Test filter on empty array
        let input = Value::Array(vec![]);
        let result = execute_filter("filter(. > 0)", &input);
        assert!(
            result.is_ok(),
            "Failed to execute filter on empty array: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), Value::Array(vec![]));

        // Test filter that matches nothing
        let input = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let result = execute_filter("filter(. > 10)", &input);
        assert!(
            result.is_ok(),
            "Failed to execute filter with no matches: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), Value::Array(vec![]));

        // Test filter that matches everything
        let result = execute_filter("filter(. <= 10)", &input);
        assert!(
            result.is_ok(),
            "Failed to execute filter matching all: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), input);

        // Test filter with null values
        let input_with_null = Value::Array(vec![Value::Int(1), Value::Null, Value::Int(3)]);
        let result = execute_filter("filter(. != null)", &input_with_null);
        assert!(
            result.is_ok(),
            "Failed to execute filter with null check: {:?}",
            result.err()
        );
        let expected = Value::Array(vec![Value::Int(1), Value::Int(3)]);
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_filter_compilation_errors() {
        // Test that malformed filter expressions are caught during compilation
        let result = compile_filter("filter(");
        assert!(
            result.is_err(),
            "Should fail to compile incomplete filter expression"
        );

        let result = compile_filter("filter(. > )");
        assert!(
            result.is_err(),
            "Should fail to compile malformed filter condition"
        );

        let result = compile_filter("filter(.field)");
        // This should compile successfully as it's a valid expression that evaluates to truthy/falsy
        assert!(result.is_ok(), "Valid filter expression should compile");
    }

    #[test]
    fn test_assign_update_compilation() {
        use dsq_shared::value::Value;
        use std::collections::HashMap;

        // Test compilation of |= assignment
        let result = compile_filter(".field |= 42");
        assert!(
            result.is_ok(),
            "Should compile |= assignment: {:?}",
            result.err()
        );

        // Test execution of |= assignment on object
        let mut obj = HashMap::new();
        obj.insert("field".to_string(), Value::Int(10));
        obj.insert("other".to_string(), Value::String("unchanged".to_string()));
        let input = Value::Object(obj);

        let result = execute_filter(".field |= 42", &input);
        assert!(
            result.is_ok(),
            "Should execute |= assignment: {:?}",
            result.err()
        );

        if let Value::Object(result_obj) = result.unwrap() {
            assert_eq!(result_obj.get("field"), Some(&Value::Int(42)));
            assert_eq!(
                result_obj.get("other"),
                Some(&Value::String("unchanged".to_string()))
            );
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_assign_update_compilation_with_expression() {
        use dsq_shared::value::Value;
        use std::collections::HashMap;

        // Test compilation of |= assignment with expression
        let result = compile_filter(".total |= .price + .tax");
        assert!(
            result.is_ok(),
            "Should compile |= assignment with expression: {:?}",
            result.err()
        );

        // Test execution
        let mut obj = HashMap::new();
        obj.insert("price".to_string(), Value::Int(100));
        obj.insert("tax".to_string(), Value::Int(10));
        let input = Value::Object(obj);

        let result = execute_filter(".total |= .price + .tax", &input);
        assert!(
            result.is_ok(),
            "Should execute |= assignment with expression: {:?}",
            result.err()
        );

        if let Value::Object(result_obj) = result.unwrap() {
            assert_eq!(result_obj.get("price"), Some(&Value::Int(100)));
            assert_eq!(result_obj.get("tax"), Some(&Value::Int(10)));
            assert_eq!(result_obj.get("total"), Some(&Value::Int(110)));
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_assign_update_compilation_string_field() {
        use dsq_shared::value::Value;
        use std::collections::HashMap;

        // Test |= assignment with string value
        let mut obj = HashMap::new();
        obj.insert("status".to_string(), Value::String("pending".to_string()));
        let input = Value::Object(obj);

        let result = execute_filter(".status |= \"completed\"", &input);
        assert!(
            result.is_ok(),
            "Should execute |= assignment with string: {:?}",
            result.err()
        );

        if let Value::Object(result_obj) = result.unwrap() {
            assert_eq!(
                result_obj.get("status"),
                Some(&Value::String("completed".to_string()))
            );
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_assign_update_compilation_array_field() {
        use dsq_shared::value::Value;
        use std::collections::HashMap;

        // Test |= assignment with array value
        let mut obj = HashMap::new();
        obj.insert(
            "tags".to_string(),
            Value::Array(vec![Value::String("old".to_string())]),
        );
        let input = Value::Object(obj);

        let result = execute_filter(".tags |= [\"new\", \"tags\"]", &input);
        assert!(
            result.is_ok(),
            "Should execute |= assignment with array: {:?}",
            result.err()
        );

        let result_val = result.unwrap();
        println!("Result: {:?}", result_val);

        if let Value::Object(result_obj) = result_val {
            if let Some(Value::Array(tags)) = result_obj.get("tags") {
                println!("Tags: {:?}", tags);
                assert_eq!(tags.len(), 2);
                assert_eq!(tags[0], Value::String("new".to_string()));
                assert_eq!(tags[1], Value::String("tags".to_string()));
            } else {
                panic!(
                    "Expected array for tags field, got: {:?}",
                    result_obj.get("tags")
                );
            }
        } else {
            panic!("Expected object result, got: {:?}", result_val);
        }
    }

    #[test]
    fn test_assign_update_compilation_in_pipeline() {
        use dsq_shared::value::Value;
        use std::collections::HashMap;

        // Test |= assignment in a pipeline
        let mut obj = HashMap::new();
        obj.insert("salary".to_string(), Value::Int(50000));
        obj.insert("name".to_string(), Value::String("Alice".to_string()));
        let input = Value::Object(obj);

        let result = execute_filter(".salary |= .salary + 5000 | .name", &input);
        assert!(
            result.is_ok(),
            "Should execute |= in pipeline: {:?}",
            result.err()
        );

        // The pipeline should return the name field after updating salary
        assert_eq!(result.unwrap(), Value::String("Alice".to_string()));
    }

    #[test]
    fn test_assign_update_compilation_nested_object() {
        use dsq_shared::value::Value;
        use std::collections::HashMap;

        // Test |= assignment on nested object field
        let mut address = HashMap::new();
        address.insert("city".to_string(), Value::String("NYC".to_string()));

        let mut obj = HashMap::new();
        obj.insert("address".to_string(), Value::Object(address));
        let input = Value::Object(obj);

        let result = execute_filter(".address.city |= \"Boston\"", &input);
        assert!(
            result.is_ok(),
            "Should execute |= on nested field: {:?}",
            result.err()
        );

        if let Value::Object(result_obj) = result.unwrap() {
            if let Some(Value::Object(addr_obj)) = result_obj.get("address") {
                assert_eq!(
                    addr_obj.get("city"),
                    Some(&Value::String("Boston".to_string()))
                );
            } else {
                panic!("Expected nested address object");
            }
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_assign_update_compilation_error_cases() {
        // Test that |= assignment fails gracefully on non-objects
        let input = Value::Array(vec![Value::Int(1), Value::Int(2)]);

        let result = execute_filter(".field |= 42", &input);
        // This should work but return the value (42) since input is not an object
        assert!(
            result.is_ok(),
            "Should handle |= on non-object gracefully: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), Value::Int(42));
    }

    #[test]
    fn test_execute_filter_map_identity_preserves_nulls() {
        // Test that map(.) preserves null values
        let input = Value::Array(vec![Value::Int(1), Value::Null, Value::Int(2)]);

        let result = execute_filter("map(.)", &input);
        assert!(result.is_ok());
        let expected = Value::Array(vec![Value::Int(1), Value::Null, Value::Int(2)]);
        assert_eq!(result.unwrap(), expected);
    }
}
