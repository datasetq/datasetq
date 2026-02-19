//! Comprehensive tests for the DSQ parser
//!
//! This module contains unit tests that verify the parser can handle
//! all DSQ filter language constructs and examples.

use super::*;

fn parse_success(input: &str) -> Filter {
    let parser = FilterParser::new();
    parser
        .parse(input)
        .unwrap_or_else(|_| panic!("Failed to parse: {}", input))
}

fn parse_failure(input: &str) {
    let parser = FilterParser::new();
    let result = parser.parse(input);
    if let Ok(filter) = result {
        panic!(
            "Expected parse failure for: {}, but got: {:?}",
            input, filter
        );
    }
}

#[test]
fn test_identity() {
    let filter = parse_success(".");
    assert!(matches!(filter.expr, Expr::Identity));
}

#[test]
fn test_field_access() {
    let filter = parse_success(".name");
    assert!(
        matches!(filter.expr, Expr::FieldAccess { ref base, ref fields } if matches!(**base, Expr::Identity) && *fields == vec!["name".to_string()])
    );

    let filter = parse_success(".user.name");
    assert!(
        matches!(filter.expr, Expr::FieldAccess { ref base, ref fields } if matches!(**base, Expr::Identity) && *fields == vec!["user".to_string(), "name".to_string()])
    );
}

#[test]
fn test_object_accessor_query() {
    // Test parsing examples/operators/object_accessors/query.dsq: .author.name
    let filter = parse_success(".author.name");
    assert!(
        matches!(filter.expr, Expr::FieldAccess { ref base, ref fields } if matches!(**base, Expr::Identity) && *fields == vec!["author".to_string(), "name".to_string()])
    );
}

#[test]
fn test_function_call() {
    let filter = parse_success("filter(has(\"city\"))");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { ref name, ref args } if name == "filter" && args.len() == 1)
    );
    if let Expr::FunctionCall { ref args, .. } = filter.expr {
        assert!(
            matches!(&args[0], Expr::FunctionCall { name, args } if name == "has" && args.len() == 1)
        );
    }
}

#[test]
fn test_comma_sequence() {
    let filter = parse_success(".name, .price");
    assert!(matches!(filter.expr, Expr::Sequence(ref exprs) if exprs.len() == 2));
    if let Expr::Sequence(ref exprs) = filter.expr {
        assert!(
            matches!(&exprs[0], Expr::FieldAccess { base, fields } if matches!(**base, Expr::Identity) && *fields == vec!["name".to_string()])
        );
        assert!(
            matches!(&exprs[1], Expr::FieldAccess { base, fields } if matches!(**base, Expr::Identity) && *fields == vec!["price".to_string()])
        );
    }
}

#[test]
fn test_array_access() {
    let filter = parse_success(".[0]");
    assert!(matches!(filter.expr, Expr::ArrayAccess { .. }));

    let filter = parse_success(".items[5]");
    assert!(matches!(filter.expr, Expr::ArrayAccess { .. }));

    // Test array access with string literal (should be treated as field access)
    let filter = parse_success(".[\"field name\"]");
    assert!(matches!(filter.expr, Expr::ArrayAccess { .. }));
}

#[test]
fn test_field_access_with_spaces() {
    // Test various field names with spaces using bracket notation
    let filter = parse_success(".[\"US City Name\"]");
    assert!(matches!(filter.expr, Expr::ArrayAccess { .. }));

    let filter = parse_success(".[\"country code\"]");
    assert!(matches!(filter.expr, Expr::ArrayAccess { .. }));

    let filter = parse_success(".[\"field with multiple   spaces\"]");
    assert!(matches!(filter.expr, Expr::ArrayAccess { .. }));

    // Test nested field access with spaces
    let filter = parse_success(".address[\"street name\"]");
    assert!(matches!(filter.expr, Expr::ArrayAccess { .. }));

    // Test bracket notation mixed with dot notation
    let filter = parse_success(".user[\"full name\"]");
    assert!(matches!(filter.expr, Expr::ArrayAccess { .. }));

    // Test bracket notation with special characters in field names
    let filter = parse_success(".[\"field-name\"]");
    assert!(matches!(filter.expr, Expr::ArrayAccess { .. }));

    let filter = parse_success(".[\"field.name\"]");
    assert!(matches!(filter.expr, Expr::ArrayAccess { .. }));
}

#[test]
fn test_array_slice() {
    let filter = parse_success(".[0:5]");
    assert!(matches!(filter.expr, Expr::ArraySlice { .. }));

    // Note: [:10] and [2:] syntax may not be supported
    // let filter = parse_success(".[:10]");
    // assert!(matches!(filter.expr, Expr::ArraySlice { .. }));

    // let filter = parse_success(".[2:]");
    // assert!(matches!(filter.expr, Expr::ArraySlice { .. }));
}

#[test]
fn test_array_iteration() {
    let filter = parse_success(".[]");
    assert!(matches!(filter.expr, Expr::ArrayIteration(_)));
}

#[test]
fn test_literals() {
    // String
    let filter = parse_success("\"hello\"");
    assert!(matches!(filter.expr, Expr::Literal(Literal::String(_))));

    // Escaped string
    let filter = parse_success("\"IT\\\"Dept\"");
    if let Expr::Literal(Literal::String(s)) = filter.expr {
        assert_eq!(s, "IT\"Dept");
    } else {
        panic!("Expected string literal");
    }

    // String with various escape sequences
    let filter = parse_success("\"line1\\nline2\\t\\r\\\\\"");
    if let Expr::Literal(Literal::String(s)) = filter.expr {
        assert_eq!(s, "line1\nline2\t\r\\");
    }

    // Single-quoted strings
    let filter = parse_success("'single quoted'");
    if let Expr::Literal(Literal::String(s)) = filter.expr {
        assert_eq!(s, "single quoted");
    }

    // Empty strings
    let filter = parse_success("\"\"");
    if let Expr::Literal(Literal::String(s)) = filter.expr {
        assert_eq!(s, "");
    }

    let filter = parse_success("''");
    if let Expr::Literal(Literal::String(s)) = filter.expr {
        assert_eq!(s, "");
    }

    // Integer
    let filter = parse_success("42");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Int(_))));

    // Boolean
    let filter = parse_success("true");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Bool(_))));

    let filter = parse_success("false");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Bool(_))));

    // Null
    let filter = parse_success("null");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Null)));

    // BigInt
    let filter = parse_success("123456789012345678901234567890");
    assert!(matches!(filter.expr, Expr::Literal(Literal::BigInt(_))));

    // Negative numbers
    let filter = parse_success("-42");
    if let Expr::Literal(Literal::Int(n)) = filter.expr {
        assert_eq!(n, -42);
    }

    let filter = parse_success("-3.14");
    if let Expr::Literal(Literal::Float(f)) = filter.expr {
        assert_eq!(f, -3.14);
    }

    // Zero
    let filter = parse_success("0");
    if let Expr::Literal(Literal::Int(n)) = filter.expr {
        assert_eq!(n, 0);
    }

    // Very large BigInt
    let filter = parse_success("999999999999999999999999999999999999999");
    assert!(matches!(filter.expr, Expr::Literal(Literal::BigInt(_))));

    // Float with exponent
    let filter = parse_success("1.23e10");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Float(_))));

    let filter = parse_success("1.23E-5");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Float(_))));

    // More number edge cases
    let filter = parse_success("0.0");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Float(f)) if f == 0.0));

    let filter = parse_success("-0.0");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Float(f)) if f == 0.0));

    let filter = parse_success("1e308");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Float(_))));

    let filter = parse_success("-1e308");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Float(_))));

    // Scientific notation edge cases
    let filter = parse_success("1e+10");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Float(_))));

    let filter = parse_success("1E-10");
    assert!(matches!(filter.expr, Expr::Literal(Literal::Float(_))));

    // BigInt edge cases
    let filter = parse_success("9223372036854775808"); // i64::MAX + 1
    assert!(matches!(filter.expr, Expr::Literal(Literal::BigInt(_))));

    let filter = parse_success("-9223372036854775809"); // i64::MIN - 1
    assert!(matches!(filter.expr, Expr::Literal(Literal::BigInt(_))));
}

#[test]
fn test_binary_operations() {
    // Arithmetic
    let filter = parse_success("1 + 2");
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::Add,
            ..
        }
    ));

    let filter = parse_success("3 * 4");
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::Mul,
            ..
        }
    ));

    let filter = parse_success("10 / 2");
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::Div,
            ..
        }
    ));

    let filter = parse_success("5 - 1");
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::Sub,
            ..
        }
    ));

    // Comparisons
    let filter = parse_success("a == b");
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::Eq,
            ..
        }
    ));

    let filter = parse_success("x > y");
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::Gt,
            ..
        }
    ));

    let filter = parse_success("a <= b");
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::Le,
            ..
        }
    ));

    // Logical
    let filter = parse_success("a and b");
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::And,
            ..
        }
    ));

    let filter = parse_success("x or y");
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::Or,
            ..
        }
    ));
}

#[test]
fn test_unary_operations() {
    let filter = parse_success("not true");
    assert!(matches!(
        filter.expr,
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        }
    ));

    let filter = parse_success("del(.field)");
    assert!(matches!(
        filter.expr,
        Expr::UnaryOp {
            op: UnaryOperator::Del,
            ..
        }
    ));
}

#[test]
fn test_if_expression() {
    let filter = parse_success("if type == \"number\" then . * 1.1 else . end");
    assert!(matches!(filter.expr, Expr::If { .. }));
    if let Expr::If {
        condition,
        then_branch,
        else_branch,
    } = filter.expr
    {
        assert!(matches!(
            *condition,
            Expr::BinaryOp {
                op: BinaryOperator::Eq,
                ..
            }
        ));
        assert!(matches!(
            *then_branch,
            Expr::BinaryOp {
                op: BinaryOperator::Mul,
                ..
            }
        ));
        assert!(matches!(*else_branch, Expr::Identity));
    }
}

#[test]
fn test_map_values_if() {
    let filter = parse_success("map(map_values(if type == \"number\" then . * 1.1 else . end))");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { ref name, ref args } if *name == "map" && args.len() == 1)
    );
    if let Expr::FunctionCall { ref args, .. } = filter.expr {
        assert!(
            matches!(&args[0], Expr::FunctionCall { name, args } if *name == "map_values" && args.len() == 1)
        );
        if let Expr::FunctionCall { args, .. } = &args[0] {
            assert!(matches!(&args[0], Expr::If { .. }));
        }
    }
}

#[test]
fn test_function_calls() {
    let filter = parse_success("length");
    println!("length parsed as: {:?}", filter.expr);
    assert!(matches!(filter.expr, Expr::FunctionCall { .. }));

    let filter = parse_success("length(.value)");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { ref name, ref args } if *name == "length" && args.len() == 1)
    );
    if let Expr::FunctionCall { ref args, .. } = filter.expr {
        assert!(
            matches!(&args[0], Expr::FieldAccess { base, fields } if matches!(**base, Expr::Identity) && *fields == vec!["value"])
        );
    }

    let filter = parse_success("map(.name)");
    assert!(matches!(filter.expr, Expr::FunctionCall { .. }));

    let filter = parse_success("select(.age > 30)");
    assert!(matches!(filter.expr, Expr::FunctionCall { .. }));

    let filter = parse_success("max_by(.price)");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { ref name, ref args } if *name == "max_by" && args.len() == 1)
    );
    if let Expr::FunctionCall { ref args, .. } = filter.expr {
        assert!(
            matches!(&args[0], Expr::FieldAccess { base, fields } if matches!(**base, Expr::Identity) && *fields == vec!["price"])
        );
    }

    let filter = parse_success("sort_by(.salary)");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { ref name, ref args } if *name == "sort_by" && args.len() == 1)
    );
    if let Expr::FunctionCall { ref args, .. } = filter.expr {
        assert!(
            matches!(&args[0], Expr::FieldAccess { base, fields } if matches!(**base, Expr::Identity) && *fields == vec!["salary"])
        );
    }
}

#[test]
fn test_object_construction() {
    let filter = parse_success("{name, age}");
    assert!(matches!(filter.expr, Expr::Object { .. }));

    let filter = parse_success("{name: .user.name}");
    assert!(matches!(filter.expr, Expr::Object { .. }));

    let filter = parse_success("{name: $name}");
    assert!(matches!(filter.expr, Expr::Object { .. }));

    let filter = parse_success("{name: .name}");
    assert!(matches!(filter.expr, Expr::Object { .. }));

    let filter = parse_success("{name: .name, brand: .brand}");
    assert!(matches!(filter.expr, Expr::Object { .. }));

    // Test more complex object constructions
    let filter = parse_success("{user: {name: .name, age: .age}, active: true}");
    assert!(matches!(filter.expr, Expr::Object { .. }));

    let filter = parse_success("{\"key with spaces\": .value}");
    assert!(matches!(filter.expr, Expr::Object { .. }));

    let filter = parse_success("{computed: .a + .b}");
    assert!(matches!(filter.expr, Expr::Object { .. }));
}

#[test]
fn test_array_construction() {
    let filter = parse_success("[1, 2, 3]");
    assert!(matches!(filter.expr, Expr::Array(_)));

    let filter = parse_success("[]");
    assert!(matches!(filter.expr, Expr::Array(_)));
}

#[test]
fn test_assignment() {
    let filter = parse_success(".salary += 5000");
    assert!(matches!(filter.expr, Expr::Assignment { .. }));

    // Test different assignment operators
    let filter = parse_success(".count += 1");
    assert!(matches!(filter.expr, Expr::Assignment { .. }));

    let filter = parse_success(".name |= \"default\"");
    assert!(matches!(filter.expr, Expr::Assignment { .. }));

    // Test assignment in complex expressions
    let filter = parse_success("map(.salary += 5000)");
    assert!(matches!(filter.expr, Expr::FunctionCall { .. }));

    let filter = parse_success(".salary += .bonus");
    assert!(matches!(filter.expr, Expr::Assignment { .. }));
}

#[test]
fn test_parentheses() {
    let filter = parse_success("(1 + 2)");
    assert!(matches!(filter.expr, Expr::Paren(_)));
}

#[test]
fn test_nested_parentheses() {
    // Test that deeply nested parentheses parse correctly
    let _filter = parse_success("(((1 + 2)))");
    // Just ensure it parses - the structure is complex to match exactly

    // Test nested parentheses around field access like in stress_006
    let _filter = parse_success("(((.salary)))");
    // Ensure it parses successfully
}

#[test]
fn test_pipelines() {
    let filter = parse_success(".name | length");
    assert!(matches!(filter.expr, Expr::Pipeline(exprs) if exprs.len() == 2));

    let filter = parse_success("map(.age) | sort | .[0]");
    assert!(matches!(filter.expr, Expr::Pipeline(exprs) if exprs.len() == 3));
}

#[test]
fn test_pipeline_starting_with_pipe() {
    let filter = parse_success("| head(3)");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { ref name, ref args } if *name == "head" && args.len() == 1)
    );
    if let Expr::FunctionCall { args, .. } = filter.expr {
        assert!(matches!(&args[0], Expr::Literal(Literal::Int(3))));
    }

    // Test | identifier
    let filter = parse_success("| length");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { ref name, ref args } if *name == "length" && args.is_empty())
    );

    // Test | variable
    let filter = parse_success("| $var");
    assert!(matches!(filter.expr, Expr::Variable(name) if name == "var"));
}

#[test]
fn test_precedence() {
    // Test operator precedence
    let filter = parse_success("1 + 2 * 3");
    // Should parse as 1 + (2 * 3)
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::Add,
            ..
        }
    ));

    let filter = parse_success("not a and b");
    // Should parse as (not a) and b
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::And,
            ..
        }
    ));
}

#[test]
fn test_complex_expressions() {
    // Test complex nested expressions
    let filter = parse_success("map(select(.age > 30 and .department == \"Engineering\"))");
    assert!(matches!(filter.expr, Expr::FunctionCall { name, .. } if name == "map"));

    // Test chained field access
    let filter = parse_success(".[0].department");
    assert!(matches!(filter.expr, Expr::FieldAccess { .. }));

    let filter =
        parse_success("group_by(.department) | map({dept: .[0].department, count: length})");
    assert!(matches!(filter.expr, Expr::Pipeline(_)));

    // Test the full example_001 query with complex object and nested pipeline
    let filter = parse_success(
        "group_by(.department) | map({\n  dept: .[0].department,\n  count: length,\n  avg_salary: (map(.salary) | add / length)\n})",
    );
    assert!(matches!(&filter.expr, Expr::Pipeline(exprs) if exprs.len() == 2));
    if let Expr::Pipeline(exprs) = &filter.expr {
        assert!(matches!(&exprs[0], Expr::FunctionCall { name, .. } if name == "group_by"));
        assert!(matches!(&exprs[1], Expr::FunctionCall { name, .. } if name == "map"));
    }
}

#[test]
fn test_salary_assignment_example() {
    // Test parsing salary assignment query: map(.salary += 5000) | map({name, new_salary: .salary, department})
    let query = "map(.salary += 5000) | map({name, new_salary: .salary, department})";
    let filter = parse_success(query);
    assert!(matches!(&filter.expr, Expr::Pipeline(exprs) if exprs.len() == 2));

    if let Expr::Pipeline(exprs) = &filter.expr {
        // First expression: map(.salary += 5000)
        assert!(
            matches!(&exprs[0], Expr::FunctionCall { name, args } if name == "map" && args.len() == 1)
        );
        if let Expr::FunctionCall { args, .. } = &exprs[0] {
            assert!(matches!(&args[0], Expr::Assignment { .. }));
        }

        // Second expression: map({name, new_salary: .salary, department})
        assert!(
            matches!(&exprs[1], Expr::FunctionCall { name, args } if name == "map" && args.len() == 1)
        );
        if let Expr::FunctionCall { args, .. } = &exprs[1] {
            assert!(matches!(&args[0], Expr::Object { pairs } if pairs.len() == 3));
            if let Expr::Object { pairs } = &args[0] {
                // Check the pairs: name (shorthand), new_salary: .salary, department (shorthand)
                assert!(matches!(&pairs[0], ObjectEntry::Shorthand(key) if key == "name"));
                assert!(
                    matches!(&pairs[1], ObjectEntry::KeyValue { key, .. } if key == "new_salary")
                );
                assert!(matches!(&pairs[2], ObjectEntry::Shorthand(key) if key == "department"));
            }
        }
    }
}

#[test]
fn test_select_age_example() {
    // Test parsing select age query: map(select(.age > 30)) | map({name, age, department})
    let query = "map(select(.age > 30)) | map({name, age, department})";
    let filter = parse_success(query);
    assert!(matches!(&filter.expr, Expr::Pipeline(exprs) if exprs.len() == 2));

    if let Expr::Pipeline(exprs) = &filter.expr {
        // First expression: map(select(.age > 30))
        assert!(
            matches!(&exprs[0], Expr::FunctionCall { name, args } if name == "map" && args.len() == 1)
        );
        if let Expr::FunctionCall { args, .. } = &exprs[0] {
            assert!(
                matches!(&args[0], Expr::FunctionCall { name, args } if name == "select" && args.len() == 1)
            );
            if let Expr::FunctionCall { args, .. } = &args[0] {
                assert!(matches!(
                    &args[0],
                    Expr::BinaryOp {
                        op: BinaryOperator::Gt,
                        ..
                    }
                ));
            }
        }

        // Second expression: map({name, age, department})
        assert!(
            matches!(&exprs[1], Expr::FunctionCall { name, args } if name == "map" && args.len() == 1)
        );
        if let Expr::FunctionCall { args, .. } = &exprs[1] {
            assert!(matches!(&args[0], Expr::Object { pairs } if pairs.len() == 3));
            if let Expr::Object { pairs } = &args[0] {
                // Check the pairs: all shorthand
                assert!(matches!(&pairs[0], ObjectEntry::Shorthand(key) if key == "name"));
                assert!(matches!(&pairs[1], ObjectEntry::Shorthand(key) if key == "age"));
                assert!(matches!(&pairs[2], ObjectEntry::Shorthand(key) if key == "department"));
            }
        }
    }
}

#[test]
fn test_group_by_department_example() {
    // Test parsing group_by department query: group_by(.department) | map({ dept: .[0].department, count: length, avg_salary: (map(.salary) | add / length) })
    let query = r#"group_by(.department) | map({
  dept: .[0].department,
  count: length,
  avg_salary: (map(.salary) | add / length)
})"#;
    let filter = parse_success(query);
    assert!(matches!(&filter.expr, Expr::Pipeline(exprs) if exprs.len() == 2));
    if let Expr::Pipeline(exprs) = &filter.expr {
        assert!(matches!(&exprs[0], Expr::FunctionCall { name, .. } if name == "group_by"));
        assert!(matches!(&exprs[1], Expr::FunctionCall { name, .. } if name == "map"));
    }
}

#[test]
fn test_sort_by_salary_example() {
    // Test parsing sort_by salary query: sort_by(.salary) | reverse | .[0:5] | map({name, salary})
    let query = "sort_by(.salary) | reverse | .[0:5] | map({name, salary})";
    let filter = parse_success(query);
    assert!(matches!(&filter.expr, Expr::Pipeline(exprs) if exprs.len() == 4));

    if let Expr::Pipeline(exprs) = &filter.expr {
        // First expression: sort_by(.salary)
        assert!(
            matches!(&exprs[0], Expr::FunctionCall { name, args } if name == "sort_by" && args.len() == 1)
        );
        if let Expr::FunctionCall { args, .. } = &exprs[0] {
            assert!(
                matches!(&args[0], Expr::FieldAccess { fields, .. } if *fields == vec!["salary".to_string()])
            );
        }

        // Second expression: reverse
        assert!(
            matches!(&exprs[1], Expr::FunctionCall { name, args } if name == "reverse" && args.is_empty())
        );

        // Third expression: .[0:5]
        assert!(
            matches!(&exprs[2], Expr::ArraySlice { start, end, .. } if start.is_some() && end.is_some())
        );
        if let Expr::ArraySlice { start, end, .. } = &exprs[2] {
            assert!(matches!(
                &**start.as_ref().unwrap(),
                Expr::Literal(Literal::Int(0))
            ));
            assert!(matches!(
                &**end.as_ref().unwrap(),
                Expr::Literal(Literal::Int(5))
            ));
        }

        // Fourth expression: map({name, salary})
        assert!(
            matches!(&exprs[3], Expr::FunctionCall { name, args } if name == "map" && args.len() == 1)
        );
        if let Expr::FunctionCall { args, .. } = &exprs[3] {
            assert!(matches!(&args[0], Expr::Object { pairs } if pairs.len() == 2));
            if let Expr::Object { pairs } = &args[0] {
                assert!(matches!(&pairs[0], ObjectEntry::Shorthand(key) if key == "name"));
                assert!(matches!(&pairs[1], ObjectEntry::Shorthand(key) if key == "salary"));
            }
        }
    }
}

#[test]
fn test_parse_errors() {
    // Invalid syntax
    parse_failure("");
    parse_failure("+++");
    parse_failure(".field(");
    parse_failure("{key: value");
    parse_failure("[1, 2,");
    parse_failure("func(");

    // Malformed strings
    parse_failure("\"unterminated string");
    parse_failure("'unterminated single");

    // Invalid syntax examples
    parse_failure("+++");

    // Mismatched brackets
    parse_failure("(1 + 2");
    parse_failure("[1, 2, 3");
    parse_failure("{key: value");
    parse_failure(".field[0");

    // Invalid operators
    parse_failure("1 +++ 2");
    parse_failure("a &&& b");
    parse_failure("x === y");

    // Invalid field access
    parse_failure("..field");
    parse_failure(".field..sub");

    // Invalid function calls
    parse_failure("func(,)");
    parse_failure("func(arg,)");
    parse_failure("func(arg,,arg2)");

    // Invalid object syntax
    parse_failure("{key value}");
    parse_failure("{key: value,}");

    // Invalid array syntax
    parse_failure("[1, 2, 3");
    parse_failure("[1,,2]");

    // Invalid assignment
    parse_failure(".field +++= value");
    parse_failure(".field = value"); // Should use == for comparison

    // Invalid if syntax
    parse_failure("if condition then expr");
    parse_failure("if condition expr else expr end");
    parse_failure("condition then expr else expr end");

    // Invalid pipelines
    parse_failure("expr |");
    parse_failure("expr || expr");

    // Invalid variables
    parse_failure("$");
    parse_failure("$123invalid");
    parse_failure("$var("); // Variables can't be called like functions directly
}

#[test]
fn test_try_catch_expression() {
    // Test parsing try-catch expression (maps to iferror function call)
    let filter = parse_success("try .field catch null");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { name, args } if name == "iferror" && args.len() == 2)
    );

    let filter = parse_success("try .value catch \"default\"");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { name, args } if name == "iferror" && args.len() == 2)
    );

    // Test try-catch in pipeline
    let filter = parse_success("map(try .field catch empty) | flatten");
    assert!(matches!(filter.expr, Expr::Pipeline(exprs) if exprs.len() == 2));
}

#[test]
fn test_unnest_function() {
    // Test parsing the unnest function example: map(.values | fromjson | map({id, value: .})) | flatten
    let query = "map(.values | fromjson | map({id, value: .})) | flatten";
    let filter = parse_success(query);

    assert!(matches!(&filter.expr, Expr::Pipeline(exprs) if exprs.len() == 2));

    if let Expr::Pipeline(exprs) = &filter.expr {
        // First expression: map(.values | fromjson | map({id, value: .}))
        assert!(
            matches!(&exprs[0], Expr::FunctionCall { name, args } if name == "map" && args.len() == 1)
        );
        if let Expr::FunctionCall { args, .. } = &exprs[0] {
            assert!(matches!(&args[0], Expr::Pipeline(inner_exprs) if inner_exprs.len() == 3));
            if let Expr::Pipeline(inner_exprs) = &args[0] {
                // .values
                assert!(
                    matches!(&inner_exprs[0], Expr::FieldAccess { base, fields } if matches!(**base, Expr::Identity) && *fields == vec!["values"])
                );
                // fromjson
                assert!(
                    matches!(&inner_exprs[1], Expr::FunctionCall { name, args } if name == "fromjson" && args.is_empty())
                );
                // map({id, value: .})
                assert!(
                    matches!(&inner_exprs[2], Expr::FunctionCall { name, args } if name == "map" && args.len() == 1)
                );
                if let Expr::FunctionCall { args, .. } = &inner_exprs[2] {
                    assert!(matches!(&args[0], Expr::Object { pairs } if pairs.len() == 2));
                    if let Expr::Object { pairs } = &args[0] {
                        assert!(matches!(&pairs[0], ObjectEntry::Shorthand(key) if key == "id"));
                        assert!(
                            matches!(&pairs[1], ObjectEntry::KeyValue { key, value } if key == "value" && matches!(*value, Expr::Identity))
                        );
                    }
                }
            }
        }

        // Second expression: flatten
        assert!(
            matches!(&exprs[1], Expr::FunctionCall { name, args } if name == "flatten" && args.is_empty())
        );
    }
}

#[test]
fn test_array_slice_edge_cases() {
    // Test various array slice patterns
    let filter = parse_success(".[1:]");
    if let Expr::ArraySlice { start, end, .. } = filter.expr {
        assert!(start.is_some());
        assert!(end.is_none());
    }

    let filter = parse_success(".[:5]");
    if let Expr::ArraySlice { start, end, .. } = filter.expr {
        assert!(start.is_none());
        assert!(end.is_some());
    }

    let filter = parse_success(".[:]");
    if let Expr::ArraySlice { start, end, .. } = filter.expr {
        assert!(start.is_none());
        assert!(end.is_none());
    }

    let filter = parse_success(".[2:10]");
    if let Expr::ArraySlice { start, end, .. } = filter.expr {
        assert!(start.is_some());
        assert!(end.is_some());
    }

    // Test array slice on field access
    let filter = parse_success(".items[1:5]");
    assert!(matches!(filter.expr, Expr::ArraySlice { .. }));

    // Test array slice in expressions
    let filter = parse_success("(.data | .[0:10])");
    assert!(matches!(filter.expr, Expr::Paren(_)));
}

#[test]
fn test_edge_cases() {
    // Test invalid inputs
    parse_failure("");
    parse_failure("+++");
    parse_failure(".field(");
    parse_failure("{key: value");
    parse_failure("[1, 2,");
    parse_failure("func(");

    // Test complex expressions with correct precedence
    let filter = parse_success("1 + 2 * 3 > 4 and true or false");
    assert!(matches!(
        filter.expr,
        Expr::BinaryOp {
            op: BinaryOperator::Or,
            ..
        }
    ));

    // Test mixed object keys
    let filter = parse_success("{name, age: .person.age, \"key\": .value}");
    assert!(matches!(filter.expr, Expr::Object { .. }));

    // Test function calls with semicolon separators
    let filter = parse_success("add(.a; .b)");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { name, args } if name == "add" && args.len() == 2)
    );

    // Test function calls with comma separators
    let filter = parse_success("add(.a, .b)");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { name, args } if name == "add" && args.len() == 2)
    );

    // Test mixed separators (should work with commas as primary)
    let filter = parse_success("func(.a, .b; .c)");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { name, args } if name == "func" && args.len() == 3)
    );

    // Test semicolon in complex expressions
    let filter = parse_success("reduce(.a; .b; . + .)");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { name, args } if name == "reduce" && args.len() == 3)
    );
}

#[test]
fn test_builtin_functions() {
    // Test that all functions in the BUILTIN_FUNCTIONS list parse correctly
    let builtin_functions = [
        "length",
        "map",
        "sort",
        "keys",
        "values",
        "add",
        "sum",
        "min",
        "max",
        "count",
        "count_if",
        "avg_if",
        "avg_ifs",
        "least_frequent",
        "most_frequent",
        "mean",
        "avg",
        "median",
        "std",
        "var",
        "first",
        "last",
        "reverse",
        "unique",
        "flatten",
        "transpose",
        "pivot",
        "unpivot",
        "join",
        "inner_join",
        "left_join",
        "right_join",
        "outer_join",
        "concat",
        "contains",
        "startswith",
        "endswith",
        "lstrip",
        "rstrip",
        "tolower",
        "toupper",
        "trim",
        "replace",
        "dos2unix",
        "type",
        "isnan",
        "isinf",
        "isnormal",
        "isfinite",
        "transform_values",
        "map_values",
        "url_set_query_string",
        "month",
        "array_shift",
        "array_unshift",
        "array_push",
        "transliterate",
        "fromjson",
        "tojson",
        "base32_encode",
        "base58_encode",
        "base58_decode",
        "base64_encode",
        "base64_decode",
        "sha512",
        "sha256",
        "url_strip_fragment",
        "abs",
        "to_ascii",
        "is_valid_utf8",
        "to_valid_utf8",
        "snake_case",
        "camel_case",
        "tostring",
        "tonumber",
        "split",
        "join",
        "concat",
        "coalesce",
        "columns",
        "shape",
        "dtypes",
        "group_by",
        "pi",
        "rand",
        "randarray",
        "randbetween",
        "floor",
        "roundup",
        "rounddown",
        "ceil",
        "mround",
        "pow",
        "atan",
        "acos",
        "asin",
        "sin",
        "tan",
        "exp",
        "minute",
        "has",
        "empty",
        "error",
        "sort_by",
        "repeat",
        "zip",
        "md5",
        "sha1",
        "SHA1",
    ];

    for func in &builtin_functions {
        // Test function without arguments
        let input = *func;
        let filter = parse_success(input);
        assert!(
            matches!(filter.expr, Expr::FunctionCall { name, .. } if name == *func),
            "Failed to parse builtin function: {}",
            func
        );

        // Test function with arguments (using a dummy argument)
        let input_with_arg = format!("{}(.dummy)", func);
        let filter = parse_success(&input_with_arg);
        assert!(
            matches!(filter.expr, Expr::FunctionCall { name, args, .. } if name == *func && args.len() == 1),
            "Failed to parse builtin function with args: {}",
            func
        );
    }
}

#[test]
fn test_variable_references() {
    // Test simple variable references
    let filter = parse_success("$config");
    assert!(matches!(filter.expr, Expr::Variable(name) if name == "config"));

    let filter = parse_success("$user");
    assert!(matches!(filter.expr, Expr::Variable(name) if name == "user"));

    // Test variable with field access
    let filter = parse_success("$user.settings.theme");
    assert!(matches!(filter.expr, Expr::FieldAccess { .. }));

    // Test variable with array access
    let filter = parse_success("$items[0]");
    assert!(matches!(filter.expr, Expr::ArrayAccess { .. }));

    // Test variable with array slice
    let filter = parse_success("$data[1:10]");
    assert!(matches!(filter.expr, Expr::ArraySlice { .. }));

    // Test variable in expressions
    let filter = parse_success("$base + $offset");
    assert!(
        matches!(filter.expr, Expr::BinaryOp { left, right, op: BinaryOperator::Add } if matches!(*left, Expr::Variable(_)) && matches!(*right, Expr::Variable(_)))
    );

    // Test variable in function calls
    let filter = parse_success("map($config.field)");
    assert!(
        matches!(filter.expr, Expr::FunctionCall { name, args } if name == "map" && args.len() == 1)
    );
}

#[test]
fn test_whitespace_handling() {
    // Test various whitespace configurations
    let expressions = vec![
        ".name",
        " .name ",
        "\t.name\n",
        ".name\t",
        "  .name  ",
        "{ name : .value }",
        "{name:.value}",
        "func(arg)",
        "a + b",
        "a\t+\tb",
        "a\n+\nb",
        "if condition then expr else expr end",
        ".field | map(.value) | sort",
        ".field|\nmap(.value)|sort",
    ];

    for expr in expressions {
        println!("Testing whitespace in: {:?}", expr);
        parse_success(expr);
    }
}

#[test]
fn test_complex_expressions_comprehensive() {
    // Test complex nested pipelines
    let filter = parse_success(
        "map(select(.age > 30)) | group_by(.department) | map({dept: .[0].department, employees: ., avg_salary: (map(.salary) | add / length)})",
    );
    assert!(matches!(filter.expr, Expr::Pipeline(exprs) if exprs.len() == 3));

    // Test conditional logic
    let filter = parse_success("if .status == \"active\" then .score * 1.1 else .score end");
    assert!(matches!(filter.expr, Expr::If { .. }));

    // Test aggregations
    let filter = parse_success(
        "group_by(.category) | map({category: .[0].category, total: map(.amount) | add, count: length})",
    );
    assert!(matches!(filter.expr, Expr::Pipeline(_)));
}

#[test]
fn test_all_examples() {
    // Since examples directory doesn't exist, test a comprehensive set of example queries
    let example_queries = vec![
        // Basic field access
        ".name",
        ".user.name",
        ".items[0].price",
        // Array operations
        ".[]",
        ".[0:5]",
        ".[1:]",
        ".[:10]",
        // Function calls
        "length",
        "length(.items)",
        "map(.name)",
        "select(.age > 30)",
        "sort_by(.price)",
        "group_by(.department)",
        // Object construction
        "{name, age}",
        "{name: .user.name, age: .user.age}",
        "{full_name: (.first + \" \" + .last)}",
        // Array construction
        "[1, 2, 3]",
        "[.name, .age]",
        // Literals
        "\"hello world\"",
        "42",
        "3.14",
        "true",
        "false",
        "null",
        // Binary operations
        "1 + 2",
        ".age > 30",
        ".name == \"John\"",
        ".active and .verified",
        // Unary operations
        "not .disabled",
        "del(.obsolete)",
        // Assignment
        ".salary += 5000",
        ".status |= \"active\"",
        // If expressions
        "if .type == \"premium\" then .price * 1.2 else .price end",
        // Pipelines
        ".users | map(.name) | sort",
        "map(select(.age > 21)) | .[0:10]",
        // Complex expressions
        "map(.salary + .bonus) | add / length",
        "group_by(.category) | map({cat: .[0].category, total: map(.amount) | add})",
        // Try-catch (mapped to iferror)
        "try .field catch null",
        // Variables
        "$config.timeout",
        "$user.preferences.theme",
        // Parentheses
        "(1 + 2) * 3",
        "(.a + .b) / 2",
        // Sequences
        ".name, .age, .email",
        // Complex nested
        "map(select(.status == \"active\" and .type != \"trial\")) | group_by(.region) | map({region: .[0].region, count: length, avg_score: (map(.score) | add / length)})",
    ];

    let mut success_count = 0;
    for query in &example_queries {
        println!("Testing example query: {}", query);
        parse_success(query);
        success_count += 1;
    }

    println!("Successfully parsed {} example queries", success_count);
    assert_eq!(
        success_count,
        example_queries.len(),
        "Some example queries failed to parse"
    );
}

#[test]
fn test_more_complex_expressions() {
    // Test more complex expressions to expand coverage
    let complex_queries = vec![
        // Nested function calls with multiple arguments
        "reduce(.a; .b; . + .)",
        "map(select(.age >= 18 and .age <= 65)) | sort_by(.name) | .[0:20]",
        // Complex object with nested structures
        "{user: {name: .first + \" \" + .last, age: .age}, metadata: {created: .timestamp}}",
        // Array slicing with complex indices
        ".data[(.start_index + 1):(.end_index - 1)]",
        // Multiple assignments in pipeline
        "map(.salary += .bonus) | map(.status |= \"updated\") | map({name, salary: .salary, status: .status})",
        // Nested if expressions
        "if .type == \"employee\" then if .level == \"senior\" then .salary * 1.5 else .salary * 1.2 end else .salary end",
        // Complex aggregations
        "group_by(.department) | map({dept: .[0].department, employees: ., stats: {count: length, avg_salary: (map(.salary) | add / length), max_salary: map(.salary) | max, min_salary: map(.salary) | min}})",
        // Function calls with complex arguments
        "map_values(if type == \"object\" then map_values(.) else . end)",
        // Variables in complex expressions
        "$config.base_salary + (.experience_years * $config.salary_multiplier)",
        // Array operations with functions
        "map(.tags | sort | unique) | flatten | group_by(.) | map({tag: .[0], count: length}) | sort_by(.count) | reverse",
        // Complex pipelines with multiple transformations
        ".data | select(.active) | map(.value * 1.1) | sort | .[10:50] | map(round(.)) | sum",
        // Try-catch in complex context
        "map(try .optional_field catch 0) | select(. > 0) | avg",
        // Mixed data types and operations
        "[1, 2, 3] | map(. * .) | add + (.data | length)",
        // Recursive structures
        "map(if type == \"array\" then map(.) else . end)",
        // Advanced string operations
        "map(.name | tolower | replace(\" \", \"_\")) | unique",
        // Date/time operations (assuming functions exist)
        "select(hour(.timestamp) >= 9 and hour(.timestamp) <= 17) | group_by(day(.timestamp)) | map({date: .[0].timestamp, count: length})",
        // More complex expressions
        "map(.transactions | select(.amount > 0) | map(.amount) | add) | sort | reverse | .[0:10]",
        "group_by(.category) | map({category: .[0].category, items: ., total: map(.price * .quantity) | add, avg_price: (map(.price) | add) / length})",
        "if .type == \"premium\" then .price * 1.2 else if .type == \"basic\" then .price * 1.1 else .price end end",
        ".data | map(select(.status == \"active\")) | flatten | group_by(.user_id) | map({user_id: .[0].user_id, activities: ., count: length})",
    ];

    for query in &complex_queries {
        println!("Testing complex query: {}", query);
        parse_success(query);
    }
}

#[test]
fn test_edge_cases_and_error_handling() {
    // Test edge cases that should parse successfully
    let valid_edge_cases = vec![
        // Empty structures
        "{}",
        "[]",
        // Single character identifiers
        "a",
        "x",
        // Numbers with various formats
        "0",
        "-0",
        "0.0",
        "-0.0",
        "1e10",
        "1E-5",
        "123456789012345678901234567890",
        // Strings with special characters
        "\"\"",
        "\"a\"",
        "\"\\n\\t\\r\"",
        "'single'",
        "'with \" double'",
        // Complex field access
        ".a.b.c.d.e",
        ".[0][1][2]",
        // Nested parentheses
        "(((1)))",
        "(((.a)))",
        // Mixed operators
        "1 + 2 * 3 - 4 / 5",
        // Function calls with no args
        "now()",
        "random()",
        // Variables
        "$a",
        "$_private",
        "$var1",
        // Assignments
        ".y += 2",
        ".z |= 3",
        // Unary ops
        "not not true",
        "del del .field",
        // If with minimal
        "if true then 1 else 0 end",
    ];

    for case in &valid_edge_cases {
        println!("Testing valid edge case: {}", case);
        parse_success(case);
    }

    // Test cases that should fail with specific errors
    let invalid_cases = vec![
        // Incomplete expressions
        "if true then",
        "{key:",
        "[1,",
        "func(",
        ".field[",
        "\"unclosed",
        "'unclosed",
        // Invalid identifiers
        "$123",
        "123invalid",
        // Invalid operators
        "1 +++ 2",
        "a && b",
        "x === y",
        // Mismatched brackets
        "(1 + 2",
        "[1, 2, 3",
        "{key: value",
        ".field[0",
        // Invalid syntax
        "..field",
        ".field..sub",
        "func(,)",
        "func(arg,)",
        "func(arg,,arg2)",
        // Invalid literals
        "0x123",  // Hex not supported
        "0123",   // Octal not supported
        "00123",  // Multiple leading zeros
        "0o123",  // Octal notation
        "0b101",  // Binary notation
        "1.2.3",  // Invalid float
        "1e10e5", // Multiple exponents
        "++123",  // Invalid
        "--123",  // Invalid
        // Invalid assignments
        ".field +++= value",
        ".field = value", // Should use ==
        // Invalid pipelines
        "expr |",
        "expr || expr",
        "expr | | expr",
        // Invalid if
        "if then else end",
        "if cond then else end",
        "if cond then expr end",
        // Invalid select
        "select , from table",
        "select col from where cond",
    ];

    for case in &invalid_cases {
        println!("Testing invalid case: {}", case);
        parse_failure(case);
    }
}

#[test]
fn test_whitespace_and_formatting() {
    // Test various whitespace configurations
    let whitespace_variants = vec![
        ".name",
        " .name ",
        "\t.name\n",
        ".name\t",
        "  .name  ",
        "{ name : .value }",
        "{name:.value}",
        "func(arg)",
        "a + b",
        "a\t+\tb",
        "a\n+\nb",
        "if condition then expr else expr end",
        ".field | map(.value) | sort",
        ".field|\nmap(.value)|sort",
        ".field\n|\n\tmap(.value)\n|\n\tsort",
    ];

    for variant in &whitespace_variants {
        println!("Testing whitespace variant: {:?}", variant);
        parse_success(variant);
    }
}

#[test]
fn test_builtin_function_coverage() {
    // Test a subset of builtin functions with various argument patterns
    let function_tests = vec![
        ("length", vec!["", "(.items)"]),
        ("map", vec!["(.name)", "(.age + 1)"]),
        ("select", vec!["(.age > 30)", "(not .disabled)"]),
        ("sort_by", vec!["(.price)", "(.name)"]),
        ("group_by", vec!["(.department)"]),
        ("add", vec!["(.a; .b)", "(.x, .y)"]),
        ("sum", vec!["", "(.values)"]),
        ("min", vec!["", "(.scores)"]),
        ("max", vec!["", "(.prices)"]),
        ("avg", vec!["", "(.numbers)"]),
        ("first", vec!["", "(.list)"]),
        ("last", vec!["", "(.array)"]),
        ("reverse", vec!["", "(.data)"]),
        ("unique", vec!["", "(.items)"]),
        ("flatten", vec!["", "(.nested)"]),
        ("contains", vec!["(\"test\")", "(.field; \"value\")"]),
        ("type", vec!["", "(.value)"]),
        ("keys", vec!["", "(.object)"]),
        ("values", vec!["", "(.object)"]),
        ("tostring", vec!["", "(.number)"]),
        ("tonumber", vec!["", "(.string)"]),
        ("split", vec!["(\",\")", "(.text; \";\")"]),
        ("join", vec!["(\",\")", "(.array; \" \")"]),
        ("abs", vec!["", "(.number)"]),
        ("round", vec!["", "(.float)"]),
        ("floor", vec!["", "(.float)"]),
        ("ceil", vec!["", "(.float)"]),
        ("pow", vec!["(.base; .exp)"]),
        ("sqrt", vec!["", "(.number)"]),
        ("sin", vec!["", "(.angle)"]),
        ("cos", vec!["", "(.angle)"]),
        ("tan", vec!["", "(.angle)"]),
        ("log", vec!["", "(.number)"]),
        ("exp", vec!["", "(.number)"]),
    ];

    for (func, arg_patterns) in &function_tests {
        for args in arg_patterns {
            let query = if args.is_empty() {
                func.to_string()
            } else {
                format!("{}{}", func, args)
            };
            println!("Testing function: {}", query);
            parse_success(&query);
        }
    }
}

#[test]
fn test_parse_error_display() {
    // Test Display implementation for each ParseError variant
    let error = ParseError::UnexpectedToken {
        found: "invalid".to_string(),
        expected: vec!["identifier".to_string(), "number".to_string()],
        position: 10,
    };
    assert_eq!(
        format!("{}", error),
        "Unexpected token 'invalid' at position 10. Expected one of: identifier, number"
    );

    let error = ParseError::UnexpectedToken {
        found: "bad".to_string(),
        expected: vec![],
        position: 5,
    };
    assert_eq!(format!("{}", error), "Unexpected token 'bad' at position 5");

    let error = ParseError::InvalidSyntax {
        message: "Invalid operator".to_string(),
        position: 15,
    };
    assert_eq!(
        format!("{}", error),
        "Invalid syntax at position 15: Invalid operator"
    );

    let error = ParseError::UnterminatedString { position: 20 };
    assert_eq!(
        format!("{}", error),
        "Unterminated string literal starting at position 20"
    );

    let error = ParseError::InvalidNumber {
        number: "12.34.56".to_string(),
        position: 25,
    };
    assert_eq!(
        format!("{}", error),
        "Invalid number '12.34.56' at position 25"
    );

    let error = ParseError::UnknownFunction {
        name: "unknown_func".to_string(),
        position: 30,
    };
    assert_eq!(
        format!("{}", error),
        "Unknown function 'unknown_func' at position 30"
    );

    let error = ParseError::InvalidFieldAccess {
        field: "invalid.field".to_string(),
        position: 35,
    };
    assert_eq!(
        format!("{}", error),
        "Invalid field access 'invalid.field' at position 35"
    );

    let error = ParseError::MismatchedBrackets {
        opening: '(',
        position: 40,
    };
    assert_eq!(
        format!("{}", error),
        "Mismatched brackets. Opening '(' at position 40 has no matching close"
    );

    let error = ParseError::EmptyInput;
    assert_eq!(format!("{}", error), "Empty input");

    let error = ParseError::General {
        message: "General error occurred".to_string(),
    };
    assert_eq!(format!("{}", error), "General error occurred");

    let error = ParseError::NomError {
        message: "Nom parsing failed".to_string(),
    };
    assert_eq!(format!("{}", error), "Nom error: Nom parsing failed");
}

#[test]
fn test_parse_error_partial_eq() {
    // Test PartialEq implementation
    let error1 = ParseError::UnexpectedToken {
        found: "test".to_string(),
        expected: vec!["a".to_string(), "b".to_string()],
        position: 10,
    };
    let error2 = ParseError::UnexpectedToken {
        found: "test".to_string(),
        expected: vec!["a".to_string(), "b".to_string()],
        position: 10,
    };
    let error3 = ParseError::UnexpectedToken {
        found: "different".to_string(),
        expected: vec!["a".to_string(), "b".to_string()],
        position: 10,
    };

    assert_eq!(error1, error2);
    assert_ne!(error1, error3);

    // Test different variants are not equal
    let error4 = ParseError::EmptyInput;
    assert_ne!(error1, error4);
}

#[test]
fn test_parse_error_clone() {
    // Test Clone implementation
    let original = ParseError::InvalidSyntax {
        message: "Test message".to_string(),
        position: 42,
    };
    let cloned = original.clone();
    assert_eq!(original, cloned);
}

#[test]
fn test_parse_error_from_nom_error() {
    use nom::error::{Error, ErrorKind};
    use std::num::NonZeroUsize;

    // Test From<nom::Err<nom::error::Error<&str>>> for ParseError
    let nom_error = nom::Err::Error(Error {
        input: "remaining input",
        code: ErrorKind::Tag,
    });
    let parse_error: ParseError = nom_error.into();
    match parse_error {
        ParseError::InvalidSyntax { message, position } => {
            assert!(message.contains("Parse error"));
            assert_eq!(position, 0); // Position is set to 0 due to limitation
        }
        _ => panic!("Expected InvalidSyntax error"),
    }

    // Test Incomplete
    let nom_incomplete = nom::Err::<nom::error::Error<&str>>::Incomplete(nom::Needed::Size(
        NonZeroUsize::new(1).unwrap(),
    ));
    let parse_error: ParseError = nom_incomplete.into();
    match parse_error {
        ParseError::General { message } => {
            assert_eq!(message, "Incomplete input");
        }
        _ => panic!("Expected General error"),
    }
}

#[test]
fn test_parse_error_construction() {
    // Test constructing each variant
    let _unexpected_token = ParseError::UnexpectedToken {
        found: "token".to_string(),
        expected: vec!["expected".to_string()],
        position: 1,
    };

    let _invalid_syntax = ParseError::InvalidSyntax {
        message: "msg".to_string(),
        position: 2,
    };

    let _unterminated_string = ParseError::UnterminatedString { position: 3 };

    let _invalid_number = ParseError::InvalidNumber {
        number: "num".to_string(),
        position: 4,
    };

    let _unknown_function = ParseError::UnknownFunction {
        name: "func".to_string(),
        position: 5,
    };

    let _invalid_field_access = ParseError::InvalidFieldAccess {
        field: "field".to_string(),
        position: 6,
    };

    let _mismatched_brackets = ParseError::MismatchedBrackets {
        opening: '[',
        position: 7,
    };

    let _empty_input = ParseError::EmptyInput;

    let _general = ParseError::General {
        message: "general".to_string(),
    };

    let _nom_error = ParseError::NomError {
        message: "nom".to_string(),
    };

    // Ensure they are all different
    assert_ne!(_unexpected_token, _invalid_syntax);
    assert_ne!(_invalid_syntax, _unterminated_string);
    assert_ne!(_unterminated_string, _invalid_number);
    assert_ne!(_invalid_number, _unknown_function);
    assert_ne!(_unknown_function, _invalid_field_access);
    assert_ne!(_invalid_field_access, _mismatched_brackets);
    assert_ne!(_mismatched_brackets, _empty_input);
    assert_ne!(_empty_input, _general);
    assert_ne!(_general, _nom_error);
}
