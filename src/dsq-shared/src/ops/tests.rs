//! Tests for operations

use super::*;
use crate::value::Value;
use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_operation() {
        let value = Value::int(42);
        let op = IdentityOperation;
        let result = op.apply(&value).unwrap();
        assert_eq!(result, value);
        assert_eq!(op.description(), "identity");
        assert!(op.is_applicable(&value));
    }

    #[test]
    fn test_literal_operation() {
        let value = Value::string("test");
        let op = LiteralOperation::new(value.clone());
        let result = op.apply(&Value::null()).unwrap();
        assert_eq!(result, value);
        assert_eq!(op.description(), format!("literal: {:?}", value));
    }

    #[test]
    fn test_field_access_operation() {
        let obj = Value::object(HashMap::from([
            ("name".to_string(), Value::string("Alice")),
            ("age".to_string(), Value::int(30)),
            (
                "nested".to_string(),
                Value::object(HashMap::from([("inner".to_string(), Value::bool(true))])),
            ),
        ]));

        let op = FieldAccessOperation::new("name".to_string());
        let result = op.apply(&obj).unwrap();
        assert_eq!(result, Value::string("Alice"));
        assert_eq!(op.description(), "field access: name");

        let nested_op =
            FieldAccessOperation::with_fields(vec!["nested".to_string(), "inner".to_string()]);
        let result = nested_op.apply(&obj).unwrap();
        assert_eq!(result, Value::bool(true));
        assert_eq!(nested_op.description(), "field access: nested.inner");

        // Missing field
        let missing_op = FieldAccessOperation::new("missing".to_string());
        let result = missing_op.apply(&obj).unwrap();
        assert_eq!(result, Value::Null);

        // Field access on non-object types should error
        let arr = Value::array(vec![Value::int(1), Value::int(2)]);
        let field_on_array = FieldAccessOperation::new("field".to_string());
        assert!(field_on_array.apply(&arr).is_err());

        let str_val = Value::string("test");
        let field_on_string = FieldAccessOperation::new("field".to_string());
        assert!(field_on_string.apply(&str_val).is_err());

        let int_val = Value::int(42);
        let field_on_int = FieldAccessOperation::new("field".to_string());
        assert!(field_on_int.apply(&int_val).is_err());

        // Field access on null returns null
        let field_on_null = FieldAccessOperation::new("field".to_string());
        let result = field_on_null.apply(&Value::Null).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_index_operation() {
        let arr = Value::array(vec![Value::int(1), Value::int(2), Value::int(3)]);
        let str_val = Value::string("hello");

        let index_op = IndexOperation::new(vec![Box::new(LiteralOperation::new(Value::int(0)))]);
        let result = index_op.apply(&arr).unwrap();
        assert_eq!(result, Value::int(1));

        let result = index_op.apply(&str_val).unwrap();
        assert_eq!(result, Value::string("h"));

        // Negative index
        let neg_index_op =
            IndexOperation::new(vec![Box::new(LiteralOperation::new(Value::int(-1)))]);
        let result = neg_index_op.apply(&arr).unwrap();
        assert_eq!(result, Value::int(3));

        // Out of bounds
        let oob_op = IndexOperation::new(vec![Box::new(LiteralOperation::new(Value::int(10)))]);
        let result = oob_op.apply(&arr).unwrap();
        assert_eq!(result, Value::Null);

        // Invalid index type
        let invalid_op = IndexOperation::new(vec![Box::new(LiteralOperation::new(Value::string(
            "not_int",
        )))]);
        assert!(invalid_op.apply(&arr).is_err());

        // Index on non-indexable types should error
        let obj = Value::object(HashMap::from([("key".to_string(), Value::int(1))]));
        let index_on_obj =
            IndexOperation::new(vec![Box::new(LiteralOperation::new(Value::int(0)))]);
        assert!(index_on_obj.apply(&obj).is_err());

        let float_val = Value::float(3.14);
        let index_on_float =
            IndexOperation::new(vec![Box::new(LiteralOperation::new(Value::int(0)))]);
        assert!(index_on_float.apply(&float_val).is_err());

        let bool_val = Value::bool(true);
        let index_on_bool =
            IndexOperation::new(vec![Box::new(LiteralOperation::new(Value::int(0)))]);
        assert!(index_on_bool.apply(&bool_val).is_err());
    }

    #[test]
    fn test_iterate_operation() {
        let arr = Value::array(vec![Value::int(1), Value::int(2)]);
        let obj = Value::object(HashMap::from([
            ("a".to_string(), Value::int(1)),
            ("b".to_string(), Value::int(2)),
        ]));

        let op = IterateOperation;
        let arr_result = op.apply(&arr).unwrap();
        assert_eq!(arr_result, arr);

        let obj_result = op.apply(&obj).unwrap();
        match obj_result {
            Value::Array(values) => {
                assert_eq!(values.len(), 2);
                // Order may vary due to HashMap
            }
            _ => panic!("Expected array"),
        }

        let primitive = Value::int(42);
        let result = op.apply(&primitive).unwrap();
        assert_eq!(result, primitive);
    }

    #[test]
    fn test_object_construct_operation() {
        let obj = Value::object(HashMap::from([
            ("name".to_string(), Value::string("Alice")),
            ("age".to_string(), Value::int(30)),
        ]));

        let field_ops = vec![
            (
                Box::new(LiteralOperation::new(Value::string("name")))
                    as Box<dyn Operation + Send + Sync>,
                Some(vec![
                    Box::new(FieldAccessOperation::new("name".to_string()))
                        as Box<dyn Operation + Send + Sync>,
                ]),
            ),
            (
                Box::new(LiteralOperation::new(Value::string("age")))
                    as Box<dyn Operation + Send + Sync>,
                None,
            ), // shorthand
        ];

        let op = ObjectConstructOperation::new(field_ops);
        let result = op.apply(&obj).unwrap();
        match result {
            Value::Object(result_obj) => {
                assert_eq!(result_obj.get("name"), Some(&Value::string("Alice")));
                assert_eq!(result_obj.get("age"), Some(&Value::int(30)));
            }
            _ => panic!("Expected object"),
        }

        // Null input
        let result = op.apply(&Value::Null).unwrap();
        assert_eq!(result, Value::Null);

        // Object construction with invalid key type
        let invalid_key_ops = vec![(
            Box::new(LiteralOperation::new(Value::int(42))) as Box<dyn Operation + Send + Sync>,
            Some(vec![
                Box::new(FieldAccessOperation::new("name".to_string()))
                    as Box<dyn Operation + Send + Sync>,
            ]),
        )];
        let invalid_op = ObjectConstructOperation::new(invalid_key_ops);
        assert!(invalid_op.apply(&obj).is_err());
    }

    #[test]
    fn test_array_construct_operation() {
        let obj = Value::object(HashMap::from([
            ("a".to_string(), Value::int(1)),
            ("b".to_string(), Value::int(2)),
        ]));

        let element_ops = vec![
            Box::new(FieldAccessOperation::new("a".to_string()))
                as Box<dyn Operation + Send + Sync>,
            Box::new(FieldAccessOperation::new("b".to_string()))
                as Box<dyn Operation + Send + Sync>,
        ];

        let op = ArrayConstructOperation::new(element_ops);
        let result = op.apply(&obj).unwrap();
        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], Value::int(1));
                assert_eq!(arr[1], Value::int(2));
            }
            _ => panic!("Expected array"),
        }

        // Empty array construction
        let empty_ops: Vec<Box<dyn Operation + Send + Sync>> = vec![];
        let empty_op = ArrayConstructOperation::new(empty_ops);
        let result = empty_op.apply(&obj).unwrap();
        match result {
            Value::Array(arr) => assert_eq!(arr.len(), 0),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_sequence_operation() {
        let obj = Value::object(HashMap::from([("x".to_string(), Value::int(1))]));

        let expr_ops = vec![
            vec![Box::new(FieldAccessOperation::new("x".to_string()))
                as Box<dyn Operation + Send + Sync>],
            vec![
                Box::new(LiteralOperation::new(Value::int(42))) as Box<dyn Operation + Send + Sync>
            ],
        ];

        let op = SequenceOperation::new(expr_ops);
        let result = op.apply(&obj).unwrap();
        match result {
            Value::Array(results) => {
                assert_eq!(results.len(), 2);
                assert_eq!(results[0], Value::int(1));
                assert_eq!(results[1], Value::int(42));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_arithmetic_operations() {
        let a = Value::int(10);
        let b = Value::int(5);

        let add_op = AddOperation::new(
            vec![Box::new(LiteralOperation::new(a.clone()))],
            vec![Box::new(LiteralOperation::new(b.clone()))],
        );
        let result = add_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::int(15));
        assert_eq!(add_op.description(), "add");

        let sub_op = SubOperation::new(
            vec![Box::new(LiteralOperation::new(a.clone()))],
            vec![Box::new(LiteralOperation::new(b.clone()))],
        );
        let result = sub_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::int(5));

        let mul_op = MulOperation::new(
            vec![Box::new(LiteralOperation::new(a.clone()))],
            vec![Box::new(LiteralOperation::new(b.clone()))],
        );
        let result = mul_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::int(50));

        let div_op = DivOperation::new(
            vec![Box::new(LiteralOperation::new(a.clone()))],
            vec![Box::new(LiteralOperation::new(b.clone()))],
        );
        let result = div_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Float(2.0));

        // Division by zero
        let zero_div = DivOperation::new(
            vec![Box::new(LiteralOperation::new(a.clone()))],
            vec![Box::new(LiteralOperation::new(Value::int(0)))],
        );
        assert!(zero_div.apply(&Value::null()).is_err());
    }

    #[test]
    fn test_arithmetic_cross_types() {
        let int_val = Value::int(5);
        let float_val = Value::float(2.0);

        let add_op = AddOperation::new(
            vec![Box::new(LiteralOperation::new(int_val))],
            vec![Box::new(LiteralOperation::new(float_val))],
        );
        let result = add_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Float(7.0));
    }

    #[test]
    fn test_comparison_operations() {
        let a = Value::int(10);
        let b = Value::int(5);

        let gt_op = GtOperation::new(
            vec![Box::new(LiteralOperation::new(a.clone()))],
            vec![Box::new(LiteralOperation::new(b.clone()))],
        );
        let result = gt_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(true));
        assert_eq!(gt_op.description(), "greater than");

        let lt_op = LtOperation::new(
            vec![Box::new(LiteralOperation::new(a.clone()))],
            vec![Box::new(LiteralOperation::new(b.clone()))],
        );
        let result = lt_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(false));

        let eq_op = EqOperation::new(
            vec![Box::new(LiteralOperation::new(a.clone()))],
            vec![Box::new(LiteralOperation::new(a.clone()))],
        );
        let result = eq_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(true));

        let ne_op = NeOperation::new(
            vec![Box::new(LiteralOperation::new(a.clone()))],
            vec![Box::new(LiteralOperation::new(b.clone()))],
        );
        let result = ne_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(true));

        let le_op = LeOperation::new(
            vec![Box::new(LiteralOperation::new(a.clone()))],
            vec![Box::new(LiteralOperation::new(a.clone()))],
        );
        let result = le_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(true));

        let ge_op = GeOperation::new(
            vec![Box::new(LiteralOperation::new(a.clone()))],
            vec![Box::new(LiteralOperation::new(a.clone()))],
        );
        let result = ge_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_logical_operations() {
        let true_val = Value::bool(true);
        let false_val = Value::bool(false);

        let and_op = AndOperation::new(
            vec![Box::new(LiteralOperation::new(true_val.clone()))],
            vec![Box::new(LiteralOperation::new(true_val.clone()))],
        );
        let result = and_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(true));

        let and_false_op = AndOperation::new(
            vec![Box::new(LiteralOperation::new(false_val.clone()))],
            vec![Box::new(LiteralOperation::new(true_val.clone()))],
        );
        let result = and_false_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(false));

        let or_op = OrOperation::new(
            vec![Box::new(LiteralOperation::new(false_val.clone()))],
            vec![Box::new(LiteralOperation::new(true_val.clone()))],
        );
        let result = or_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(true));

        let or_false_op = OrOperation::new(
            vec![Box::new(LiteralOperation::new(false_val.clone()))],
            vec![Box::new(LiteralOperation::new(false_val.clone()))],
        );
        let result = or_false_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(false));

        // Test truthiness with various types
        let truthy_values = vec![
            Value::int(1),
            Value::float(1.0),
            Value::string("hello"),
            Value::array(vec![Value::int(1)]),
            Value::object(HashMap::from([("key".to_string(), Value::int(1))])),
        ];

        let falsy_values = vec![
            Value::int(0),
            Value::float(0.0),
            Value::string(""),
            Value::array(vec![]),
            Value::object(HashMap::new()),
            Value::Null,
        ];

        for truthy in &truthy_values {
            let and_op = AndOperation::new(
                vec![Box::new(LiteralOperation::new(truthy.clone()))],
                vec![Box::new(LiteralOperation::new(Value::bool(true)))],
            );
            let result = and_op.apply(&Value::null()).unwrap();
            assert_eq!(result, Value::Bool(true));
        }

        for falsy in &falsy_values {
            let or_op = OrOperation::new(
                vec![Box::new(LiteralOperation::new(falsy.clone()))],
                vec![Box::new(LiteralOperation::new(Value::bool(true)))],
            );
            let result = or_op.apply(&Value::null()).unwrap();
            assert_eq!(result, Value::Bool(true));
        }
    }

    #[test]
    fn test_if_operation() {
        let condition = Value::bool(true);
        let then_val = Value::int(42);
        let else_val = Value::int(24);

        let if_op = IfOperation::new(
            vec![Box::new(LiteralOperation::new(condition))],
            vec![Box::new(LiteralOperation::new(then_val.clone()))],
            vec![Box::new(LiteralOperation::new(else_val.clone()))],
        );

        let result = if_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::int(42));

        let false_if_op = IfOperation::new(
            vec![Box::new(LiteralOperation::new(Value::bool(false)))],
            vec![Box::new(LiteralOperation::new(then_val))],
            vec![Box::new(LiteralOperation::new(else_val))],
        );

        let result = false_if_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::int(24));
    }

    #[test]
    fn test_negation_operation() {
        let true_val = Value::bool(true);
        let false_val = Value::bool(false);
        let int_val = Value::int(0);
        let str_val = Value::string("");

        let neg_op = NegationOperation::new(vec![Box::new(LiteralOperation::new(true_val))]);
        let result = neg_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(false));

        let neg_false_op = NegationOperation::new(vec![Box::new(LiteralOperation::new(false_val))]);
        let result = neg_false_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(true));

        let neg_int_op = NegationOperation::new(vec![Box::new(LiteralOperation::new(int_val))]);
        let result = neg_int_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(true)); // 0 is falsy, so !0 = true

        let neg_str_op = NegationOperation::new(vec![Box::new(LiteralOperation::new(str_val))]);
        let result = neg_str_op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::Bool(true)); // empty string is falsy
    }

    #[test]
    fn test_function_call_operation() {
        let builtin_func = Arc::new(|args: &[Value]| -> Result<Value> {
            if args.len() == 2 {
                match (&args[0], &args[1]) {
                    (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                    _ => Err(crate::error::operation_error("Invalid arguments")),
                }
            } else {
                Err(crate::error::operation_error("Wrong number of arguments"))
            }
        });

        let arg_ops = vec![
            Box::new(LiteralOperation::new(Value::int(10))) as Box<dyn Operation + Send + Sync>,
            Box::new(LiteralOperation::new(Value::int(5))) as Box<dyn Operation + Send + Sync>,
        ];

        let op = FunctionCallOperation::new("add".to_string(), arg_ops, Some(builtin_func));
        let result = op.apply(&Value::null()).unwrap();
        assert_eq!(result, Value::int(15));
        assert_eq!(op.description(), "call function add");

        // No builtin function
        let no_func_op = FunctionCallOperation::new("unknown".to_string(), vec![], None);
        assert!(no_func_op.apply(&Value::null()).is_err());
    }

    #[test]
    fn test_del_operation() {
        // Currently just returns the value unchanged
        let op = DelOperation::new(vec![]);
        let value = Value::int(42);
        let result = op.apply(&value).unwrap();
        assert_eq!(result, value);
        assert_eq!(op.description(), "delete");
    }

    #[test]
    fn test_assignment_operation() {
        // Test the basic assignment logic (currently simplified)
        let obj = Value::object(HashMap::from([("salary".to_string(), Value::int(50000))]));

        let target_ops = vec![
            Box::new(IdentityOperation) as Box<dyn Operation + Send + Sync>,
            Box::new(FieldAccessOperation::new("salary".to_string()))
                as Box<dyn Operation + Send + Sync>,
        ];
        let value_ops = vec![
            Box::new(LiteralOperation::new(Value::int(10))) as Box<dyn Operation + Send + Sync>
        ];

        let add_assign_op =
            AssignmentOperation::new(target_ops, AssignmentOperator::AddAssign, value_ops);
        let result = add_assign_op.apply(&obj).unwrap();
        match result {
            Value::Object(result_obj) => {
                assert_eq!(result_obj.get("salary"), Some(&Value::int(50010)));
            }
            _ => panic!("Expected object"),
        }

        assert_eq!(add_assign_op.description(), "assignment");
    }

    #[test]
    fn test_join_from_file_operation() {
        // Currently just returns the value unchanged
        let op = JoinFromFileOperation::new(
            "test.json".to_string(),
            "id".to_string(),
            "user_id".to_string(),
        );
        let value = Value::int(42);
        let result = op.apply(&value).unwrap();
        assert_eq!(result, value);
        assert_eq!(op.description(), "join from file");
    }

    #[test]
    fn test_compare_values() {
        assert_eq!(
            compare_values(&Value::int(5), &Value::int(3)).unwrap(),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::int(3), &Value::int(5)).unwrap(),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::int(5), &Value::int(5)).unwrap(),
            std::cmp::Ordering::Equal
        );

        assert_eq!(
            compare_values(&Value::float(5.0), &Value::int(5)).unwrap(),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::string("b"), &Value::string("a")).unwrap(),
            std::cmp::Ordering::Greater
        );

        // NaN comparison
        assert!(compare_values(&Value::float(f64::NAN), &Value::int(1)).is_err());

        // Incomparable types
        assert!(compare_values(&Value::int(1), &Value::array(vec![])).is_err());

        // Cross-type comparisons
        assert_eq!(
            compare_values(&Value::int(5), &Value::float(5.0)).unwrap(),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::float(5.0), &Value::int(5)).unwrap(),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::int(5), &Value::float(6.0)).unwrap(),
            std::cmp::Ordering::Less
        );

        // String comparisons
        assert_eq!(
            compare_values(&Value::string("abc"), &Value::string("def")).unwrap(),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::string("abc"), &Value::string("abc")).unwrap(),
            std::cmp::Ordering::Equal
        );

        // Boolean comparisons
        assert_eq!(
            compare_values(&Value::bool(false), &Value::bool(true)).unwrap(),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::bool(true), &Value::bool(true)).unwrap(),
            std::cmp::Ordering::Equal
        );

        // Null comparisons
        assert_eq!(
            compare_values(&Value::Null, &Value::Null).unwrap(),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::Null, &Value::int(1)).unwrap(),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::int(1), &Value::Null).unwrap(),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn test_add_values() {
        assert_eq!(
            add_values(&Value::int(5), &Value::int(3)).unwrap(),
            Value::int(8)
        );
        assert_eq!(
            add_values(&Value::float(5.5), &Value::int(3)).unwrap(),
            Value::Float(8.5)
        );
        assert_eq!(
            add_values(&Value::int(5), &Value::float(3.5)).unwrap(),
            Value::Float(8.5)
        );

        // String concatenation
        assert_eq!(
            add_values(&Value::string("hello"), &Value::string(" world")).unwrap(),
            Value::string("hello world")
        );
        assert_eq!(
            add_values(&Value::string("test"), &Value::string("")).unwrap(),
            Value::string("test")
        );
        assert_eq!(
            add_values(&Value::string(""), &Value::string("test")).unwrap(),
            Value::string("test")
        );

        // Invalid addition
        assert!(add_values(&Value::string("a"), &Value::int(1)).is_err());
        assert!(add_values(&Value::int(1), &Value::bool(true)).is_err());
    }

    #[test]
    fn test_sub_values() {
        assert_eq!(
            sub_values(&Value::int(5), &Value::int(3)).unwrap(),
            Value::int(2)
        );
        assert_eq!(
            sub_values(&Value::float(5.5), &Value::int(3)).unwrap(),
            Value::Float(2.5)
        );

        // Invalid subtraction
        assert!(sub_values(&Value::string("a"), &Value::int(1)).is_err());
    }

    #[test]
    fn test_mul_values() {
        assert_eq!(
            mul_values(&Value::int(5), &Value::int(3)).unwrap(),
            Value::int(15)
        );
        assert_eq!(
            mul_values(&Value::float(5.5), &Value::int(3)).unwrap(),
            Value::Float(16.5)
        );

        // Invalid multiplication
        assert!(mul_values(&Value::string("a"), &Value::int(1)).is_err());
    }

    #[test]
    fn test_div_values() {
        assert_eq!(
            div_values(&Value::int(6), &Value::int(3)).unwrap(),
            Value::Float(2.0)
        );
        assert_eq!(
            div_values(&Value::float(5.5), &Value::int(2)).unwrap(),
            Value::Float(2.75)
        );

        // Division by zero
        assert!(div_values(&Value::int(1), &Value::int(0)).is_err());
        assert!(div_values(&Value::int(1), &Value::float(0.0)).is_err());

        // Invalid division
        assert!(div_values(&Value::string("a"), &Value::int(1)).is_err());
    }

    #[test]
    fn test_simple_context() {
        let value = Value::int(42);
        let mut context = SimpleContext {
            value: value.clone(),
        };

        assert_eq!(context.get_variable("any"), Some(&value));
        context.set_variable("test", Value::string("ignored"));
        assert_eq!(context.get_variable("any"), Some(&value)); // Still returns the original value
    }

    #[test]
    fn test_operations_with_null_input() {
        let null_val = Value::Null;

        // Identity operation on null
        let identity = IdentityOperation;
        assert_eq!(identity.apply(&null_val).unwrap(), Value::Null);

        // Literal operation returns its value regardless of input
        let literal = LiteralOperation::new(Value::int(42));
        assert_eq!(literal.apply(&null_val).unwrap(), Value::int(42));

        // Arithmetic operations with null should generally error
        let add_op = AddOperation::new(
            vec![Box::new(LiteralOperation::new(Value::int(1)))],
            vec![Box::new(LiteralOperation::new(Value::Null))],
        );
        assert!(add_op.apply(&null_val).is_err());

        // Comparison operations with null
        let eq_op = EqOperation::new(
            vec![Box::new(LiteralOperation::new(Value::Null))],
            vec![Box::new(LiteralOperation::new(Value::Null))],
        );
        assert_eq!(eq_op.apply(&null_val).unwrap(), Value::Bool(true));

        let ne_op = NeOperation::new(
            vec![Box::new(LiteralOperation::new(Value::Null))],
            vec![Box::new(LiteralOperation::new(Value::int(1)))],
        );
        assert_eq!(ne_op.apply(&null_val).unwrap(), Value::Bool(true));

        // Logical operations with null (null is falsy)
        let and_op = AndOperation::new(
            vec![Box::new(LiteralOperation::new(Value::Null))],
            vec![Box::new(LiteralOperation::new(Value::bool(true)))],
        );
        assert_eq!(and_op.apply(&null_val).unwrap(), Value::Bool(false));
    }
}
