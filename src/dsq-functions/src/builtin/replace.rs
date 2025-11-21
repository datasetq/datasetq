use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_replace(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(dsq_shared::error::operation_error(
            "replace() expects 3 arguments",
        ));
    }

    match (&args[0], &args[1], &args[2]) {
        (Value::String(s), Value::String(from), Value::String(to)) => {
            Ok(Value::String(s.replace(from, to)))
        }
        _ => Err(dsq_shared::error::operation_error(
            "replace() requires string arguments",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "replace",
        func: builtin_replace,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_replace_basic() {
        let result = builtin_replace(&[
            Value::String("hello world".to_string()),
            Value::String("world".to_string()),
            Value::String("universe".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("hello universe".to_string()));
    }

    #[test]
    fn test_replace_no_match() {
        let result = builtin_replace(&[
            Value::String("hello world".to_string()),
            Value::String("foo".to_string()),
            Value::String("bar".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    #[test]
    fn test_replace_multiple_occurrences() {
        let result = builtin_replace(&[
            Value::String("foo foo foo".to_string()),
            Value::String("foo".to_string()),
            Value::String("bar".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("bar bar bar".to_string()));
    }

    #[test]
    fn test_replace_empty_from() {
        let result = builtin_replace(&[
            Value::String("ab".to_string()),
            Value::String("".to_string()),
            Value::String("x".to_string()),
        ])
        .unwrap();
        // In Rust, s.replace("", "x") inserts x between every character
        assert_eq!(result, Value::String("xaxbx".to_string()));
    }

    #[test]
    fn test_replace_non_string_args() {
        let result = builtin_replace(&[
            Value::Int(123),
            Value::String("2".to_string()),
            Value::String("3".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires string arguments"));
    }

    #[test]
    fn test_replace_wrong_number_of_args() {
        let result = builtin_replace(&[Value::String("test".to_string())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 3 arguments"));

        let result = builtin_replace(&[
            Value::String("test".to_string()),
            Value::String("t".to_string()),
            Value::String("r".to_string()),
            Value::String("extra".to_string()),
        ]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 3 arguments"));
    }

    #[test]
    fn test_replace_unicode() {
        let result = builtin_replace(&[
            Value::String("héllo wörld".to_string()),
            Value::String("wörld".to_string()),
            Value::String("universe".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("héllo universe".to_string()));
    }

    #[test]
    fn test_replace_overlapping() {
        let result = builtin_replace(&[
            Value::String("aaa".to_string()),
            Value::String("aa".to_string()),
            Value::String("b".to_string()),
        ])
        .unwrap();
        // Rust's replace doesn't handle overlapping, it replaces non-overlapping
        assert_eq!(result, Value::String("ba".to_string()));
    }
}
