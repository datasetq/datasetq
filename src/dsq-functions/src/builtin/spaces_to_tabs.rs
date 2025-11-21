use dsq_shared::value::Value;
use dsq_shared::Result;

pub fn builtin_spaces_to_tabs(args: &[Value]) -> Result<Value> {
    if args.len() < 1 || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "spaces_to_tabs() expects 1 or 2 arguments",
        ));
    }

    let spaces_per_tab = if args.len() == 2 {
        match &args[1] {
            Value::Int(i) if *i > 0 => *i as usize,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "spaces_to_tabs() second argument must be a positive integer",
                ))
            }
        }
    } else {
        4
    };

    match &args[0] {
        Value::String(s) => {
            let result = s.replace(&" ".repeat(spaces_per_tab), "\t");
            Ok(Value::String(result))
        }
        _ => Err(dsq_shared::error::operation_error(
            "spaces_to_tabs() requires string argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "spaces_to_tabs",
        func: builtin_spaces_to_tabs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_builtin_spaces_to_tabs_default() {
        let result =
            builtin_spaces_to_tabs(&[Value::String("hello    world".to_string())]).unwrap();
        assert_eq!(result, Value::String("hello\tworld".to_string()));
    }

    #[test]
    fn test_builtin_spaces_to_tabs_custom_spaces() {
        let result =
            builtin_spaces_to_tabs(&[Value::String("a  b  c".to_string()), Value::Int(2)]).unwrap();
        assert_eq!(result, Value::String("a\tb\tc".to_string()));
    }

    #[test]
    fn test_builtin_spaces_to_tabs_leading_spaces() {
        let result =
            builtin_spaces_to_tabs(&[Value::String("    indented text".to_string())]).unwrap();
        assert_eq!(result, Value::String("\tindented text".to_string()));
    }

    #[test]
    fn test_builtin_spaces_to_tabs_multiple() {
        let result = builtin_spaces_to_tabs(&[Value::String(
            "Multiple    tabs    in    one    line".to_string(),
        )])
        .unwrap();
        assert_eq!(
            result,
            Value::String("Multiple\ttabs\tin\tone\tline".to_string())
        );
    }

    #[test]
    fn test_builtin_spaces_to_tabs_no_replacement() {
        let result = builtin_spaces_to_tabs(&[Value::String("no  spaces".to_string())]).unwrap();
        assert_eq!(result, Value::String("no  spaces".to_string()));
    }

    #[test]
    fn test_builtin_spaces_to_tabs_invalid_args() {
        let result = builtin_spaces_to_tabs(&[]);
        assert!(result.is_err());

        let result = builtin_spaces_to_tabs(&[Value::Int(1)]);
        assert!(result.is_err());

        let result = builtin_spaces_to_tabs(&[Value::String("test".to_string()), Value::Int(0)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_spaces_to_tabs_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        let result = registry
            .call_function(
                "spaces_to_tabs",
                &[Value::String("test    string".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("test\tstring".to_string()));
    }
}
