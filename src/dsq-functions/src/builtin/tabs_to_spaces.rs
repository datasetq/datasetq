use dsq_shared::value::Value;
use dsq_shared::Result;

pub fn builtin_tabs_to_spaces(args: &[Value]) -> Result<Value> {
    if args.len() < 1 || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "tabs_to_spaces() expects 1 or 2 arguments",
        ));
    }

    let spaces_per_tab = if args.len() == 2 {
        match &args[1] {
            Value::Int(i) if *i > 0 => *i as usize,
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "tabs_to_spaces() second argument must be a positive integer",
                ))
            }
        }
    } else {
        4
    };

    match &args[0] {
        Value::String(s) => {
            let result = s.replace('\t', &" ".repeat(spaces_per_tab));
            Ok(Value::String(result))
        }
        _ => Err(dsq_shared::error::operation_error(
            "tabs_to_spaces() requires string argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "tabs_to_spaces",
        func: builtin_tabs_to_spaces,
    }
}
