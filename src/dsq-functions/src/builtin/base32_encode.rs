use dsq_shared::value::Value;
use dsq_shared::Result;

pub fn builtin_base32_encode(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "base32_encode() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => {
            let encoded =
                base32::encode(base32::Alphabet::Rfc4648 { padding: false }, s.as_bytes());
            Ok(Value::String(encoded))
        }
        _ => Err(dsq_shared::error::operation_error(
            "base32_encode() requires string argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "base32_encode",
        func: builtin_base32_encode,
    }
}
