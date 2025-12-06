use dsq_shared::value::Value;
use dsq_shared::Result;

pub fn builtin_base32_decode(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "base32_decode() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::String(s) => match base32::decode(base32::Alphabet::Rfc4648 { padding: false }, s) {
            Some(bytes) => match String::from_utf8(bytes) {
                Ok(decoded) => Ok(Value::String(decoded)),
                Err(_) => Err(dsq_shared::error::operation_error(
                    "base32_decode() invalid UTF-8",
                )),
            },
            None => Err(dsq_shared::error::operation_error(
                "base32_decode() invalid base32",
            )),
        },
        _ => Err(dsq_shared::error::operation_error(
            "base32_decode() requires string argument",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "base32_decode",
        func: builtin_base32_decode,
    }
}
