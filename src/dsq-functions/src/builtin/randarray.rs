#[allow(unused_imports)]
use crate::FunctionRegistration;
#[allow(unused_imports)]
use dsq_shared::value::Value;
#[allow(unused_imports)]
use dsq_shared::Result;
#[allow(unused_imports)]
use inventory;

#[cfg(feature = "rand")]
inventory::submit! {
    FunctionRegistration {
        name: "randarray",
        func: builtin_randarray,
    }
}

#[cfg(feature = "rand")]
pub fn builtin_randarray(args: &[Value]) -> Result<Value> {
    use rand::Rng;

    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "randarray() expects 1 argument: size",
        ));
    }

    let size = match &args[0] {
        Value::Int(i) if *i >= 0 => *i as usize,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "randarray() size must be a non-negative integer",
            ))
        }
    };

    let mut rng = rand::rng();
    let mut result = Vec::with_capacity(size);
    for _ in 0..size {
        result.push(Value::Float(rng.random()));
    }
    Ok(Value::Array(result))
}

#[cfg(target_arch = "wasm32")]
inventory::submit! {
    FunctionRegistration {
        name: "randarray",
        func: builtin_randarray_wasm,
    }
}

#[cfg(target_arch = "wasm32")]
pub fn builtin_randarray_wasm(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "randarray() expects 1 argument: size",
        ));
    }

    let size = match &args[0] {
        Value::Int(i) if *i >= 0 => *i as usize,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "randarray() size must be a non-negative integer",
            ))
        }
    };

    let mut result = Vec::with_capacity(size);
    for _ in 0..size {
        result.push(Value::Float(js_sys::Math::random()));
    }
    Ok(Value::Array(result))
}
