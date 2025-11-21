use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

#[cfg(feature = "rand")]
inventory::submit! {
    FunctionRegistration {
        name: "rand",
        func: builtin_rand,
    }
}

#[cfg(feature = "rand")]
pub fn builtin_rand(args: &[Value]) -> Result<Value> {
    use rand::Rng;

    if args.len() > 1 {
        return Err(dsq_shared::error::operation_error(
            "rand() expects 0 or 1 arguments",
        ));
    }

    let mut rng = rand::thread_rng();
    Ok(Value::Float(rng.gen()))
}

#[cfg(target_arch = "wasm32")]
inventory::submit! {
    FunctionRegistration {
        name: "rand",
        func: builtin_rand_wasm,
    }
}

#[cfg(target_arch = "wasm32")]
pub fn builtin_rand_wasm(args: &[Value]) -> Result<Value> {
    if args.len() > 1 {
        return Err(dsq_shared::error::operation_error(
            "rand() expects 0 or 1 arguments",
        ));
    }

    let random = js_sys::Math::random();
    Ok(Value::Float(random))
}
