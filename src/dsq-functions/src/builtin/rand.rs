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

    let mut rng = rand::rng();
    Ok(Value::Float(rng.random()))
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
