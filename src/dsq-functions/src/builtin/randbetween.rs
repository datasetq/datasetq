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
        name: "randbetween",
        func: builtin_randbetween,
    }
}

#[cfg(feature = "rand")]
pub fn builtin_randbetween(args: &[Value]) -> Result<Value> {
    use rand::Rng;

    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "randbetween() expects 2 arguments: min and max",
        ));
    }

    let min = match &args[0] {
        Value::Int(i) => *i,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "randbetween() min must be an integer",
            ));
        }
    };

    let max = match &args[1] {
        Value::Int(i) => *i,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "randbetween() max must be an integer",
            ));
        }
    };

    if min > max {
        return Err(dsq_shared::error::operation_error(
            "randbetween() min must be less than or equal to max",
        ));
    }

    let mut rng = rand::rng();
    let random = rng.random_range(min..=max);
    Ok(Value::Int(random))
}

#[cfg(target_arch = "wasm32")]
inventory::submit! {
    FunctionRegistration {
        name: "randbetween",
        func: builtin_randbetween_wasm,
    }
}

#[cfg(target_arch = "wasm32")]
pub fn builtin_randbetween_wasm(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "randbetween() expects 2 arguments: min and max",
        ));
    }

    let min = match &args[0] {
        Value::Int(i) => *i as f64,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "randbetween() min must be an integer",
            ));
        }
    };

    let max = match &args[1] {
        Value::Int(i) => *i as f64,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "randbetween() max must be an integer",
            ));
        }
    };

    if min > max {
        return Err(dsq_shared::error::operation_error(
            "randbetween() min must be less than or equal to max",
        ));
    }

    let random = js_sys::Math::random() * (max - min + 1.0) + min;
    Ok(Value::Int(random.floor() as i64))
}
