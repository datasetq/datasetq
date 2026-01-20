use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Maximum Drawdown
///
/// Computes the maximum peak-to-trough decline
///
/// # Arguments
/// * `values` - Array of equity/price values
///
/// # Returns
/// Maximum drawdown as a percentage (0.0 to 1.0)
pub fn builtin_max_drawdown(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "max_drawdown() expects 1 argument (array of values)",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let values: Vec<f64> = arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();

            if values.is_empty() {
                return Ok(Value::Float(0.0));
            }

            let mut peak = values[0];
            let mut max_dd = 0.0;

            for &val in &values {
                if val > peak {
                    peak = val;
                }
                let drawdown = (peak - val) / peak;
                if drawdown > max_dd {
                    max_dd = drawdown;
                }
            }

            Ok(Value::Float(max_dd))
        }
        _ => Err(dsq_shared::error::operation_error(
            "max_drawdown() requires an array of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "max_drawdown",
        func: builtin_max_drawdown,
    }
}
