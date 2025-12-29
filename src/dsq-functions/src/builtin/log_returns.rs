use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Logarithmic returns calculation
///
/// Computes log returns: ln(price[i] / price[i-1])
///
/// # Arguments
/// * `values` - Array of price values
///
/// # Returns
/// Array of log returns (first element is null)
pub fn builtin_log_returns(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "log_returns() expects 1 argument (array of prices)",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let prices: Vec<f64> = arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();

            if prices.len() < 2 {
                return Ok(Value::Array(vec![Value::Null]));
            }

            let mut returns = vec![Value::Null]; // First return is null
            for i in 1..prices.len() {
                if prices[i - 1] > 0.0 && prices[i] > 0.0 {
                    let log_ret = (prices[i] / prices[i - 1]).ln();
                    returns.push(Value::Float(log_ret));
                } else {
                    returns.push(Value::Null);
                }
            }

            Ok(Value::Array(returns))
        }
        _ => Err(dsq_shared::error::operation_error(
            "log_returns() requires an array of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "log_returns",
        func: builtin_log_returns,
    }
}
