use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Sharpe Ratio
///
/// Computes Sharpe ratio: (mean_return - risk_free_rate) / std_dev
///
/// # Arguments
/// * `returns` - Array of returns
/// * `risk_free_rate` - Risk-free rate (default 0.0)
///
/// # Returns
/// Sharpe ratio as a single float value
pub fn builtin_sharpe_ratio(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "sharpe_ratio() expects 1-2 arguments (array of returns, optional risk_free_rate)",
        ));
    }

    let risk_free_rate = if args.len() >= 2 {
        match &args[1] {
            Value::Float(r) => *r,
            Value::Int(r) => *r as f64,
            _ => 0.0,
        }
    } else {
        0.0
    };

    match &args[0] {
        Value::Array(arr) => {
            let returns: Vec<f64> = arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();

            if returns.is_empty() {
                return Ok(Value::Float(f64::NAN));
            }

            let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
            let variance = returns
                .iter()
                .map(|x| (x - mean_return).powi(2))
                .sum::<f64>()
                / returns.len() as f64;
            let std_dev = variance.sqrt();

            let sharpe = if std_dev > 0.0 {
                (mean_return - risk_free_rate) / std_dev
            } else {
                f64::NAN
            };

            Ok(Value::Float(sharpe))
        }
        _ => Err(dsq_shared::error::operation_error(
            "sharpe_ratio() requires an array of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "sharpe_ratio",
        func: builtin_sharpe_ratio,
    }
}
