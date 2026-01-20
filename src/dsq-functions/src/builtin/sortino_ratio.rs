use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Sortino Ratio
///
/// Computes Sortino ratio (like Sharpe but uses downside deviation)
///
/// # Arguments
/// * `returns` - Array of returns
/// * `risk_free_rate` - Risk-free rate (default 0.0)
///
/// # Returns
/// Sortino ratio as a single float value
pub fn builtin_sortino_ratio(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "sortino_ratio() expects 1-2 arguments (array of returns, optional risk_free_rate)",
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

            // Only consider downside returns (below risk-free rate)
            let downside_returns: Vec<f64> = returns
                .iter()
                .filter(|&&r| r < risk_free_rate)
                .copied()
                .collect();

            if downside_returns.is_empty() {
                return Ok(Value::Float(f64::INFINITY));
            }

            let downside_variance = downside_returns
                .iter()
                .map(|x| (x - risk_free_rate).powi(2))
                .sum::<f64>()
                / downside_returns.len() as f64;
            let downside_std = downside_variance.sqrt();

            let sortino = if downside_std > 0.0 {
                (mean_return - risk_free_rate) / downside_std
            } else {
                f64::NAN
            };

            Ok(Value::Float(sortino))
        }
        _ => Err(dsq_shared::error::operation_error(
            "sortino_ratio() requires an array of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "sortino_ratio",
        func: builtin_sortino_ratio,
    }
}
