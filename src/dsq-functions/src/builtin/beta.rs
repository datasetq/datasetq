use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Beta Coefficient
///
/// Computes beta: Cov(asset, market) / Var(market)
///
/// # Arguments
/// * `asset_returns` - Array of asset returns
/// * `market_returns` - Array of market returns
///
/// # Returns
/// Beta coefficient as a single float value
pub fn builtin_beta(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "beta() expects 2 arguments (asset_returns, market_returns)",
        ));
    }

    match (&args[0], &args[1]) {
        (Value::Array(asset_arr), Value::Array(market_arr)) => {
            let asset_returns: Vec<f64> = asset_arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();

            let market_returns: Vec<f64> = market_arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();

            if asset_returns.len() != market_returns.len() || asset_returns.is_empty() {
                return Ok(Value::Float(f64::NAN));
            }

            let asset_mean = asset_returns.iter().sum::<f64>() / asset_returns.len() as f64;
            let market_mean = market_returns.iter().sum::<f64>() / market_returns.len() as f64;

            let covariance = asset_returns
                .iter()
                .zip(market_returns.iter())
                .map(|(a, m)| (a - asset_mean) * (m - market_mean))
                .sum::<f64>()
                / asset_returns.len() as f64;

            let market_variance = market_returns
                .iter()
                .map(|m| (m - market_mean).powi(2))
                .sum::<f64>()
                / market_returns.len() as f64;

            let beta_val = if market_variance > 0.0 {
                covariance / market_variance
            } else {
                f64::NAN
            };

            Ok(Value::Float(beta_val))
        }
        _ => Err(dsq_shared::error::operation_error(
            "beta() requires two arrays of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "beta",
        func: builtin_beta,
    }
}
