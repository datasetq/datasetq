use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Alpha Coefficient (Jensen's Alpha)
///
/// Computes Jensen's alpha:
/// alpha = mean(asset_returns) - (risk_free_rate + beta * (mean(market_returns) - risk_free_rate))
///
/// # Arguments
/// * `asset_returns` - Array of asset returns
/// * `market_returns` - Array of market returns
/// * `risk_free_rate` - Risk-free rate (default 0.0)
///
/// # Returns
/// Alpha as a single float value
pub fn builtin_alpha(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(dsq_shared::error::operation_error(
            "alpha() expects 2-3 arguments (asset_returns, market_returns, optional risk_free_rate)",
        ));
    }

    let risk_free_rate = if args.len() == 3 {
        match &args[2] {
            Value::Float(r) => *r,
            Value::Int(r) => *r as f64,
            _ => 0.0,
        }
    } else {
        0.0
    };

    // First compute beta
    let beta_result = super::beta::builtin_beta(&args[0..2])?;
    let beta_val = match beta_result {
        Value::Float(b) => b,
        _ => return Ok(Value::Float(f64::NAN)),
    };

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

            if asset_returns.is_empty() || market_returns.is_empty() {
                return Ok(Value::Float(f64::NAN));
            }

            let asset_mean = asset_returns.iter().sum::<f64>() / asset_returns.len() as f64;
            let market_mean = market_returns.iter().sum::<f64>() / market_returns.len() as f64;

            let alpha_val =
                asset_mean - (risk_free_rate + beta_val * (market_mean - risk_free_rate));

            Ok(Value::Float(alpha_val))
        }
        _ => Err(dsq_shared::error::operation_error(
            "alpha() requires two arrays of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "alpha",
        func: builtin_alpha,
    }
}
