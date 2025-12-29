use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Relative Strength Index (RSI)
///
/// Computes RSI momentum indicator
///
/// # Arguments
/// * `values` - Array of price values
/// * `period` - Period for RSI calculation (typically 14)
///
/// # Returns
/// Array of RSI values (0-100)
pub fn builtin_rsi(args: &[Value]) -> Result<Value> {
    if args.len() < 1 || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "rsi() expects 1-2 arguments (array of prices, optional period)",
        ));
    }

    let period = if args.len() == 2 {
        match &args[1] {
            Value::Int(p) => *p as usize,
            _ => 14,
        }
    } else {
        14
    };

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

            if prices.len() < period + 1 {
                return Ok(Value::Array(vec![Value::Null; prices.len()]));
            }

            use ta::indicators::RelativeStrengthIndex;
            use ta::Next;

            let mut rsi_indicator = RelativeStrengthIndex::new(period)
                .map_err(|e| dsq_shared::error::operation_error(&format!("RSI error: {}", e)))?;

            let rsi_values: Vec<Value> = prices
                .iter()
                .map(|&price| {
                    let rsi_val = rsi_indicator.next(price);
                    if rsi_val.is_finite() && rsi_val >= 0.0 {
                        Value::Float(rsi_val)
                    } else {
                        Value::Null
                    }
                })
                .collect();

            Ok(Value::Array(rsi_values))
        }
        _ => Err(dsq_shared::error::operation_error(
            "rsi() requires an array of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "rsi",
        func: builtin_rsi,
    }
}
