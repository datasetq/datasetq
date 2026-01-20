use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Stochastic Oscillator %K
///
/// Computes Stochastic %K
///
/// # Arguments
/// * `high` - Array of high prices
/// * `low` - Array of low prices
/// * `close` - Array of close prices
/// * `period` - Period for calculation (default 14)
///
/// # Returns
/// Array of %K values (0-100)
pub fn builtin_stoch_k(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(dsq_shared::error::operation_error(
            "stoch_k() expects 3-4 arguments (high, low, close arrays, optional period)",
        ));
    }

    let period = if args.len() >= 4 {
        match &args[3] {
            Value::Int(p) => *p as usize,
            _ => 14,
        }
    } else {
        14
    };

    match (&args[0], &args[1], &args[2]) {
        (Value::Array(high_arr), Value::Array(low_arr), Value::Array(close_arr)) => {
            let highs: Vec<f64> = high_arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();

            let lows: Vec<f64> = low_arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();

            let closes: Vec<f64> = close_arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();

            if highs.len() != lows.len() || lows.len() != closes.len() {
                return Err(dsq_shared::error::operation_error(
                    "stoch_k() requires all arrays to have the same length",
                ));
            }

            use ta::indicators::FastStochastic;
            use ta::Next;

            let mut stoch_indicator = FastStochastic::new(period).map_err(|e| {
                dsq_shared::error::operation_error(format!("Stochastic error: {}", e))
            })?;

            let stoch_values: Vec<Value> = (0..highs.len())
                .map(|i| {
                    let data_item = ta::DataItem::builder()
                        .high(highs[i])
                        .low(lows[i])
                        .close(closes[i])
                        .volume(0.0)
                        .build()
                        .unwrap();
                    let stoch_val = stoch_indicator.next(&data_item);
                    if stoch_val.is_finite() {
                        Value::Float(stoch_val)
                    } else {
                        Value::Null
                    }
                })
                .collect();

            Ok(Value::Array(stoch_values))
        }
        _ => Err(dsq_shared::error::operation_error(
            "stoch_k() requires three arrays of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "stoch_k",
        func: builtin_stoch_k,
    }
}
