use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Stochastic Oscillator %D
///
/// Computes Stochastic %D (smoothed %K)
///
/// # Arguments
/// * `high` - Array of high prices
/// * `low` - Array of low prices
/// * `close` - Array of close prices
/// * `k_period` - Period for %K calculation (default 14)
/// * `d_period` - Period for %D smoothing (default 3)
///
/// # Returns
/// Array of %D values (0-100)
pub fn builtin_stoch_d(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(dsq_shared::error::operation_error(
            "stoch_d() expects 3-5 arguments (high, low, close arrays, optional k_period, optional d_period)",
        ));
    }

    let k_period = if args.len() >= 4 {
        match &args[3] {
            Value::Int(p) => *p as usize,
            _ => 14,
        }
    } else {
        14
    };

    let d_period = if args.len() >= 5 {
        match &args[4] {
            Value::Int(p) => *p as usize,
            _ => 3,
        }
    } else {
        3
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
                    "stoch_d() requires all arrays to have the same length",
                ));
            }

            use ta::indicators::SlowStochastic;
            use ta::Next;

            let mut stoch_indicator = SlowStochastic::new(k_period, d_period).map_err(|e| {
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
            "stoch_d() requires three arrays of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "stoch_d",
        func: builtin_stoch_d,
    }
}
