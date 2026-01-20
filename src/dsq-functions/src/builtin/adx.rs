use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Average Directional Index (ADX)
///
/// Computes ADX trend strength indicator
///
/// # Arguments
/// * `high` - Array of high prices
/// * `low` - Array of low prices
/// * `close` - Array of close prices
/// * `period` - Period for ADX calculation (default 14)
///
/// # Returns
/// Array of ADX values (0-100)
pub fn builtin_adx(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(dsq_shared::error::operation_error(
            "adx() expects 3-4 arguments (high, low, close arrays, optional period)",
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
                    "adx() requires all arrays to have the same length",
                ));
            }

            // Custom ADX implementation
            let mut adx_values = vec![Value::Null; highs.len()];

            if highs.len() < period * 2 {
                return Ok(Value::Array(adx_values));
            }

            // Calculate True Range and directional movements
            let mut tr_vec = Vec::new();
            let mut plus_dm = Vec::new();
            let mut minus_dm = Vec::new();

            tr_vec.push(highs[0] - lows[0]);
            plus_dm.push(0.0);
            minus_dm.push(0.0);

            for i in 1..highs.len() {
                let hl = highs[i] - lows[i];
                let hpc = (highs[i] - closes[i - 1]).abs();
                let lpc = (lows[i] - closes[i - 1]).abs();
                tr_vec.push(hl.max(hpc).max(lpc));

                let up_move = highs[i] - highs[i - 1];
                let down_move = lows[i - 1] - lows[i];

                plus_dm.push(if up_move > down_move && up_move > 0.0 {
                    up_move
                } else {
                    0.0
                });
                minus_dm.push(if down_move > up_move && down_move > 0.0 {
                    down_move
                } else {
                    0.0
                });
            }

            // Smooth the values
            if tr_vec.len() >= period {
                let atr: Vec<f64> = (period - 1..tr_vec.len())
                    .map(|i| tr_vec[i - period + 1..=i].iter().sum::<f64>() / period as f64)
                    .collect();

                for (idx, atr_val) in atr.iter().enumerate().skip(period - 1) {
                    if *atr_val > 0.0 {
                        let di_plus = 100.0 * plus_dm[idx] / atr_val;
                        let di_minus = 100.0 * minus_dm[idx] / atr_val;
                        let dx = if di_plus + di_minus > 0.0 {
                            100.0 * (di_plus - di_minus).abs() / (di_plus + di_minus)
                        } else {
                            0.0
                        };
                        adx_values[idx + period - 1] = Value::Float(dx);
                    }
                }
            }

            Ok(Value::Array(adx_values))
        }
        _ => Err(dsq_shared::error::operation_error(
            "adx() requires three arrays of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "adx",
        func: builtin_adx,
    }
}
