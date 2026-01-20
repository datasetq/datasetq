use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Parabolic SAR
///
/// Computes Parabolic SAR trend indicator
///
/// # Arguments
/// * `high` - Array of high prices
/// * `low` - Array of low prices
/// * `af_step` - Acceleration factor step (default 0.02)
/// * `af_max` - Maximum acceleration factor (default 0.2)
///
/// # Returns
/// Array of SAR values
pub fn builtin_parabolic_sar(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(dsq_shared::error::operation_error(
            "parabolic_sar() expects 2-4 arguments (high, low arrays, optional af_step, optional af_max)",
        ));
    }

    let af_step = if args.len() >= 3 {
        match &args[2] {
            Value::Float(s) => *s,
            _ => 0.02,
        }
    } else {
        0.02
    };

    let af_max = if args.len() >= 4 {
        match &args[3] {
            Value::Float(m) => *m,
            _ => 0.2,
        }
    } else {
        0.2
    };

    match (&args[0], &args[1]) {
        (Value::Array(high_arr), Value::Array(low_arr)) => {
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

            if highs.len() != lows.len() {
                return Err(dsq_shared::error::operation_error(
                    "parabolic_sar() requires both arrays to have the same length",
                ));
            }

            // Custom Parabolic SAR implementation
            let mut psar_values = vec![Value::Null; highs.len()];

            if highs.len() < 3 {
                return Ok(Value::Array(psar_values));
            }

            let mut is_long = highs[1] > highs[0];
            let mut sar = if is_long { lows[0] } else { highs[0] };
            let mut ep = if is_long { highs[1] } else { lows[1] };
            let mut af = af_step;

            psar_values[0] = Value::Float(sar);
            psar_values[1] = Value::Float(sar);

            for i in 2..highs.len() {
                let prev_sar = sar;

                sar = prev_sar + af * (ep - prev_sar);

                if is_long {
                    if lows[i] < sar {
                        is_long = false;
                        sar = ep;
                        ep = lows[i];
                        af = af_step;
                    } else if highs[i] > ep {
                        ep = highs[i];
                        af = (af + af_step).min(af_max);
                    }
                } else if highs[i] > sar {
                    is_long = true;
                    sar = ep;
                    ep = highs[i];
                    af = af_step;
                } else if lows[i] < ep {
                    ep = lows[i];
                    af = (af + af_step).min(af_max);
                }

                psar_values[i] = Value::Float(sar);
            }

            Ok(Value::Array(psar_values))
        }
        _ => Err(dsq_shared::error::operation_error(
            "parabolic_sar() requires two arrays of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "parabolic_sar",
        func: builtin_parabolic_sar,
    }
}
