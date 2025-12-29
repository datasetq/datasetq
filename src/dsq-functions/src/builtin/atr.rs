use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Average True Range (ATR)
///
/// Computes ATR volatility indicator
///
/// # Arguments
/// * `high` - Array of high prices
/// * `low` - Array of low prices
/// * `close` - Array of close prices
/// * `period` - Period for ATR calculation (default 14)
///
/// # Returns
/// Array of ATR values
pub fn builtin_atr(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(dsq_shared::error::operation_error(
            "atr() expects 3-4 arguments (high, low, close arrays, optional period)",
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
                    "atr() requires all arrays to have the same length",
                ));
            }

            use ta::indicators::AverageTrueRange;
            use ta::Next;

            let mut atr_indicator = AverageTrueRange::new(period)
                .map_err(|e| dsq_shared::error::operation_error(&format!("ATR error: {}", e)))?;

            let atr_values: Vec<Value> = (0..highs.len())
                .map(|i| {
                    let data_item = ta::DataItem::builder()
                        .high(highs[i])
                        .low(lows[i])
                        .close(closes[i])
                        .volume(0.0)
                        .build()
                        .unwrap();
                    let atr_val = atr_indicator.next(&data_item);
                    if atr_val.is_finite() {
                        Value::Float(atr_val)
                    } else {
                        Value::Null
                    }
                })
                .collect();

            Ok(Value::Array(atr_values))
        }
        _ => Err(dsq_shared::error::operation_error(
            "atr() requires three arrays of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "atr",
        func: builtin_atr,
    }
}
