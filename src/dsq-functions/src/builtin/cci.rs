use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Commodity Channel Index (CCI)
///
/// Computes CCI momentum indicator
///
/// # Arguments
/// * `high` - Array of high prices
/// * `low` - Array of low prices
/// * `close` - Array of close prices
/// * `period` - Period for CCI calculation (default 20)
///
/// # Returns
/// Array of CCI values
pub fn builtin_cci(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(dsq_shared::error::operation_error(
            "cci() expects 3-4 arguments (high, low, close arrays, optional period)",
        ));
    }

    let period = if args.len() >= 4 {
        match &args[3] {
            Value::Int(p) => *p as usize,
            _ => 20,
        }
    } else {
        20
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
                    "cci() requires all arrays to have the same length",
                ));
            }

            use ta::indicators::CommodityChannelIndex;
            use ta::Next;

            let mut cci_indicator = CommodityChannelIndex::new(period)
                .map_err(|e| dsq_shared::error::operation_error(format!("CCI error: {}", e)))?;

            let cci_values: Vec<Value> = (0..highs.len())
                .map(|i| {
                    let data_item = ta::DataItem::builder()
                        .high(highs[i])
                        .low(lows[i])
                        .close(closes[i])
                        .volume(0.0)
                        .build()
                        .unwrap();
                    let cci_val = cci_indicator.next(&data_item);
                    if cci_val.is_finite() {
                        Value::Float(cci_val)
                    } else {
                        Value::Null
                    }
                })
                .collect();

            Ok(Value::Array(cci_values))
        }
        _ => Err(dsq_shared::error::operation_error(
            "cci() requires three arrays of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "cci",
        func: builtin_cci,
    }
}
