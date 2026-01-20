use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// On-Balance Volume (OBV)
///
/// Computes OBV indicator
///
/// # Arguments
/// * `close` - Array of close prices
/// * `volume` - Array of volumes
///
/// # Returns
/// Array of OBV values
pub fn builtin_obv(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "obv() expects 2 arguments (close prices, volumes)",
        ));
    }

    match (&args[0], &args[1]) {
        (Value::Array(close_arr), Value::Array(volume_arr)) => {
            let closes: Vec<f64> = close_arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();

            let volumes: Vec<f64> = volume_arr
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();

            if closes.len() != volumes.len() {
                return Err(dsq_shared::error::operation_error(
                    "obv() requires both arrays to have the same length",
                ));
            }

            use ta::indicators::OnBalanceVolume;
            use ta::Next;

            let mut obv_indicator = OnBalanceVolume::new();

            let obv_values: Vec<Value> = (0..closes.len())
                .map(|i| {
                    let data_item = ta::DataItem::builder()
                        .close(closes[i])
                        .volume(volumes[i])
                        .high(0.0)
                        .low(0.0)
                        .build()
                        .unwrap();
                    let obv_val = obv_indicator.next(&data_item);
                    Value::Float(obv_val)
                })
                .collect();

            Ok(Value::Array(obv_values))
        }
        _ => Err(dsq_shared::error::operation_error(
            "obv() requires two arrays of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "obv",
        func: builtin_obv,
    }
}
