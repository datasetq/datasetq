use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Bollinger Bands
///
/// Computes Bollinger Bands (middle, upper, lower)
///
/// # Arguments
/// * `values` - Array of price values
/// * `period` - Period for moving average (default 20)
/// * `std_dev_mult` - Standard deviation multiplier (default 2.0)
///
/// # Returns
/// Array of objects with {middle, upper, lower} values
pub fn builtin_bbands(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "bbands() expects 1-3 arguments (array of prices, optional period, optional std_dev_mult)",
        ));
    }

    let period = if args.len() >= 2 {
        match &args[1] {
            Value::Int(p) => *p as usize,
            _ => 20,
        }
    } else {
        20
    };

    let std_dev_mult = if args.len() >= 3 {
        match &args[2] {
            Value::Float(m) => *m,
            Value::Int(m) => *m as f64,
            _ => 2.0,
        }
    } else {
        2.0
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

            use ta::indicators::BollingerBands;
            use ta::Next;

            let mut bb_indicator = BollingerBands::new(period, std_dev_mult).map_err(|e| {
                dsq_shared::error::operation_error(&format!("Bollinger Bands error: {}", e))
            })?;

            let bb_values: Vec<Value> = prices
                .iter()
                .map(|&price| {
                    let output = bb_indicator.next(price);
                    let mut obj = std::collections::HashMap::new();
                    obj.insert("middle".to_string(), Value::Float(output.average));
                    obj.insert("upper".to_string(), Value::Float(output.upper));
                    obj.insert("lower".to_string(), Value::Float(output.lower));
                    Value::Object(obj)
                })
                .collect();

            Ok(Value::Array(bb_values))
        }
        _ => Err(dsq_shared::error::operation_error(
            "bbands() requires an array of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "bbands",
        func: builtin_bbands,
    }
}
