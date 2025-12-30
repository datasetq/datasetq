use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// Rate of Change (ROC)
///
/// Computes ROC momentum indicator
///
/// # Arguments
/// * `values` - Array of price values
/// * `period` - Period for ROC calculation (default 12)
///
/// # Returns
/// Array of ROC values (percentage change)
pub fn builtin_roc(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "roc() expects 1-2 arguments (array of prices, optional period)",
        ));
    }

    let period = if args.len() >= 2 {
        match &args[1] {
            Value::Int(p) => *p as usize,
            _ => 12,
        }
    } else {
        12
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

            use ta::indicators::RateOfChange;
            use ta::Next;

            let mut roc_indicator = RateOfChange::new(period)
                .map_err(|e| dsq_shared::error::operation_error(format!("ROC error: {}", e)))?;

            let roc_values: Vec<Value> = prices
                .iter()
                .map(|&price| {
                    let roc_val = roc_indicator.next(price);
                    if roc_val.is_finite() {
                        Value::Float(roc_val)
                    } else {
                        Value::Null
                    }
                })
                .collect();

            Ok(Value::Array(roc_values))
        }
        _ => Err(dsq_shared::error::operation_error(
            "roc() requires an array of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "roc",
        func: builtin_roc,
    }
}
