use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

/// MACD (Moving Average Convergence Divergence)
///
/// Computes MACD indicator
///
/// # Arguments
/// * `values` - Array of price values
/// * `fast_period` - Fast EMA period (default 12)
/// * `slow_period` - Slow EMA period (default 26)
/// * `signal_period` - Signal line period (default 9)
///
/// # Returns
/// Array of objects with {macd, signal, histogram} values
pub fn builtin_macd(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(dsq_shared::error::operation_error(
            "macd() expects 1-4 arguments (array of prices, optional fast/slow/signal periods)",
        ));
    }

    let fast_period = if args.len() >= 2 {
        match &args[1] {
            Value::Int(p) => *p as usize,
            _ => 12,
        }
    } else {
        12
    };

    let slow_period = if args.len() >= 3 {
        match &args[2] {
            Value::Int(p) => *p as usize,
            _ => 26,
        }
    } else {
        26
    };

    let signal_period = if args.len() >= 4 {
        match &args[3] {
            Value::Int(p) => *p as usize,
            _ => 9,
        }
    } else {
        9
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

            use ta::indicators::MovingAverageConvergenceDivergence;
            use ta::Next;

            let mut macd_indicator =
                MovingAverageConvergenceDivergence::new(fast_period, slow_period, signal_period)
                    .map_err(|e| {
                        dsq_shared::error::operation_error(format!("MACD error: {}", e))
                    })?;

            let macd_values: Vec<Value> = prices
                .iter()
                .map(|&price| {
                    let output = macd_indicator.next(price);
                    let mut obj = std::collections::HashMap::new();
                    obj.insert("macd".to_string(), Value::Float(output.macd));
                    obj.insert("signal".to_string(), Value::Float(output.signal));
                    obj.insert("histogram".to_string(), Value::Float(output.histogram));
                    Value::Object(obj)
                })
                .collect();

            Ok(Value::Array(macd_values))
        }
        _ => Err(dsq_shared::error::operation_error(
            "macd() requires an array of numeric values",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "macd",
        func: builtin_macd,
    }
}
