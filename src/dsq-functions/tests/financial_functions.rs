use dsq_shared::value::Value;

#[test]
fn test_log_returns() {
    let prices = Value::Array(vec![
        Value::Float(100.0),
        Value::Float(105.0),
        Value::Float(103.0),
        Value::Float(107.0),
    ]);

    let result = dsq_functions::builtin::log_returns::builtin_log_returns(&[prices]);
    assert!(result.is_ok());

    if let Ok(Value::Array(returns)) = result {
        assert_eq!(returns.len(), 4);
        assert!(matches!(returns[0], Value::Null)); // First value is null
        assert!(matches!(returns[1], Value::Float(_))); // Rest are calculated
    }
}

#[test]
fn test_zscore() {
    let values = Value::Array(vec![
        Value::Float(10.0),
        Value::Float(12.0),
        Value::Float(14.0),
        Value::Float(16.0),
        Value::Float(18.0),
    ]);

    let result = dsq_functions::builtin::zscore::builtin_zscore(&[values]);
    assert!(result.is_ok());

    if let Ok(Value::Array(zscores)) = result {
        assert_eq!(zscores.len(), 5);
        // Z-scores should sum to approximately 0
        let sum: f64 = zscores
            .iter()
            .filter_map(|v| match v {
                Value::Float(f) => Some(*f),
                _ => None,
            })
            .sum();
        assert!((sum).abs() < 0.0001);
    }
}

#[test]
fn test_rsi() {
    let prices = Value::Array(vec![
        Value::Float(100.0),
        Value::Float(102.0),
        Value::Float(101.0),
        Value::Float(103.0),
        Value::Float(105.0),
        Value::Float(104.0),
        Value::Float(106.0),
        Value::Float(108.0),
        Value::Float(107.0),
        Value::Float(109.0),
        Value::Float(111.0),
        Value::Float(110.0),
        Value::Float(112.0),
        Value::Float(114.0),
        Value::Float(113.0),
    ]);

    let period = Value::Int(14);
    let result = dsq_functions::builtin::rsi::builtin_rsi(&[prices, period]);
    assert!(result.is_ok());

    if let Ok(Value::Array(rsi_values)) = result {
        assert_eq!(rsi_values.len(), 15);
    }
}

#[test]
fn test_sharpe_ratio() {
    let returns = Value::Array(vec![
        Value::Float(0.01),
        Value::Float(0.02),
        Value::Float(-0.01),
        Value::Float(0.03),
        Value::Float(0.01),
    ]);

    let result = dsq_functions::builtin::sharpe_ratio::builtin_sharpe_ratio(&[returns]);
    assert!(result.is_ok());

    if let Ok(Value::Float(sharpe)) = result {
        assert!(sharpe.is_finite());
    }
}

#[test]
fn test_max_drawdown() {
    let equity = Value::Array(vec![
        Value::Float(100.0),
        Value::Float(110.0),
        Value::Float(105.0),
        Value::Float(115.0),
        Value::Float(100.0), // 13% drawdown from 115
        Value::Float(95.0),  // 17.4% drawdown from 115
    ]);

    let result = dsq_functions::builtin::max_drawdown::builtin_max_drawdown(&[equity]);
    assert!(result.is_ok());

    if let Ok(Value::Float(mdd)) = result {
        assert!(mdd > 0.17 && mdd < 0.18); // Around 17.4%
    }
}

#[test]
fn test_beta() {
    let asset_returns = Value::Array(vec![
        Value::Float(0.01),
        Value::Float(0.02),
        Value::Float(-0.01),
        Value::Float(0.03),
    ]);

    let market_returns = Value::Array(vec![
        Value::Float(0.015),
        Value::Float(0.01),
        Value::Float(-0.005),
        Value::Float(0.02),
    ]);

    let result = dsq_functions::builtin::beta::builtin_beta(&[asset_returns, market_returns]);
    assert!(result.is_ok());

    if let Ok(Value::Float(beta_val)) = result {
        assert!(beta_val.is_finite());
    }
}

#[test]
fn test_macd() {
    let prices = Value::Array((0..30).map(|i| Value::Float(100.0 + i as f64)).collect());

    let result = dsq_functions::builtin::macd::builtin_macd(&[prices]);
    assert!(result.is_ok());

    if let Ok(Value::Array(macd_values)) = result {
        assert_eq!(macd_values.len(), 30);
        // Each element should be an object with macd, signal, histogram
        if let Value::Object(obj) = &macd_values[29] {
            assert!(obj.contains_key("macd"));
            assert!(obj.contains_key("signal"));
            assert!(obj.contains_key("histogram"));
        }
    }
}

#[test]
fn test_bbands() {
    let prices = Value::Array(
        (0..25)
            .map(|i| Value::Float(100.0 + (i % 10) as f64))
            .collect(),
    );

    let result = dsq_functions::builtin::bbands::builtin_bbands(&[prices]);
    assert!(result.is_ok());

    if let Ok(Value::Array(bb_values)) = result {
        assert_eq!(bb_values.len(), 25);
        // Each element should be an object with upper, middle, lower
        if let Value::Object(obj) = &bb_values[24] {
            assert!(obj.contains_key("upper"));
            assert!(obj.contains_key("middle"));
            assert!(obj.contains_key("lower"));
        }
    }
}
