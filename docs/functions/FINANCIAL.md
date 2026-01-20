# Financial and Statistical Functions

DatasetQ provides comprehensive financial analysis and statistical functions for time series data.

## Statistical Functions

### `log_returns(prices)`

Computes logarithmic returns from a price series.

**Formula**: `log_return[i] = ln(price[i] / price[i-1])`

**Arguments**:
- `prices`: Array of price values

**Returns**: Array of log returns (first element is null)

**Example**:
```
log_returns([100, 105, 103, 107])
```

### `zscore(values)`

Computes z-score (standard score) normalization.

**Formula**: `zscore = (x - mean) / std_dev`

**Arguments**:
- `values`: Array of numeric values

**Returns**: Array of z-scores

**Example**:
```
zscore([10, 12, 14, 16, 18])
```

## Momentum Indicators

### `rsi(prices[, period])`

Computes the Relative Strength Index momentum indicator.

**Arguments**:
- `prices`: Array of price values
- `period`: Period for RSI calculation (default: 14)

**Returns**: Array of RSI values (0-100)

**Example**:
```
rsi(.close, 14)
```

### `roc(prices[, period])`

Computes the Rate of Change momentum indicator.

**Arguments**:
- `prices`: Array of price values
- `period`: Period for ROC calculation (default: 12)

**Returns**: Array of ROC values (percentage change)

**Example**:
```
roc(.close, 12)
```

### `macd(prices[, fast_period, slow_period, signal_period])`

Computes the Moving Average Convergence Divergence indicator.

**Arguments**:
- `prices`: Array of price values
- `fast_period`: Fast EMA period (default: 12)
- `slow_period`: Slow EMA period (default: 26)
- `signal_period`: Signal line period (default: 9)

**Returns**: Array of objects with `{macd, signal, histogram}`

**Example**:
```
macd(.close, 12, 26, 9)
```

### `cci(high, low, close[, period])`

Computes the Commodity Channel Index.

**Arguments**:
- `high`: Array of high prices
- `low`: Array of low prices
- `close`: Array of close prices
- `period`: Period for CCI calculation (default: 20)

**Returns**: Array of CCI values

**Example**:
```
cci(.high, .low, .close, 20)
```

## Volatility Indicators

### `bbands(prices[, period, std_dev_mult])`

Computes Bollinger Bands.

**Arguments**:
- `prices`: Array of price values
- `period`: Period for moving average (default: 20)
- `std_dev_mult`: Standard deviation multiplier (default: 2.0)

**Returns**: Array of objects with `{upper, middle, lower}`

**Example**:
```
bbands(.close, 20, 2.0)
```

### `atr(high, low, close[, period])`

Computes the Average True Range volatility indicator.

**Arguments**:
- `high`: Array of high prices
- `low`: Array of low prices
- `close`: Array of close prices
- `period`: Period for ATR calculation (default: 14)

**Returns**: Array of ATR values

**Example**:
```
atr(.high, .low, .close, 14)
```

## Volume Indicators

### `obv(close, volume)`

Computes the On-Balance Volume indicator.

**Arguments**:
- `close`: Array of close prices
- `volume`: Array of volumes

**Returns**: Array of OBV values

**Example**:
```
obv(.close, .volume)
```

## Stochastic Oscillators

### `stoch_k(high, low, close[, period])`

Computes Stochastic %K.

**Arguments**:
- `high`: Array of high prices
- `low`: Array of low prices
- `close`: Array of close prices
- `period`: Period for calculation (default: 14)

**Returns**: Array of %K values (0-100)

**Example**:
```
stoch_k(.high, .low, .close, 14)
```

### `stoch_d(high, low, close[, k_period, d_period])`

Computes Stochastic %D (smoothed %K).

**Arguments**:
- `high`: Array of high prices
- `low`: Array of low prices
- `close`: Array of close prices
- `k_period`: Period for %K calculation (default: 14)
- `d_period`: Period for %D smoothing (default: 3)

**Returns**: Array of %D values (0-100)

**Example**:
```
stoch_d(.high, .low, .close, 14, 3)
```

## Trend Indicators

### `adx(high, low, close[, period])`

Computes the Average Directional Index trend strength indicator.

**Arguments**:
- `high`: Array of high prices
- `low`: Array of low prices
- `close`: Array of close prices
- `period`: Period for ADX calculation (default: 14)

**Returns**: Array of ADX values (0-100)

**Example**:
```
adx(.high, .low, .close, 14)
```

### `parabolic_sar(high, low[, af_step, af_max])`

Computes the Parabolic SAR trend indicator.

**Arguments**:
- `high`: Array of high prices
- `low`: Array of low prices
- `af_step`: Acceleration factor step (default: 0.02)
- `af_max`: Maximum acceleration factor (default: 0.2)

**Returns**: Array of SAR values

**Example**:
```
parabolic_sar(.high, .low, 0.02, 0.2)
```

## Risk Metrics

### `sharpe_ratio(returns[, risk_free_rate])`

Computes the Sharpe ratio.

**Formula**: `sharpe = (mean_return - risk_free_rate) / std_dev`

**Arguments**:
- `returns`: Array of returns
- `risk_free_rate`: Risk-free rate (default: 0.0)

**Returns**: Single float value (Sharpe ratio)

**Example**:
```
sharpe_ratio(.returns, 0.02)
```

### `sortino_ratio(returns[, risk_free_rate])`

Computes the Sortino ratio (like Sharpe but uses downside deviation).

**Arguments**:
- `returns`: Array of returns
- `risk_free_rate`: Risk-free rate (default: 0.0)

**Returns**: Single float value (Sortino ratio)

**Example**:
```
sortino_ratio(.returns, 0.02)
```

### `max_drawdown(equity)`

Computes the maximum peak-to-trough decline.

**Arguments**:
- `equity`: Array of equity/price values

**Returns**: Single float value (maximum drawdown as percentage 0.0-1.0)

**Example**:
```
max_drawdown(.portfolio_value)
```

## Portfolio Analysis

### `beta(asset_returns, market_returns)`

Computes the beta coefficient.

**Formula**: `beta = Cov(asset, market) / Var(market)`

**Arguments**:
- `asset_returns`: Array of asset returns
- `market_returns`: Array of market returns

**Returns**: Single float value (beta coefficient)

**Example**:
```
beta(.stock_returns, .spy_returns)
```

### `alpha(asset_returns, market_returns[, risk_free_rate])`

Computes Jensen's alpha.

**Formula**: `alpha = mean(asset) - (rf + beta * (mean(market) - rf))`

**Arguments**:
- `asset_returns`: Array of asset returns
- `market_returns`: Array of market returns
- `risk_free_rate`: Risk-free rate (default: 0.0)

**Returns**: Single float value (alpha)

**Example**:
```
alpha(.stock_returns, .spy_returns, 0.02)
```

## Notes

- All array-based functions expect numeric values (Int or Float)
- Null values are handled gracefully in calculations
- Period parameters default to commonly used values (e.g., 14 for RSI, 20 for Bollinger Bands)
- Risk/return metrics return NaN when insufficient data is available
- Functions using the `ta` crate provide industry-standard implementations
