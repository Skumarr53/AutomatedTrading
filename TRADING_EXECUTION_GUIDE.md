# Trading Execution System - Implementation Guide

## Overview

A comprehensive automated trading system integrated with Fyers broker API for executing trades based on ML predictions and technical indicators.

---

## 🚀 Features Implemented

### 1. **FyersTradeExecutor** (`src/trading_logic/fyers_trade_executor.py`)

A robust trading execution class with the following capabilities:

#### Account Management
- ✅ `get_funds()` - Fetch account balance and available funds
- ✅ `get_positions()` - Get current open positions
- ✅ `get_holdings()` - Get long-term holdings
- ✅ `get_position_for_symbol()` - Check position for specific symbol

#### Order Execution
- ✅ `place_order()` - Place market or limit orders
- ✅ `place_market_order()` - Convenience method for market orders
- ✅ `place_limit_order()` - Convenience method for limit orders
- ✅ `exit_position()` - Close an existing position

#### Risk Management
- ✅ `calculate_position_size()` - Calculate position size based on:
  - Available account balance
  - Risk percentage (default: 2%)
  - Maximum position size limit
  - Current price

#### Order Management
- ✅ `get_order_book()` - View all orders (pending, executed, cancelled)
- ✅ `get_tradebook()` - View all executed trades
- ✅ `cancel_order()` - Cancel a pending order

#### Performance Features
- ✅ **Caching**: 30-second cache for funds and positions (configurable)
- ✅ **Error Handling**: Comprehensive try-catch blocks with logging
- ✅ **Automatic Cache Invalidation**: Updates after trade execution

---

## 📋 Integration in main.py

### Initialization

The trade executor is initialized in `setup_based_on_mode()`:

```python
self.trade_executor = FyersTradeExecutor(
    fyers=self.fyers_instance,
    default_product_type="INTRADAY"  # or "CNC" for delivery
)
```

### Trading Logic in `start_live_trading()`

The complete workflow:

1. **Data Collection** → Fetch ticker and order book data
2. **Feature Aggregation** → Combine features using DataAggregator
3. **ML Predictions** → Generate predictions for multiple timeframes
4. **Signal Scoring** → Compute weighted signal score
5. **Position Check** → Check current position for symbol
6. **Trade Decision** → Decide BUY/SELL/HOLD based on:
   - Weighted prediction score
   - Current position
   - Cooldown period
   - Signal confirmation
7. **Position Sizing** → Calculate quantity based on risk management
8. **Trade Execution** → Place order via Fyers API

---

## ⚙️ Configuration

### Trading Parameters (`src/config/trading.yaml`)

```yaml
execution:
  product_type: "INTRADAY"  # INTRADAY or CNC
  order_type: "MARKET"      # MARKET or LIMIT
  risk_per_trade_percent: 2.0  # Risk 2% per trade
  max_position_size: 100    # Max 100 shares per trade
  enable_short_trading: false
  cooldown_minutes: 5       # 5 min cooldown between trades

risk_management:
  max_portfolio_risk_percent: 10.0
  max_positions: 5
  stop_loss_atr_multiplier: 2.0
  take_profit_atr_multiplier: 3.0

signal_thresholds:
  buy_score_threshold: 3.0  # Score ≥ 3.0 → BUY
  sell_score_threshold: 2.0  # Score ≤ 2.0 → SELL
  confirmation_required: true
```

---

## 🎯 Trading Decision Logic

### Signal Scoring

Predictions from multiple timeframes are weighted:

```python
Weights:
- 5min:  1x
- 15min: 2x
- 30min: 3x (implied)
- 1h:    4x

Mapping:
'Low' → 1, 'Medium Low' → 2, 'Neutral' → 2.5, 
'Medium High' → 3, 'High' → 4

Weighted Score = Σ(prediction_score × weight) / Σ(weights)
```

### Trade Signals

- **BUY**: `weighted_score >= 3.0`
- **SELL**: `weighted_score <= 2.0`
- **HOLD**: `2.0 < weighted_score < 3.0`

### Position Management

#### BUY Signal
```
IF position_qty <= 0:
    IF position_qty < 0:  # Has short position
        → Exit short position first
    
    IF signal_confirmed AND cooldown_over:
        → Calculate position size
        → Place BUY market order
```

#### SELL Signal
```
IF position_qty > 0:  # Has long position
    IF signal_confirmed AND cooldown_over:
        → Exit long position
        
ELSE:
    → Log: "No position to exit"
```

---

## 📊 Example Usage

### Check Account Balance

```python
funds = self.trade_executor.get_funds()
print(f"Available: ₹{funds['available_balance']:.2f}")
```

### Check Current Positions

```python
positions = self.trade_executor.get_positions()
for pos in positions:
    print(f"{pos['symbol']}: Qty={pos['netQty']}, P&L=₹{pos['pl']}")
```

### Execute a Trade

```python
# Automatic position sizing and execution
current_price = 500.0
qty = self.trade_executor.calculate_position_size(
    symbol="NSE:SBIN-EQ",
    price=current_price,
    risk_percent=2.0,
    max_position_size=100
)

response = self.trade_executor.place_market_order(
    symbol="NSE:SBIN-EQ",
    qty=qty,
    side="BUY"
)
```

### Exit a Position

```python
success = self.trade_executor.exit_position("NSE:SBIN-EQ")
```

---

## 🔐 Security & Best Practices

### API Credentials
- Stored securely in `config/environment/development.yaml`
- Never commit credentials to git
- Use environment variables for production

### Rate Limiting
- Fyers API has rate limits
- Caching reduces API calls
- 30-second cache for positions/funds

### Error Handling
- All API calls wrapped in try-catch
- Comprehensive logging with loguru
- Failed trades don't crash the system

### Risk Management
- Default 2% risk per trade
- Maximum position size enforced
- Cooldown period prevents overtrading
- Signal confirmation prevents false signals

---

## 📝 Logs & Monitoring

### Log Levels

```python
logger.info()     # Regular operations
logger.warning()  # Trade executions
logger.success()  # Successful orders
logger.error()    # Failures
logger.debug()    # Detailed info
```

### Sample Log Output

```
2025-10-05 16:30:00 | INFO | Available Balance: ₹50000.00
2025-10-05 16:30:01 | INFO | Processing trading signals for NSE:SBIN-EQ
2025-10-05 16:30:02 | INFO | Position size for NSE:SBIN-EQ: 20 shares (Price: ₹500.00, Risk: 2%)
2025-10-05 16:30:03 | WARNING | NSE:SBIN-EQ: EXECUTING BUY - Qty: 20, Price: ₹500.00, Score: 3.25
2025-10-05 16:30:04 | INFO | Placing MARKET BUY order: NSE:SBIN-EQ, Qty: 20, Price: MARKET
2025-10-05 16:30:05 | SUCCESS | ✓ Order placed successfully! Order ID: 2410051630001
2025-10-05 16:30:05 | SUCCESS | ✓ NSE:SBIN-EQ: BUY order executed successfully
```

---

## 🧪 Testing

### Test Mode
Before going live, test with:
1. Paper trading account
2. Small position sizes
3. Limited number of symbols
4. Monitor logs carefully

### Validation Checklist
- [ ] MLflow server is running
- [ ] Models are loaded successfully
- [ ] Fyers authentication works
- [ ] Account balance is fetched
- [ ] Positions are retrieved correctly
- [ ] Test orders execute successfully
- [ ] Logging is comprehensive
- [ ] Error handling works

---

## 🔄 Workflow Summary

```
Data Collection (every 5 min)
    ↓
Feature Aggregation
    ↓
ML Predictions (5min, 15min, 30min, 1h)
    ↓
Weighted Signal Score
    ↓
Trade Signal (BUY/SELL/HOLD)
    ↓
Position Check
    ↓
Signal Confirmation + Cooldown Check
    ↓
Position Sizing (risk-based)
    ↓
Order Execution via Fyers API
    ↓
Logging & Cache Update
```

---

## 🚨 Important Notes

1. **Live Trading Risk**: This system executes real trades with real money. Always test thoroughly first!

2. **Symbol Format**: Fyers requires format like `"NSE:SBIN-EQ"`. Update your symbols in `config/symbols.yaml`.

3. **MLflow Connection**: Ensure MLflow server is running and models are registered with proper naming convention: `{symbol}_{timeframe}_{metric}`

4. **Market Hours**: System respects trading hours via scheduler configuration.

5. **Manual Override**: You can always manually intervene by:
   - Canceling orders via `trade_executor.cancel_order(order_id)`
   - Exiting positions via `trade_executor.exit_position(symbol)`
   - Checking status via `get_order_book()` and `get_tradebook()`

---

## 📚 Additional Resources

- [Fyers API Documentation](https://myapi.fyers.in/docsv3)
- [Fyers Python Client](https://github.com/fyers-api/fyers-api-v3)
- Project README: `README.md`

---

## 🎉 Ready to Trade!

Your automated trading system is now fully integrated with:
- ✅ ML model predictions
- ✅ Risk management
- ✅ Position sizing
- ✅ Order execution
- ✅ Portfolio monitoring
- ✅ Comprehensive logging

**Start the system:**
```bash
cd /home/skumar/DaatScience/AutomatedTrading
python main.py
```

Happy Trading! 🚀📈

