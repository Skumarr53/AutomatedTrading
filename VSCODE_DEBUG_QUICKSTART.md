# VS Code Debugging Quick Start

## 🚀 Setup (One Time)

1. **Create `.vscode/launch.json`** (copy from `DEBUGGING_GUIDE.md`)
2. **Install Python extension** in VS Code
3. **Install debugpy**: `pip install debugpy`

## 🎯 Quick Debug Commands

### Start Debugging
- Press `F5` → Select configuration from dropdown
- Or: Run → Start Debugging

### Debug Configurations Available

| Configuration | What It Does |
|--------------|--------------|
| **Debug: Data Collection Verification** | Tests data collection script |
| **Debug: Backtest Engine** | Runs backtest with debugging |
| **Debug: Live Trading Flow Test** | Tests complete trading flow |
| **Debug: Live Trading Flow - Single Step** | Tests one step (e.g., predictions) |
| **Debug: Alerting System** | Tests Slack/Telegram alerts |
| **Debug: Monitoring Verification** | Checks monitoring infrastructure |
| **Debug: Health API Server** | Runs health check API |
| **Debug: Main App (Live Trading)** | Debugs main trading loop |
| **Debug: Pytest - Data Integrity** | Runs data integrity tests |
| **Debug: Pytest - E2E Tests** | Runs end-to-end tests |
| **Debug: Current Python File** | Debugs currently open file |

## 🔍 Essential Breakpoints

### Data Collection (`scripts/verify_data_collection.py`)
- Line ~150: `async def verify_symbol()` - Entry point
- Line ~200: Data query execution
- Line ~250: Status determination

### Backtesting (`src/backtesting/backtest_engine.py`)
- Line ~200: `async def run_backtest()` - Main entry
- Line ~300: Model loading
- Line ~400: Simulation start
- Line ~500: Decision making

### Live Trading Flow (`scripts/test_live_trading_flow.py`)
- Line ~150: InfluxDB connection
- Line ~200: Data freshness check
- Line ~250: Model loading
- Line ~300: Prediction generation
- Line ~350: Decision making
- Line ~400: Trade execution

### Main App (`main.py`)
- Line ~570: `start_live_trading()` entry
- Line ~585: Prediction generation
- Line ~596: Signal calculation
- Line ~647: Order execution

## ⌨️ Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `F5` | Start/Continue debugging |
| `Shift+F5` | Stop debugging |
| `F9` | Toggle breakpoint |
| `F10` | Step Over (next line) |
| `F11` | Step Into (enter function) |
| `Shift+F11` | Step Out (exit function) |
| `Ctrl+Shift+F5` | Restart debugging |

## 👀 What to Watch

### During Data Collection Debug
- `result.ticker_record_count`
- `result.ticker_freshness_minutes`
- `df` (DataFrame with data)

### During Backtest Debug
- `ticker_df` - Historical data
- `models` - Loaded models
- `predictions` - Generated predictions
- `result.trades` - Trade records
- `result.metrics` - Performance metrics

### During Live Trading Debug
- `symbol` - Current symbol
- `predictions` - Model predictions
- `weighted_score` - Signal score
- `trade_signal` - BUY/SELL/HOLD
- `response` - Order execution result

## 🎓 Example: Debug Predictions

1. Open `scripts/test_live_trading_flow.py`
2. Set breakpoint at line ~300 (`_test_prediction_generation`)
3. Press `F5` → "Debug: Live Trading Flow - Single Step"
4. **Watch**:
   - `self._models` - Are models loaded?
   - `latest` - Input data row
   - `predictions` - Output predictions
5. **Step through** with `F10` to see each step
6. **Inspect** prediction values in Variables panel

## 💡 Pro Tips

1. **Conditional Breakpoints**: Right-click breakpoint → Add condition
   - Example: `symbol == "RELIANCE"`

2. **Logpoints**: Log without stopping
   - Right-click line → Add Logpoint
   - Example: `Symbol: {symbol}, Score: {weighted_score}`

3. **Debug Console**: Execute code during debugging
   - Type: `print(predictions)` or `symbol = "TCS"`

4. **Call Stack**: See function call hierarchy
   - Click different frames to see context

5. **Watch Expressions**: Add custom expressions
   - Example: `len(predictions)`, `weighted_score > 3.0`

## 📋 Debugging Checklist

- [ ] Set breakpoints at key locations
- [ ] Add variables to Watch panel
- [ ] Use Step Over/Into to trace execution
- [ ] Check Variables panel for current state
- [ ] Use Debug Console for quick tests
- [ ] Review Call Stack for context

---

**Full Guide**: See `DEBUGGING_GUIDE.md` for detailed instructions.
