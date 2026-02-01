# VS Code Debugging Guide

This guide provides step-by-step instructions for debugging the trading system using VS Code's interactive debugger.

## 🎯 Quick Setup

### 1. Create VS Code Launch Configuration

Create `.vscode/launch.json` in your project root:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug: Data Collection Verification",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/scripts/verify_data_collection.py",
            "console": "integratedTerminal",
            "args": ["--symbols", "RELIANCE", "TCS", "--days", "7"],
            "env": {
                "PYTHONPATH": "${workspaceFolder}"
            },
            "justMyCode": false
        },
        {
            "name": "Debug: Backtest Engine",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/scripts/run_backtest.py",
            "args": ["--symbol", "RELIANCE", "--days", "30"],
            "console": "integratedTerminal",
            "env": {"PYTHONPATH": "${workspaceFolder}"},
            "justMyCode": false
        },
        {
            "name": "Debug: Live Trading Flow Test",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/scripts/test_live_trading_flow.py",
            "args": ["--symbol", "RELIANCE", "--paper", "--verbose"],
            "console": "integratedTerminal",
            "env": {"PYTHONPATH": "${workspaceFolder}"},
            "justMyCode": false
        },
        {
            "name": "Debug: Live Trading Flow - Single Step",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/scripts/test_live_trading_flow.py",
            "args": ["--symbol", "RELIANCE", "--step", "predictions", "--paper"],
            "console": "integratedTerminal",
            "env": {"PYTHONPATH": "${workspaceFolder}"},
            "justMyCode": false
        },
        {
            "name": "Debug: Alerting System",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/scripts/test_alerting.py",
            "args": ["--slack"],
            "console": "integratedTerminal",
            "env": {"PYTHONPATH": "${workspaceFolder}"},
            "justMyCode": false
        },
        {
            "name": "Debug: Monitoring Verification",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/scripts/verify_monitoring.py",
            "console": "integratedTerminal",
            "env": {"PYTHONPATH": "${workspaceFolder}"},
            "justMyCode": false
        },
        {
            "name": "Debug: Health API Server",
            "type": "debugpy",
            "request": "launch",
            "module": "src.api.health",
            "console": "integratedTerminal",
            "env": {"PYTHONPATH": "${workspaceFolder}"},
            "justMyCode": false
        },
        {
            "name": "Debug: Main App (Live Trading)",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/main.py",
            "console": "integratedTerminal",
            "env": {
                "PYTHONPATH": "${workspaceFolder}",
                "VERBOSE_LOGGING": "1"
            },
            "justMyCode": false
        },
        {
            "name": "Debug: Pytest - Data Integrity",
            "type": "debugpy",
            "request": "launch",
            "module": "pytest",
            "args": ["tests/test_data_integrity.py", "-v", "-s"],
            "console": "integratedTerminal",
            "env": {"PYTHONPATH": "${workspaceFolder}"},
            "justMyCode": false
        },
        {
            "name": "Debug: Pytest - E2E Tests",
            "type": "debugpy",
            "request": "launch",
            "module": "pytest",
            "args": ["tests/test_live_trading_e2e.py", "-v", "-s"],
            "console": "integratedTerminal",
            "env": {"PYTHONPATH": "${workspaceFolder}"},
            "justMyCode": false
        },
        {
            "name": "Debug: MLflow Diagnostic",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/scripts/diagnose_mlflow.py",
            "console": "integratedTerminal",
            "args": [],
            "env": {"PYTHONPATH": "${workspaceFolder}"},
            "justMyCode": false
        },
        {
            "name": "Debug: MLflow Container Check",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/scripts/check_mlflow_container.py",
            "console": "integratedTerminal",
            "args": [],
            "env": {"PYTHONPATH": "${workspaceFolder}"},
            "justMyCode": false
        },
        {
            "name": "Debug: Current Python File",
            "type": "debugpy",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal",
            "env": {"PYTHONPATH": "${workspaceFolder}"},
            "justMyCode": false
        }
    ]
}
```

---

## 🔍 Debugging Scenarios

### Scenario 1: Debug Data Collection Verification

**Steps:**
1. Open `scripts/verify_data_collection.py`
2. Set breakpoints at:
   - Line ~150: `async def verify_symbol()` - Entry point
   - Line ~200: `ticker_df = await self.client.query_ticker_data()` - Data query
   - Line ~250: `result.status = self._determine_status()` - Status calculation
3. Press `F5` → Select "Debug: Data Collection Verification"
4. **Watch variables:**
   - `result.ticker_record_count`
   - `result.ticker_freshness_minutes`
   - `result.schema_valid`
5. **Step through:**
   - `F10` (Step Over) to go line by line
   - `F11` (Step Into) to enter functions
   - `Shift+F11` (Step Out) to exit current function
   - `F5` (Continue) to run to next breakpoint

**What to inspect:**
- ✅ Data query results
- ✅ Freshness calculations
- ✅ Schema validation logic
- ✅ Status determination

---

### Scenario 2: Debug Backtest Engine

**Steps:**
1. Open `src/backtesting/backtest_engine.py`
2. Set breakpoints at:
   - Line ~200: `async def run_backtest()` - Main entry
   - Line ~250: `ticker_df, orderbook_df = await self.load_historical_data()` - Data loading
   - Line ~300: `models = self.load_models()` - Model loading
   - Line ~400: `result = self._run_simulation()` - Simulation start
   - Line ~500: `decision = self._decision_maker.make_decision()` - Decision making
3. Press `F5` → Select "Debug: Backtest Engine"
4. **Watch variables:**
   - `ticker_df` - Historical data
   - `models` - Loaded models dict
   - `predictions` - Generated predictions
   - `result.trades` - Trade records
   - `result.metrics` - Performance metrics

**What to inspect:**
- ✅ Historical data loading
- ✅ Model prediction generation
- ✅ Trade simulation logic
- ✅ P&L calculations
- ✅ Performance metric computation

---

### Scenario 3: Debug Live Trading Flow

**Steps:**
1. Open `scripts/test_live_trading_flow.py`
2. Set breakpoints at:
   - Line ~150: `async def _test_influx_connection()` - Connection test
   - Line ~200: `async def _test_data_freshness()` - Data freshness
   - Line ~250: `async def _test_model_loading()` - Model loading
   - Line ~300: `async def _test_prediction_generation()` - Predictions
   - Line ~350: `async def _test_decision_making()` - Decisions
   - Line ~400: `async def _test_trade_execution()` - Execution
3. Press `F5` → Select "Debug: Live Trading Flow Test"
4. **Watch variables:**
   - `self._current_data` - Current market data
   - `self._current_predictions` - Model predictions
   - `self._current_decision` - Trade decision
   - `result.status` - Test result status

**What to inspect:**
- ✅ Each step's execution flow
- ✅ Data transformations
- ✅ Prediction values
- ✅ Decision logic
- ✅ Error handling

---

### Scenario 4: Debug Single Component (e.g., Predictions)

**Steps:**
1. Open `scripts/test_live_trading_flow.py`
2. Set breakpoint in `_test_prediction_generation()` method
3. Press `F5` → Select "Debug: Live Trading Flow - Single Step"
4. **Inspect:**
   - `latest` - Input data row
   - `predictions` - Generated predictions dict
   - `model.predict()` calls

**Alternative: Debug specific function**
```python
# Add this to a test file or scratch file
import asyncio
from scripts.test_live_trading_flow import LiveTradingFlowTester

async def debug_predictions():
    tester = LiveTradingFlowTester(symbol="RELIANCE", paper_mode=True)
    await tester.initialize()
    
    # Set breakpoint here
    result = await tester._test_prediction_generation()
    print(result)

asyncio.run(debug_predictions())
```

---

### Scenario 5: Debug Main App Live Trading

**Steps:**
1. Open `main.py`
2. Set breakpoints at:
   - Line ~570: `def start_live_trading()` - Entry point
   - Line ~585: `predictions = self.generate_live_predictions()` - Predictions
   - Line ~596: `weighted_score = self.trade_decision_maker.compute_weighted_signal()` - Signal
   - Line ~647: `response = self.trade_executor.place_market_order()` - Order execution
3. Press `F5` → Select "Debug: Main App (Live Trading)"
4. **Watch variables:**
   - `symbol` - Current symbol being processed
   - `predictions` - Generated predictions
   - `weighted_score` - Calculated signal score
   - `trade_signal` - BUY/SELL/HOLD decision
   - `response` - Order execution response

**What to inspect:**
- ✅ Symbol iteration
- ✅ Prediction generation per symbol
- ✅ Decision making logic
- ✅ Order execution flow
- ✅ Error handling

---

### Scenario 6: Debug Trade Execution Verification

**Steps:**
1. Open `src/trading_logic/fyers_trade_executor.py`
2. Set breakpoints at:
   - Line ~720: `def verify_trade_execution()` - Entry point
   - Line ~750: `order_book = self.get_order_book()` - Fetch orders
   - Line ~780: `order = next((o for o in order_book...))` - Find order
   - Line ~820: `result['slippage_pct'] = ...` - Slippage calculation
3. Create a test script:
```python
from src.trading_logic.fyers_trade_executor import FyersTradeExecutor

executor = FyersTradeExecutor(paper_mode=True)
# Set breakpoint here
result = executor.verify_trade_execution("test_order_id", expected_price=100.0)
print(result)
```
4. Debug the test script

**What to inspect:**
- ✅ Order lookup logic
- ✅ Status determination
- ✅ Slippage calculations
- ✅ Timeout handling

---

## 🛠️ Debugging Tips

### Breakpoint Strategies

1. **Entry Points**: Set breakpoints at function entry points
2. **Error Handling**: Set breakpoints in `except` blocks
3. **Data Transformations**: Set breakpoints before/after data transformations
4. **Decision Points**: Set breakpoints at if/else conditions

### Useful Debugging Features

1. **Watch Window**: Add variables to watch panel
   - Right-click variable → "Add to Watch"
   - Or manually add expressions

2. **Call Stack**: Inspect function call hierarchy
   - Shows how you got to current breakpoint
   - Click to navigate to different frames

3. **Variables Panel**: See all local variables
   - Automatically shows variables in current scope
   - Expand objects to see attributes

4. **Debug Console**: Execute Python code in current context
   - Type expressions to evaluate
   - Modify variables: `symbol = "TCS"`

5. **Conditional Breakpoints**: Break only when condition is true
   - Right-click breakpoint → "Edit Breakpoint"
   - Add condition: `symbol == "RELIANCE"`

6. **Logpoints**: Log without stopping execution
   - Right-click line → "Add Logpoint"
   - Enter: `Symbol: {symbol}, Score: {weighted_score}`

### Common Debugging Patterns

**Pattern 1: Inspect Data Flow**
```python
# Set breakpoint here
data_agg = self.data_aggregator.aggregate_features(ticker_data, order_book_data)
# Inspect: data_agg.columns, data_agg.shape, data_agg.head()

# Set breakpoint here
predictions = self.generate_live_predictions(data_agg, symbol)
# Inspect: predictions keys, prediction values
```

**Pattern 2: Step Through Decision Logic**
```python
# Set breakpoint here
decision = self._decision_maker.make_decision(pct_predictions=predictions)
# Step into make_decision() with F11
# Watch: confidence calculation, timeframe agreement, volatility assessment
```

**Pattern 3: Debug Async Code**
```python
# For async functions, use:
await tester._test_prediction_generation()
# VS Code debugger handles async/await correctly
# Use "Step Over" (F10) to go through await statements
```

---

## 📋 Debugging Checklist

### Before Starting
- [ ] Set up `.vscode/launch.json`
- [ ] Ensure Python extension is installed
- [ ] Verify environment variables are set
- [ ] Check that services (InfluxDB, MLflow) are running

### During Debugging
- [ ] Set breakpoints at key locations
- [ ] Use Watch window for important variables
- [ ] Check Call Stack for context
- [ ] Use Debug Console for quick tests
- [ ] Step through code systematically

### After Debugging
- [ ] Remove temporary breakpoints
- [ ] Document findings
- [ ] Fix issues found
- [ ] Re-run tests to verify fixes

---

## 🎯 Quick Reference

| Action | Shortcut | Description |
|--------|----------|------------|
| Start Debugging | `F5` | Start debug session |
| Stop Debugging | `Shift+F5` | Stop current session |
| Continue | `F5` | Continue to next breakpoint |
| Step Over | `F10` | Execute current line |
| Step Into | `F11` | Enter function call |
| Step Out | `Shift+F11` | Exit current function |
| Restart | `Ctrl+Shift+F5` | Restart debug session |
| Toggle Breakpoint | `F9` | Add/remove breakpoint |

---

## 🔧 Troubleshooting

### Debugger Not Starting
- Check Python extension is installed
- Verify `debugpy` is installed: `pip install debugpy`
- Check launch.json syntax is valid JSON

### Breakpoints Not Hitting
- Ensure `"justMyCode": false` in launch.json
- Check file paths are correct
- Verify code is actually executing (add print statements)

### Variables Not Showing
- Check variable is in current scope
- Use Debug Console to evaluate expressions
- Expand objects in Variables panel

### Async Code Issues
- VS Code debugger handles async correctly
- Use "Step Over" for await statements
- Check event loop is running

---

## 📝 Example: Complete Debugging Session

**Goal**: Debug why predictions are not being generated

1. **Set up**: Open `scripts/test_live_trading_flow.py`
2. **Breakpoint**: Line 300 in `_test_prediction_generation()`
3. **Start**: Press `F5` → "Debug: Live Trading Flow - Single Step"
4. **Inspect**:
   - `self._models` - Are models loaded?
   - `self._current_data` - Is data available?
   - `latest` - What's the input data?
5. **Step into**: `F11` on `model.predict(latest)`
6. **Watch**: `pred` variable - What's the prediction?
7. **Continue**: `F5` to see final result
8. **Fix**: Based on findings, fix the issue
9. **Re-test**: Run again to verify fix

---

### Scenario 7: Debug MLflow Issues

**Goal**: Debug why models aren't showing in MLflow UI

**Steps:**
1. Open `scripts/diagnose_mlflow.py`
2. Set breakpoints at:
   - Line ~50: `check_server_running()` - Server connectivity
   - Line ~80: `get_database_location()` - Database path resolution
   - Line ~90: `mlflow.set_tracking_uri()` - Tracking URI setup
   - Line ~100: `client.search_experiments()` - Experiment query
3. Press `F5` → Select "Debug: MLflow Diagnostic"
4. **Watch variables**:
   - `tracking_uri` - What URI is being used
   - `backend_uri` - Database location
   - `db_path` - Resolved database path
   - `experiments` - Found experiments
5. **Step through** to see:
   - Server connection status
   - Database file location
   - Experiment queries
   - Path resolution issues

**Common Issues to Check**:
- ✅ Server running but wrong database path
- ✅ Tracking URI mismatch
- ✅ Database exists but in wrong location
- ✅ Paths are relative instead of absolute

**Fix**: Use `scripts/fix_mlflow_paths.sh` or update config with absolute paths

### Scenario 8: Debug MLflow Permission Errors (Rootless Podman)

**Goal**: Fix `PermissionError: [Errno 13] Permission denied: '/mlflow'`

**Quick Fix**:
```bash
bash scripts/fix_mlflow_permissions.sh
```

**Debug Steps**:
1. **Check permissions**:
   ```bash
   ls -ld data/mlflow data/mlartifacts
   ```

2. **Check container user**:
   ```bash
   podman exec trading-mlflow id
   ```

3. **Check UID mapping**:
   ```bash
   podman unshare cat /proc/self/uid_map
   ```

4. **Fix permissions**:
   ```bash
   podman unshare chmod 777 data/mlflow data/mlartifacts
   ```

5. **Test write**:
   ```bash
   podman exec trading-mlflow touch /mlflow/test && podman exec trading-mlflow rm /mlflow/test
   ```

**Root Cause**: Rootless Podman maps container UID 0 → host UID 1000, but directories owned by 101000

**Fix**: Make directories world-writable using `podman unshare`

See: `MLFLOW_PERMISSIONS_FIX.md` for detailed explanation

---

### Scenario 9: Debug MLflow Container Database

**Goal**: Debug why models aren't showing when MLflow runs in container

**Quick Check (Recommended)**:
1. Open `scripts/check_mlflow_db.py`
2. Press `F5` → Run without debugging (or add breakpoint at `check_database()`)
3. **Check output**: Look for "Runs: 0" - this means no models logged!

**Detailed Debug**:
1. Open `scripts/check_mlflow_db.py`
2. Set breakpoint at line ~25: `check_database()` function
3. Press `F5` → Select "Debug: MLflow Database Check"
4. **Watch variables**:
   - `db_info["exists"]` - Database file exists?
   - `db_info["runs"]` - **CRITICAL**: Number of runs (0 = no models logged!)
   - `db_info["experiments"]` - Number of experiments
   - `db_info["experiment_list"]` - List of experiments
5. **Step through** to see:
   - Database file check
   - SQLite queries
   - Experiment/run counts

**Common Issues**:
- ✅ **Database empty (0 runs)** - No models have been logged yet (most common!)
- ✅ Container not running
- ✅ Database in wrong location (check bind mount: `data/mlflow/mlflow.db`)

**Fix**: 
- **If runs = 0**: Run model training: `python main.py --mode train`
- Check container: `podman ps | grep trading-mlflow`
- Verify database: `ls -lh data/mlflow/mlflow.db`
- Check bind mount: `podman inspect trading-mlflow | grep Mounts`

---

**Remember**: Interactive debugging is the best way to understand code flow and find issues. Use breakpoints liberally and step through code to see exactly what's happening!
