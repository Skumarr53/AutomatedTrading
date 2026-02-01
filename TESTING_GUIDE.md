# Testing Guide - Live Trading End-to-End Testing

This guide provides step-by-step instructions to test all the new features and verify they work correctly.

## Prerequisites

1. **Environment Setup**
   ```bash
   # Ensure all services are running
   podman ps | grep -E "influxdb|prometheus|grafana|mlflow"
   
   # If not running, start them:
   # (Refer to your infrastructure setup scripts)
   ```

2. **Environment Variables**
   ```bash
   # Verify required env vars are set
   cat .env | grep -E "INFLUXDB|MLFLOW|FYERS|SLACK"
   
   # Required:
   # - INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG
   # - MLFLOW_TRACKING_URI
   # - FYERS_CLIENT_ID, FYERS_ACCESS_TOKEN (for live trading)
   # - SLACK_WEBHOOK_URL (for alerts)
   ```

---

## Part 1: Data Collection Verification

### Test 1.1: Verify Data Collection Script

```bash
# Test data verification for specific symbols
python scripts/verify_data_collection.py --symbols RELIANCE TCS INFY --days 7

# Expected output:
# ✅ Symbols with Ticker Data: 3/3
# ✅ Data freshness within threshold
# ✅ Schema validation passed

# Test all configured symbols
python scripts/verify_data_collection.py --all-symbols --freshness 5

# Generate JSON report
python scripts/verify_data_collection.py --all-symbols --report data_verification.json
```

**What to check:**
- ✅ All symbols have data
- ✅ Data freshness < 5 minutes (or your threshold)
- ✅ No missing columns
- ✅ No data quality issues

### Test 1.2: Run Data Integrity Tests

```bash
# Run pytest tests
pytest tests/test_data_integrity.py -v

# Run specific test class
pytest tests/test_data_integrity.py::TestDataExistence -v

# Run with custom symbols
pytest tests/test_data_integrity.py --symbols RELIANCE TCS -v
```

**Expected:**
- ✅ All tests pass (or skip if data not available)
- ✅ No failures in schema/quality checks

### Test 1.3: Verify Prometheus Metrics

```bash
# Start metrics server (if not already running)
# The metrics server starts automatically when DataIngestorActor is initialized

# Check metrics endpoint
curl http://localhost:8000/metrics | grep data_ingestion

# Expected metrics:
# - data_ingestion_success_total
# - data_ingestion_failure_total
# - data_records_ingested_total
# - data_freshness_seconds
# - data_ingestion_latency_seconds
```

---

## Part 2: Backtesting Framework

### Test 2.1: Quick Backtest (Single Symbol)

```bash
# Run a quick 30-day backtest
python scripts/run_backtest.py --symbol RELIANCE --days 30

# Expected output:
# Backtest Result
# Symbol: RELIANCE
# Total Return: X.XX%
# Sharpe Ratio: X.XX
# Max Drawdown: X.XX%
# Total Trades: XX
```

**What to check:**
- ✅ Backtest completes without errors
- ✅ Metrics are calculated correctly
- ✅ Trades are recorded

### Test 2.2: Model Comparison

```bash
# Compare multiple experiments
python scripts/run_backtest.py \
  --compare \
  --experiments TradingModels_Production TradingModels_Staging \
  --symbols RELIANCE TCS \
  --days 90

# Expected output:
# Comparison Report showing:
# - Best model by Sharpe ratio
# - Best model by return
# - Comparison table
```

### Test 2.3: Generate HTML Report

```bash
# Run backtest and generate report
python scripts/run_backtest.py \
  --symbol RELIANCE \
  --days 30 \
  --report backtest_report.json

# Then generate HTML (if you add report generation to CLI)
# Or use Python:
python -c "
from src.backtesting.backtest_engine import quick_backtest
from src.backtesting.report_generator import ReportGenerator
import asyncio

async def gen_report():
    result = await quick_backtest('RELIANCE', days=30)
    generator = ReportGenerator()
    path = generator.generate_html(result, 'backtest_report.html')
    print(f'Report: {path}')

asyncio.run(gen_report())
"
```

**Check the HTML report:**
- ✅ Equity curve chart displays
- ✅ Drawdown chart shows
- ✅ Trade log table populated
- ✅ Performance metrics correct

---

## Part 3: Live Trading Flow Testing

### Test 3.1: Test Complete Flow (Paper Mode)

```bash
# Test end-to-end flow without executing trades
python scripts/test_live_trading_flow.py --symbol RELIANCE --paper --verbose

# Expected output:
# ✅ influx_connection: PASS
# ✅ data_freshness: PASS
# ✅ model_loading: PASS
# ✅ feature_engineering: PASS
# ✅ prediction_generation: PASS
# ✅ decision_making: PASS
# ✅ trade_execution: PASS (paper mode)
```

**What to check:**
- ✅ All steps complete successfully
- ✅ Predictions are generated
- ✅ Decisions are made
- ✅ No errors in any step

### Test 3.2: Test Individual Steps

```bash
# Test only data collection
python scripts/test_live_trading_flow.py --step influx_connection

# Test only predictions
python scripts/test_live_trading_flow.py --step predictions

# Test only decision making
python scripts/test_live_trading_flow.py --step decision_making
```

### Test 3.3: Run E2E Tests

```bash
# Run all E2E tests
pytest tests/test_live_trading_e2e.py -v

# Run specific test class
pytest tests/test_live_trading_e2e.py::TestDataPipeline -v
pytest tests/test_live_trading_e2e.py::TestModelPipeline -v
pytest tests/test_live_trading_e2e.py::TestDecisionMaking -v
pytest tests/test_live_trading_e2e.py::TestTradeExecution -v
```

**Expected:**
- ✅ All tests pass (or skip gracefully if dependencies unavailable)
- ✅ No critical failures

---

## Part 4: Monitoring and Alerting

### Test 4.1: Verify Monitoring Infrastructure

```bash
# Check all monitoring components
python scripts/verify_monitoring.py

# Expected output:
# ✅ Prometheus: UP
# ✅ Prometheus Targets: UP (X/X targets healthy)
# ✅ Grafana: UP
# ✅ InfluxDB: UP
# ✅ App Metrics: UP
```

**What to check:**
- ✅ All components show UP status
- ✅ Prometheus targets are being scraped
- ✅ Metrics endpoint is accessible

### Test 4.2: Test Alerting System

```bash
# Test Slack notifications
python scripts/test_alerting.py --slack

# Expected output:
# ✅ webhook_connectivity: PASS
# ✅ message_type_good: PASS
# ✅ message_type_bad: PASS
# ✅ trading_alert_format: PASS

# Test Telegram (if configured)
python scripts/test_alerting.py --telegram

# Test all channels
python scripts/test_alerting.py --all

# Dry run (validate config only)
python scripts/test_alerting.py --dry-run
```

**Check Slack/Telegram:**
- ✅ Test messages appear in your channels
- ✅ Formatting is correct
- ✅ Trading alerts include all details

### Test 4.3: Health Check Endpoints

```bash
# Start health API server (if not integrated into main app)
python -m src.api.health

# In another terminal, test endpoints:
curl http://localhost:8080/health
curl http://localhost:8080/health/data
curl http://localhost:8080/health/models
curl http://localhost:8080/health/trading
curl http://localhost:8080/health/alerts
curl http://localhost:8080/health/live
curl http://localhost:8080/health/ready
```

**Expected JSON responses:**
```json
{
  "status": "healthy",
  "timestamp": "2024-...",
  "components": [...]
}
```

---

## Part 5: Trade Execution Verification

### Test 5.1: Verify Trade Execution Method

```python
# Test verify_trade_execution method
python -c "
from src.trading_logic.fyers_trade_executor import FyersTradeExecutor
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize executor (paper mode)
executor = FyersTradeExecutor(paper_mode=True)

# Place a test order (if you have test credentials)
# order_id = 'test_order_id'

# Verify execution
# result = executor.verify_trade_execution(order_id, expected_price=100.0)
# print(result)
"
```

### Test 5.2: Check Order Status

```python
# Test get_order_status method
python -c "
from src.trading_logic.fyers_trade_executor import FyersTradeExecutor

executor = FyersTradeExecutor(paper_mode=True)

# Get order book
orders = executor.get_order_book()
if orders:
    order_id = orders[0].get('id')
    status = executor.get_order_status(order_id)
    print(status)
"
```

---

## Part 6: Live Trading with Instrumentation

### Test 6.1: Run Live Trading with Logging

```bash
# Start the main application with verbose logging
export VERBOSE_LOGGING=1
python main.py

# Watch for:
# - "=== Live Trading Iteration Started ==="
# - Detailed logs for each symbol
# - "=== Live Trading Iteration Complete ===" with summary
# - Slack alerts for trade executions
```

**What to monitor:**
- ✅ Iteration summaries show correct counts
- ✅ Trade execution logs include all details
- ✅ Errors trigger Slack alerts
- ✅ Prometheus metrics are updated

### Test 6.2: Monitor Prometheus Metrics

```bash
# Query Prometheus for trading metrics
curl 'http://localhost:9090/api/v1/query?query=data_ingestion_success_total'

# Check latency
curl 'http://localhost:9090/api/v1/query?query=data_ingestion_latency_seconds'

# Check data freshness
curl 'http://localhost:9090/api/v1/query?query=data_freshness_seconds'
```

### Test 6.3: View Metrics in Grafana

1. Open Grafana: http://localhost:3000
2. Navigate to dashboards
3. Create/import dashboard for trading metrics
4. Add panels for:
   - `data_ingestion_success_total`
   - `data_ingestion_latency_seconds`
   - `data_freshness_seconds`
   - `data_records_ingested_total`

---

## Part 7: Complete End-to-End Test

### Full System Test Script

Create `scripts/test_complete_system.sh`:

```bash
#!/bin/bash
set -e

echo "=== Complete System Test ==="

echo "1. Testing Data Collection..."
python scripts/verify_data_collection.py --symbols RELIANCE --days 7

echo "2. Testing Data Integrity..."
pytest tests/test_data_integrity.py -v -k "test_ticker_data_exists"

echo "3. Testing Backtesting..."
python scripts/run_backtest.py --symbol RELIANCE --days 30 --quiet

echo "4. Testing Live Trading Flow..."
python scripts/test_live_trading_flow.py --symbol RELIANCE --paper

echo "5. Testing Alerting..."
python scripts/test_alerting.py --slack --dry-run

echo "6. Testing Monitoring..."
python scripts/verify_monitoring.py --prometheus-only

echo "7. Testing Health Endpoints..."
curl -s http://localhost:8080/health | jq '.status' || echo "Health API not running (OK if not started)"

echo "=== All Tests Complete ==="
```

Make it executable and run:
```bash
chmod +x scripts/test_complete_system.sh
./scripts/test_complete_system.sh
```

---

## Troubleshooting

### Common Issues

1. **InfluxDB Connection Failed**
   ```bash
   # Check if InfluxDB is running
   podman ps | grep influxdb
   
   # Check connection
   curl http://localhost:8086/health
   
   # Verify token
   echo $INFLUXDB_TOKEN
   ```

2. **MLflow Not Accessible**
   ```bash
   # Check MLflow server
   curl http://localhost:5000/health
   
   # List experiments
   mlflow experiments list
   ```

3. **Prometheus Metrics Not Appearing**
   ```bash
   # Check if metrics server started
   curl http://localhost:8000/metrics
   
   # Check Prometheus targets
   curl http://localhost:9090/api/v1/targets
   ```

4. **Slack Alerts Not Working**
   ```bash
   # Test webhook directly
   curl -X POST $SLACK_WEBHOOK_URL \
     -H 'Content-Type: application/json' \
     -d '{"text":"Test message"}'
   ```

---

## Success Criteria

✅ **Data Collection:**
- All symbols have data in InfluxDB
- Data freshness < 5 minutes
- No schema/quality issues

✅ **Backtesting:**
- Backtests complete successfully
- Reports generate correctly
- Metrics are accurate

✅ **Live Trading:**
- Flow completes without errors
- Predictions are generated
- Decisions are made correctly
- Trades execute (in paper mode)

✅ **Monitoring:**
- Prometheus metrics are collected
- Health endpoints respond correctly
- Alerts are sent successfully

✅ **Integration:**
- All components work together
- No critical errors in logs
- System is production-ready

---

## Next Steps

1. **Run backtests** on historical data to validate models
2. **Monitor live trading** in paper mode for a few days
3. **Review metrics** in Grafana dashboards
4. **Tune thresholds** based on backtest results
5. **Enable live trading** when confident

---

## Quick Reference

```bash
# Data verification
python scripts/verify_data_collection.py --all-symbols

# Backtest
python scripts/run_backtest.py --symbol RELIANCE --days 30

# Test flow
python scripts/test_live_trading_flow.py --all-steps

# Test alerts
python scripts/test_alerting.py --all

# Check monitoring
python scripts/verify_monitoring.py

# Health check
curl http://localhost:8080/health
```
