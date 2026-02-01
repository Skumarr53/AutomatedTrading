# Quick Test Reference Card

## 🚀 Quick Start - Test Everything

```bash
# Run complete system test
./scripts/test_complete_system.sh
```

## 📋 Individual Test Commands

### Data Collection
```bash
# Verify data exists and is fresh
python scripts/verify_data_collection.py --all-symbols --freshness 5

# Run integrity tests
pytest tests/test_data_integrity.py -v
```

### Backtesting
```bash
# Quick backtest (30 days)
python scripts/run_backtest.py --symbol RELIANCE --days 30

# Compare models
python scripts/run_backtest.py --compare --experiments Exp1 Exp2 --symbols RELIANCE
```

### Live Trading Flow
```bash
# Test complete flow (paper mode)
python scripts/test_live_trading_flow.py --symbol RELIANCE --paper --verbose

# Test specific step
python scripts/test_live_trading_flow.py --step predictions
```

### Monitoring & Alerts
```bash
# Check monitoring infrastructure
python scripts/verify_monitoring.py

# Test alerts
python scripts/test_alerting.py --slack

# Health check
curl http://localhost:8080/health
```

## 🎯 What to Look For

### ✅ Success Indicators

**Data Collection:**
- ✅ All symbols show data
- ✅ Freshness < 5 minutes
- ✅ No schema errors

**Backtesting:**
- ✅ Returns calculated
- ✅ Sharpe ratio > 0
- ✅ Trades recorded

**Live Trading:**
- ✅ All steps PASS
- ✅ Predictions generated
- ✅ Decisions made

**Monitoring:**
- ✅ All components UP
- ✅ Metrics available
- ✅ Alerts working

## 🔍 Quick Verification

```bash
# Check services
podman ps | grep -E "influxdb|prometheus|mlflow"

# Check metrics
curl http://localhost:8000/metrics | grep data_ingestion

# Check health
curl http://localhost:8080/health | jq '.status'

# Test alert
python scripts/test_alerting.py --slack --dry-run
```

## 📊 Expected Outputs

### Data Verification
```
✅ Symbols with Ticker Data: 10/10
✅ Data freshness within threshold
Total Ticker Records: 50,000+
```

### Backtest
```
Total Return: 5.23%
Sharpe Ratio: 1.45
Max Drawdown: 8.50%
Total Trades: 45
Win Rate: 55.56%
```

### Live Trading Flow
```
✅ influx_connection: PASS (150ms)
✅ data_freshness: PASS (120ms)
✅ model_loading: PASS (800ms)
✅ prediction_generation: PASS (200ms)
✅ decision_making: PASS (50ms)
✅ trade_execution: PASS (paper mode)
```

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| InfluxDB connection failed | Check `podman ps \| grep influxdb` |
| MLflow not accessible | Verify `MLFLOW_TRACKING_URI` |
| Metrics not appearing | Check port 8000 is available |
| Alerts not working | Test webhook: `curl -X POST $SLACK_WEBHOOK_URL` |

## 📝 Test Checklist

- [ ] Data collection verified
- [ ] Backtest runs successfully
- [ ] Live trading flow completes
- [ ] Monitoring infrastructure UP
- [ ] Alerts sent successfully
- [ ] Health endpoints respond
- [ ] Prometheus metrics visible
- [ ] No critical errors in logs

---

**Full Guide:** See `TESTING_GUIDE.md` for detailed instructions.
