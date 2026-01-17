# Automated Trading - Agent Context

This file provides context for AI agents working on the Automated Trading project, including build commands, architecture guidelines, and coding standards.

## Global Rules

Global rules from `~/.zed/rules/` are automatically available:
- System environment standards (CachyOS, fish, uv, podman)
- Socratic mentor persona (rigorous questioning)
- Self-improvement protocol
- Cleanup protocol
- Troubleshooting protocol (diagnostic-first)
- Python standards (for `*.py` files)

See `~/.zed/AGENTS.md` for complete global rules reference.

## Project-Specific Rules

Project-specific rules are located in `.zed/rules/`:
- `trading_context.md` - Risk-aware trading patterns (Fyers API, ML predictions, backtesting)

These rules are automatically loaded when working in this project.

## Project Overview

Distributed automated trading system with Ray and InfluxDB. Risk-aware, latency-critical systems with ML predictions for algorithmic trading.

## Build & Test Commands

### Installation
```bash
# Install dependencies using uv
uv sync

# Or install specific package
uv add package-name
```

### Code Quality
```bash
# Format code (Black)
make format
# or: uv run black src tests

# Lint code (Ruff)
make lint
# or: uv run ruff check src tests

# Type checking (Mypy)
make typecheck
# or: uv run mypy src

# Run all checks
make precommit
```

### Testing
```bash
# Run all tests
make test
# or: uv run pytest -q

# Run specific test
uv run pytest tests/test_trading_logic.py::test_place_order -v

# Run with coverage
uv run pytest --cov=src --cov-report=html
```

### Running
```bash
# Run main trading system
uv run python main.py

# Run with dry-run mode (no actual trades)
uv run python main.py --mode live --dry-run

# Run API server
make run-api
# or: uv run uvicorn src.app.main:app --reload --host 0.0.0.0 --port 8000
```

## Architecture Guidelines

### Risk-First Architecture

Every trading component must address:
- **Position Limits**: Hard caps on position size, enforced in code
- **Loss Limits**: Daily/weekly drawdown limits with auto-shutdown
- **Data Staleness**: Max age for price data before rejecting trades
- **Execution Slippage**: Expected vs actual fill price monitoring
- **API Rate Limits**: Backoff strategy for Fyers API (8 req/sec limit)
- **Circuit Breakers**: Auto-disable trading after N consecutive failures
- **Cooldown Periods**: Prevent rapid-fire trades on same symbol

### Stack Components

| Component | Tech | Critical Config |
|-----------|------|-----------------|
| Broker API | Fyers | Rate limit: 8 req/sec, auth token refresh |
| ML Models | MLflow | Model naming: `{symbol}_{timeframe}_{metric}` |
| Data Pipeline | Pandas/NumPy | Decimal for money, UTC timestamps |
| Distributed | Ray (optional) | Actor-based execution, circuit breakers |
| Risk Management | Custom | Position limits, drawdown tracking |
| Time-Series DB | InfluxDB | Retention policies, downsampling |

### Trading Decision Flow

```
Data Collection (every 5 min)
    ↓
Feature Aggregation (technical indicators)
    ↓
ML Predictions (5min, 15min, 30min, 1h timeframes)
    ↓
Weighted Signal Score (timeframe weights: 5m=1x, 15m=2x, 30m=3x, 1h=4x)
    ↓
Trade Signal (BUY if score ≥ 3.0, SELL if ≤ 2.0, HOLD otherwise)
    ↓
Position Check (current position for symbol)
    ↓
Signal Confirmation + Cooldown Check
    ↓
Position Sizing (risk-based: risk_per_trade_percent of account)
    ↓
Pre-trade Validation (position limits, market hours, API rate limit)
    ↓
Order Execution via Fyers API (with rate limiting)
    ↓
Post-trade Logging & Cache Update
```

## Coding Standards

### Python Standards

- **Type Hints**: Mandatory for all functions, use `typing` module
- **Async/Await**: Use async for I/O operations (API calls, database)
- **Error Handling**: Always catch specific exceptions, log context
- **Decimal for Money**: NEVER use float for financial calculations
- **UTC Timestamps**: Always use UTC for market data timestamps

### Code Example: Safe Order Placement

```python
async def place_order(
    symbol: str,
    qty: Decimal,
    max_position: Decimal,
    current_position: Decimal,
    risk_per_trade: float = 2.0
) -> OrderResult:
    # Pre-trade checks
    if qty <= 0:
        raise InvalidOrderError("Quantity must be positive")
    if current_position + qty > max_position:
        raise PositionLimitExceeded(f"Would exceed {max_position}")
    if not await is_market_open(symbol):
        raise MarketClosedError(symbol)
    
    # Calculate position size based on risk
    account_balance = await get_account_balance()
    max_risk_amount = account_balance * (risk_per_trade / 100)
    calculated_qty = calculate_position_size(symbol, max_risk_amount)
    qty = min(qty, calculated_qty)  # Enforce risk limit
    
    # Execute with timeout and rate limiting
    async with timeout(5.0):
        async with rate_limiter.acquire():
            result = await fyers.buy(symbol, qty)
    
    # Post-trade logging and validation
    logger.info("Order filled", extra={
        "symbol": symbol,
        "qty": str(qty),
        "fill_price": str(result.price),
        "slippage_bps": calculate_slippage(expected_price, result.price),
    })
    
    return result
```

### ML Prediction Integration

```python
# Validate predictions before trading
prediction = model.predict(features)
confidence = model.predict_proba(features).max()

if confidence < 0.6:  # Low confidence threshold
    logger.warning("Low prediction confidence", extra={"confidence": confidence})
    return "HOLD"  # Don't trade on uncertain predictions

# Map prediction to trading signal with thresholds
if prediction in ["High", "Medium High"] and confidence >= 0.6:
    return "BUY"
elif prediction in ["Low", "Medium Low"] and confidence >= 0.6:
    return "SELL"
else:
    return "HOLD"
```

## Formatting Rules

- **Formatter**: Black (line length: 88)
- **Linter**: Ruff
- **Type Checker**: Mypy (basic mode)
- **Import Sorting**: Ruff (isort)

## Preferred Libraries

- **Data Processing**: pandas, numpy
- **Async HTTP**: httpx (not requests)
- **Time-Series**: influxdb-client[async]
- **ML**: scikit-learn, mlflow
- **Distributed**: ray[default]
- **Trading API**: fyers-apiv3
- **Technical Analysis**: ta-lib, yfinance
- **Logging**: loguru
- **Config**: hydra-core, pydantic

## Project Structure

```
AutomatedTrading/
├── src/
│   ├── config/          # Hydra configs
│   ├── pipelines/        # ML pipelines
│   ├── trading_logic/    # Trading execution
│   ├── financial_analysis/  # Technical indicators
│   ├── distributed/     # Ray actors
│   └── utils/           # Utilities
├── tests/               # Test suite
├── scripts/             # Utility scripts
├── data/                # Data cache
└── model_artifacts/     # MLflow models
```

## Risk Management Checklist

Before ANY trade execution, verify:
- [ ] Position size ≤ max_position_size
- [ ] Total portfolio risk ≤ max_portfolio_risk_percent
- [ ] Number of positions ≤ max_positions
- [ ] Cooldown period elapsed since last trade
- [ ] Market is open (not after hours)
- [ ] ML prediction confidence ≥ threshold
- [ ] Circuit breaker not triggered
- [ ] Rate limiter has available tokens
- [ ] Account balance sufficient for trade

## Key Facts

- **Decimal, not float** for money (`Decimal('0.1') + Decimal('0.2') == Decimal('0.3')`)
- **Always use UTC** for timestamps (market data timestamps are UTC)
- **Log every order** with full context for audit trail
- **Paper trade before live** with identical code paths (dry-run mode)
- **Fyers API limits**: ~8 requests/second, token refresh required periodically
- **Symbol format**: `"NSE:SBIN-EQ"` (exchange:symbol-product)
- **Position sizing**: Based on risk_per_trade_percent (default 2%), not fixed quantities
- **Cooldown periods**: Prevent rapid trades on same symbol (default 5 minutes)
- **Circuit breakers**: Auto-disable trading after 5 consecutive failures, recover after 30s

## Debug & Verification

### Test Trading Logic
```bash
python -m pytest tests/test_trading_logic.py::test_place_order -v
```

### Dry-Run Mode
```bash
python main.py --mode live --dry-run
```

### Validate Position Limits
```python
from src.trading_logic import FyersTradeExecutor
executor = FyersTradeExecutor(...)
print(executor.calculate_position_size('NSE:SBIN-EQ', 10000))
```

### REPL Test
```python
from src.trading_logic.fyers_trade_executor import FyersTradeExecutor
executor = FyersTradeExecutor(fyers=..., default_product_type="INTRADAY")
balance = await executor.get_funds()
print(f"Available: {balance}")
```

## Backtesting Requirements

- **NEVER use future data** (look-ahead bias)
- Account for slippage, fees, and spread
- Test on out-of-sample data
- Report Sharpe ratio, max drawdown, win rate
- Validate against multiple market conditions (bull, bear, sideways)
