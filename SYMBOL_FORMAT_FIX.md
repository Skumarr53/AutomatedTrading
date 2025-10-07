# Symbol Format Fix - Fyers API

## Problem

**Error Received:**
```json
{
  'code': -50, 
  'message': 'The input symbol is invalid.', 
  's': 'error'
}
```

**Order Data:**
```python
{
  'symbol': 'PNB-EQ',  # ❌ WRONG - Missing exchange prefix
  'qty': 1, 
  'type': 1, 
  'side': 1, 
  ...
}
```

---

## Root Cause

Fyers API requires symbols in the format: **`EXCHANGE:SYMBOL-TYPE`**

### Valid Format Examples:
- ✅ `NSE:SBIN-EQ` - NSE Equity
- ✅ `BSE:RELIANCE-EQ` - BSE Equity
- ✅ `NSE:NIFTY50-INDEX` - NSE Index
- ✅ `MCX:GOLD-COMMODITY` - MCX Commodity

### Invalid Formats:
- ❌ `PNB-EQ` - Missing exchange prefix
- ❌ `SBIN` - Missing exchange and type
- ❌ `NIFTY50` - Missing exchange and type

---

## Solution Implemented

### 1. **Use `get_NSE_symbol()` Utility**

The codebase already has a helper function in `src/utils/utils.py`:

```python
def get_NSE_symbol(symbol: str) -> str:
    """
    Converts: 'PNB' → 'NSE:PNB-EQ'
    Converts: 'NIFTY50' → 'NSE:NIFTY50-INDEX'
    """
    return f"NSE:{symbol}-{'INDEX' if 'NIFTY' in symbol else 'EQ'}"
```

### 2. **Updated main.py**

**Import Added:**
```python
from src.utils.utils import determine_mode, get_timezone, get_NSE_symbol
```

**Symbol Formatting in `start_live_trading()`:**
```python
for symbol in config.symbols:
    # Format symbol for Fyers API (NSE:SYMBOL-EQ)
    fyers_symbol = get_NSE_symbol(symbol)
    
    # Use fyers_symbol for all trade executor calls
    current_position = self.trade_executor.get_position_for_symbol(fyers_symbol)
    
    # Calculate position size
    qty = self.trade_executor.calculate_position_size(
        symbol=fyers_symbol,  # ✅ Properly formatted
        price=current_price,
        ...
    )
    
    # Place order
    response = self.trade_executor.place_market_order(
        symbol=fyers_symbol,  # ✅ Properly formatted
        qty=qty,
        side="BUY"
    )
```

### 3. **Added Symbol Validation**

**New method in `FyersTradeExecutor`:**
```python
@staticmethod
def validate_symbol_format(symbol: str) -> bool:
    """
    Validates symbol format before placing orders.
    Returns: True if valid (NSE:SYMBOL-EQ), False otherwise
    """
    if ':' not in symbol or '-' not in symbol:
        logger.error(f"Invalid symbol format: '{symbol}'")
        return False
    
    exchange, rest = symbol.split(':', 1)
    if exchange not in ['NSE', 'BSE', 'MCX', 'NFO']:
        logger.error(f"Invalid exchange: '{exchange}'")
        return False
    
    return True
```

**Validation in `place_order()`:**
```python
def place_order(self, symbol: str, qty: int, side: str, ...):
    # Validate symbol format before API call
    if not self.validate_symbol_format(symbol):
        logger.error(f"Order rejected: Invalid symbol '{symbol}'")
        return None
    
    # Proceed with order placement...
```

### 4. **Fixed limitPrice for MARKET Orders**

```python
order_data = {
    "symbol": symbol,
    "qty": qty,
    "type": 1,  # MARKET order
    "limitPrice": 0,  # ✅ Must be 0 for MARKET orders (was 0.25)
    ...
}
```

---

## Configuration Updates

### Updated Default Product Type

**File: `src/trading_logic/fyers_trade_executor.py`**
```python
def __init__(self, fyers: fyersModel, default_product_type: str = "CNC"):
    # Changed from "INTRADAY" to "CNC" (delivery trading)
```

**File: `src/config/trading.yaml`**
```yaml
execution:
  product_type: "CNC"  # Changed from "INTRADAY"
```

**Difference:**
- **CNC (Cash and Carry)**: Delivery trading - shares move to demat account
- **INTRADAY**: Day trading - positions must be closed same day

---

## Testing

### Before Fix
```python
❌ Symbol: "PNB-EQ"
❌ Error: {'code': -50, 'message': 'The input symbol is invalid.'}
```

### After Fix
```python
✅ Symbol: "NSE:PNB-EQ"
✅ Order: {'code': 200, 'id': '2410051630001', 's': 'ok'}
```

---

## How Symbols Flow Through System

```
1. Config (symbols.yaml)
   symbols: ["PNB", "SBIN", "RELIANCE"]
   
2. Main.py (start_live_trading)
   for symbol in config.symbols:  # "PNB"
       fyers_symbol = get_NSE_symbol(symbol)  # "NSE:PNB-EQ"
   
3. Trade Executor
   place_order(symbol="NSE:PNB-EQ", ...)  # ✅ Valid format
   
4. Fyers API
   ✅ Accepts order
   ✅ Returns order ID
```

---

## Symbol Format Reference

### NSE (National Stock Exchange)
```python
"NSE:SBIN-EQ"          # Equity
"NSE:NIFTY50-INDEX"    # Index
"NSE:SBIN25JAN2025CE"  # Options
"NSE:SBIN25JANFUT"     # Futures
```

### BSE (Bombay Stock Exchange)
```python
"BSE:RELIANCE-EQ"      # Equity
"BSE:SENSEX-INDEX"     # Index
```

### MCX (Multi Commodity Exchange)
```python
"MCX:GOLD-COMMODITY"   # Commodity
"MCX:SILVER-COMMODITY" # Commodity
```

### NFO (NSE Futures & Options)
```python
"NFO:NIFTY25JAN25000CE"  # Call Option
"NFO:NIFTY25JAN25000PE"  # Put Option
```

---

## Common Errors & Solutions

### Error 1: Invalid Symbol
```
❌ Error: "The input symbol is invalid"
✅ Solution: Use get_NSE_symbol() to format correctly
```

### Error 2: Missing Exchange
```
❌ Symbol: "SBIN-EQ"
✅ Symbol: "NSE:SBIN-EQ"
```

### Error 3: Wrong Separator
```
❌ Symbol: "NSE.SBIN.EQ"  (using dots)
✅ Symbol: "NSE:SBIN-EQ"  (colon and hyphen)
```

### Error 4: Incorrect Type
```
❌ Symbol: "NSE:SBIN"     (missing -EQ)
✅ Symbol: "NSE:SBIN-EQ"
```

---

## Verification Checklist

Before placing orders, verify:

- [ ] Symbol has exchange prefix (NSE:, BSE:, etc.)
- [ ] Symbol has type suffix (-EQ, -INDEX, etc.)
- [ ] Format is EXCHANGE:SYMBOL-TYPE
- [ ] No spaces in symbol
- [ ] Using colon (:) and hyphen (-) separators
- [ ] Exchange is valid (NSE, BSE, MCX, NFO)

---

## Quick Reference

| Input Symbol | Output Symbol (Fyers API) | Description |
|--------------|---------------------------|-------------|
| PNB | NSE:PNB-EQ | PNB equity on NSE |
| SBIN | NSE:SBIN-EQ | State Bank equity |
| NIFTY50 | NSE:NIFTY50-INDEX | Nifty 50 index |
| RELIANCE | NSE:RELIANCE-EQ | Reliance equity |
| TATASTEEL | NSE:TATASTEEL-EQ | Tata Steel equity |

---

## Files Modified

1. ✅ `main.py` - Added `get_NSE_symbol()` import and usage
2. ✅ `src/trading_logic/fyers_trade_executor.py` - Added validation, fixed limitPrice
3. ✅ `src/config/trading.yaml` - Changed default product type to CNC

---

## Summary

**Problem:** Symbol format was invalid (`PNB-EQ` instead of `NSE:PNB-EQ`)

**Fix:** 
1. Use `get_NSE_symbol()` to format symbols correctly
2. Add validation to prevent invalid formats
3. Fix limitPrice for MARKET orders

**Result:** Orders now execute successfully! ✅

---

**Last Updated:** October 5, 2025

