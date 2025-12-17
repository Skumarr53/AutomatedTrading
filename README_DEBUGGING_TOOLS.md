# 🛠️ Debugging Tools for AutomatedTrading

## Overview
This directory contains comprehensive debugging tools to help identify and fix the `ValueError: could not convert string to float` error that occurs during parallel model fitting.

## 📁 Files Created

### 1. **Quick Reference** 
- **`QUICK_FIX_GUIDE.md`** - 2-minute quick fix guide (START HERE!)
- **`SOLUTION_SUMMARY.md`** - Complete solution overview

### 2. **Diagnostic Scripts**
- **`test_preprocessing_isolation.py`** - Test preprocessing components independently
- **`debug_model_fit_error.py`** - Full diagnostic with real data

### 3. **Utilities**
- **`src/utils/data_validation.py`** - Validation utilities for production use

### 4. **Documentation**
- **`DEBUG_STRING_TO_FLOAT_ERROR.md`** - Detailed troubleshooting guide
- **`PATCH_add_validation.md`** - Code modification instructions
- **`README_DEBUGGING_TOOLS.md`** - This file

## 🚀 Getting Started

### Quick Fix (5 minutes)
1. Read **`QUICK_FIX_GUIDE.md`**
2. Add validation to `base_pipeline.py`
3. Run your code to see which columns have strings
4. Update your config accordingly

### Full Diagnostic (15 minutes)
1. Run isolation test:
   ```bash
   python test_preprocessing_isolation.py
   ```

2. Run full diagnostic:
   ```bash
   python debug_model_fit_error.py
   ```

3. Review output and apply fixes from `SOLUTION_SUMMARY.md`

## 📖 Documentation Guide

### For Quick Fixes
Start with these files in order:
1. `QUICK_FIX_GUIDE.md` - Get up and running
2. `PATCH_add_validation.md` - Apply code changes
3. `SOLUTION_SUMMARY.md` - Understand the solution

### For Deep Understanding
Read these for comprehensive knowledge:
1. `DEBUG_STRING_TO_FLOAT_ERROR.md` - All possible issues & solutions
2. `SOLUTION_SUMMARY.md` - Complete overview
3. Source code comments in diagnostic scripts

## 🔍 Diagnostic Scripts

### test_preprocessing_isolation.py
**Purpose:** Test each preprocessing step independently

**When to use:** First diagnostic step, or when modifying preprocessing

**What it tests:**
- Configuration is correct
- `CategoricalPreprocessor` works
- `DFFeatureUnion` combines features properly
- Full chain produces numeric output

**Run:**
```bash
python test_preprocessing_isolation.py
```

**Expected output:**
```
✓ PASSED: Configuration Check
✓ PASSED: Categorical Preprocessing
✓ PASSED: Feature Union
✓ PASSED: Full Preprocessing Chain
```

### debug_model_fit_error.py
**Purpose:** Test your actual pipeline with real data

**When to use:** After isolation test passes, or with production data

**What it does:**
- Loads real data from your sources
- Runs each pipeline step sequentially
- Validates dtypes at each stage
- Identifies problematic columns

**Run:**
```bash
python debug_model_fit_error.py
```

**Expected output:**
```
1. Initializing authentication... ✓
2. Initializing data handlers... ✓
3. Testing with symbol: AAPL
4. Fetching data... ✓
...
✓ Model fitted successfully!
```

## 🛠️ Validation Utility

### src/utils/data_validation.py
**Purpose:** Production-ready validation functions

**Functions:**
- `validate_data_for_model_fit()` - Comprehensive pre-fit validation
- `inspect_dataframe()` - Detailed DataFrame inspection
- `log_pipeline_step()` - Log transformation steps
- `find_column_with_value()` - Find columns with specific values

**Usage example:**
```python
from src.utils.data_validation import validate_data_for_model_fit

# Before model fitting
validate_data_for_model_fit(
    X_train, 
    y_train, 
    stage_name="pre_fit",
    raise_on_error=True
)
```

## 💡 Common Issues & Solutions

### Issue 1: Non-numeric Columns
**Error:** `ValueError: could not convert string to float: 'Medium Low'`

**Solution:** Add columns to `cat_cols` in config:
```yaml
cat_cols:
  - trend
  - sector
  - signal
```

### Issue 2: Encoding Disabled
**Error:** Categorical columns not being encoded

**Solution:** Enable in pipeline config:
```yaml
pipeline_configs:
  main_pipeline:
    cat_encode: true
```

### Issue 3: Parallel Execution Hides Errors
**Error:** Can't debug with debugger

**Solution:** Change to sequential execution:
```python
n_jobs=1,  # In RandomizedSearchCV
```

## 📊 Workflow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    ERROR ENCOUNTERED                         │
│  ValueError: could not convert string to float: 'Medium Low' │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│         Read QUICK_FIX_GUIDE.md (2 min)                     │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│    Run test_preprocessing_isolation.py                       │
│    ├─ ✓ All pass? → Issue is with data                     │
│    └─ ✗ Some fail? → Issue is with preprocessing logic     │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│         Run debug_model_fit_error.py                         │
│         (Shows exact columns with strings)                   │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Apply Fix from Guide                            │
│  ├─ Add columns to cat_cols                                 │
│  ├─ Enable cat_encode                                       │
│  └─ Add validation (recommended)                            │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                   Test Fix                                   │
│                python main.py                                │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
                   ✓ SUCCESS!
```

## 🎯 Best Practices

1. **Always validate before fitting:**
   ```python
   validate_data_for_model_fit(X_train, y_train)
   pipeline.model.fit(X_train, y_train)
   ```

2. **Use n_jobs=1 during development:**
   - Makes debugging easier
   - Clearer error messages
   - Can use debugger

3. **Test with small datasets first:**
   ```python
   symbols = ['AAPL']  # One symbol
   n_iter = 2          # Fewer iterations
   ```

4. **Log pipeline steps:**
   ```python
   from src.utils.data_validation import log_pipeline_step
   log_pipeline_step("preprocessing", X_before, X_after)
   ```

## 📝 File Summary

| File | Size | Purpose | Priority |
|------|------|---------|----------|
| `QUICK_FIX_GUIDE.md` | 3KB | Quick reference | HIGH |
| `SOLUTION_SUMMARY.md` | 10KB | Complete guide | HIGH |
| `test_preprocessing_isolation.py` | 8KB | Test preprocessing | HIGH |
| `debug_model_fit_error.py` | 12KB | Full diagnostic | HIGH |
| `src/utils/data_validation.py` | 7KB | Validation utils | MEDIUM |
| `DEBUG_STRING_TO_FLOAT_ERROR.md` | 15KB | Detailed docs | MEDIUM |
| `PATCH_add_validation.md` | 5KB | Code patches | MEDIUM |
| `README_DEBUGGING_TOOLS.md` | 4KB | This file | LOW |

## 🔗 Quick Links

**Start here:** `QUICK_FIX_GUIDE.md`  
**Need details?** `SOLUTION_SUMMARY.md`  
**Apply fix:** `PATCH_add_validation.md`  
**Troubleshooting:** `DEBUG_STRING_TO_FLOAT_ERROR.md`

## ⚡ One-Line Fixes

**Add validation:**
```python
# In base_pipeline.py before pipeline.model.fit()
from src.utils.data_validation import validate_data_for_model_fit; validate_data_for_model_fit(X_train, y_train, "pre_fit")
```

**Debug mode:**
```python
# In custom_pipelines.py
self.model = RandomizedSearchCV(..., n_jobs=1, verbose=3)
```

**Find problematic column:**
```python
# Quick check
non_numeric = X_train.select_dtypes(exclude=['number']).columns.tolist()
print(f"Non-numeric columns: {non_numeric}")
```

## 🎓 Learning Resources

- **sklearn Pipeline:** https://scikit-learn.org/stable/modules/compose.html
- **OneHotEncoder:** https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html
- **joblib Parallel:** https://joblib.readthedocs.io/en/latest/parallel.html

---

**Created:** For debugging parallel model fitting errors  
**Maintained by:** AutomatedTrading project  
**Last updated:** Today


