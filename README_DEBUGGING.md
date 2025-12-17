# 🐛 Complete Debugging Solution for AutomatedTrading

## Problem Solved ✅

**Error**: `Error patching args (debugger not attached to subprocess)`  
**Cause**: scikit-learn's `RandomizedSearchCV` with `n_jobs=4` creates subprocesses that VS Code debugger couldn't attach to.

## What Was Implemented

### 🔧 Files Created/Modified

1. **`.vscode/launch.json`** - 3 debug configurations
2. **`src/pipelines/custom_pipelines.py`** - Support for `SKLEARN_N_JOBS` env variable
3. **`src/utils/pipeline_debugger.py`** - NEW debugging utilities
4. **`debug_model_fit_error.py`** - UPDATED to use new utilities
5. **`DEBUGGING_GUIDE.md`** - Complete debugging guide
6. **`DEBUGGING_SUMMARY.md`** - Quick reference
7. **`example_debug_pipeline.py`** - Usage examples

## 🚀 Quick Start

### Option 1: Run the Debug Script (Easiest)

```bash
cd /home/skumar/DaatScience/AutomatedTrading
python debug_model_fit_error.py
```

This will:
- ✅ Automatically set `n_jobs=1` (no subprocess issues)
- ✅ Test each pipeline step
- ✅ Show exactly where the error occurs
- ✅ Display input/output dtypes for each transformer

### Option 2: Use VS Code Debugger

1. Press `Ctrl+Shift+D` (Debug panel)
2. Select **"AutoTrading: Debug (No Parallel)"** from dropdown
3. Press `F5`

This configuration:
- ✅ Sets `SKLEARN_N_JOBS=1` (sequential execution)
- ✅ Allows stepping through code
- ✅ Shows full stack traces
- ✅ No subprocess errors

### Option 3: Manual Testing in Python

```python
from src.utils.pipeline_debugger import test_pipeline_steps, set_sequential_mode

# Force sequential mode
set_sequential_mode()

# Your existing code
ml_pipeline = MLPipelineBase()
pipeline = ml_pipeline.pipelines[0]
X_train, y_train = ...  # Your data

# Test pipeline step-by-step
test_pipeline_steps(pipeline, X_train, y_train)
```

## 📋 Debug Configurations Available

### 1. AutoTrading: Main Script
- Uses `subProcess: true` for subprocess debugging
- Runs with `n_jobs=4` (parallel)
- Best for: Production-like debugging

### 2. AutoTrading: Debug (No Parallel) ⭐ RECOMMENDED
- Uses `n_jobs=1` (sequential)
- `justMyCode: false` (step into libraries)
- Best for: Finding errors in transformers

### 3. AutoTrading: Debug Script
- Runs `debug_model_fit_error.py`
- Automated diagnostics
- Best for: Quick error identification

## 🛠️ Debugging Utilities

### Core Functions (`src/utils/pipeline_debugger.py`)

#### 1. `test_pipeline_steps(pipeline, X_train, y_train)`
Tests all steps in pipeline sequentially.

```python
test_pipeline_steps(pipeline, X_train, y_train)
```

#### 2. `inspect_dataframe(df, stage_name)`
Shows dtypes, non-numeric columns, NaN values, and sample data.

```python
inspect_dataframe(X_train, "Before Pipeline")
```

#### 3. `test_transformer_step(transformer, X, y, step_name)`
Tests a single transformer with detailed logging.

```python
X_new, y_new = test_transformer_step(
    transformer, X_train, y_train, "my_step"
)
```

#### 4. `find_non_numeric_columns(df)`
Returns dict of non-numeric columns with details.

```python
non_numeric = find_non_numeric_columns(X_train)
for col, info in non_numeric.items():
    print(f"{col}: {info['sample_values']}")
```

#### 5. `compare_dataframes(df1, df2, name1, name2)`
Compares two DataFrames showing differences.

```python
compare_dataframes(X_before, X_after, "Before", "After")
```

#### 6. `set_sequential_mode()` / `set_parallel_mode(n_jobs)`
Control parallel execution.

```python
set_sequential_mode()  # Force n_jobs=1
set_parallel_mode(4)   # Set n_jobs=4
```

## 🔍 How to Find Your Error

### Step 1: Run Debug Script

```bash
python debug_model_fit_error.py
```

Look for output like:
```
❌ ERROR OCCURRED IN STEP: company_metadata
⚠️  NON-NUMERIC COLUMNS: ['sector', 'industry']
  - sector: dtype=object, unique=['Technology', 'Finance', ...]
```

### Step 2: Identify the Problem

The output shows:
- **Which step** failed
- **Which columns** are non-numeric
- **Sample values** from those columns

### Step 3: Fix the Transformer

Common fixes:
- Add `OneHotEncoder` or `OrdinalEncoder` for categorical columns
- Check if transformer's output is properly encoded
- Ensure categorical transformers run before numeric ones

### Step 4: Test Again

Re-run the debug script to verify the fix.

## 📝 Common Issues & Solutions

### Issue 1: "could not convert string to float: 'Medium Low'"
**Cause**: A transformer is outputting categorical/string data  
**Solution**: Add proper encoding in that transformer

```python
# In your transformer
from sklearn.preprocessing import OneHotEncoder
encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
X_encoded = encoder.fit_transform(X[categorical_cols])
```

### Issue 2: CompanyMetadataTransformer outputs strings
**Cause**: Sector, industry, etc. are strings  
**Solution**: Ensure encoding happens in the transformer

```python
# In CompanyMetadataTransformer
self.categorical_cols = ['sector', 'industry', 'marketCap_category']
# Apply OneHotEncoder before returning
```

### Issue 3: Can't step into library code
**Cause**: `justMyCode: true` in debug config  
**Solution**: Use "Debug (No Parallel)" configuration

### Issue 4: Subprocess errors persist
**Cause**: Still using `n_jobs > 1`  
**Solution**: 
```bash
export SKLEARN_N_JOBS=1
```
Or use "Debug (No Parallel)" configuration

## 💡 Best Practices

1. **Always start debugging in sequential mode** (`n_jobs=1`)
2. **Check dtypes after each transformer** using `inspect_dataframe()`
3. **Use the debug script first** before setting breakpoints
4. **Test transformers individually** before testing the full pipeline
5. **Keep categorical encoders early** in the pipeline

## 📚 Documentation Files

- **`DEBUGGING_GUIDE.md`** - Comprehensive debugging guide with examples
- **`DEBUGGING_SUMMARY.md`** - Quick reference card
- **`example_debug_pipeline.py`** - 5 different debugging examples
- **`README_DEBUGGING.md`** - This file

## 🎯 Next Steps

1. Run `python debug_model_fit_error.py` to identify the error
2. Read the error output to find which step/column is problematic
3. Fix the transformer that outputs non-numeric data
4. Re-test to verify the fix
5. Switch back to parallel mode for production

## 📞 Quick Reference

```python
# Import utilities
from src.utils.pipeline_debugger import *

# Set sequential mode
set_sequential_mode()

# Test entire pipeline
test_pipeline_steps(pipeline, X_train, y_train)

# Inspect data
inspect_dataframe(X_train, "My Data")

# Find non-numeric columns
non_numeric = find_non_numeric_columns(X_train)

# Test single transformer
X_new, y_new = test_transformer_step(transformer, X, y, "step_name")

# Compare DataFrames
compare_dataframes(X_before, X_after, "Before", "After")
```

## ✨ Features

- ✅ No subprocess errors
- ✅ Step-by-step pipeline testing
- ✅ Automatic dtype checking
- ✅ Non-numeric column detection
- ✅ Detailed error logging
- ✅ DataFrame comparison
- ✅ Works with VS Code debugger
- ✅ Environment variable control
- ✅ Reusable utility functions

---

**Happy Debugging! 🐛🔨**


