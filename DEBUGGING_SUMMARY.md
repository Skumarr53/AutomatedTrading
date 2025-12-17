# Debugging Setup Complete ✅

## What Was Fixed

### 1. Subprocess Debugging Error
**Problem**: `Error patching args (debugger not attached to subprocess)`
- Caused by `n_jobs=4` in RandomizedSearchCV creating subprocesses

**Solutions Implemented**:
- ✅ Added `subProcess: true` to main debug configuration
- ✅ Created dedicated "Debug (No Parallel)" configuration with `n_jobs=1`
- ✅ Modified pipeline to respect `SKLEARN_N_JOBS` environment variable

## New Files Created

### 1. `.vscode/launch.json` (Updated)
Three debug configurations:
- **AutoTrading: Main Script** - With subprocess support
- **AutoTrading: Debug (No Parallel)** ⭐ - Sequential mode, best for debugging
- **AutoTrading: Debug Script** - For running debug_model_fit_error.py

### 2. `DEBUGGING_GUIDE.md`
Complete guide with:
- How to use each debug configuration
- Manual pipeline testing examples
- Common issues and solutions
- Tips and tricks

### 3. `src/utils/pipeline_debugger.py`
Utility functions for pipeline debugging:
- `inspect_dataframe()` - Inspect DataFrame properties
- `test_transformer_step()` - Test a single transformer
- `test_pipeline_steps()` - Test entire pipeline step-by-step
- `find_non_numeric_columns()` - Find problematic columns
- `compare_dataframes()` - Compare before/after transformations
- `set_sequential_mode()` - Force n_jobs=1
- `set_parallel_mode()` - Enable parallel execution

### 4. `debug_model_fit_error.py` (Updated)
- Now uses the new utility functions
- Automatically sets sequential mode
- Cleaner, more maintainable code

## How to Debug Your Error

### Quick Start (Recommended)

1. **In VS Code**:
   - Open Debug panel (Ctrl+Shift+D)
   - Select "AutoTrading: Debug Script"
   - Press F5

2. **The script will**:
   - Set `n_jobs=1` automatically
   - Test each pipeline step
   - Show you exactly where "could not convert string to float: 'Medium Low'" occurs
   - Display input/output dtypes for each transformer

### Manual Testing in Python

```python
from src.utils.pipeline_debugger import test_pipeline_steps, set_sequential_mode

# Force sequential mode
set_sequential_mode()

# Initialize your pipeline
ml_pipeline = MLPipelineBase()
pipeline = ml_pipeline.pipelines[0]

# Prepare your data
X_train, X_test, y_train, y_test = ml_pipeline.transform_and_split(...)

# Test each step
test_pipeline_steps(pipeline, X_train, y_train)
```

### Using Breakpoints

1. Select "AutoTrading: Debug (No Parallel)" configuration
2. Set breakpoints in:
   - `src/preprocessing/custom_transformers.py`
   - `src/preprocessing/company_metadata_transformer.py`
   - Your specific transformer's `fit()` or `transform()` method
3. Press F5 and step through code

## Key Changes to Your Code

### `src/pipelines/custom_pipelines.py`
```python
# Now respects SKLEARN_N_JOBS environment variable
import os
n_jobs = int(os.environ.get('SKLEARN_N_JOBS', 4))

self.model = RandomizedSearchCV(
    ...
    n_jobs=n_jobs,  # Can be controlled via environment variable
    ...
)
```

## Next Steps

1. **Run the debug script**:
   ```bash
   cd /home/skumar/DaatScience/AutomatedTrading
   python debug_model_fit_error.py
   ```

2. **Look for output like**:
   ```
   ❌ ERROR OCCURRED IN STEP: <step_name>
   ⚠️  NON-NUMERIC COLUMNS: ['column_name']
   ```

3. **Fix the transformer** that outputs non-numeric data

4. **Common fixes**:
   - Ensure categorical encoders are applied before numeric transformers
   - Check if CompanyMetadataTransformer is outputting strings
   - Verify OneHotEncoder is working correctly
   - Check for missing handling in custom transformers

## Useful Commands

```python
# In Python/IPython
from src.utils.pipeline_debugger import *

# Force sequential mode
set_sequential_mode()

# Inspect a DataFrame
inspect_dataframe(df, "My DataFrame")

# Find non-numeric columns
non_numeric = find_non_numeric_columns(df)
print(non_numeric)

# Compare before/after
compare_dataframes(df_before, df_after, "Before Transform", "After Transform")

# Test pipeline
test_pipeline_steps(pipeline, X_train, y_train)
```

## Tips

1. Always use "Debug (No Parallel)" when investigating errors
2. Start with the debug script before setting breakpoints
3. Check dtypes after each transformer
4. Look for transformers that output pd.DataFrame with object dtypes
5. Verify categorical encoders run before the final estimator

## Support Files

- `DEBUGGING_GUIDE.md` - Detailed debugging guide
- `src/utils/pipeline_debugger.py` - Utility functions
- `debug_model_fit_error.py` - Automated diagnostic script


