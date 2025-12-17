# Debugging "ValueError: could not convert string to float" Error

## Problem
During model fitting with `pipeline.model.fit(X_train, y_train)`, you're encountering:
```
ValueError: could not convert string to float: 'Medium Low'
```

This error occurs in parallel jobs (via joblib), making it hard to debug with a debugger.

## Root Cause
The error indicates that **string values** (like `'Medium Low'`) are being passed to the final estimator (GradientBoostingClassifier), which expects only numeric data. This means:

1. **Categorical columns are not being properly encoded** before reaching the model
2. **The preprocessing pipeline is missing or failing** to convert categorical features to numeric
3. **The `CategoricalPreprocessor` step might not be included or is failing silently**

## Diagnostic Steps

### Step 1: Run the Isolation Test
This tests each preprocessing component independently:

```bash
cd /home/skumar/DaatScience/AutomatedTrading
source .venv/bin/activate
python test_preprocessing_isolation.py
```

**What it tests:**
- Configuration columns are properly defined
- `CategoricalPreprocessor` works correctly
- `DFFeatureUnion` combines features correctly
- Full preprocessing chain produces only numeric output

### Step 2: Run the Full Diagnostic Script
This tests your actual pipeline with real data:

```bash
python debug_model_fit_error.py
```

**What it does:**
- Loads real data from your data sources
- Runs each pipeline step sequentially
- Checks dtypes after each transformation
- Identifies which step leaves string values
- Shows exactly which columns contain non-numeric values

### Step 3: Check Your Pipeline Configuration

Check `src/config/custom.yaml` or wherever your pipeline is configured:

```yaml
pipeline_configs:
  your_pipeline_name:
    cat_encode: true  # ← Make sure this is TRUE
    # ... other settings
```

**If `cat_encode: false`**, categorical columns won't be encoded!

## Common Issues & Solutions

### Issue 1: Categorical Encoding Disabled
**Symptom:** `cat_encode: false` in config

**Solution:** Enable categorical encoding:
```yaml
pipeline_configs:
  main_pipeline:
    cat_encode: true
```

### Issue 2: Categorical Columns Not Defined
**Symptom:** `config.columns.cat_cols` is empty or incorrect

**Solution:** Check `src/config/columns/common_column_defs.yaml`:
```yaml
cat_cols:
  - trend
  - signal
  - sector
  - any_other_categorical_column
```

### Issue 3: CategoricalPreprocessor Not in Pipeline
**Symptom:** Pipeline doesn't include `CategoricalPreprocessor` step

**Solution:** In `custom_pipelines.py`, ensure the pipeline includes categorical preprocessing:
```python
def get_prepocessed_categorical_features(self):
    cat_feature_transformers = []
    cat_feature_transformers.append(('cat_extract', ColumnExtractor(self.cat_cols)))
    
    # THIS MUST BE TRUE to encode categoricals:
    if self.feature_config.get('cat_encode', False):
        cat_feature_transformers.append(('cat_normalize', CategoricalPreprocessor(self.cat_cols)))
    
    return cat_feature_transformers
```

### Issue 4: New Categorical Column Not Recognized
**Symptom:** A column that should be numeric is actually categorical, or vice versa

**Solution:** 
1. Identify the column with `'Medium Low'` value
2. Add it to `cat_cols` in your column definitions
3. Ensure it's included in the preprocessing

## Quick Fix: Add Pre-Fit Validation

Add this validation **before** `pipeline.model.fit()` in `base_pipeline.py`:

```python
# In base_pipeline.py, line ~343, BEFORE pipeline.model.fit(X_train, y_train)

# Validation check
non_numeric_cols = X_train.select_dtypes(exclude=['number']).columns.tolist()
if non_numeric_cols:
    logger.error(f"Non-numeric columns found before model fit: {non_numeric_cols}")
    for col in non_numeric_cols:
        logger.error(f"  {col}: dtype={X_train[col].dtype}, "
                    f"sample_values={X_train[col].unique()[:10]}")
    raise ValueError(
        f"Cannot fit model with non-numeric columns: {non_numeric_cols}. "
        f"Check your preprocessing pipeline configuration."
    )

# Validation check for NaN/Inf
if X_train.isna().any().any():
    nan_cols = X_train.columns[X_train.isna().any()].tolist()
    logger.warning(f"NaN values found in columns: {nan_cols}")

if np.isinf(X_train.select_dtypes(include=['number']).values).any():
    logger.warning("Infinite values found in training data")

logger.info(f"Validation passed: X_train shape={X_train.shape}, all numeric, "
           f"no NaN/Inf values")

# NOW fit the model
pipeline.model.fit(X_train, y_train)
```

## Debugging with Sequential Execution

To avoid parallel execution (which hides errors), modify your `RandomizedSearchCV` parameters:

In `custom_pipelines.py`, line ~202:

```python
self.model = RandomizedSearchCV(
    self.pipeline,
    param_distributions=self.params,
    n_iter=20,
    scoring='accuracy',
    n_jobs=1,  # ← Change from 4 to 1 for sequential execution
    cv=3,
    random_state=42,
    verbose=3,  # ← Increase verbosity to see more details
    return_train_score=True,
    error_score='raise'
)
```

This makes debugging easier because:
- Errors occur in the main process (debugger can catch them)
- Stack traces are clearer
- You can add breakpoints

## Expected Output After Fix

After properly encoding categoricals, you should see:

```
✓ Validation passed: X_train shape=(1000, 50), all numeric, no NaN/Inf values
Fitting 3 folds for each of 20 candidates, totalling 60 fits
[CV] END ...model_fit__max_depth=10, model_fit__n_estimators=100; total time=   2.1s
...
```

## Additional Resources

1. **Check column definitions:** `src/config/columns/common_column_defs.yaml`
2. **Check pipeline config:** `src/config/custom.yaml` (or trading.yaml)
3. **Check preprocessing:** `src/preprocessing/custom_transformers.py`
4. **Check pipeline setup:** `src/pipelines/custom_pipelines.py`

## Still Stuck?

Run both diagnostic scripts and share the output:
```bash
python test_preprocessing_isolation.py > isolation_test.log 2>&1
python debug_model_fit_error.py > full_diagnostic.log 2>&1
```

The logs will show:
- Which preprocessing step fails
- Which columns contain string values
- What the pipeline configuration is
- Sample data at each transformation step


