# Quick Fix Guide: String to Float Error

## 🚨 The Error
```
ValueError: could not convert string to float: 'Medium Low'
```

## ⚡ Quick Fix (2 minutes)

### Step 1: Add Validation to Catch the Error Early
In `src/pipelines/base_pipeline.py`, line 343, **REPLACE:**
```python
pipeline.model.fit(X_train, y_train)
```

**WITH:**
```python
# Validation
non_numeric = X_train.select_dtypes(exclude=['number']).columns.tolist()
if non_numeric:
    logger.error(f"❌ Non-numeric columns: {non_numeric}")
    for col in non_numeric:
        logger.error(f"  {col}: {X_train[col].unique()[:10]}")
    raise ValueError(f"Cannot fit model with non-numeric columns: {non_numeric}")

# Now fit
pipeline.model.fit(X_train, y_train)
```

### Step 2: Run Your Code
```bash
python main.py
```

### Step 3: You'll See the Problem
```
❌ Non-numeric columns: ['trend_category', 'market_sentiment']
  trend_category: ['High' 'Medium High' 'Medium' 'Medium Low' 'Low']
  market_sentiment: ['Bullish' 'Bearish' 'Neutral']
```

### Step 4: Fix the Configuration

#### Option A: Add to cat_cols
Edit `src/config/columns/common_column_defs.yaml`:
```yaml
cat_cols:
  - trend_category      # ← Add these
  - market_sentiment    # ← Add these
  - any_other_string_column
```

#### Option B: Enable Categorical Encoding
Edit `src/config/custom.yaml` (or wherever your pipeline config is):
```yaml
pipeline_configs:
  main_pipeline:
    cat_encode: true  # ← Make sure this is true!
```

### Step 5: Re-run
```bash
python main.py
```

Should work now! ✅

## 🔍 Need More Details?

### Run Full Diagnostics
```bash
# Test preprocessing
python test_preprocessing_isolation.py

# Test with real data
python debug_model_fit_error.py
```

### Read Full Guide
See `SOLUTION_SUMMARY.md` for complete documentation.

## 💡 Quick Tips

1. **Disable parallel execution during debugging:**
   In `src/pipelines/custom_pipelines.py` line ~207:
   ```python
   n_jobs=1,  # Change from 4 to 1
   ```

2. **Use fewer iterations for faster testing:**
   ```python
   n_iter=2,  # Change from 20 to 2
   cv=2,      # Change from 3 to 2
   ```

3. **Test with one symbol first:**
   In your config:
   ```python
   symbols = ['AAPL']  # Just one
   ```

## ✅ Success Looks Like
```
✓ Validation passed: all columns numeric, shape=(1000, 50)
Fitting 3 folds for each of 20 candidates, totalling 60 fits
[CV] END ...max_depth=10, n_estimators=100; total time=2.1s
✓ Model fit completed
```

## 🆘 Still Not Working?

1. Check if `CategoricalPreprocessor` is imported:
   ```python
   from src.preprocessing.custom_transformers import CategoricalPreprocessor
   ```

2. Check if categorical columns are actually being processed:
   In `src/pipelines/custom_pipelines.py`, look for:
   ```python
   if self.feature_config.get('cat_encode', False):  # Must be True!
       cat_feature_transformers.append(('cat_normalize', CategoricalPreprocessor(self.cat_cols)))
   ```

3. Print your pipeline config to verify:
   ```python
   logger.info(f"Pipeline config: {self.feature_config}")
   logger.info(f"Categorical columns: {self.cat_cols}")
   logger.info(f"cat_encode enabled: {self.feature_config.get('cat_encode', False)}")
   ```

---

**Time to fix:** 2-5 minutes  
**Difficulty:** Easy  
**Files to edit:** 1-2 files


