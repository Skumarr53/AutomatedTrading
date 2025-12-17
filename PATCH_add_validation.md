# Patch: Add Validation to base_pipeline.py

## Location
File: `src/pipelines/base_pipeline.py`
Line: ~343 (before `pipeline.model.fit(X_train, y_train)`)

## What to Add

Replace this line:
```python
pipeline.model.fit(X_train, y_train)
```

With this block:

```python
# ============================================================================
# VALIDATION: Check data before model fitting
# ============================================================================
from src.utils.data_validation import validate_data_for_model_fit

logger.info(f"Validating data before model fit for {symbol} {run_id} {target}...")

# Validate X_train
try:
    validate_data_for_model_fit(
        X_train, 
        y_train, 
        stage_name=f"{symbol}_{run_id}_{target}_pre_fit",
        raise_on_error=True
    )
except ValueError as validation_error:
    logger.error(f"Data validation failed: {validation_error}")
    
    # Additional debugging: try to find 'Medium Low' or other string values
    from src.utils.data_validation import find_column_with_value
    
    # Common problematic values
    test_values = ['Medium Low', 'Medium High', 'High', 'Low', 'Medium']
    for test_val in test_values:
        cols = find_column_with_value(X_train, test_val, case_sensitive=False)
        if cols:
            logger.error(f"Found '{test_val}' in columns: {cols}")
    
    # Re-raise the error
    raise

# ============================================================================
# Model fitting
# ============================================================================
logger.info(f"Starting model fit for {symbol} {run_id} {target}...")
pipeline.model.fit(X_train, y_train)
logger.success(f"Model fit completed for {symbol} {run_id} {target}")
```

## Alternative: Minimal Validation

If you want a simpler version without creating the validation utility:

```python
# Quick validation before model fit
non_numeric_cols = X_train.select_dtypes(exclude=['number']).columns.tolist()
if non_numeric_cols:
    logger.error(f"❌ Non-numeric columns found: {non_numeric_cols}")
    for col in non_numeric_cols:
        logger.error(f"   {col}: {X_train[col].dtype}, values: {X_train[col].unique()[:10]}")
    raise ValueError(f"Cannot fit model with non-numeric columns: {non_numeric_cols}")

logger.info(f"✓ Validation passed: all columns numeric, shape={X_train.shape}")

# Now fit
pipeline.model.fit(X_train, y_train)
```

## How to Apply

### Option 1: Using the full validation utility
1. Make sure `src/utils/data_validation.py` exists (already created above)
2. Add the import at the top of `base_pipeline.py`:
   ```python
   from src.utils.data_validation import validate_data_for_model_fit, find_column_with_value
   ```
3. Replace line 343 with the full validation block above

### Option 2: Minimal inline validation
1. Just replace line 343 with the minimal validation block above
2. No additional imports needed

## What This Does

When you run your code, instead of getting a cryptic error from joblib:
```
ValueError: could not convert string to float: 'Medium Low'
```

You'll get a clear diagnostic message:
```
❌ Non-numeric columns found: ['trend_category', 'signal_strength']
   trend_category: object, values: ['High' 'Medium High' 'Medium' 'Medium Low' 'Low']
   signal_strength: object, values: ['Strong' 'Weak' 'Neutral']
ValueError: Cannot fit model with non-numeric columns: ['trend_category', 'signal_strength']
```

This tells you EXACTLY which columns need to be added to your categorical preprocessing!

## After Applying the Patch

1. Run your training code
2. If validation fails, you'll see which columns have string values
3. Add those columns to `config.columns.cat_cols`
4. Ensure `cat_encode: true` in your pipeline config
5. Re-run

## Full Example

Here's how the modified `_train_single` method should look:

```python
def _train_single(self, pipeline: CustomModelPipeline, run_id: str, target: str, X: pd.DataFrame, symbol: str) -> None:
    try:
        logger.info(
            f"Training model for {symbol} {run_id} {target} with config: {pp.pformat(pipeline.params)}"
        )
        with mlflow.start_run(run_name=f"{symbol}_{run_id}_{target}_{self.model_id}"):
            mlflow.set_tag("mode", self.mode)
            mlflow.set_tag("model_id", self.model_id)
            mlflow.log_param("symbol", symbol)
            mlflow.log_param("run_id", run_id) 
            mlflow.log_param("target", target)
            
            X_train, X_test, y_train, y_test = self.transform_and_split(X, target, run_id)

            if y_train is None:
                logger.warning(f"Target {target} could not be prepared for {symbol} {run_id}")
                return

            if pipeline.model is None:
                raise ValueError("Model has not been defined. Call setup() before running.")

            # ============= VALIDATION BLOCK (NEW) =============
            from src.utils.data_validation import validate_data_for_model_fit
            
            logger.info(f"Validating data before model fit...")
            validate_data_for_model_fit(
                X_train, 
                y_train, 
                stage_name=f"{symbol}_{run_id}_{target}_pre_fit",
                raise_on_error=True
            )
            # ==================================================

            logger.info(f"Starting model fit...")
            pipeline.model.fit(X_train, y_train)
            
            mlflow.log_params(pipeline.model.best_params_)
            y_pred = pipeline.model.predict(X_test)

            log_model_performance(y_test, y_pred, pipeline.model.best_estimator_, X_test)

            registered_model_name = f"{symbol}_{run_id}_{target}"
            mlflow.sklearn.log_model(
                sk_model=pipeline.model.best_estimator_,
                artifact_path="model",
                registered_model_name=registered_model_name,
            )

            if symbol not in self.best_model_dict:
                self.best_model_dict[symbol] = {}
            if run_id not in self.best_model_dict[symbol]:
                self.best_model_dict[symbol][run_id] = {}
            self.best_model_dict[symbol][run_id][target] = clone(pipeline.model.best_estimator_)
    except Exception as e:
        logger.error(f"Training failed: {str(e)}")
        traceback.print_exc()
        raise
```

## Testing

After applying the patch, run:
```bash
python main.py
```

If there are any non-numeric columns, you'll immediately see which ones before the model tries to fit!


