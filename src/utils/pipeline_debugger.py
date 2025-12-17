"""
Utility functions for debugging ML pipelines.

Helps identify issues with transformers, especially non-numeric data
that causes "could not convert string to float" errors.
"""

import pandas as pd
import numpy as np
from typing import Any, Tuple, Optional
from loguru import logger


def inspect_dataframe(df: pd.DataFrame, stage_name: str, show_rows: int = 3) -> None:
    """
    Inspect and log DataFrame properties.
    
    Args:
        df: DataFrame to inspect
        stage_name: Name of the current stage for logging
        show_rows: Number of rows to display (default: 3)
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"INSPECTING: {stage_name}")
    logger.info(f"{'='*80}")
    logger.info(f"Shape: {df.shape}")
    
    # Check for non-numeric columns
    non_numeric_cols = df.select_dtypes(exclude=['number']).columns.tolist()
    if non_numeric_cols:
        logger.warning(f"⚠️  NON-NUMERIC COLUMNS: {non_numeric_cols}")
        for col in non_numeric_cols:
            unique_vals = df[col].unique()[:10]
            logger.warning(f"  - {col}: dtype={df[col].dtype}, unique={unique_vals}")
    else:
        logger.success("✓ All columns are numeric")
    
    # Check for NaN values
    nan_counts = df.isna().sum()
    nan_cols = nan_counts[nan_counts > 0]
    if not nan_cols.empty:
        logger.warning(f"⚠️  NaN VALUES: {len(nan_cols)} columns")
        for col, count in nan_cols.items():
            logger.warning(f"  - {col}: {count} NaN values ({count/len(df)*100:.1f}%)")
    else:
        logger.success("✓ No NaN values")
    
    # Show sample
    logger.info(f"\nFirst {show_rows} rows:")
    logger.info(f"\n{df.head(show_rows)}")
    logger.info(f"{'='*80}\n")


def test_transformer_step(
    transformer: Any,
    X: pd.DataFrame,
    y: pd.Series,
    step_name: str
) -> Tuple[Any, pd.Series]:
    """
    Test a single transformer and inspect its output.
    
    Args:
        transformer: The transformer to test
        X: Input features
        y: Target variable
        step_name: Name of the step for logging
        
    Returns:
        Tuple of (transformed_X, transformed_y)
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"TESTING STEP: {step_name}")
    logger.info(f"Transformer: {transformer.__class__.__name__}")
    logger.info(f"{'='*80}")
    
    # Inspect input
    if isinstance(X, pd.DataFrame):
        inspect_dataframe(X, f"Input to {step_name}")
    else:
        logger.info(f"Input type: {type(X)}, shape: {X.shape}")
    
    try:
        # Transform
        if hasattr(transformer, 'fit_transform'):
            # Check if it's a resampler (returns X and y)
            result = transformer.fit_transform(X, y)
            if isinstance(result, tuple):
                X_out, y_out = result
                logger.success(f"✓ Resampler completed: X {X.shape} → {X_out.shape}")
            else:
                X_out = result
                y_out = y
                logger.success(f"✓ Transform completed: {X.shape} → {X_out.shape}")
        else:
            transformer.fit(X, y)
            X_out = transformer.transform(X)
            y_out = y
            logger.success(f"✓ Fit+Transform completed: {X.shape} → {X_out.shape}")
        
        # Inspect output
        if isinstance(X_out, pd.DataFrame):
            inspect_dataframe(X_out, f"Output from {step_name}")
        elif isinstance(X_out, np.ndarray):
            logger.info(f"Output: numpy array, shape={X_out.shape}, dtype={X_out.dtype}")
            if not np.issubdtype(X_out.dtype, np.number):
                logger.error(f"❌ Output is not numeric! dtype: {X_out.dtype}")
        
        return X_out, y_out
        
    except Exception as e:
        logger.error(f"❌ ERROR in {step_name}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        raise


def test_pipeline_steps(pipeline: Any, X: pd.DataFrame, y: pd.Series) -> None:
    """
    Test all steps in a pipeline sequentially.
    
    Args:
        pipeline: CustomModelPipeline object
        X: Training features
        y: Training target
    """
    # Get the sklearn pipeline
    if hasattr(pipeline, 'model') and hasattr(pipeline.model, 'estimator'):
        sklearn_pipeline = pipeline.model.estimator
    elif hasattr(pipeline, 'pipeline'):
        sklearn_pipeline = pipeline.pipeline
    else:
        raise ValueError("Cannot find sklearn pipeline in the provided object")
    
    logger.info("\n" + "="*80)
    logger.info("PIPELINE STEPS:")
    for i, (name, _) in enumerate(sklearn_pipeline.steps):
        logger.info(f"  {i+1}. {name}")
    logger.info("="*80)
    
    X_current = X.copy()
    y_current = y.copy()
    
    # Test each step except the final estimator
    for step_idx, (step_name, transformer) in enumerate(sklearn_pipeline.steps[:-1]):
        X_current, y_current = test_transformer_step(
            transformer, X_current, y_current, step_name
        )
    
    # Test final estimator
    final_name, final_estimator = sklearn_pipeline.steps[-1]
    logger.info(f"\n{'='*80}")
    logger.info(f"TESTING FINAL ESTIMATOR: {final_name}")
    logger.info(f"{'='*80}")
    
    if isinstance(X_current, pd.DataFrame):
        inspect_dataframe(X_current, f"Input to {final_name}")
    
    try:
        final_estimator.fit(X_current, y_current)
        logger.success(f"✓ Final estimator fitted successfully!")
    except Exception as e:
        logger.error(f"❌ ERROR in final estimator")
        logger.error(f"Error: {str(e)}")
        raise


def find_non_numeric_columns(df: pd.DataFrame) -> dict:
    """
    Find all non-numeric columns and their unique values.
    
    Args:
        df: DataFrame to check
        
    Returns:
        Dict mapping column names to their unique values
    """
    non_numeric_cols = df.select_dtypes(exclude=['number']).columns
    result = {}
    
    for col in non_numeric_cols:
        result[col] = {
            'dtype': str(df[col].dtype),
            'unique_count': df[col].nunique(),
            'sample_values': df[col].unique()[:20].tolist(),
            'null_count': df[col].isna().sum()
        }
    
    return result


def compare_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    name1: str = "Before",
    name2: str = "After"
) -> None:
    """
    Compare two DataFrames and log differences.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        name1: Name for first DataFrame
        name2: Name for second DataFrame
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"COMPARING: {name1} vs {name2}")
    logger.info(f"{'='*80}")
    
    # Shape comparison
    logger.info(f"Shape: {df1.shape} → {df2.shape}")
    if df1.shape != df2.shape:
        rows_diff = df2.shape[0] - df1.shape[0]
        cols_diff = df2.shape[1] - df1.shape[1]
        logger.info(f"  Rows: {rows_diff:+d}")
        logger.info(f"  Columns: {cols_diff:+d}")
    
    # Column comparison
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    
    added_cols = cols2 - cols1
    removed_cols = cols1 - cols2
    common_cols = cols1 & cols2
    
    if added_cols:
        logger.info(f"✓ Added columns ({len(added_cols)}): {list(added_cols)}")
    if removed_cols:
        logger.warning(f"⚠️  Removed columns ({len(removed_cols)}): {list(removed_cols)}")
    
    # Dtype changes
    dtype_changes = []
    for col in common_cols:
        if df1[col].dtype != df2[col].dtype:
            dtype_changes.append(f"  - {col}: {df1[col].dtype} → {df2[col].dtype}")
    
    if dtype_changes:
        logger.info("Dtype changes:")
        for change in dtype_changes:
            logger.info(change)
    
    logger.info(f"{'='*80}\n")


def set_sequential_mode() -> None:
    """
    Set environment variable to force sequential execution in sklearn.
    Useful for debugging.
    """
    import os
    os.environ['SKLEARN_N_JOBS'] = '1'
    logger.info("✓ Set SKLEARN_N_JOBS=1 (sequential mode for debugging)")


def set_parallel_mode(n_jobs: int = 4) -> None:
    """
    Set environment variable for parallel execution in sklearn.
    
    Args:
        n_jobs: Number of parallel jobs (default: 4)
    """
    import os
    os.environ['SKLEARN_N_JOBS'] = str(n_jobs)
    logger.info(f"✓ Set SKLEARN_N_JOBS={n_jobs} (parallel mode)")


