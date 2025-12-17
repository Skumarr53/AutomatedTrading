"""
Data validation utilities for debugging preprocessing and model fitting issues.
"""

import pandas as pd
import numpy as np
from loguru import logger
from typing import Optional, Tuple, List


def validate_data_for_model_fit(
    X: pd.DataFrame,
    y: Optional[pd.Series] = None,
    stage_name: str = "pre-fit",
    raise_on_error: bool = True
) -> Tuple[bool, List[str]]:
    """
    Validate that data is ready for model fitting.
    
    Checks:
    - All columns are numeric
    - No NaN values (or logs warning)
    - No infinite values
    - Reasonable data ranges
    
    Args:
        X: Feature DataFrame
        y: Target Series (optional)
        stage_name: Name of the stage for logging
        raise_on_error: If True, raises ValueError on validation failure
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    logger.info(f"\n{'='*80}")
    logger.info(f"VALIDATION: {stage_name}")
    logger.info(f"{'='*80}")
    logger.info(f"Shape: {X.shape}")
    
    # Check 1: All columns are numeric
    non_numeric_cols = X.select_dtypes(exclude=['number']).columns.tolist()
    if non_numeric_cols:
        error_msg = f"Non-numeric columns found: {non_numeric_cols}"
        errors.append(error_msg)
        logger.error(f"❌ {error_msg}")
        
        # Show sample values from non-numeric columns
        for col in non_numeric_cols:
            unique_vals = X[col].unique()[:10]
            logger.error(f"   Column '{col}': dtype={X[col].dtype}, sample_values={unique_vals}")
    else:
        logger.success("✓ All columns are numeric")
    
    # Check 2: NaN values
    nan_cols = X.columns[X.isna().any()].tolist()
    if nan_cols:
        nan_counts = X[nan_cols].isna().sum()
        logger.warning(f"⚠️  NaN values found in {len(nan_cols)} columns:")
        for col in nan_cols:
            logger.warning(f"   {col}: {nan_counts[col]} NaN values ({nan_counts[col]/len(X)*100:.2f}%)")
        
        # This is a warning, not an error (some models can handle NaN)
        # errors.append(f"NaN values found in columns: {nan_cols}")
    else:
        logger.success("✓ No NaN values")
    
    # Check 3: Infinite values
    numeric_cols = X.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        inf_mask = np.isinf(X[numeric_cols].values)
        if inf_mask.any():
            inf_cols = numeric_cols[np.isinf(X[numeric_cols].values).any(axis=0)].tolist()
            error_msg = f"Infinite values found in columns: {inf_cols}"
            errors.append(error_msg)
            logger.error(f"❌ {error_msg}")
        else:
            logger.success("✓ No infinite values")
    
    # Check 4: Data type consistency
    logger.info(f"\nData types summary:")
    dtype_counts = X.dtypes.value_counts()
    for dtype, count in dtype_counts.items():
        logger.info(f"   {dtype}: {count} columns")
    
    # Check 5: Target validation (if provided)
    if y is not None:
        logger.info(f"\nTarget validation:")
        logger.info(f"   Shape: {y.shape}")
        logger.info(f"   Dtype: {y.dtype}")
        logger.info(f"   Unique values: {y.unique()}")
        logger.info(f"   Value counts:\n{y.value_counts()}")
        
        if y.isna().any():
            logger.error(f"❌ Target contains {y.isna().sum()} NaN values")
            errors.append("Target contains NaN values")
    
    # Summary
    is_valid = len(errors) == 0
    
    if is_valid:
        logger.success(f"\n✓ Validation passed for '{stage_name}'")
    else:
        logger.error(f"\n❌ Validation failed for '{stage_name}' with {len(errors)} errors:")
        for i, error in enumerate(errors, 1):
            logger.error(f"   {i}. {error}")
    
    logger.info(f"{'='*80}\n")
    
    if raise_on_error and not is_valid:
        raise ValueError(
            f"Data validation failed at '{stage_name}' with errors: {'; '.join(errors)}"
        )
    
    return is_valid, errors


def inspect_dataframe(
    df: pd.DataFrame,
    name: str = "DataFrame",
    n_samples: int = 5,
    show_stats: bool = True
) -> None:
    """
    Print detailed information about a DataFrame for debugging.
    
    Args:
        df: DataFrame to inspect
        name: Name for logging
        n_samples: Number of sample rows to show
        show_stats: Whether to show statistical summary
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"INSPECT: {name}")
    logger.info(f"{'='*80}")
    
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    logger.info(f"\nColumn types:")
    logger.info(f"{df.dtypes}")
    
    logger.info(f"\nFirst {n_samples} rows:")
    logger.info(f"\n{df.head(n_samples)}")
    
    if show_stats:
        logger.info(f"\nStatistical summary:")
        logger.info(f"\n{df.describe()}")
    
    # Check for potential issues
    logger.info(f"\nData quality checks:")
    
    # Missing values
    missing = df.isna().sum()
    if missing.any():
        logger.warning(f"  Missing values:")
        for col, count in missing[missing > 0].items():
            logger.warning(f"    {col}: {count} ({count/len(df)*100:.2f}%)")
    else:
        logger.success("  ✓ No missing values")
    
    # Non-numeric columns
    non_numeric = df.select_dtypes(exclude=['number']).columns.tolist()
    if non_numeric:
        logger.warning(f"  Non-numeric columns: {non_numeric}")
        for col in non_numeric:
            logger.warning(f"    {col}: {df[col].dtype}, unique={df[col].nunique()}")
    else:
        logger.success("  ✓ All columns numeric")
    
    logger.info(f"{'='*80}\n")


def log_pipeline_step(
    step_name: str,
    X_before: pd.DataFrame,
    X_after: pd.DataFrame,
    y_before: Optional[pd.Series] = None,
    y_after: Optional[pd.Series] = None
) -> None:
    """
    Log information about a pipeline transformation step.
    
    Args:
        step_name: Name of the pipeline step
        X_before: Features before transformation
        X_after: Features after transformation
        y_before: Target before transformation (optional)
        y_after: Target after transformation (optional)
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"PIPELINE STEP: {step_name}")
    logger.info(f"{'='*80}")
    
    logger.info(f"X shape: {X_before.shape} → {X_after.shape}")
    
    if isinstance(X_before, pd.DataFrame) and isinstance(X_after, pd.DataFrame):
        # Check column changes
        cols_before = set(X_before.columns)
        cols_after = set(X_after.columns)
        
        added = cols_after - cols_before
        removed = cols_before - cols_after
        
        if added:
            logger.info(f"  Columns added ({len(added)}): {list(added)[:10]}")
        if removed:
            logger.info(f"  Columns removed ({len(removed)}): {list(removed)[:10]}")
        
        # Check dtype changes
        dtype_before = X_before.dtypes.value_counts().to_dict()
        dtype_after = X_after.dtypes.value_counts().to_dict()
        
        logger.info(f"  Dtypes before: {dtype_before}")
        logger.info(f"  Dtypes after: {dtype_after}")
    
    if y_before is not None and y_after is not None:
        logger.info(f"y shape: {y_before.shape} → {y_after.shape}")
        if hasattr(y_before, 'unique') and hasattr(y_after, 'unique'):
            logger.info(f"  y unique values: {len(y_before.unique())} → {len(y_after.unique())}")
    
    logger.info(f"{'='*80}\n")


def find_column_with_value(
    df: pd.DataFrame,
    value: str,
    case_sensitive: bool = False
) -> List[str]:
    """
    Find which column(s) contain a specific value.
    
    Useful for finding which column has 'Medium Low' or other problematic values.
    
    Args:
        df: DataFrame to search
        value: Value to search for
        case_sensitive: Whether to do case-sensitive search
        
    Returns:
        List of column names containing the value
    """
    matching_cols = []
    
    for col in df.columns:
        try:
            if case_sensitive:
                mask = df[col].astype(str) == value
            else:
                mask = df[col].astype(str).str.lower() == value.lower()
            
            if mask.any():
                count = mask.sum()
                matching_cols.append(col)
                logger.info(f"Found '{value}' in column '{col}': {count} occurrences")
                logger.info(f"  Column dtype: {df[col].dtype}")
                logger.info(f"  Unique values: {df[col].unique()[:20]}")
        except Exception as e:
            # Skip columns that can't be converted to string
            pass
    
    return matching_cols


