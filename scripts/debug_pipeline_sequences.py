#!/usr/bin/env python3
"""
Debug script to identify which transformer is creating sequences in DataFrame columns.

Usage:
    python scripts/debug_pipeline_sequences.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import numpy as np
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import config
from src.pipelines.base_pipeline import MLPipelineBase


def check_for_sequences(df: pd.DataFrame, name: str = "DataFrame") -> list[str]:
    """
    Check DataFrame for columns containing sequences (lists/arrays).
    
    Returns:
        List of column names that contain sequences
    """
    sequence_cols = []
    
    for col in df.columns:
        try:
            # Sample values
            sample = df[col].dropna().head(20)
            if len(sample) == 0:
                continue
            
            for idx, val in sample.items():
                # Check if value is a sequence (but not string)
                if isinstance(val, (list, tuple, np.ndarray)) and not isinstance(val, str):
                    sequence_cols.append(col)
                    logger.error(
                        f"❌ Column '{col}' contains sequences in {name}"
                    )
                    logger.error(f"   Row {idx}: type={type(val)}, value={val}")
                    logger.error(f"   Sequence length: {len(val) if hasattr(val, '__len__') else 'N/A'}")
                    break
                
                # Check if dtype is object and might contain sequences
                elif df[col].dtype == 'object':
                    try:
                        import ast
                        if isinstance(val, str) and (val.startswith('[') or val.startswith('(')):
                            parsed = ast.literal_eval(val)
                            if isinstance(parsed, (list, tuple, np.ndarray)):
                                sequence_cols.append(col)
                                logger.error(
                                    f"❌ Column '{col}' contains string representations of sequences"
                                )
                                logger.error(f"   Row {idx}: value={val}")
                                break
                    except (ValueError, SyntaxError):
                        pass
        except Exception as e:
            logger.debug(f"Error checking column {col}: {e}")
    
    return sequence_cols


def inspect_dataframe_structure(df: pd.DataFrame, name: str) -> None:
    """Inspect DataFrame structure in detail."""
    logger.info(f"\n{'='*80}")
    logger.info(f"INSPECTING: {name}")
    logger.info(f"{'='*80}")
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Columns: {len(df.columns)}")
    
    if len(df.columns) > 50:
        logger.warning(f"⚠️  Very large number of columns: {len(df.columns)}")
        logger.info(f"First 20 columns: {list(df.columns[:20])}")
        logger.info(f"Last 20 columns: {list(df.columns[-20])}")
    else:
        logger.info(f"All columns: {list(df.columns)}")
    
    # Check dtypes
    logger.info(f"\nDtype summary:")
    dtype_counts = df.dtypes.value_counts()
    for dtype, count in dtype_counts.items():
        logger.info(f"  {dtype}: {count} columns")
    
    # Check for sequences
    sequence_cols = check_for_sequences(df, name)
    if sequence_cols:
        logger.error(f"\n❌ Found {len(sequence_cols)} columns with sequences:")
        for col in sequence_cols[:10]:
            logger.error(f"  - {col}")
    else:
        logger.success("\n✓ No sequences found (all scalar values)")
    
    # Try to convert to numpy array
    logger.info(f"\nTesting numpy array conversion...")
    try:
        test_array = np.asarray(df.head(100))
        logger.success(f"✓ Successfully converted to array: shape={test_array.shape}")
    except ValueError as e:
        logger.error(f"❌ Failed to convert to array: {e}")
        logger.error("This will cause sklearn to fail!")
    
    logger.info(f"{'='*80}\n")


def main():
    """Main debugging function."""
    logger.info("=" * 80)
    logger.info("Pipeline Sequence Debugging Tool")
    logger.info("=" * 80)
    
    # This is a template - you'll need to provide actual data
    logger.info("\n💡 To use this script:")
    logger.info("1. Set breakpoint in pipeline.transform() or pipeline.fit()")
    logger.info("2. Call inspect_dataframe_structure(X, 'name') at each step")
    logger.info("3. Check which transformer creates sequences")
    
    logger.info("\nExample usage in debugger:")
    logger.info("  from scripts.debug_pipeline_sequences import inspect_dataframe_structure")
    logger.info("  inspect_dataframe_structure(X_train, 'X_train_before_pipeline')")
    logger.info("  # ... run pipeline step ...")
    logger.info("  inspect_dataframe_structure(X_transformed, 'X_train_after_transformer')")


if __name__ == "__main__":
    main()
