"""
Simplified test script to isolate preprocessing issues.
Tests each preprocessing step independently without the full pipeline.
"""

import sys
import pandas as pd
import numpy as np
from loguru import logger
from src import config
from src.preprocessing.custom_transformers import (
    ColumnExtractor,
    ShortTermNormalizer,
    LongTermNormalizer,
    CategoricalPreprocessor,
    ResamplerTransformer,
    DFFeatureUnion
)

logger.remove()
logger.add(sys.stderr, level="INFO")


def create_sample_data():
    """
    Create sample data similar to your actual data structure.
    This helps test preprocessing independently.
    """
    logger.info("Creating sample data...")
    
    # Create a sample DataFrame with mixed types
    np.random.seed(42)
    n_samples = 100
    
    data = {
        'price': np.random.uniform(100, 500, n_samples),
        'volume': np.random.uniform(1000, 50000, n_samples),
        'rsi': np.random.uniform(0, 100, n_samples),
        'macd': np.random.uniform(-10, 10, n_samples),
        # Categorical columns that might have strings
        'trend': np.random.choice(['High', 'Medium High', 'Medium', 'Medium Low', 'Low'], n_samples),
        'signal': np.random.choice(['BUY', 'SELL', 'HOLD'], n_samples),
        'symbol': np.random.choice(['AAPL', 'GOOGL', 'MSFT'], n_samples),
    }
    
    df = pd.DataFrame(data)
    target = pd.Series(np.random.choice(['Up', 'Down', 'Neutral'], n_samples), name='target')
    
    logger.info(f"Sample data shape: {df.shape}")
    logger.info(f"Dtypes:\n{df.dtypes}")
    
    return df, target


def test_categorical_preprocessing():
    """
    Test categorical preprocessing in isolation.
    """
    logger.info("\n" + "="*80)
    logger.info("TEST: Categorical Preprocessing")
    logger.info("="*80)
    
    # Create sample data
    df, target = create_sample_data()
    
    # Define categorical columns
    cat_cols = ['trend', 'signal']
    
    logger.info(f"\nTesting CategoricalPreprocessor with columns: {cat_cols}")
    logger.info(f"Sample values before encoding:")
    for col in cat_cols:
        logger.info(f"  {col}: {df[col].unique()}")
    
    # Test ColumnExtractor
    logger.info("\n1. Testing ColumnExtractor...")
    col_extractor = ColumnExtractor(cat_cols)
    extracted = col_extractor.fit_transform(df)
    logger.info(f"   Shape after extraction: {extracted.shape}")
    logger.info(f"   Dtypes: {extracted.dtypes.tolist()}")
    
    # Test CategoricalPreprocessor
    logger.info("\n2. Testing CategoricalPreprocessor...")
    try:
        cat_processor = CategoricalPreprocessor(cat_cols)
        cat_processor.fit(extracted, target)
        transformed = cat_processor.transform(extracted)
        
        logger.success(f"✓ Categorical preprocessing successful!")
        logger.info(f"   Shape after encoding: {transformed.shape}")
        logger.info(f"   Dtypes: {transformed.dtypes.tolist()}")
        logger.info(f"   All numeric: {transformed.select_dtypes(include=['number']).shape[1] == transformed.shape[1]}")
        logger.info(f"   Sample output:\n{transformed.head()}")
        
        return True
    except Exception as e:
        logger.error(f"❌ Categorical preprocessing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def test_feature_union():
    """
    Test the DFFeatureUnion that combines numeric and categorical features.
    """
    logger.info("\n" + "="*80)
    logger.info("TEST: Feature Union")
    logger.info("="*80)
    
    # Create sample data
    df, target = create_sample_data()
    
    # Define column groups
    numeric_cols = ['price', 'volume', 'rsi', 'macd']
    cat_cols = ['trend', 'signal']
    
    logger.info(f"Numeric columns: {numeric_cols}")
    logger.info(f"Categorical columns: {cat_cols}")
    
    try:
        # Create transformers for each feature type
        numeric_transformers = [
            ('numeric_extract', ColumnExtractor(numeric_cols)),
        ]
        
        cat_transformers = [
            ('cat_extract', ColumnExtractor(cat_cols)),
            ('cat_encode', CategoricalPreprocessor(cat_cols)),
        ]
        
        # Combine all transformers
        all_transformers = numeric_transformers + cat_transformers
        
        logger.info("\n1. Testing DFFeatureUnion...")
        feature_union = DFFeatureUnion(all_transformers)
        feature_union.fit(df, target)
        transformed = feature_union.transform(df)
        
        logger.success(f"✓ Feature union successful!")
        logger.info(f"   Shape after union: {transformed.shape}")
        logger.info(f"   Dtypes:\n{transformed.dtypes}")
        
        # Check for non-numeric columns
        non_numeric = transformed.select_dtypes(exclude=['number']).columns.tolist()
        if non_numeric:
            logger.error(f"❌ Non-numeric columns found: {non_numeric}")
            for col in non_numeric:
                logger.error(f"   {col}: {transformed[col].unique()[:10]}")
            return False
        else:
            logger.success("✓ All columns are numeric after feature union!")
            return True
            
    except Exception as e:
        logger.error(f"❌ Feature union failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def test_full_preprocessing_chain():
    """
    Test the full preprocessing chain as it appears in your pipeline.
    """
    logger.info("\n" + "="*80)
    logger.info("TEST: Full Preprocessing Chain")
    logger.info("="*80)
    
    # Create sample data
    df, target = create_sample_data()
    
    # Get column definitions from config if available
    try:
        numeric_cols = ['price', 'volume', 'rsi', 'macd']
        cat_cols = ['trend', 'signal']
        
        logger.info(f"Input shape: {df.shape}")
        logger.info(f"Input dtypes:\n{df.dtypes}")
        
        # Build the preprocessing chain
        steps = []
        
        # 1. Feature extraction and encoding
        feature_transformers = [
            ('numeric_extract', ColumnExtractor(numeric_cols)),
            ('cat_extract', ColumnExtractor(cat_cols)),
            ('cat_encode', CategoricalPreprocessor(cat_cols)),
        ]
        
        steps.append(('features', DFFeatureUnion(feature_transformers)))
        
        # Execute each step
        X_current = df.copy()
        y_current = target.copy()
        
        for step_name, transformer in steps:
            logger.info(f"\nExecuting step: {step_name}")
            transformer.fit(X_current, y_current)
            X_current = transformer.transform(X_current)
            
            logger.info(f"  Shape: {X_current.shape}")
            logger.info(f"  Dtypes: {X_current.dtypes.tolist()}")
            
            # Check for non-numeric
            if isinstance(X_current, pd.DataFrame):
                non_numeric = X_current.select_dtypes(exclude=['number']).columns.tolist()
                if non_numeric:
                    logger.error(f"  ❌ Non-numeric columns: {non_numeric}")
                    return False
        
        logger.success("✓ Full preprocessing chain completed successfully!")
        logger.success(f"✓ Final shape: {X_current.shape}")
        logger.success(f"✓ All columns are numeric: {isinstance(X_current, pd.DataFrame) and X_current.select_dtypes(include=['number']).shape[1] == X_current.shape[1]}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Full preprocessing chain failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def check_config_columns():
    """
    Check if config columns are properly defined.
    """
    logger.info("\n" + "="*80)
    logger.info("TEST: Configuration Columns")
    logger.info("="*80)
    
    try:
        logger.info(f"Custom columns: {len(config.columns.custom_cs_cols)} columns")
        logger.info(f"Categorical columns: {config.columns.cat_cols}")
        logger.info(f"Short numeric columns: {len(config.columns.short_num_cols)} columns")
        logger.info(f"Long numeric columns: {len(config.columns.long_num_cols)} columns")
        
        # Check for overlap
        all_numeric = set(config.columns.short_num_cols + config.columns.long_num_cols)
        cat_set = set(config.columns.cat_cols)
        
        overlap = all_numeric.intersection(cat_set)
        if overlap:
            logger.warning(f"⚠️  Columns defined as both numeric and categorical: {overlap}")
        else:
            logger.success("✓ No overlap between numeric and categorical columns")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Error checking config: {e}")
        return False


if __name__ == "__main__":
    logger.info("""
    ╔════════════════════════════════════════════════════════════════════╗
    ║          PREPROCESSING ISOLATION TEST                              ║
    ║                                                                    ║
    ║  Tests each preprocessing component independently                  ║
    ╚════════════════════════════════════════════════════════════════════╝
    """)
    
    all_passed = True
    
    # Run tests
    tests = [
        ("Configuration Check", check_config_columns),
        ("Categorical Preprocessing", test_categorical_preprocessing),
        ("Feature Union", test_feature_union),
        ("Full Preprocessing Chain", test_full_preprocessing_chain),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            passed = test_func()
            results.append((test_name, passed))
            all_passed = all_passed and passed
        except Exception as e:
            logger.error(f"\n❌ {test_name} raised exception: {e}")
            results.append((test_name, False))
            all_passed = False
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("TEST SUMMARY")
    logger.info("="*80)
    for test_name, passed in results:
        status = "✓ PASSED" if passed else "❌ FAILED"
        logger.info(f"{status}: {test_name}")
    
    if all_passed:
        logger.success("\n✓ All tests passed!")
        sys.exit(0)
    else:
        logger.error("\n❌ Some tests failed. Please review the output above.")
        sys.exit(1)


