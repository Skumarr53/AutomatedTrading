"""
Test script for the new Multi-Model Pipeline Architecture.
Verifies model families, auto-preprocessing, and model type tuning.
"""

import sys
import pandas as pd
import numpy as np
from loguru import logger
from sklearn.model_selection import RandomizedSearchCV
from sklearn.pipeline import Pipeline as SklearnPipeline

# Add project root to path
import os
sys.path.append(os.getcwd())

from src import config
from src.pipelines.family_pipeline import FamilyPipelineBuilder
from src.pipelines.custom_pipelines import FamilyModelPipeline
from src.pipelines.model_selector import ModelSelector

logger.remove()
logger.add(sys.stderr, level="INFO")

def create_test_data(n_samples=200):
    """Create sample data with numeric and categorical features."""
    np.random.seed(42)
    
    # 1. Features
    data = {
        # Numeric
        'price': np.random.uniform(100, 500, n_samples),
        'volume': np.random.uniform(1000, 50000, n_samples),
        'rsi': np.random.uniform(0, 100, n_samples),
        'macd': np.random.uniform(-10, 10, n_samples),
        # Categorical
        'trend': np.random.choice(['High', 'Medium', 'Low'], n_samples),
        'signal': np.random.choice(['BUY', 'SELL', 'HOLD'], n_samples),
    }
    
    X = pd.DataFrame(data)
    
    # 2. Target (Binary classification)
    y = pd.Series(np.random.choice([0, 1], n_samples), name='target')
    
    # Define columns for the builder
    numeric_cols = ['price', 'volume', 'rsi', 'macd']
    cat_cols = ['trend', 'signal']
    
    return X, y, numeric_cols, cat_cols

def test_family_configs():
    """Verify that model families are correctly loaded from config."""
    logger.info("\n" + "="*80)
    logger.info("TEST 1: Configuration Loading")
    logger.info("="*80)
    
    try:
        families = config.model.model_families
        logger.info(f"Loaded {len(families)} model families: {list(families.keys())}")
        
        for name, fam_config in families.items():
            logger.info(f"Family '{name}': {len(fam_config.models)} models, "
                       f"Scaling: {fam_config.preprocessing.scale_numeric}, "
                       f"Encoding: {fam_config.preprocessing.encode_categorical}")
        
        return True
    except Exception as e:
        logger.error(f"❌ Config loading failed: {e}")
        return False

def test_pipeline_construction():
    """Test that FamilyPipelineBuilder creates valid pipelines for each family."""
    logger.info("\n" + "="*80)
    logger.info("TEST 2: Pipeline Construction")
    logger.info("="*80)
    
    X, y, num_cols, cat_cols = create_test_data()
    families = ['baseline', 'tree', 'linear']
    
    success = True
    for family in families:
        logger.info(f"\nBuilding pipeline for family: {family}")
        try:
            builder = FamilyPipelineBuilder(
                family_name=family,
                config=config,
                numeric_cols=num_cols,
                categorical_cols=cat_cols
            )
            
            pipeline = builder.build_pipeline()
            logger.info(f"✓ Pipeline built: {[name for name, _ in pipeline.steps]}")
            
            # Check for expected steps
            step_names = [name for name, _ in pipeline.steps]
            if family in ['baseline', 'linear']:
                assert 'preprocessor' in step_names, f"{family} family should have preprocessor"
            
            assert 'model' in step_names, "All pipelines should have 'model' step"
            assert isinstance(pipeline.named_steps['model'], ModelSelector), "Model step should be ModelSelector"
            
        except Exception as e:
            logger.error(f"❌ {family} pipeline failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            success = False
            
    return success

def test_model_type_tuning():
    """Test that model type can be tuned as a hyperparameter."""
    logger.info("\n" + "="*80)
    logger.info("TEST 3: Model Type Tuning (RandomizedSearchCV)")
    logger.info("="*80)
    
    X, y, num_cols, cat_cols = create_test_data(n_samples=100)
    
    try:
        # Use baseline family for fast test
        builder = FamilyPipelineBuilder(
            family_name='baseline',
            config=config,
            numeric_cols=num_cols,
            categorical_cols=cat_cols
        )
        
        pipeline = builder.build_pipeline()
        param_grid = builder.get_param_grid()
        
        logger.info(f"Param grid includes model types: {param_grid['model__model_type']}")
        
        search = RandomizedSearchCV(
            pipeline,
            param_grid,
            n_iter=4,  # Just try a few
            cv=2,
            n_jobs=1,
            random_state=42
        )
        
        logger.info("Running quick search...")
        search.fit(X, y)
        
        logger.success(f"✓ Search completed successfully!")
        logger.info(f"Best model type found: {search.best_params_['model__model_type']}")
        logger.info(f"Best score: {search.best_score_:.4f}")
        
        # Test prediction
        preds = search.predict(X.head(5))
        logger.info(f"Sample predictions: {preds}")
        
        return True
    except Exception as e:
        logger.error(f"❌ Model tuning test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def test_multi_family_comparison():
    """Test the FamilyModelPipeline comparison logic."""
    logger.info("\n" + "="*80)
    logger.info("TEST 4: Multi-Family Comparison")
    logger.info("="*80)
    
    X, y, num_cols, cat_cols = create_test_data(n_samples=100)
    
    try:
        # Create pipeline for multiple families
        pipeline = FamilyModelPipeline(
            families=['baseline', 'tree'],
            feature_config={'imbalance_technique': 'random'}
        )
        
        # Force small features list for test
        pipeline.features = num_cols + cat_cols
        pipeline._classify_columns()
        
        logger.info("Comparing families: baseline vs tree...")
        results = pipeline.compare_families(X, y, n_iter=2, n_splits=2)
        
        logger.success(f"✓ Comparison completed!")
        for fam, res in results.items():
            if 'error' in res:
                logger.warning(f"  {fam}: FAILED with {res['error']}")
            else:
                logger.info(f"  {fam}: score={res['best_score']:.4f}, model={res['best_model_type']}")
        
        logger.info(f"Best family selected: {pipeline.best_family}")
        
        return True
    except Exception as e:
        logger.error(f"❌ Multi-family comparison failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    logger.info("""
    ╔════════════════════════════════════════════════════════════════════╗
    ║          MULTI-MODEL ARCHITECTURE TEST SUITE                       ║
    ║                                                                    ║
    ║  Verifies families, auto-preprocessing, and model type tuning.     ║
    ╚════════════════════════════════════════════════════════════════════╝
    """)
    
    tests = [
        ("Family Config Loading", test_family_configs),
        ("Pipeline Construction", test_pipeline_construction),
        ("Model Type Tuning", test_model_type_tuning),
        ("Multi-Family Comparison", test_multi_family_comparison),
    ]
    
    all_passed = True
    results = []
    
    for name, func in tests:
        passed = func()
        results.append((name, passed))
        all_passed = all_passed and passed
        
    logger.info("\n" + "="*80)
    logger.info("FINAL TEST SUMMARY")
    logger.info("="*80)
    for name, passed in results:
        status = "✓ PASSED" if passed else "❌ FAILED"
        logger.info(f"{status}: {name}")
        
    if all_passed:
        logger.success("\n✨ ALL ARCHITECTURE TESTS PASSED! ✨")
        sys.exit(0)
    else:
        logger.error("\n❌ Some tests failed. Please review the logs.")
        sys.exit(1)
