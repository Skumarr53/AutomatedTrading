"""
Deep diagnostic script to trace INSIDE the model.fit() call.
Error: "ValueError: could not convert string to float: 'Medium Low'"

This script:
1. Bypasses RandomizedSearchCV/GridSearchCV
2. Manually executes the pipeline steps to see where the error occurs
3. Traces through the actual pipeline.fit_transform() call
4. Identifies the exact transformer causing the issue
"""

import sys
import traceback
import pandas as pd
import numpy as np
from loguru import logger
from src import config
from src.data.data_fetcher import DataHandler
from src.feature_engineering.feature_aggregator import DataAggregator
from src.data.order_book_handler import OrderBookHandler
from src.pipelines.base_pipeline import MLPipelineBase
from src.auth.fyers_auth import AuthCodeGenerator
from src.utils.pipeline_debugger import (
    test_pipeline_steps,
    set_sequential_mode,
    inspect_dataframe
)

# Configure logger
logger.remove()
logger.add(sys.stderr, level="DEBUG")

# Force sequential execution for easier debugging
set_sequential_mode()




def test_single_symbol_training():
    """Test training for a single symbol with deep tracing."""
    logger.info("\n" + "="*100)
    logger.info("DEEP DIAGNOSTIC: TRACING INSIDE MODEL.FIT() CALL")
    logger.info("="*100)
    
    # Initialize authentication
    logger.info("\n1. Initializing authentication...")
    generator = AuthCodeGenerator()
    fyers_instance = generator.initialize_fyers_model()
    logger.success("✓ Authentication successful")
    
    # Initialize data handlers
    logger.info("\n2. Initializing data handlers...")
    ticker_data_handler = DataHandler(fyers_instance, scheduler=None)
    order_data_handler = OrderBookHandler(fyers_instance, scheduler=None)
    data_aggregator = DataAggregator()
    logger.success("✓ Data handlers initialized")
    
    # Use the first symbol for testing
    test_symbol = config.symbols[0]
    logger.info(f"\n3. Testing with symbol: {test_symbol}")
    
    # Fetch data
    logger.info("\n4. Fetching data...")
    ticker_data = ticker_data_handler.data.get(test_symbol)
    order_book_data = order_data_handler.data.get(test_symbol)
    
    if ticker_data is None or ticker_data.empty:
        logger.error("❌ No ticker data available")
        return
    
    logger.success(f"✓ Ticker data shape: {ticker_data.shape}")
    
    # Aggregate features
    logger.info("\n5. Aggregating features...")
    data_agg = data_aggregator.aggregate_features(ticker_data, order_book_data)
    data_agg['symbol'] = test_symbol
    inspect_dataframe(data_agg, "After feature aggregation")
    
    # Initialize pipeline
    logger.info("\n6. Initializing ML pipeline...")
    ml_pipeline = MLPipelineBase()
    pipeline = ml_pipeline.pipelines[0]
    
    # Prepare data
    logger.info("\n7. Preparing training data...")
    run_id = config.model_settings.run_ids[0]
    target = config.model_settings.model_targets[0]
    
    logger.info(f"   Run ID: {run_id}")
    logger.info(f"   Target: {target}")
    
    # Transform and split
    X_train, X_test, y_train, y_test = ml_pipeline.transform_and_split(
        data_agg, target, run_id
    )
    
    logger.info("\n8. Training data prepared:")
    logger.info(f"   X_train shape: {X_train.shape}")
    logger.info(f"   y_train shape: {y_train.shape}")
    logger.info(f"   y_train unique values: {y_train.unique()}")
    
    inspect_dataframe(X_train, "X_train before pipeline")
    
    # NOW THE CRITICAL PART: Trace through what happens INSIDE model.fit()
    logger.info("\n9. TRACING THROUGH PIPELINE TRANSFORMATION (WHAT HAPPENS IN MODEL.FIT)...")
    try:
        # Use the utility function to test all pipeline steps
        test_pipeline_steps(pipeline, X_train, y_train)
        
        logger.success("\n" + "="*100)
        logger.success("🎉 ALL STEPS COMPLETED SUCCESSFULLY!")
        logger.success("="*100)
    except Exception:
        logger.error("\n" + "="*100)
        logger.error("❌ ERROR IDENTIFIED - SEE DETAILS ABOVE")
        logger.error("="*100)
        raise


if __name__ == "__main__":
    logger.info("""
    ╔════════════════════════════════════════════════════════════════════╗
    ║     DEEP MODEL FITTING ERROR DIAGNOSTIC SCRIPT                     ║
    ║                                                                    ║
    ║  This script traces INSIDE the model.fit() call to identify:      ║
    ║  ValueError: could not convert string to float: 'Medium Low'       ║
    ║                                                                    ║
    ║  We bypass RandomizedSearchCV and manually trace each step         ║
    ╚════════════════════════════════════════════════════════════════════╝
    """)
    
    try:
        test_single_symbol_training()
    except Exception:
        logger.error("\n❌ Test failed. Please review the diagnostic output above.")
        sys.exit(1)
