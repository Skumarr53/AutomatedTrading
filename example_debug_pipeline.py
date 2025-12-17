"""
Example: How to debug pipeline transformations step-by-step

This shows different ways to test and debug your ML pipeline
to identify where non-numeric data is being introduced.
"""

from src import config
from src.data.data_fetcher import DataHandler
from src.feature_engineering.feature_aggregator import DataAggregator
from src.data.order_book_handler import OrderBookHandler
from src.pipelines.base_pipeline import MLPipelineBase
from src.auth.fyers_auth import AuthCodeGenerator
from src.utils.pipeline_debugger import (
    test_pipeline_steps,
    inspect_dataframe,
    test_transformer_step,
    find_non_numeric_columns,
    compare_dataframes,
    set_sequential_mode
)
from loguru import logger


def example_1_full_pipeline_test():
    """Example 1: Test entire pipeline at once"""
    print("\n" + "="*80)
    print("EXAMPLE 1: Test Entire Pipeline")
    print("="*80)
    
    # Force sequential mode (easier to debug)
    set_sequential_mode()
    
    # Setup (your existing code)
    generator = AuthCodeGenerator()
    fyers_instance = generator.initialize_fyers_model()
    
    ticker_data_handler = DataHandler(fyers_instance, scheduler=None)
    order_data_handler = OrderBookHandler(fyers_instance, scheduler=None)
    data_aggregator = DataAggregator()
    
    test_symbol = config.symbols[0]
    ticker_data = ticker_data_handler.data.get(test_symbol)
    order_book_data = order_data_handler.data.get(test_symbol)
    
    data_agg = data_aggregator.aggregate_features(ticker_data, order_book_data)
    data_agg['symbol'] = test_symbol
    
    ml_pipeline = MLPipelineBase()
    pipeline = ml_pipeline.pipelines[0]
    
    run_id = config.model_settings.run_ids[0]
    target = config.model_settings.model_targets[0]
    
    X_train, X_test, y_train, y_test = ml_pipeline.transform_and_split(
        data_agg, target, run_id
    )
    
    # Test all pipeline steps at once
    try:
        test_pipeline_steps(pipeline, X_train, y_train)
        print("✓ All steps passed!")
    except Exception as e:
        print(f"❌ Error found: {e}")


def example_2_step_by_step():
    """Example 2: Test each step manually with inspection"""
    print("\n" + "="*80)
    print("EXAMPLE 2: Manual Step-by-Step Testing")
    print("="*80)
    
    # ... setup code same as example 1 ...
    # (omitted for brevity - copy from example_1)
    
    # Get pipeline steps
    pipeline = None  # Your pipeline object
    sklearn_pipeline = pipeline.model.estimator
    
    X_current = None  # Your X_train
    y_current = None  # Your y_train
    
    # Test each step with detailed inspection
    for step_name, transformer in sklearn_pipeline.steps[:-1]:
        print(f"\n--- Testing: {step_name} ---")
        
        # Before transformation
        inspect_dataframe(X_current, f"Before {step_name}")
        
        # Transform
        X_new, y_new = test_transformer_step(
            transformer, X_current, y_current, step_name
        )
        
        # After transformation
        inspect_dataframe(X_new, f"After {step_name}")
        
        # Compare
        if isinstance(X_new, pd.DataFrame) and isinstance(X_current, pd.DataFrame):
            compare_dataframes(X_current, X_new, f"Before {step_name}", f"After {step_name}")
        
        X_current = X_new
        y_current = y_new


def example_3_find_non_numeric():
    """Example 3: Just find non-numeric columns at any stage"""
    print("\n" + "="*80)
    print("EXAMPLE 3: Find Non-Numeric Columns")
    print("="*80)
    
    # After getting your data
    X_train = None  # Your training data
    
    non_numeric = find_non_numeric_columns(X_train)
    
    if non_numeric:
        print(f"⚠️  Found {len(non_numeric)} non-numeric columns:")
        for col_name, info in non_numeric.items():
            print(f"\n  Column: {col_name}")
            print(f"    dtype: {info['dtype']}")
            print(f"    unique values: {info['unique_count']}")
            print(f"    sample: {info['sample_values'][:5]}")
            print(f"    null count: {info['null_count']}")
    else:
        print("✓ All columns are numeric!")


def example_4_test_single_transformer():
    """Example 4: Test just one specific transformer"""
    print("\n" + "="*80)
    print("EXAMPLE 4: Test Single Transformer")
    print("="*80)
    
    # Setup
    pipeline = None  # Your pipeline
    X_train = None  # Your data
    y_train = None  # Your target
    
    # Get a specific transformer by name
    sklearn_pipeline = pipeline.model.estimator
    
    # Test just the company_metadata transformer
    company_metadata = sklearn_pipeline.named_steps.get('company_metadata')
    if company_metadata:
        print("Testing company_metadata transformer...")
        X_transformed, y_transformed = test_transformer_step(
            company_metadata, X_train, y_train, 'company_metadata'
        )
        
        # Check for non-numeric output
        if isinstance(X_transformed, pd.DataFrame):
            non_numeric = find_non_numeric_columns(X_transformed)
            if non_numeric:
                print(f"⚠️  Company metadata added non-numeric columns: {list(non_numeric.keys())}")
            else:
                print("✓ Company metadata output is all numeric")


def example_5_inspect_at_checkpoints():
    """Example 5: Inspect data at key checkpoints"""
    print("\n" + "="*80)
    print("EXAMPLE 5: Checkpoint Inspection")
    print("="*80)
    
    # Checkpoint 1: After feature aggregation
    data_agg = None  # Your aggregated data
    inspect_dataframe(data_agg, "Checkpoint 1: After Feature Aggregation")
    
    # Checkpoint 2: After transform_and_split
    X_train = None  # Your X_train
    inspect_dataframe(X_train, "Checkpoint 2: X_train Before Pipeline")
    
    # Checkpoint 3: After pipeline preprocessing (before model)
    pipeline = None  # Your pipeline
    sklearn_pipeline = pipeline.model.estimator
    
    # Fit all steps except the final estimator
    X_current = X_train.copy()
    y_current = None  # Your y_train
    
    for step_name, transformer in sklearn_pipeline.steps[:-1]:
        if hasattr(transformer, 'fit_transform'):
            X_current = transformer.fit_transform(X_current, y_current)
        else:
            transformer.fit(X_current, y_current)
            X_current = transformer.transform(X_current)
    
    inspect_dataframe(X_current, "Checkpoint 3: After All Preprocessing, Before Model")


if __name__ == "__main__":
    print("""
    ╔════════════════════════════════════════════════════════════════════╗
    ║          PIPELINE DEBUGGING EXAMPLES                               ║
    ║                                                                    ║
    ║  This file shows different ways to debug your ML pipeline          ║
    ║  Uncomment the example you want to run                             ║
    ╚════════════════════════════════════════════════════════════════════╝
    """)
    
    # Uncomment the example you want to run:
    
    # example_1_full_pipeline_test()
    # example_2_step_by_step()
    # example_3_find_non_numeric()
    # example_4_test_single_transformer()
    # example_5_inspect_at_checkpoints()
    
    print("\n💡 TIP: Uncomment one of the examples above to run it")
    print("💡 Or use the functions in src/utils/pipeline_debugger.py directly")


