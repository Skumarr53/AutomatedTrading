#!/usr/bin/env python3
"""
Live Trading Flow Test Script

Tests the end-to-end live trading flow:
1. Data collection verification
2. Model loading
3. Feature engineering
4. Prediction generation
5. Trade decision making
6. Order execution (paper mode)

Usage:
    python scripts/test_live_trading_flow.py --symbol RELIANCE
    python scripts/test_live_trading_flow.py --all-steps --verbose
    python scripts/test_live_trading_flow.py --step predictions
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestStatus(str, Enum):
    """Status of a test step."""
    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIPPED"
    WARN = "WARNING"


@dataclass
class TestResult:
    """Result of a single test step."""
    step_name: str
    status: TestStatus
    duration_ms: float = 0.0
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


@dataclass
class FlowTestReport:
    """Complete test report."""
    symbol: str
    timestamp: datetime = field(default_factory=datetime.now)
    results: List[TestResult] = field(default_factory=list)
    overall_status: TestStatus = TestStatus.PASS
    total_duration_ms: float = 0.0
    
    def add_result(self, result: TestResult) -> None:
        """Add test result and update overall status."""
        self.results.append(result)
        self.total_duration_ms += result.duration_ms
        
        # Update overall status
        if result.status == TestStatus.FAIL:
            self.overall_status = TestStatus.FAIL
        elif result.status == TestStatus.WARN and self.overall_status != TestStatus.FAIL:
            self.overall_status = TestStatus.WARN


class LiveTradingFlowTester:
    """
    Test the complete live trading flow.
    
    Tests each component independently and as part of the full pipeline.
    """
    
    def __init__(
        self,
        symbol: str = "RELIANCE",
        paper_mode: bool = True,
        verbose: bool = False,
    ):
        """
        Initialize flow tester.
        
        Args:
            symbol: Symbol to test with
            paper_mode: If True, don't execute real trades
            verbose: Enable verbose output
        """
        self.symbol = symbol
        self.paper_mode = paper_mode
        self.verbose = verbose
        
        # Component references (lazy loaded)
        self._influx_client = None
        self._models = {}
        self._decision_maker = None
        self._trade_executor = None
        
        # Test state
        self._current_data = None
        self._current_predictions = None
        self._current_decision = None
    
    async def run_all_tests(self) -> FlowTestReport:
        """Run all test steps in sequence."""
        report = FlowTestReport(symbol=self.symbol)
        
        # Define test steps
        steps = [
            ("influx_connection", self._test_influx_connection),
            ("data_freshness", self._test_data_freshness),
            ("model_loading", self._test_model_loading),
            ("feature_engineering", self._test_feature_engineering),
            ("prediction_generation", self._test_prediction_generation),
            ("decision_making", self._test_decision_making),
            ("trade_execution", self._test_trade_execution),
        ]
        
        for step_name, test_func in steps:
            if self.verbose:
                logger.info(f"Running test: {step_name}")
            
            start_time = time.time()
            try:
                result = await test_func()
                result.duration_ms = (time.time() - start_time) * 1000
            except Exception as e:
                result = TestResult(
                    step_name=step_name,
                    status=TestStatus.FAIL,
                    duration_ms=(time.time() - start_time) * 1000,
                    message=f"Exception: {str(e)}",
                    errors=[str(e)],
                )
            
            report.add_result(result)
            
            # Stop on failure unless we want to see all results
            if result.status == TestStatus.FAIL and not self.verbose:
                break
        
        return report
    
    async def run_single_test(self, step_name: str) -> TestResult:
        """Run a single test step."""
        test_map = {
            "influx_connection": self._test_influx_connection,
            "data_freshness": self._test_data_freshness,
            "model_loading": self._test_model_loading,
            "feature_engineering": self._test_feature_engineering,
            "prediction_generation": self._test_prediction_generation,
            "predictions": self._test_prediction_generation,  # Alias
            "decision_making": self._test_decision_making,
            "trade_execution": self._test_trade_execution,
        }
        
        test_func = test_map.get(step_name)
        if not test_func:
            return TestResult(
                step_name=step_name,
                status=TestStatus.FAIL,
                message=f"Unknown test step: {step_name}",
            )
        
        start_time = time.time()
        try:
            result = await test_func()
            result.duration_ms = (time.time() - start_time) * 1000
        except Exception as e:
            result = TestResult(
                step_name=step_name,
                status=TestStatus.FAIL,
                duration_ms=(time.time() - start_time) * 1000,
                message=f"Exception: {str(e)}",
                errors=[str(e)],
            )
        
        return result
    
    async def _test_influx_connection(self) -> TestResult:
        """Test InfluxDB connection."""
        from src.utils.influx_client import InfluxDBClient_Wrapper, InfluxDBConfig
        
        result = TestResult(step_name="influx_connection", status=TestStatus.PASS)
        
        try:
            influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
            influx_token = os.getenv("INFLUXDB_TOKEN")
            influx_org = os.getenv("INFLUXDB_ORG", "trading")
            
            if not influx_token:
                result.status = TestStatus.FAIL
                result.message = "INFLUXDB_TOKEN not set"
                return result
            
            config = InfluxDBConfig(
                url=influx_url,
                token=influx_token,
                org=influx_org,
            )
            
            self._influx_client = InfluxDBClient_Wrapper(config)
            connected = await self._influx_client.connect()
            
            if not connected:
                result.status = TestStatus.FAIL
                result.message = "Could not connect to InfluxDB"
            else:
                result.message = f"Connected to {influx_url}"
                result.details["url"] = influx_url
                result.details["org"] = influx_org
        except Exception as e:
            result.status = TestStatus.FAIL
            result.message = str(e)
            result.errors.append(str(e))
        
        return result
    
    async def _test_data_freshness(self) -> TestResult:
        """Test that we have fresh data for the symbol."""
        result = TestResult(step_name="data_freshness", status=TestStatus.PASS)
        
        if not self._influx_client:
            result.status = TestStatus.SKIP
            result.message = "InfluxDB not connected"
            return result
        
        try:
            # Query last 24 hours of data
            df = await self._influx_client.query_ticker_data(self.symbol, hours=24)
            
            if df is None or df.empty:
                result.status = TestStatus.FAIL
                result.message = f"No data found for {self.symbol}"
                return result
            
            result.details["record_count"] = len(df)
            
            # Check freshness
            if 'timestamp' in df.columns:
                latest = df['timestamp'].max()
                if hasattr(latest, 'to_pydatetime'):
                    latest = latest.to_pydatetime()
                
                # Make timezone naive
                if hasattr(latest, 'tzinfo') and latest.tzinfo:
                    latest = latest.replace(tzinfo=None)
                
                age_minutes = (datetime.now() - latest).total_seconds() / 60
                result.details["latest_timestamp"] = str(latest)
                result.details["age_minutes"] = round(age_minutes, 1)
                
                if age_minutes > 60:  # More than 1 hour old
                    result.status = TestStatus.WARN
                    result.message = f"Data is {age_minutes:.0f} minutes old"
                else:
                    result.message = f"Data is {age_minutes:.0f} minutes old ({len(df)} records)"
            
            # Store for next tests
            self._current_data = df
            
        except Exception as e:
            result.status = TestStatus.FAIL
            result.message = str(e)
            result.errors.append(str(e))
        
        return result
    
    async def _test_model_loading(self) -> TestResult:
        """Test MLflow model loading."""
        result = TestResult(step_name="model_loading", status=TestStatus.PASS)
        
        try:
            import mlflow
            import mlflow.sklearn
            
            mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
            
            # Try to load models for the symbol
            from src import config
            
            timeframes = list(config.model_settings.run_ids)[:3]  # First 3 timeframes
            model_symbol = "ALL_SYMBOLS" if config.training.combine_all_symbols else self.symbol
            
            loaded_models = []
            failed_models = []
            
            for tf in timeframes:
                model_name = f"{model_symbol}_{tf}_PctChange"
                try:
                    model_uri = f"models:/{model_name}/Production"
                    model = mlflow.sklearn.load_model(model_uri)
                    loaded_models.append(model_name)
                    self._models[f"{tf}_PctChange"] = model
                except Exception as e:
                    failed_models.append(f"{model_name}: {str(e)[:50]}")
            
            result.details["loaded"] = loaded_models
            result.details["failed"] = failed_models
            
            if not loaded_models:
                result.status = TestStatus.FAIL
                result.message = "No models could be loaded"
            elif failed_models:
                result.status = TestStatus.WARN
                result.message = f"Loaded {len(loaded_models)}/{len(timeframes)} models"
            else:
                result.message = f"Loaded {len(loaded_models)} models"
        
        except ImportError:
            result.status = TestStatus.FAIL
            result.message = "MLflow not available"
        except Exception as e:
            result.status = TestStatus.FAIL
            result.message = str(e)
            result.errors.append(str(e))
        
        return result
    
    async def _test_feature_engineering(self) -> TestResult:
        """Test feature engineering pipeline."""
        result = TestResult(step_name="feature_engineering", status=TestStatus.PASS)
        
        if self._current_data is None or self._current_data.empty:
            result.status = TestStatus.SKIP
            result.message = "No data available"
            return result
        
        try:
            from src.feature_engineering.feature_aggregator import DataAggregator
            
            aggregator = DataAggregator()
            
            # Get a sample of data
            sample = self._current_data.tail(100).copy()
            
            # Run feature engineering
            features = aggregator.aggregate_features(sample)
            
            result.details["input_columns"] = len(sample.columns)
            result.details["output_columns"] = len(features.columns)
            result.details["rows"] = len(features)
            
            if features.empty:
                result.status = TestStatus.FAIL
                result.message = "Feature engineering produced empty result"
            else:
                result.message = f"Generated {len(features.columns)} features from {len(sample.columns)} columns"
                self._current_data = features  # Use features for next steps
        
        except Exception as e:
            result.status = TestStatus.WARN
            result.message = f"Feature engineering skipped: {str(e)[:50]}"
            # Continue with raw data
        
        return result
    
    async def _test_prediction_generation(self) -> TestResult:
        """Test prediction generation."""
        result = TestResult(step_name="prediction_generation", status=TestStatus.PASS)
        
        if not self._models:
            result.status = TestStatus.SKIP
            result.message = "No models loaded"
            return result
        
        if self._current_data is None or self._current_data.empty:
            result.status = TestStatus.SKIP
            result.message = "No data available"
            return result
        
        try:
            # Get latest row for prediction
            latest = self._current_data.tail(1)
            
            predictions = {}
            errors = []
            
            for key, model in self._models.items():
                try:
                    pred = model.predict(latest)
                    predictions[key] = pred[0] if len(pred) > 0 else "Unknown"
                except Exception as e:
                    errors.append(f"{key}: {str(e)[:30]}")
                    predictions[key] = "Error"
            
            result.details["predictions"] = predictions
            result.details["errors"] = errors
            
            self._current_predictions = predictions
            
            successful = sum(1 for p in predictions.values() if p not in ["Error", "Unknown"])
            
            if successful == 0:
                result.status = TestStatus.FAIL
                result.message = "All predictions failed"
            elif errors:
                result.status = TestStatus.WARN
                result.message = f"{successful}/{len(predictions)} predictions successful"
            else:
                result.message = f"Generated {len(predictions)} predictions"
        
        except Exception as e:
            result.status = TestStatus.FAIL
            result.message = str(e)
            result.errors.append(str(e))
        
        return result
    
    async def _test_decision_making(self) -> TestResult:
        """Test trade decision making."""
        result = TestResult(step_name="decision_making", status=TestStatus.PASS)
        
        if not self._current_predictions:
            result.status = TestStatus.SKIP
            result.message = "No predictions available"
            return result
        
        try:
            from src.trading_logic.trade_decision_maker import TradeDecisionMaker
            
            decision_maker = TradeDecisionMaker()
            
            # Filter to PctChange predictions
            pct_predictions = {
                k.split('_')[0]: v for k, v in self._current_predictions.items()
                if 'PctChange' in k and v not in ["Error", "Unknown"]
            }
            
            if not pct_predictions:
                result.status = TestStatus.WARN
                result.message = "No valid predictions for decision making"
                return result
            
            decision = decision_maker.make_decision(pct_predictions=pct_predictions)
            
            result.details["direction"] = decision.direction
            result.details["confidence"] = round(decision.confidence, 2)
            result.details["timeframe_agreement"] = decision.timeframe_agreement
            result.details["reason"] = decision.reason
            
            self._current_decision = decision
            result.message = f"Decision: {decision.direction} (confidence: {decision.confidence:.2f})"
        
        except Exception as e:
            result.status = TestStatus.FAIL
            result.message = str(e)
            result.errors.append(str(e))
        
        return result
    
    async def _test_trade_execution(self) -> TestResult:
        """Test trade execution (paper mode)."""
        result = TestResult(step_name="trade_execution", status=TestStatus.PASS)
        
        if not self._current_decision:
            result.status = TestStatus.SKIP
            result.message = "No decision available"
            return result
        
        if self._current_decision.direction == "HOLD":
            result.message = "No trade signal (HOLD decision)"
            result.details["decision"] = "HOLD"
            return result
        
        try:
            if self.paper_mode:
                # Paper mode - just validate the execution flow
                result.message = f"[PAPER MODE] Would execute {self._current_decision.direction} trade"
                result.details["paper_mode"] = True
                result.details["direction"] = self._current_decision.direction
                result.details["confidence"] = round(self._current_decision.confidence, 2)
            else:
                # Real execution - check executor
                from src.trading_logic.fyers_trade_executor import FyersTradeExecutor
                
                # Check if executor can trade
                executor = FyersTradeExecutor()
                
                if not executor.can_trade():
                    result.status = TestStatus.WARN
                    result.message = "Executor cannot trade (circuit breaker or other issue)"
                    result.details["can_trade"] = False
                else:
                    # In real mode, we'd execute the trade here
                    result.message = f"Trade executor ready for {self._current_decision.direction}"
                    result.details["can_trade"] = True
        
        except Exception as e:
            result.status = TestStatus.FAIL
            result.message = str(e)
            result.errors.append(str(e))
        
        return result
    
    async def cleanup(self) -> None:
        """Clean up resources."""
        if self._influx_client:
            await self._influx_client.close()


def print_report(report: FlowTestReport) -> None:
    """Print test report to console."""
    status_emoji = {
        TestStatus.PASS: "✅",
        TestStatus.FAIL: "❌",
        TestStatus.WARN: "⚠️",
        TestStatus.SKIP: "⏭️",
    }
    
    print("\n" + "=" * 60)
    print("LIVE TRADING FLOW TEST REPORT")
    print("=" * 60)
    print(f"Symbol: {report.symbol}")
    print(f"Timestamp: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Overall Status: {status_emoji[report.overall_status]} {report.overall_status.value}")
    print(f"Total Duration: {report.total_duration_ms:.0f}ms")
    print("-" * 60)
    
    for result in report.results:
        status = f"{status_emoji[result.status]} {result.status.value}"
        print(f"{result.step_name:<25} {status:<15} {result.duration_ms:>8.0f}ms")
        if result.message:
            print(f"  └─ {result.message}")
        if result.errors:
            for error in result.errors[:2]:
                print(f"     └─ Error: {error[:60]}")
    
    print("=" * 60)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test live trading flow end-to-end",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument("--symbol", type=str, default="RELIANCE", help="Symbol to test")
    parser.add_argument("--step", type=str, help="Run only specific step")
    parser.add_argument("--all-steps", action="store_true", help="Run all steps even on failure")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--paper", action="store_true", default=True, help="Paper trading mode (default)")
    parser.add_argument("--live", action="store_true", help="Live trading mode (caution!)")
    
    args = parser.parse_args()
    
    load_dotenv()
    
    paper_mode = not args.live
    
    tester = LiveTradingFlowTester(
        symbol=args.symbol,
        paper_mode=paper_mode,
        verbose=args.verbose or args.all_steps,
    )
    
    try:
        if args.step:
            # Run single test
            result = await tester.run_single_test(args.step)
            report = FlowTestReport(symbol=args.symbol)
            report.add_result(result)
        else:
            # Run all tests
            report = await tester.run_all_tests()
        
        print_report(report)
        
        # Exit with appropriate code
        if report.overall_status == TestStatus.FAIL:
            sys.exit(1)
        else:
            sys.exit(0)
    
    finally:
        await tester.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
