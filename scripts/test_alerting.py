#!/usr/bin/env python3
"""
Alerting System Test Script

Tests the alerting system including:
- Slack webhook connectivity
- Message formatting
- Alert channels configuration
- Telegram notifications (optional)

Usage:
    python scripts/test_alerting.py --slack
    python scripts/test_alerting.py --telegram
    python scripts/test_alerting.py --all
    python scripts/test_alerting.py --dry-run  # Just validate config
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.slack_notifier import (
    send_slack_message,
    send_trading_alert,
    format_trading_alert,
)


class AlertChannel(str, Enum):
    """Supported alert channels."""
    SLACK = "slack"
    TELEGRAM = "telegram"


class TestStatus(str, Enum):
    """Test result status."""
    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIPPED"


@dataclass
class AlertTestResult:
    """Result of an alerting test."""
    channel: AlertChannel
    test_name: str
    status: TestStatus
    message: str = ""
    response_time_ms: float = 0.0
    details: Dict[str, Any] = field(default_factory=dict)


def test_slack_webhook() -> AlertTestResult:
    """Test Slack webhook connectivity."""
    result = AlertTestResult(
        channel=AlertChannel.SLACK,
        test_name="webhook_connectivity",
        status=TestStatus.PASS,
    )
    
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        result.status = TestStatus.SKIP
        result.message = "SLACK_WEBHOOK_URL not set"
        return result
    
    # Validate URL format
    if not webhook_url.startswith("https://hooks.slack.com/"):
        result.status = TestStatus.FAIL
        result.message = "Invalid Slack webhook URL format"
        return result
    
    try:
        import time
        start = time.time()
        
        # Send a test message
        test_message = f"🧪 Test message from AutomatedTrading at {datetime.now().strftime('%H:%M:%S')}"
        
        response = requests.post(
            webhook_url,
            json={"text": test_message},
            timeout=10
        )
        
        result.response_time_ms = (time.time() - start) * 1000
        
        if response.status_code == 200:
            result.message = f"Webhook connected ({result.response_time_ms:.0f}ms)"
            result.details["response"] = response.text
        else:
            result.status = TestStatus.FAIL
            result.message = f"HTTP {response.status_code}: {response.text}"
    
    except requests.exceptions.Timeout:
        result.status = TestStatus.FAIL
        result.message = "Request timed out"
    except requests.exceptions.RequestException as e:
        result.status = TestStatus.FAIL
        result.message = str(e)
    
    return result


def test_slack_message_types() -> List[AlertTestResult]:
    """Test different Slack message types."""
    results = []
    
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return [AlertTestResult(
            channel=AlertChannel.SLACK,
            test_name="message_types",
            status=TestStatus.SKIP,
            message="SLACK_WEBHOOK_URL not set",
        )]
    
    message_types = ["Good", "Bad", "Info", "Warning"]
    
    for msg_type in message_types:
        result = AlertTestResult(
            channel=AlertChannel.SLACK,
            test_name=f"message_type_{msg_type.lower()}",
            status=TestStatus.PASS,
        )
        
        try:
            import time
            start = time.time()
            
            success = send_slack_message(
                type=msg_type,
                message=f"Test {msg_type} message at {datetime.now().strftime('%H:%M:%S')}",
                webhook_url=webhook_url,
            )
            
            result.response_time_ms = (time.time() - start) * 1000
            
            if success:
                result.message = f"Sent {msg_type} message"
            else:
                result.status = TestStatus.FAIL
                result.message = f"Failed to send {msg_type} message"
        
        except Exception as e:
            result.status = TestStatus.FAIL
            result.message = str(e)
        
        results.append(result)
    
    return results


def test_trading_alert_format() -> AlertTestResult:
    """Test trading alert formatting."""
    result = AlertTestResult(
        channel=AlertChannel.SLACK,
        test_name="trading_alert_format",
        status=TestStatus.PASS,
    )
    
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        result.status = TestStatus.SKIP
        result.message = "SLACK_WEBHOOK_URL not set"
        return result
    
    try:
        import time
        start = time.time()
        
        # Test trading alert
        success = send_trading_alert(
            alert_type="Test Trade Signal",
            symbol="RELIANCE",
            message="This is a test alert from the alerting system",
            details={
                "Direction": "LONG",
                "Confidence": "85%",
                "Price": "₹2,500.00",
                "Quantity": "10",
            },
            webhook_url=webhook_url,
        )
        
        result.response_time_ms = (time.time() - start) * 1000
        
        if success:
            result.message = "Trading alert sent successfully"
        else:
            result.status = TestStatus.FAIL
            result.message = "Failed to send trading alert"
    
    except Exception as e:
        result.status = TestStatus.FAIL
        result.message = str(e)
    
    return result


def test_telegram_webhook() -> AlertTestResult:
    """Test Telegram bot connectivity."""
    result = AlertTestResult(
        channel=AlertChannel.TELEGRAM,
        test_name="webhook_connectivity",
        status=TestStatus.PASS,
    )
    
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        result.status = TestStatus.SKIP
        result.message = "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set"
        return result
    
    try:
        import time
        start = time.time()
        
        # Send test message via Telegram Bot API
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        
        response = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": f"🧪 Test message from AutomatedTrading at {datetime.now().strftime('%H:%M:%S')}",
                "parse_mode": "HTML",
            },
            timeout=10
        )
        
        result.response_time_ms = (time.time() - start) * 1000
        
        if response.status_code == 200:
            data = response.json()
            if data.get("ok"):
                result.message = f"Telegram connected ({result.response_time_ms:.0f}ms)"
            else:
                result.status = TestStatus.FAIL
                result.message = data.get("description", "Unknown error")
        else:
            result.status = TestStatus.FAIL
            result.message = f"HTTP {response.status_code}"
    
    except requests.exceptions.Timeout:
        result.status = TestStatus.FAIL
        result.message = "Request timed out"
    except requests.exceptions.RequestException as e:
        result.status = TestStatus.FAIL
        result.message = str(e)
    
    return result


def validate_config() -> List[AlertTestResult]:
    """Validate alerting configuration without sending messages."""
    results = []
    
    # Check Slack config
    slack_result = AlertTestResult(
        channel=AlertChannel.SLACK,
        test_name="config_validation",
        status=TestStatus.PASS,
    )
    
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        slack_result.status = TestStatus.SKIP
        slack_result.message = "SLACK_WEBHOOK_URL not set"
    elif not webhook_url.startswith("https://hooks.slack.com/"):
        slack_result.status = TestStatus.FAIL
        slack_result.message = "Invalid Slack webhook URL format"
    else:
        slack_result.message = "Slack config valid"
        slack_result.details["url_prefix"] = webhook_url[:40] + "..."
    
    results.append(slack_result)
    
    # Check Telegram config
    telegram_result = AlertTestResult(
        channel=AlertChannel.TELEGRAM,
        test_name="config_validation",
        status=TestStatus.PASS,
    )
    
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not bot_token:
        telegram_result.status = TestStatus.SKIP
        telegram_result.message = "TELEGRAM_BOT_TOKEN not set"
    elif not chat_id:
        telegram_result.status = TestStatus.FAIL
        telegram_result.message = "TELEGRAM_CHAT_ID not set (required with bot token)"
    else:
        telegram_result.message = "Telegram config valid"
        telegram_result.details["chat_id"] = chat_id
    
    results.append(telegram_result)
    
    return results


def print_results(results: List[AlertTestResult]) -> None:
    """Print test results to console."""
    status_emoji = {
        TestStatus.PASS: "✅",
        TestStatus.FAIL: "❌",
        TestStatus.SKIP: "⏭️",
    }
    
    print("\n" + "=" * 60)
    print("ALERTING SYSTEM TEST RESULTS")
    print("=" * 60)
    
    # Group by channel
    channels = {}
    for r in results:
        if r.channel not in channels:
            channels[r.channel] = []
        channels[r.channel].append(r)
    
    for channel, channel_results in channels.items():
        print(f"\n{channel.value.upper()}")
        print("-" * 40)
        
        for result in channel_results:
            status = f"{status_emoji[result.status]} {result.status.value}"
            time_str = f"{result.response_time_ms:.0f}ms" if result.response_time_ms > 0 else "-"
            print(f"  {result.test_name:<25} {status:<15} {time_str:>10}")
            if result.message:
                print(f"    └─ {result.message}")
    
    # Summary
    print("\n" + "=" * 60)
    passed = sum(1 for r in results if r.status == TestStatus.PASS)
    failed = sum(1 for r in results if r.status == TestStatus.FAIL)
    skipped = sum(1 for r in results if r.status == TestStatus.SKIP)
    
    print(f"Summary: {passed} passed, {failed} failed, {skipped} skipped")
    print("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test alerting system channels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument("--slack", action="store_true", help="Test Slack notifications")
    parser.add_argument("--telegram", action="store_true", help="Test Telegram notifications")
    parser.add_argument("--all", action="store_true", help="Test all channels")
    parser.add_argument("--dry-run", action="store_true", help="Validate config only")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    load_dotenv()
    
    results = []
    
    if args.dry_run:
        # Just validate configuration
        results = validate_config()
    else:
        # Run tests based on flags
        test_slack = args.slack or args.all or not (args.slack or args.telegram)
        test_telegram = args.telegram or args.all
        
        if test_slack:
            results.append(test_slack_webhook())
            results.extend(test_slack_message_types())
            results.append(test_trading_alert_format())
        
        if test_telegram:
            results.append(test_telegram_webhook())
    
    print_results(results)
    
    # Exit with failure code if any tests failed
    has_failures = any(r.status == TestStatus.FAIL for r in results)
    sys.exit(1 if has_failures else 0)


if __name__ == "__main__":
    main()
