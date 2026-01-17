# scripts/slack_notifier.py
"""
Slack notification integration for AutomatedTrading system.

Sends messages to Slack via webhook URL for system alerts and notifications.
"""
from __future__ import annotations

import os
from typing import Optional

import requests
from loguru import logger


def send_slack_message(
    type: str,
    message: str,
    webhook_url: Optional[str] = None
) -> bool:
    """
    Send a message to Slack via webhook.

    Args:
        type: Message type - "Good", "Bad", "Info", "Warning"
        message: The message text to send
        webhook_url: Slack webhook URL (defaults to SLACK_WEBHOOK_URL env var)

    Returns:
        True if message sent successfully, False otherwise
    """
    webhook_url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        logger.debug("SLACK_WEBHOOK_URL not set, skipping Slack notification")
        return False
    
    # Map message types to Slack formatting
    type_emoji = {
        "Good": "✅",
        "Bad": "❌",
        "Info": "ℹ️",
        "Warning": "⚠️",
    }
    
    emoji = type_emoji.get(type, "📢")
    
    # Format message with type prefix
    formatted_message = f"{emoji} *{type}*: {message}"
    
    # Slack webhook payload
    payload = {
        "text": formatted_message,
    }
    
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10
        )
        response.raise_for_status()
        
        logger.debug(f"Slack message sent successfully: {type}")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Slack message: {e}")
        return False


def format_trading_alert(
    alert_type: str,
    symbol: Optional[str] = None,
    message: str = "",
    details: Optional[dict] = None
) -> str:
    """
    Format a trading-specific alert for Slack.

    Args:
        alert_type: Type of alert (e.g., "Trade Executed", "Error", "Signal Generated")
        symbol: Trading symbol (optional)
        message: Alert message
        details: Additional details dictionary

    Returns:
        Formatted message string
    """
    parts = [f"*{alert_type}*"]
    
    if symbol:
        parts.append(f"Symbol: `{symbol}`")
    
    if message:
        parts.append(message)
    
    if details:
        detail_lines = [f"  • {k}: {v}" for k, v in details.items()]
        parts.append("\n".join(detail_lines))
    
    return "\n".join(parts)


def send_trading_alert(
    alert_type: str,
    symbol: Optional[str] = None,
    message: str = "",
    details: Optional[dict] = None,
    webhook_url: Optional[str] = None
) -> bool:
    """
    Send a formatted trading alert to Slack.

    Args:
        alert_type: Type of alert
        symbol: Trading symbol (optional)
        message: Alert message
        details: Additional details dictionary
        webhook_url: Slack webhook URL (optional)

    Returns:
        True if sent successfully, False otherwise
    """
    formatted = format_trading_alert(alert_type, symbol, message, details)
    return send_slack_message("Info", formatted, webhook_url)
