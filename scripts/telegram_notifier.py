# notifier.py
from flask import Flask, request, jsonify
from dotenv import load_dotenv  # Correct import statement
import requests
import os
import logging 

load_dotenv()

app = Flask(__name__)

# Configuration - you can also use environment variables
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

def send_telegram_message(type: str, message: str) -> bool:
    """
    Sends a message to a Telegram chat using the Telegram Bot API.

    Args:
    - message (str): The message to be sent.

    Returns:
    - bool: True if the message is sent successfully, False otherwise.
    """

    # Telegram Bot API endpoint URL
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    print(url)

    # Payload for the POST request
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'  # Optional: for formatting
    }

    try:
        # Send the POST request
        response = requests.post(url, json=payload)

        # Check if the request was successful
        response.raise_for_status()

        # Log the successful message send
        logging.info(f"Message sent successfully: {message}")

        return True

    except requests.exceptions.RequestException as e:
        # Log the error
        logging.error(f"Failed to send message: {e}")

        return False

def format_grafana_alert(data: dict) -> str:
    """
    Formats a Grafana alert message from the given data.

    Args:
        data (dict): The data from the Grafana webhook.

    Returns:
        str: The formatted message.
    """
    status = data.get('status', 'No status')
    alerts = data.get('alerts', [])

    message = f"*Grafana Alert*: `{status}`\n"

    for alert in alerts:
        name = alert.get('labels', {}).get('alertname', 'No Alert Name')
        state = alert.get('state', 'No State')

        # Use an f-string to format the alert message
        alert_message = f"- *{name}*: `{state}`\n"

        # Use a dictionary comprehension to format annotations
        annotations = {f"*{key}*": value for key, value in alert.get('annotations', {}).items()}

        # Use a join to concatenate the annotations
        annotation_messages = '\n'.join(f"  - {key}: {value}" for key, value in annotations.items())

        message += alert_message + annotation_messages + "\n"

    return message

@app.route('/telegram-webhook', methods=['POST'])
def telegram_webhook():
    """
    Handles the Telegram webhook request from Grafana.

    This endpoint receives a POST request from Grafana containing the alert data.
    It formats the data into a message and sends it to the configured Telegram chat.

    Returns:
        A JSON object with a status message.
    """
    data = request.get_json() 

    if not data:
        error_response = jsonify({'error': 'No data received'})
        error_response.status_code = 400
        return error_response

    try:
        message = format_grafana_alert(data)
        send_telegram_message(message)
        return jsonify({'status': 'Message sent'}), 200
    except Exception as e:
        error_response = jsonify({'error': 'Error processing request'})
        error_response.status_code = 500
        return error_response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
