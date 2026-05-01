import json
import logging
import requests
from datetime import datetime
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'

class ErrorType(Enum):
    VALIDATION_ERROR = "validation_error"
    PROCESSING_ERROR = "processing_error"
    TIMEOUT_ERROR = "timeout_error"
    EXTERNAL_SERVICE_ERROR = "external_service_error"

class DLQHandler:
    """Handle Dead Letter Queue messages with error categorization and Slack alerts"""
    
    def __init__(self):
        self.slack_webhook = SLACK_WEBHOOK_URL
    
    def categorize_error(self, error_message):
        """Categorize error for routing and handling"""
        if "validation" in error_message.lower():
            return ErrorType.VALIDATION_ERROR
        elif "timeout" in error_message.lower():
            return ErrorType.TIMEOUT_ERROR
        elif "external" in error_message.lower():
            return ErrorType.EXTERNAL_SERVICE_ERROR
        else:
            return ErrorType.PROCESSING_ERROR
    
    def handle_dlq_message(self, message, error_details):
        """Process message in Dead Letter Queue"""
        error_category = self.categorize_error(error_details.get("error_message", ""))
        
        dlq_message = {
            "original_message": message,
            "error_details": error_details,
            "error_category": error_category.value,
            "timestamp": datetime.now().isoformat(),
            "retry_count": error_details.get("retry_count", 0)
        }
        
        logger.info(f"DLQ Message: {json.dumps(dlq_message)}")
        return dlq_message
    
    def send_slack_alert(self, error_details, error_type):
        """Send alert to Slack channel"""
        message = {
            "channel": "#data-alerts",
            "text": f"🚨 *ETL Pipeline Alert - {error_type.value}*",
            "attachments": [{
                "color": "danger",
                "fields": [
                    {"title": "Error Message", "value": error_details.get("error_message", "N/A"), "short": False},
                    {"title": "Record ID", "value": error_details.get("record_id", "N/A"), "short": True},
                    {"title": "Retry Count", "value": str(error_details.get("retry_count", 0)), "short": True},
                    {"title": "Timestamp", "value": datetime.now().isoformat(), "short": False}
                ]
            }]
        }
        
        try:
            requests.post(self.slack_webhook, json=message)
            logger.info("Alert sent to Slack successfully")
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {str(e)}")
    
    def process_dlq(self, event):
        """Main DLQ processing function"""
        for record in event.get('Records', []):
            try:
                message = json.loads(record['body'])
                error_details = message.get('error_details', {})
                error_type = self.categorize_error(error_details.get("error_message", ""))
                
                self.handle_dlq_message(message, error_details)
                self.send_slack_alert(error_details, error_type)
                logger.info(f"Processed DLQ message: {message.get('record_id')}")
                
            except Exception as e:
                logger.error(f"Error processing DLQ: {str(e)}")
    
    def lambda_handler(self, event, context):
        """DLQ Lambda handler"""
        self.process_dlq(event)
        return {'statusCode': 200, 'body': 'DLQ processed'}
