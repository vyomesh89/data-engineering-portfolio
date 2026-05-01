import json
import boto3
import logging
from datetime import datetime
from validation import DataValidator

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client('sqs')
s3 = boto3.client('s3')
cloudwatch = boto3.client('cloudwatch')

PROCESSED_BUCKET = 'processed-data-bucket'
DLQ_URL = 'https://sqs.region.amazonaws.com/account/dlq'

class LambdaETLHandler:
    """Main Lambda handler for ETL pipeline"""
    
    def __init__(self):
        self.validator = DataValidator()
        self.processed_count = 0
        self.failed_count = 0
    
    def process_message(self, message_body):
        """Process individual message"""
        try:
            data = json.loads(message_body)
            logger.info(f"Processing message: {data.get('id', 'unknown')}")
            
            is_valid, error = self.validator.validate_data_quality(data)
            if not is_valid:
                logger.error(f"Validation failed: {error}")
                return False, error
            
            processed_data = self.transform_data(data)
            self.save_to_s3(processed_data)
            
            self.processed_count += 1
            logger.info(f"Successfully processed message: {data.get('id')}")
            return True, None
            
        except json.JSONDecodeError as e:
            error = f"Invalid JSON format: {str(e)}"
            logger.error(error)
            return False, error
        except Exception as e:
            error = f"Processing error: {str(e)}"
            logger.error(error, exc_info=True)
            return False, error
    
    def transform_data(self, data):
        """Transform raw data"""
        data['processed_at'] = datetime.now().isoformat()
        data['version'] = '1.0'
        return data
    
    def save_to_s3(self, data):
        """Save processed data to S3"""
        key = f"processed/{data['id']}/{datetime.now().strftime('%Y-%m-%d')}.json"
        try:
            s3.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=key,
                Body=json.dumps(data)
            )
            logger.info(f"Saved to S3: {key}")
        except Exception as e:
            logger.error(f"S3 save failed: {str(e)}")
            raise
    
    def publish_metrics(self):
        """Publish metrics to CloudWatch"""
        try:
            cloudwatch.put_metric_data(
                Namespace='DataETLPipeline',
                MetricData=[
                    {'MetricName': 'SuccessfulRecords', 'Value': self.processed_count, 'Unit': 'Count'},
                    {'MetricName': 'FailedRecords', 'Value': self.failed_count, 'Unit': 'Count'}
                ]
            )
        except Exception as e:
            logger.error(f"Failed to publish metrics: {str(e)}")
    
    def handle_event(self, event):
        """Main Lambda handler"""
        logger.info(f"Processing {len(event.get('Records', []))} messages")
        
        for record in event.get('Records', []):
            try:
                message_body = record['body']
                success, error = self.process_message(message_body)
                
                if not success:
                    self.failed_count += 1
                    
                if success:
                    sqs.delete_message(
                        QueueUrl=record['eventSourceARN'],
                        ReceiptHandle=record['receiptHandle']
                    )
                    
            except Exception as e:
                logger.error(f"Error processing record: {str(e)}", exc_info=True)
                self.failed_count += 1
        
        self.publish_metrics()
        logger.info(f"Batch complete - Processed: {self.processed_count}, Failed: {self.failed_count}")

def lambda_handler(event, context):
    """AWS Lambda entry point"""
    try:
        handler = LambdaETLHandler()
        handler.handle_event(event)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'processed': handler.processed_count, 'failed': handler.failed_count})
        }
    except Exception as e:
        logger.error(f"Lambda handler error: {str(e)}", exc_info=True)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
