import json
import boto3
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    logger.info('Received event: %s', json.dumps(event))

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        try:
            # Process S3 object (example: read text file)
            response = s3_client.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read().decode('utf-8')
            logger.info('Successfully processed file: %s', key)
            # Add your processing logic here

        except ClientError as e:
            logger.error('ClientError: %s', e)
            raise e  # Raise an error for retry logic
        except Exception as e:
            logger.error('Processing error: %s', e)
            raise e  # Raise error for retry logic

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete!')
    }
