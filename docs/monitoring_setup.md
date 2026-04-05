# Monitoring Setup

## CloudWatch Configuration

### Custom Metrics
\`\`\`python
import boto3
cloudwatch = boto3.client('cloudwatch')

cloudwatch.put_metric_data(
    Namespace='DataETLPipeline',
    MetricData=[{
        'MetricName': 'SuccessfulRecords',
        'Value': 1000,
        'Unit': 'Count'
    }]
)
\`\`\`

## Slack Integration

### Create Webhook
1. Go to https://api.slack.com/apps
2. Create Incoming Webhook
3. Use webhook URL for alerts

## Key Alarms
- Error Rate > 5%: Critical
- Processing Time > 500ms: Warning
- DLQ Messages > 10: Investigate
