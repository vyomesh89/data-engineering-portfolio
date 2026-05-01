# Deployment Guide

## AWS Lambda Deployment

### Step 1: Package Code
\`\`\`bash
cd aws-etl-pipeline/src
pip install -r requirements.txt -t package/
cp lambda_functions.py package/
zip -r ../lambda_function.zip .
\`\`\`

### Step 2: Deploy with CloudFormation
\`\`\`bash
aws cloudformation create-stack \
  --stack-name data-etl-pipeline \
  --template-body file://cloudformation/infrastructure.yaml \
  --capabilities CAPABILITY_IAM
\`\`\`

## Databricks Deployment

### Step 1: Create Cluster
\`\`\`bash
databricks clusters create --config cluster_config.json
\`\`\`

### Step 2: Upload Notebooks
\`\`\`bash
databricks workspace import_directory ./notebooks /ETL/notebooks --overwrite
\`\`\`

### Step 3: Create DLT Pipeline
\`\`\`bash
databricks pipelines create --name data-etl-pipeline --notebook /ETL/notebooks/data_ingestion
\`\`\`
