# AWS ETL Pipeline Architecture

## Overview
This document explains the architecture of an AWS ETL (Extract, Transform, Load) pipeline that utilizes Amazon S3, Amazon SQS, and AWS Lambda to efficiently handle data workflows.

## Architecture Components
1. **Amazon S3 (Simple Storage Service)**
   - Used to store raw data (e.g., CSV, JSON files) that is ingested from various sources.
   - Provides a scalable and durable storage solution.

2. **Amazon SQS (Simple Queue Service)**
   - Acts as a messaging queue to decouple the services in the pipeline.
   - Ensures that messages (or events) are held until processed by the consumers.

3. **AWS Lambda**
   - Serverless compute service that allows you to run code without provisioning servers.
   - Can be triggered by events from S3 or messages from SQS.

## Event Flow Diagram
```plaintext
 +-----------+  PUT object  +----------------+  Send Message  +------------+
 |   Data    |-------------->|     Amazon      |--------------->|   Amazon   |
 |   Source   |              |        S3       |                |    SQS    |
 +-----------+  +-------+   +----------------+                +------------+
               |       |      |  Event Triggered |  Pull Message   +------------+
               |       |      +----------------
               |       |       |  Trigger Lambda  |
               |       |<-----------------------------+  +------------+
               |       |  Lambda Function  |  +----->  |  AWS Lambda |
               +----------------+          |          |            |
               |  Process Data  |          +----------+            |
               |                 |  +----------------+             |
               |                 |  |  Output to     |             |
               |                 |  |  Another S3    |-------------+
               |                 |  |  Bucket /       |
               |                 |  |  Database       |
               +-----------------+  +----------------+
