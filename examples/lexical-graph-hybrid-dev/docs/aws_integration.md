# AWS Integration Guide

This document explains how to integrate the hybrid development environment with AWS services for cloud-scale GraphRAG processing.

---

## Overview

The hybrid development environment combines local Docker services with AWS cloud capabilities, enabling:

- **Local Development**: Fast iteration with Jupyter Lab and local databases
- **Cloud Processing**: Scalable document processing with AWS Bedrock
- **Cloud Storage**: Centralized document and data storage with S3
- **Batch Operations**: Large-scale processing with Bedrock batch inference

---

## AWS Services Used

### Amazon Bedrock
- **Purpose**: LLM processing for extraction and generation
- **Models**: Claude 3.5 Sonnet, Cohere embeddings
- **Features**: Batch processing, prompt management
- **Cost**: Pay-per-token usage

### Amazon S3
- **Purpose**: Document storage and batch processing
- **Features**: Streaming large files, batch input/output storage
- **Integration**: Direct S3 URL support in readers
- **Cost**: Storage and data transfer charges

### Amazon DynamoDB (Optional)
- **Purpose**: Batch job tracking and metadata
- **Features**: Job status monitoring, progress tracking
- **Cost**: Minimal for job tracking use case

---

## Prerequisites

### 1. AWS Account Setup

```bash
# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Configure credentials
aws configure --profile your-profile
# Enter: Access Key ID, Secret Access Key, Region, Output format
```

### 2. Bedrock Model Access

Enable required models in the [Bedrock console](https://console.aws.amazon.com/bedrock/home#/modelaccess):

- `anthropic.claude-3-5-sonnet-20240620-v1:0`
- `cohere.embed-english-v3`

### 3. S3 Bucket Creation

```bash
# Create S3 bucket for GraphRAG data
aws s3 mb s3://your-graphrag-bucket --profile your-profile

# Verify bucket creation
aws s3 ls --profile your-profile
```

### 4. IAM Permissions

Your AWS user/role needs permissions for:
- Bedrock model invocation
- S3 read/write access
- DynamoDB access (if using batch tracking)

---

## Environment Configuration

### Update .env File

```bash
# AWS Configuration
AWS_REGION="us-east-1"
AWS_PROFILE="your-profile"
AWS_ACCOUNT="123456789012"

# S3 Storage
S3_BUCKET_EXTRACK_BUILD_BATCH_NAME="your-graphrag-bucket"
S3_BATCH_BUCKET_NAME="your-graphrag-bucket"

# Bedrock Models
EXTRACTION_MODEL="us.anthropic.claude-3-5-sonnet-20240620-v1:0"
EMBEDDINGS_MODEL="cohere.embed-english-v3"

# Batch Processing (Optional)
BATCH_ROLE_NAME="GraphRAGBatchRole"
DYNAMODB_NAME="graphrag-batch-jobs"
```

### AWS Credentials Mounting

The Docker environment automatically mounts your AWS credentials:

```yaml
# In docker-compose.yml
volumes:
  - ~/.aws:/home/jovyan/.aws  # AWS credentials
```

---

## S3 Integration

### Direct S3 URL Support

All reader providers support S3 URLs directly:

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import PDFReaderProvider, PDFReaderConfig

# Works with local files
docs = reader.read('/local/path/document.pdf')

# Also works with S3 URLs
docs = reader.read('s3://your-bucket/documents/document.pdf')

# Mix local and S3 files
docs = reader.read([
    '/local/path/doc1.pdf',
    's3://your-bucket/doc2.pdf',
    's3://your-bucket/folder/doc3.pdf'
])
```

### S3 Streaming for Large Files

Configure automatic streaming for large S3 files:

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import StructuredDataReaderConfig

config = StructuredDataReaderConfig(
    stream_s3=True,              # Enable S3 streaming
    stream_threshold_mb=100,     # Stream files larger than 100MB
    pandas_config={"sep": ","}
)
```

### S3-Based Document Storage

Use S3 for extracted document storage:

```python
from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs

extracted_docs = S3BasedDocs(
    region=os.environ['AWS_REGION'],
    bucket_name=os.environ['S3_BUCKET_EXTRACK_BUILD_BATCH_NAME'],
    key_prefix="extracted-documents",
    collection_id='my-collection'
)

# Documents are automatically stored in S3
graph_index.extract(docs, handler=extracted_docs, show_progress=True)
```

---

## Bedrock Integration

### Basic Bedrock Usage

The environment automatically uses Bedrock for LLM processing:

```python
# Bedrock is used automatically for extraction
graph_index.extract_and_build(docs, show_progress=True)

# Models configured in .env are used:
# - EXTRACTION_MODEL for text processing
# - EMBEDDINGS_MODEL for vector generation
```

### Batch Processing

For large-scale processing, enable Bedrock batch inference:

```python
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig
from graphrag_toolkit.lexical_graph import IndexingConfig

# Configure batch processing
batch_config = BatchConfig(
    region=os.environ["AWS_REGION"],
    bucket_name=os.environ["S3_BUCKET_EXTRACK_BUILD_BATCH_NAME"],
    key_prefix=os.environ["BATCH_PREFIX"],
    role_arn=f'arn:aws:iam::{os.environ["AWS_ACCOUNT"]}:role/{os.environ["BATCH_ROLE_NAME"]}'
)

indexing_config = IndexingConfig(batch_config=batch_config)

# Create index with batch processing
graph_index = LexicalGraphIndex(
    graph_store,
    vector_store,
    indexing_config=indexing_config
)
```

### Prompt Management with Bedrock

Use Bedrock's prompt management service:

```python
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import BedrockPromptProviderConfig

# Configure Bedrock prompt provider
prompt_provider = BedrockPromptProviderConfig(
    aws_region="us-east-1",
    aws_profile="your-profile",
    system_prompt_arn="KEOXPXUM00",  # Your prompt ARN
    user_prompt_arn="TSF4PI4A6C",
    system_prompt_version="1",
    user_prompt_version="1"
).build()

# Use with query engine
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    streaming=True,
    prompt_provider=prompt_provider
)
```

---

## Setup Automation

### AWS Infrastructure Setup

Use the provided setup scripts:

```bash
cd examples/lexical-graph-hybrid-dev/aws

# Create S3 buckets, DynamoDB tables, and IAM roles
./setup-bedrock-batch.sh

# Create Bedrock prompts
./create_custom_prompt.sh

# Setup IAM roles for prompt access
./create_prompt_role.sh
```

### Verification Script

Verify your AWS setup:

```python
import boto3
import os

def verify_aws_setup():
    """Verify AWS configuration and access."""
    
    # Check credentials
    session = boto3.Session(profile_name=os.environ.get('AWS_PROFILE'))
    sts = session.client('sts')
    identity = sts.get_caller_identity()
    print(f"AWS Account: {identity['Account']}")
    print(f"User/Role: {identity['Arn']}")
    
    # Check S3 bucket
    s3 = session.client('s3')
    bucket = os.environ['S3_BUCKET_EXTRACK_BUILD_BATCH_NAME']
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"S3 Bucket '{bucket}': ✓ Accessible")
    except Exception as e:
        print(f"S3 Bucket '{bucket}': ✗ Error - {e}")
    
    # Check Bedrock access
    bedrock = session.client('bedrock')
    try:
        models = bedrock.list_foundation_models()
        print(f"Bedrock Models: ✓ {len(models['modelSummaries'])} available")
    except Exception as e:
        print(f"Bedrock Access: ✗ Error - {e}")

# Run verification
verify_aws_setup()
```

---

## Cost Management

### Understanding Costs

**Bedrock Costs:**
- **Input tokens**: ~$3 per 1M tokens (Claude 3.5 Sonnet)
- **Output tokens**: ~$15 per 1M tokens
- **Embeddings**: ~$0.10 per 1M tokens (Cohere)

**S3 Costs:**
- **Storage**: ~$0.023 per GB/month
- **Requests**: ~$0.0004 per 1K requests
- **Data transfer**: Free within same region

**DynamoDB Costs:**
- **On-demand**: ~$1.25 per million writes
- **Storage**: ~$0.25 per GB/month

### Cost Optimization

```python
# 1. Use batch processing for large datasets
batch_config = BatchConfig(...)  # 50% cost reduction for large jobs

# 2. Enable S3 streaming to avoid local storage
config = StructuredDataReaderConfig(stream_s3=True)

# 3. Monitor token usage
def track_token_usage(response):
    if hasattr(response, 'usage'):
        print(f"Tokens used: {response.usage}")

# 4. Use appropriate batch sizes
GraphRAGConfig.extraction_batch_size = 50  # Balance cost vs speed
```

### Cost Monitoring

```python
import boto3
from datetime import datetime, timedelta

def get_bedrock_costs(days=7):
    """Get Bedrock costs for the last N days."""
    ce = boto3.client('ce')
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': start_date.strftime('%Y-%m-%d'),
            'End': end_date.strftime('%Y-%m-%d')
        },
        Granularity='DAILY',
        Metrics=['BlendedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
    )
    
    for result in response['ResultsByTime']:
        for group in result['Groups']:
            if 'Bedrock' in group['Keys'][0]:
                cost = group['Metrics']['BlendedCost']['Amount']
                print(f"Bedrock cost: ${float(cost):.2f}")
```

---

## Troubleshooting

### Common AWS Issues

**Credentials Not Found:**
```bash
# Check AWS configuration
aws sts get-caller-identity --profile your-profile

# Verify credentials are mounted in container
docker exec -it jupyter-hybrid ls -la /home/jovyan/.aws
```

**Bedrock Access Denied:**
```python
# Check model access in Bedrock console
# Ensure your region supports the models
# Verify IAM permissions for bedrock:InvokeModel
```

**S3 Permission Errors:**
```python
# Test S3 access
import boto3
s3 = boto3.client('s3')
s3.list_objects_v2(Bucket='your-bucket', MaxKeys=1)
```

**Batch Processing Failures:**
```python
# Check IAM role exists and has proper trust relationship
# Verify role ARN format in .env file
# Ensure role has bedrock:CreateModelInvocationJob permission
```

### Performance Issues

**Slow S3 Access:**
- Use same AWS region for S3 bucket and processing
- Enable S3 streaming for large files
- Consider S3 Transfer Acceleration

**High Bedrock Costs:**
- Use batch processing for large datasets
- Monitor token usage and optimize prompts
- Consider model selection (Claude vs other models)

**Network Timeouts:**
- Increase timeout settings for large files
- Use retry logic for transient failures
- Monitor AWS service health status

---

## Best Practices

### 1. Security
- Use IAM roles with minimal required permissions
- Enable S3 bucket encryption
- Rotate AWS access keys regularly
- Use AWS CloudTrail for audit logging

### 2. Performance
- Use same AWS region for all services
- Enable S3 streaming for files > 100MB
- Batch process multiple documents together
- Monitor and optimize token usage

### 3. Cost Control
- Set up AWS billing alerts
- Use batch processing for large jobs
- Monitor usage with CloudWatch
- Regular cost reviews and optimization

### 4. Reliability
- Implement retry logic for AWS API calls
- Use checkpointing for long-running jobs
- Monitor AWS service health
- Have fallback strategies for service outages

This AWS integration enables powerful cloud-scale GraphRAG processing while maintaining the flexibility of local development.