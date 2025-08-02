# Batch Processing Guide

This document explains how to use AWS Bedrock batch processing capabilities in the hybrid development environment for large-scale GraphRAG operations.

---

## Overview

Batch processing enables cost-effective, large-scale document processing using AWS Bedrock's batch inference capabilities. This is ideal for:

- **Large document collections** (hundreds to thousands of documents)
- **Cost optimization** (up to 50% savings compared to real-time processing)
- **Background processing** of non-urgent workloads
- **Scheduled processing** of regular document updates

---

## Batch Processing Architecture

### Components

1. **Local Jupyter Environment**: Development and job submission
2. **AWS S3**: Input/output file storage and job artifacts
3. **AWS Bedrock**: Batch inference processing
4. **AWS DynamoDB**: Job tracking and metadata (optional)
5. **IAM Roles**: Secure access control for batch operations

### Workflow

```
Documents → S3 Input → Bedrock Batch Job → S3 Output → Local Processing
```

---

## Prerequisites

### 1. AWS Infrastructure

Required AWS resources (created by `setup-bedrock-batch.sh`):

```bash
cd examples/lexical-graph-hybrid-dev/aws
./setup-bedrock-batch.sh
```

This creates:
- S3 bucket for batch processing
- DynamoDB table for job tracking
- IAM role with Bedrock batch permissions
- Required policies and trust relationships

### 2. Environment Configuration

Update your `.env` file with batch processing settings:

```bash
# Batch Processing Configuration
AWS_ACCOUNT="123456789012"
BATCH_ROLE_NAME="GraphRAGBatchRole"
S3_BATCH_BUCKET_NAME="your-batch-bucket"
DYNAMODB_NAME="graphrag-batch-jobs"

# Batch Settings
EXTRACTION_BATCH_SIZE=100        # Documents per batch
MAX_BATCH_SIZE=25000            # Maximum batch size
MAX_NUM_CONCURRENT_BATCHES=3    # Concurrent job limit
```

### 3. IAM Role Permissions

The batch role needs these permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "bedrock:CreateModelInvocationJob",
                "bedrock:GetModelInvocationJob",
                "bedrock:ListModelInvocationJobs"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-batch-bucket",
                "arn:aws:s3:::your-batch-bucket/*"
            ]
        }
    ]
}
```

---

## Enabling Batch Processing

### Basic Configuration

```python
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig
from graphrag_toolkit.lexical_graph import IndexingConfig, GraphRAGConfig

# Configure batch processing
batch_config = BatchConfig(
    region=os.environ["AWS_REGION"],
    bucket_name=os.environ["S3_BATCH_BUCKET_NAME"],
    key_prefix=os.environ["BATCH_PREFIX"],
    role_arn=f'arn:aws:iam::{os.environ["AWS_ACCOUNT"]}:role/{os.environ["BATCH_ROLE_NAME"]}'
)

# Set batch size (minimum 100 for Bedrock batch processing)
GraphRAGConfig.extraction_batch_size = int(os.environ.get("EXTRACTION_BATCH_SIZE", 100))

# Create indexing configuration
indexing_config = IndexingConfig(batch_config=batch_config)

# Create graph index with batch processing
graph_index = LexicalGraphIndex(
    graph_store,
    vector_store,
    indexing_config=indexing_config
)
```

### Document Processing with Batching

```python
from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs
from graphrag_toolkit.lexical_graph.indexing.build import Checkpoint

# Setup S3-based document storage
extracted_docs = S3BasedDocs(
    region=os.environ['AWS_REGION'],
    bucket_name=os.environ['S3_BATCH_BUCKET_NAME'],
    key_prefix="extracted-documents",
    collection_id='batch-processing-demo'
)

# Create checkpoint for progress tracking
checkpoint = Checkpoint('batch-extraction-checkpoint')

# Process documents with batch processing
docs = reader.read('path/to/large/document/collection')

# This will automatically use batch processing for large document sets
graph_index.extract(
    docs, 
    handler=extracted_docs, 
    checkpoint=checkpoint, 
    show_progress=True
)
```

---

## Batch Job Management

### Job Monitoring

```python
import boto3
import time

def monitor_batch_jobs(region, job_name_prefix="graphrag"):
    """Monitor Bedrock batch jobs."""
    bedrock = boto3.client('bedrock', region_name=region)
    
    # List recent jobs
    response = bedrock.list_model_invocation_jobs(
        nameContains=job_name_prefix,
        maxResults=10
    )
    
    for job in response['modelInvocationJobSummaries']:
        print(f"Job: {job['jobName']}")
        print(f"Status: {job['status']}")
        print(f"Created: {job['creationTime']}")
        if 'endTime' in job:
            print(f"Completed: {job['endTime']}")
        print("-" * 40)

# Monitor jobs
monitor_batch_jobs(os.environ['AWS_REGION'])
```

### Job Status Tracking

```python
def wait_for_batch_completion(job_name, region, check_interval=60):
    """Wait for batch job completion with progress updates."""
    bedrock = boto3.client('bedrock', region_name=region)
    
    while True:
        response = bedrock.get_model_invocation_job(jobIdentifier=job_name)
        status = response['status']
        
        print(f"Job {job_name} status: {status}")
        
        if status == 'Completed':
            print("✓ Batch job completed successfully")
            return True
        elif status == 'Failed':
            print("✗ Batch job failed")
            print(f"Failure reason: {response.get('failureMessage', 'Unknown')}")
            return False
        elif status in ['InProgress', 'Submitted']:
            print(f"Job in progress... checking again in {check_interval} seconds")
            time.sleep(check_interval)
        else:
            print(f"Unexpected status: {status}")
            return False
```

### Cost Tracking

```python
def estimate_batch_cost(num_documents, avg_tokens_per_doc=1000):
    """Estimate batch processing costs."""
    
    # Bedrock batch pricing (approximate)
    input_cost_per_1k_tokens = 0.003  # $3 per 1M tokens
    output_cost_per_1k_tokens = 0.015  # $15 per 1M tokens
    
    # Assume 1:1 input to output ratio for estimation
    total_input_tokens = num_documents * avg_tokens_per_doc
    total_output_tokens = total_input_tokens * 0.5  # Typical output ratio
    
    input_cost = (total_input_tokens / 1000) * input_cost_per_1k_tokens
    output_cost = (total_output_tokens / 1000) * output_cost_per_1k_tokens
    
    # Batch processing discount (approximately 50%)
    batch_discount = 0.5
    total_cost = (input_cost + output_cost) * batch_discount
    
    print(f"Estimated batch processing cost:")
    print(f"  Documents: {num_documents:,}")
    print(f"  Input tokens: {total_input_tokens:,}")
    print(f"  Output tokens: {total_output_tokens:,}")
    print(f"  Total cost: ${total_cost:.2f}")
    
    return total_cost
```

---

## Advanced Batch Configuration

### Custom Batch Settings

```python
# Fine-tune batch processing parameters
class CustomBatchConfig:
    def __init__(self):
        # Batch size optimization
        self.min_batch_size = 100          # Bedrock minimum
        self.optimal_batch_size = 500      # Cost-performance sweet spot
        self.max_batch_size = 25000        # Bedrock maximum
        
        # Concurrency control
        self.max_concurrent_jobs = 3       # Avoid throttling
        self.job_retry_attempts = 3        # Retry failed jobs
        
        # Timeout settings
        self.job_timeout_hours = 24        # Maximum job duration
        self.check_interval_seconds = 300  # Status check frequency

# Apply custom configuration
custom_config = CustomBatchConfig()
GraphRAGConfig.extraction_batch_size = custom_config.optimal_batch_size
```

### Conditional Batch Processing

```python
def smart_batch_processing(documents, threshold=50):
    """Use batch processing only for large document sets."""
    
    doc_count = len(documents)
    
    if doc_count >= threshold:
        print(f"Using batch processing for {doc_count} documents")
        
        # Configure for batch processing
        batch_config = BatchConfig(...)
        indexing_config = IndexingConfig(batch_config=batch_config)
        
        graph_index = LexicalGraphIndex(
            graph_store,
            vector_store,
            indexing_config=indexing_config
        )
    else:
        print(f"Using real-time processing for {doc_count} documents")
        
        # Use standard real-time processing
        graph_index = LexicalGraphIndex(graph_store, vector_store)
    
    return graph_index
```

### Batch Job Scheduling

```python
import schedule
import time

def schedule_batch_processing():
    """Schedule regular batch processing jobs."""
    
    def run_daily_batch():
        """Process new documents daily."""
        print("Starting daily batch processing...")
        
        # Get new documents from S3 or other source
        new_docs = get_new_documents_since_yesterday()
        
        if len(new_docs) >= 10:  # Only batch if enough documents
            graph_index = create_batch_enabled_index()
            graph_index.extract_and_build(new_docs, show_progress=True)
        else:
            print("Not enough documents for batch processing")
    
    def run_weekly_full_batch():
        """Full reprocessing weekly."""
        print("Starting weekly full batch processing...")
        all_docs = get_all_documents()
        graph_index = create_batch_enabled_index()
        graph_index.extract_and_build(all_docs, show_progress=True)
    
    # Schedule jobs
    schedule.every().day.at("02:00").do(run_daily_batch)
    schedule.every().sunday.at("01:00").do(run_weekly_full_batch)
    
    # Run scheduler
    while True:
        schedule.run_pending()
        time.sleep(3600)  # Check every hour
```

---

## Troubleshooting Batch Processing

### Common Issues

**Batch Size Too Small:**
```
Error: Not enough records to run batch extraction. List of nodes contains fewer records (87) than the minimum required by Bedrock (100)
```

**Solution:**
```python
# Increase batch size or accumulate more documents
GraphRAGConfig.extraction_batch_size = 100  # Minimum for Bedrock
```

**IAM Role Issues:**
```
Error: Cross-account pass role is not allowed
```

**Solution:**
```bash
# Verify role exists in correct account
aws iam get-role --role-name GraphRAGBatchRole --profile your-profile

# Check role ARN format in .env
BATCH_ROLE_NAME="GraphRAGBatchRole"  # Just the role name, not full ARN
```

**S3 Permission Errors:**
```
Error: Access Denied when uploading to S3
```

**Solution:**
```python
# Test S3 access
import boto3
s3 = boto3.client('s3')
s3.put_object(
    Bucket=os.environ['S3_BATCH_BUCKET_NAME'],
    Key='test-file.txt',
    Body=b'test content'
)
```

### Debugging Batch Jobs

```python
def debug_batch_job(job_name, region):
    """Debug failed batch jobs."""
    bedrock = boto3.client('bedrock', region_name=region)
    
    # Get job details
    response = bedrock.get_model_invocation_job(jobIdentifier=job_name)
    
    print(f"Job Name: {response['jobName']}")
    print(f"Status: {response['status']}")
    print(f"Model: {response['modelId']}")
    
    if response['status'] == 'Failed':
        print(f"Failure Message: {response.get('failureMessage', 'No message')}")
        
        # Check input/output locations
        print(f"Input S3: {response['inputDataConfig']['s3InputDataConfig']['s3Uri']}")
        print(f"Output S3: {response['outputDataConfig']['s3OutputDataConfig']['s3Uri']}")
        
        # List S3 objects to verify files exist
        s3 = boto3.client('s3')
        input_bucket = response['inputDataConfig']['s3InputDataConfig']['s3Uri'].split('/')[2]
        input_prefix = '/'.join(response['inputDataConfig']['s3InputDataConfig']['s3Uri'].split('/')[3:])
        
        objects = s3.list_objects_v2(Bucket=input_bucket, Prefix=input_prefix)
        print(f"Input files found: {objects.get('KeyCount', 0)}")
```

---

## Performance Optimization

### Batch Size Optimization

```python
def optimize_batch_size(document_count, target_job_duration_hours=2):
    """Calculate optimal batch size based on document count."""
    
    # Estimate processing time per document (minutes)
    avg_processing_time_per_doc = 0.5
    
    # Calculate optimal batch size
    target_duration_minutes = target_job_duration_hours * 60
    optimal_batch_size = int(target_duration_minutes / avg_processing_time_per_doc)
    
    # Ensure within Bedrock limits
    optimal_batch_size = max(100, min(optimal_batch_size, 25000))
    
    # Calculate number of jobs needed
    num_jobs = (document_count + optimal_batch_size - 1) // optimal_batch_size
    
    print(f"Optimization results:")
    print(f"  Documents: {document_count}")
    print(f"  Optimal batch size: {optimal_batch_size}")
    print(f"  Number of jobs: {num_jobs}")
    print(f"  Estimated duration per job: {target_job_duration_hours} hours")
    
    return optimal_batch_size
```

### Parallel Job Management

```python
import asyncio
import boto3

async def process_multiple_batches(document_batches, max_concurrent=3):
    """Process multiple batches concurrently."""
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_batch(batch_docs, batch_id):
        async with semaphore:
            print(f"Starting batch {batch_id} with {len(batch_docs)} documents")
            
            # Create batch-specific configuration
            batch_config = BatchConfig(
                region=os.environ["AWS_REGION"],
                bucket_name=os.environ["S3_BATCH_BUCKET_NAME"],
                key_prefix=f"batch-{batch_id}",
                role_arn=f'arn:aws:iam::{os.environ["AWS_ACCOUNT"]}:role/{os.environ["BATCH_ROLE_NAME"]}'
            )
            
            # Process batch
            graph_index = create_batch_index(batch_config)
            await asyncio.to_thread(
                graph_index.extract_and_build,
                batch_docs,
                show_progress=True
            )
            
            print(f"Completed batch {batch_id}")
    
    # Process all batches concurrently
    tasks = [
        process_batch(batch, i) 
        for i, batch in enumerate(document_batches)
    ]
    
    await asyncio.gather(*tasks)

# Usage
document_batches = split_documents_into_batches(all_documents, batch_size=500)
asyncio.run(process_multiple_batches(document_batches))
```

---

## Best Practices

### 1. Batch Size Selection
- **Minimum**: 100 documents (Bedrock requirement)
- **Optimal**: 500-1000 documents (cost-performance balance)
- **Maximum**: 25,000 documents (Bedrock limit)

### 2. Job Management
- Monitor job status regularly
- Implement retry logic for failed jobs
- Use meaningful job names for tracking
- Set appropriate timeouts

### 3. Cost Optimization
- Use batch processing for jobs with 100+ documents
- Monitor token usage and costs
- Schedule non-urgent jobs during off-peak hours
- Consider model selection based on requirements

### 4. Error Handling
- Implement comprehensive error handling
- Log job details for debugging
- Have fallback to real-time processing
- Monitor AWS service limits and quotas

### 5. Security
- Use least-privilege IAM roles
- Encrypt S3 buckets and objects
- Regularly rotate access keys
- Monitor access patterns and usage

This batch processing capability enables cost-effective, large-scale GraphRAG operations while maintaining the flexibility of the hybrid development environment.