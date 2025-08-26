[[Home](./)]

## Configuring Batch Extraction

### Topics

  - [Overview](#overview)
  - [BatchConfig parameters](#batchconfig-parameters)
    - [Required parameters](#required-parameters)
      - [bucket_name](#bucket_name)
      - [region](#region)
      - [role_arn](#role_arn)
    - [Optional parameters](#optional-parameters)
      - [key_prefix](#key_prefix)
      - [max_batch_size](#max_batch_size)
      - [max_num_concurrent_batches](#max_num_concurrent_batches)
      - [s3_encryption_key_id](#s3_encryption_key_id)
    - [VPC security parameters (optional)]
      - [subnet_ids](#subnet_ids)
      - [security_group_ids](#security_group_ids)
    - [File management](#file-management)
      - [delete_on_success](#delete_on_success)
  - [Optimizing batch extraction performance](#optimizing-batch-extraction-performance)

### Overview

### `BatchConfig` parameters

The `BatchConfig` object manages the configuration settings for Amazon Bedrock batch inference jobs. Here's a detailed explanation of each parameter:

#### Required parameters

##### `bucket_name`

You must specify the name of an Amazon S3 bucket where your batch processing files (both input and output) will be stored.

##### `region`

You need to provide the AWS Region name (such as "us-east-1") where both your S3 bucket is located and where the Amazon Bedrock batch inference job will run.

##### `role_arn`

This is the Amazon Resource Name (ARN) for the service role that handles batch inference operations. You can either create a default service role through the console or follow the instructions in the [Create a service role for batch inference](https://docs.aws.amazon.com/bedrock/latest/userguide/batch-iam-sr.html) documentation.

#### Optional parameters

##### `key_prefix` 

If desired, you can specify an S3 key prefix for organizing your input and output files.

##### `max_batch_size` 

Controls how many records (chunks) can be included in each batch inference job. The default value is `25000` records.

##### `max_num_concurrent_batches`

Determines how many batch inference jobs can run simultaneously per worker. This setting works in conjunction with `GraphRAGConfig.extraction_num_workers`. The default is `3` concurrent batches per worker.

##### `s3_encryption_key_id` 

You can provide the unique identifier for an encryption key to secure the output data in S3.

#### VPC security parameters (optional)

For more information about VPC protection, see [Protect batch inference jobs using a VPC](https://docs.aws.amazon.com/bedrock/latest/userguide/batch-vpc).

##### `subnet_ids` 

An array of subnet IDs within your Virtual Private Cloud (VPC) for protecting batch inference jobs.

##### `security_group_ids` 

An array of security group IDs within your VPC for protecting batch inference jobs.

#### File management

##### `delete_on_success` 

Controls whether input and output JSON files are automatically deleted from the local filesystem after successful batch job completion. By default, this is set to `True`. Note that this setting does not affect files stored in S3, which are preserved regardless.

### Optimizing batch extraction performance

The most important setting for controlling batch extraction performance are:

  - `GraphRAGConfig.extraction_batch_size`: Sets how many source documents go to the extraction pipeline. When calculating this value, consider that the total number of chunks (source documents × average chunks per document) should be sufficient to fill your planned simultaneous batch jobs.
  - `GraphRAGConfig.extraction_num_workers`: Sets how many CPUs run batch jobs simultaneously.
  - `BatchConfig.max_num_concurrent_batches`: Sets how many concurrent batch jobs each worker runs.
  - `BatchConfig.max_batch_size`: Sets the maximum number of chunks per batch job.

To maximize the efficiency of batch extraction, follow these three key principles:

  - **Maximize file capacity** Each batch job file can hold up to 50,000 records. However, Amazon Bedrock enforces input file size limits, typically between 1-5 GB. Check the specific limits for your model in the Amazon Bedrock service quotas section (see the **Batch inference job size** quotas in the [Amazon Bedrock service quotas section](https://docs.aws.amazon.com/general/latest/gr/bedrock.html#limits_bedrock ) for the limits particuar to the model you are using). Note that the toolkit doesn't automatically verify file sizes, so jobs may fail if they exceed these quotas. You may need to use fewer records than the maximum limit to stay within file size boundaries. Configure the `BatchConfig.max_batch_size` to set the maximum number of records per batch job.
  - **Use larger, fewer files** Focus on using a minimal number of large files rather than splitting the work across many smaller ones. For example, it's more efficient to process 40,000 records in a single job than to divide them into four parallel jobs of 10,000 records each.
  - **Leverage parallel processing** Take advantage of parallel job execution using `GraphRAGConfig.extraction_num_workers` and `BatchConfig.max_num_concurrent_batches`. The total number of jobs (number of workers × number of concurrent batches) must stay within Bedrock's quota of 20 combined in-progress and submitted batch inference jobs per region. If you exceed this limit, additional jobs will wait in the queue until capacity becomes available.
