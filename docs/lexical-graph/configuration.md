[[Home](./)]

## Configuration

### Topics

  - [Overview](#overview)
  - [GraphRAGConfig](#graphragconfig)
    - [LLM configuration](#llm-configuration)
    - [Embedding model configuration](#embedding-model-configuration)
    - [Batch writes](#batch-writes)
    - [Caching Amazon Bedrock LLM responses](#caching-amazon-bedrock-llm-responses)
  - [Logging configuration](#logging-configuration)
  - [AWS profile configuration](#aws-profile-configuration)

### Overview

The lexical-graph provides a `GraphRAGConfig` object that allows you to configure the LLMs and embedding models used by the indexing and retrieval processes, as well as the parallel and batch processing behaviours of the indexing pipelines. (The lexical-graph doesn't use the LlamaIndex `Settings` object: attributes configured in `Settings` will have no impact in the graphrag-toolkit.)

The lexical-graph also allows you to set the logging level and apply logging filters from within your application.

### GraphRAGConfig

`GraphRAGConfig` allows you to configure LLMs, embedding models, and the extract and build processes. The configuration includes the following parameters:

| Parameter  | Description | Default Value | Environment Variable |
| ------------- | ------------- | ------------- | ------------- |
| `extraction_llm` | LLM used to perform graph extraction (see [LLM configuration](#llm-configuration)) | `us.anthropic.claude-3-5-sonnet-20240620-v1:0` | `EXTRACTION_MODEL` |
| `response_llm` | LLM used to generate responses (see [LLM configuration](#llm-configuration)) | `us.anthropic.claude-3-5-sonnet-20240620-v1:0` | `RESPONSE_MODEL` |
| `embed_model` | Embedding model used to generate embeddings for indexed data and queries (see [Embedding model configuration](#embedding-model-configuration)) | `cohere.embed-english-v3` | `EMBEDDINGS_MODEL` |
| `embed_dimensions` | Number of dimensions in each vector | `1024` | `EMBEDDINGS_DIMENSIONS` |
| `extraction_num_workers` | The number of parallel processes to use when running the extract stage | `2` | `EXTRACTION_NUM_WORKERS` |
| `extraction_num_threads_per_worker` | The number of threads used by each process in the extract stage | `4` | `EXTRACTION_NUM_THREADS_PER_WORKER` |
| `extraction_batch_size` | The number of input nodes to be processed in parallel across all workers in the extract stage | `4` | `EXTRACTION_BATCH_SIZE` |
| `build_num_workers` | The number of parallel processes to use when running the build stage | `2` | `BUILD_NUM_WORKERS` |
| `build_batch_size` | The number of input nodes to be processed in parallel across all workers in the build stage | `4` | `BUILD_BATCH_SIZE` |
| `build_batch_write_size` | The number of elements to be written in a bulk operation to the graph and vector stores (see [Batch writes](#batch-writes)) | `25` | `BUILD_BATCH_WRITE_SIZE` |
| `batch_writes_enabled` | Determines whether, on a per-worker basis, to write all elements (nodes and edges, or vectors) emitted by a batch of input nodes as a bulk operation, or singly, to the graph and vector stores (see [Batch writes](#batch-writes)) | `True` | `BATCH_WRITES_ENABLED` |
| `include_domain_labels` | Determines whether entities will have a domain-specific label (e.g. `Company`) as well as the [graph model's](./graph-model.md#entity-relationship-tier) `__Entity__` label | `False` | `DEFAULT_INCLUDE_DOMAIN_LABELS` |
| `enable_cache` | Determines whether the results of LLM calls to models on Amazon Bedrock are cached to the local filesystem (see [Caching Amazon Bedrock LLM responses](#caching-amazon-bedrock-llm-responses)) | `False` | `ENABLE_CACHE` |
| `aws_profile` | AWS CLI named profile used to authenticate requests to Bedrock and other services | *None* | `AWS_PROFILE` |
| `aws_region` | AWS region used to scope Bedrock service calls | `us-east-1` | `AWS_REGION` |
To set a configuration parameter in your application code:

```python
from graphrag_toolkit.lexical_graph import GraphRAGConfig

GraphRAGConfig.response_llm = 'anthropic.claude-3-haiku-20240307-v1:0' 
GraphRAGConfig.extraction_num_workers = 4
```

You can also set configuration parameters via environment variables, as per the variable names in the table above.

#### LLM configuration

The `extraction_llm` and `response_llm` configuration parameters accept three different types of value:

  - You can pass an instance of a LlamaIndex `LLM` object. This allows you to configure the lexical-graph for LLM backends other than Amazon Bedrock.
  - You can pass the model id of an Amazon Bedrock model or [inference profile](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-profiles.html). For example: `anthropic.claude-3-5-sonnet-20240620-v1:0` (model id) or `us.anthropic.claude-3-5-sonnet-20240620-v1:0` (inference profile).
  - You can pass a JSON string representation of a LlamaIndex `BedrockConverse` instance. For example:
  
  ```
  {
    "model": "anthropic.claude-3-5-sonnet-20240620-v1:0",
    "temperature": 0.0,
    "max_tokens": 4096,
    "streaming": true
  }
  ```
  
#### Embedding model configuration

The `embed_model` configuration parameter accepts three different types of value:

  - You can pass an instance of a LlamaIndex `BaseEmbedding` object. This allows you to configure the lexical-graph for embedding backends other than Amazon Bedrock.
  - You can pass the model name of an Amazon Bedrock model. For example: `amazon.titan-embed-text-v1`.
  - You can pass a JSON string representation of a LlamaIndex `Bedrock` instance. For example:
  
  ```
  {
    "model_name": "amazon.titan-embed-text-v2:0",
    "dimensions": 512
  }
  ```
  
When configuring an embedding model, you must also set the `embed_dimensions` configuration parameter.

#### Batch writes

The lexical-graph uses microbatching to progress source data through the extract and build stages.

  - In the extract stage a batch of source nodes is processed in parallel by one or more workers, with each worker performing chunking, proposition extraction and topic/statement/fact/entity extraction over its allocated source nodes. For a given batch of source nodes, the extract stage emits a collection of chunks derived from those source nodes.
  - In the build stage, chunks from the extract stage are broken down into smaller *indexable* nodes representing sources, chunks, topics, statements and facts. These indexable nodes are then processed by the graph construction and vector indexing handlers.

The `batch_writes_enabled` configuration parameter determines whether all of the indexable nodes derived from a batch of incoming chunks are written to the graph and vector stores singly, or as a bulk operation. Bulk/batch operations tend to improve the throughput of the build stage, at the expense of some additonal latency with regard to this data becoming available to query.

#### Caching Amazon Bedrock LLM responses

If you're using Amazon Bedrock, you can use the local filesystem to cache and reuse LLM responses. Set `GraphRAGoOnfig.enable_cache` to `True`. LLM responses will then be saved in clear text to a `cache` directory. Subsequent invocations of the same model with the exact same prompt will return the cached response.

The `cache` directory can grow very large, particularly if you are caching extraction responses for a very large ingest. The lexical-graph will not manage the size of this directory or delete old entries. If you enable the cache, ensure you clear or prune the cache directory regularly.

### Logging configuration

The graphrag_toolkit's `set_logging_config` method allows you to set the [logging level](https://docs.python.org/3/library/logging.html#logging-levels), and apply filters to `DEBUG` log lines. Besides the logging level, you can supply an array of prefixes to include when outputting debug information, and an array of prefixes to exclude.

The following example sets the logging level to `DEBUG`, but also applies a filter that specifies that only messages from the storage module are to be emitted:

```python
from graphrag_toolkit.lexical_graph import set_logging_config

set_logging_config(
  'DEBUG', 
  ['graphrag_toolkit.lexical_graph.storage']
)
```

The following example sets the logging level to `DEBUG`, together with a filter that specifies that only messages from the storage module are to be emitted, and another filter that excludes messages from the graph store factory within the storage module:

```python
from graphrag_toolkit.lexical_graph import set_logging_config

set_logging_config(
  'DEBUG', 
  ['graphrag_toolkit.lexical_graph.storage'],
  ['graphrag_toolkit.lexical_graph.storage.graph_store_factory']
)
```
### AWS profile configuration

You can explicitly configure the AWS CLI profile and region to use when initializing Bedrock clients or other AWS service clients in `GraphRAGConfig`. This ensures compatibility across local development, EC2/ECS environments, or federated environments such as AWS SSO.

You may set the AWS profile and region in your application code:

```python
from graphrag_toolkit.lexical_graph import GraphRAGConfig

GraphRAGConfig.aws_profile = 'padmin'
GraphRAGConfig.aws_region = 'us-east-1'
```

Alternatively, use environment variables:

```bash
export AWS_PROFILE=padmin
export AWS_REGION=us-east-1
```

If no profile or region is set explicitly, the system will fall back to environment variables or use the default AWS CLI configuration.

See [Using AWS Profiles in `GraphRAGConfig`](./aws-profile.md) for more details on configuring and using **AWS named profiles** in the lexical-graph by leveraging the `GraphRAGConfig` class.
