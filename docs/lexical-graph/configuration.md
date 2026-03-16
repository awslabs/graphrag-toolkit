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

`GraphRAGConfig` is a module-level singleton (not a class to instantiate). It is created once at import time ([`config.py`](../../lexical-graph/src/graphrag_toolkit/lexical_graph/config.py#L1171)) and shared across the process. Set attributes directly on the imported object:

```python
from graphrag_toolkit.lexical_graph import GraphRAGConfig

GraphRAGConfig.aws_region = 'eu-west-1'
GraphRAGConfig.extraction_llm = 'anthropic.claude-3-5-sonnet-20241022-v2:0'
```

Setting `aws_profile` or `aws_region` automatically clears all cached boto3 clients.

**Important**: Change configuration values early in your code, before creating any graph store or vector store.

The configuration includes the following parameters:

| Parameter  | Description | Default Value | Environment Variable |
| ------------- | ------------- | ------------- | ------------- |
| `extraction_llm` | LLM used to perform graph extraction (see [LLM configuration](#llm-configuration)) | `us.anthropic.claude-3-7-sonnet-20250219-v1:0` | `EXTRACTION_MODEL` |
| `response_llm` | LLM used to generate responses (see [LLM configuration](#llm-configuration)) | `us.anthropic.claude-3-7-sonnet-20250219-v1:0` | `RESPONSE_MODEL` |
| `embed_model` | Embedding model used to generate embeddings for indexed data and queries (see [Embedding model configuration](#embedding-model-configuration)) | `cohere.embed-english-v3` | `EMBEDDINGS_MODEL` |
| `embed_dimensions` | Number of dimensions in each vector | `1024` | `EMBEDDINGS_DIMENSIONS` |
| `extraction_num_workers` | The number of parallel processes to use when running the extract stage | `2` | `EXTRACTION_NUM_WORKERS` |
| `extraction_num_threads_per_worker` | The number of threads used by each process in the extract stage | `4` | `EXTRACTION_NUM_THREADS_PER_WORKER` |
| `extraction_batch_size` | The number of input nodes to be processed in parallel across all workers in the extract stage | `4` | `EXTRACTION_BATCH_SIZE` |
| `build_num_workers` | The number of parallel processes to use when running the build stage | `2` | `BUILD_NUM_WORKERS` |
| `build_batch_size` | The number of input nodes to be processed in parallel across all workers in the build stage | `4` | `BUILD_BATCH_SIZE` |
| `build_batch_write_size` | The number of elements to be written in a bulk operation to the graph and vector stores (see [Batch writes](#batch-writes)) | `25` | `BUILD_BATCH_WRITE_SIZE` |
| `batch_writes_enabled` | Determines whether, on a per-worker basis, to write all elements (nodes and edges, or vectors) emitted by a batch of input nodes as a bulk operation, or singly, to the graph and vector stores (see [Batch writes](#batch-writes)) | `True` | `BATCH_WRITES_ENABLED` |
| `include_domain_labels` | Determines whether entities will have a domain-specific label (e.g. `Company`) as well as the [graph model's](./graph-model.md#entity-relationship-tier) `__Entity__` label | `False` | `INCLUDE_DOMAIN_LABELS` |
| `include_local_entities` | Whether to include local-context entities in the graph | `False` | `INCLUDE_LOCAL_ENTITIES` |
| `include_classification_in_entity_id` | Whether to include an entity's classification in its graph node id | `True` | `INCLUDE_CLASSIFICATION_IN_ENTITY_ID` |
| `enable_versioning` | Whether to enable versioned updates (see [Versioned Updates](./versioned-updates.md)) | `False` | `ENABLE_VERSIONING` |
| `enable_cache` | Determines whether the results of LLM calls to models on Amazon Bedrock are cached to the local filesystem (see [Caching Amazon Bedrock LLM responses](#caching-amazon-bedrock-llm-responses)) | `False` | `ENABLE_CACHE` |
| `aws_profile` | AWS CLI named profile used to authenticate requests to Bedrock and other services | *None* | `AWS_PROFILE` |
| `aws_region` | AWS region used to scope Bedrock service calls | *Default boto3 session region* | `AWS_REGION` |

The following parameters configure the rerankers used by query retrievers:

| Parameter | Description | Default | Environment Variable |
| ------------- | ------------- | ------------- | ------------- |
| `reranking_model` | Local reranker model (mixedbread-ai) | `mixedbread-ai/mxbai-rerank-xsmall-v1` | `RERANKING_MODEL` |
| `bedrock_reranking_model` | Amazon Bedrock reranker model | `cohere.rerank-v3-5:0` | `BEDROCK_RERANKING_MODEL` |

The following parameter applies only when using Amazon OpenSearch Serverless as a vector store:

| Parameter | Description | Default | Environment Variable |
| ------------- | ------------- | ------------- | ------------- |
| `opensearch_engine` | OpenSearch kNN engine | `nmslib` | `OPENSEARCH_ENGINE` |

The following parameters configure local filesystem paths for container/EKS deployments:

| Parameter | Description | Default | Environment Variable |
| ------------- | ------------- | ------------- | ------------- |
| `local_output_dir` | Local staging directory for batch files and temporary extraction outputs | `output` | `LOCAL_OUTPUT_DIR` |
| `log_output_dir` | Directory prefix for log files (when filename is relative) | *None* | `LOG_OUTPUT_DIR` |

To set a configuration parameter in your application code:

```python
from graphrag_toolkit.lexical_graph import GraphRAGConfig

GraphRAGConfig.response_llm = 'anthropic.claude-3-haiku-20240307-v1:0'
GraphRAGConfig.extraction_num_workers = 4
```

You can also set any of these via environment variables using the variable names in the tables above.

#### LLM configuration

The `extraction_llm` and `response_llm` configuration parameters accept three different types of value:

  - You can pass an instance of a LlamaIndex `LLM` object. This allows you to configure the lexical-graph for LLM backends other than Amazon Bedrock.
  - You can pass the model id of an Amazon Bedrock model or [inference profile](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-profiles.html). For example: `anthropic.claude-3-7-sonnet-20250219-v1:0` (model id) or `us.anthropic.claude-3-7-sonnet-20250219-v1:0` (inference profile).
  - You can pass a JSON string representation of a LlamaIndex `BedrockConverse` instance. For example:
  
  ```
  {
    "model": "anthropic.claude-3-7-sonnet-20250219-v1:0",
    "temperature": 0.0,
    "max_tokens": 4096
  }
  ```
  
#### Embedding model configuration

The `embed_model` configuration parameter accepts three different types of value:

  - You can pass an instance of a LlamaIndex `BaseEmbedding` object. This allows you to configure the lexical-graph for embedding backends other than Amazon Bedrock.
  - You can pass the model name of an Amazon Bedrock model. For example: `amazon.titan-embed-text-v1`.
  - You can pass a JSON string representation of a LlamaIndex `BedrockEmbedding` instance. For example:
  
  ```
  {
    "model_name": "amazon.titan-embed-text-v2:0"
  }
  ```

When configuring an embedding model, you must also set the `embed_dimensions` configuration parameter to match the model's output dimensions. For example:

```python
GraphRAGConfig.embed_model = '{"model_name": "amazon.titan-embed-text-v2:0"}'
GraphRAGConfig.embed_dimensions = 512
```

##### Nova 2 Multimodal Embeddings

Amazon Nova 2 multimodal embedding models (`amazon.nova-2-multimodal-embeddings-v1:0`) use a different API format than standard Bedrock embedding models. To use Nova 2 models, you must explicitly import and instantiate the `Nova2MultimodalEmbedding` class.

**Usage:**

```python
from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils.bedrock_utils import Nova2MultimodalEmbedding

GraphRAGConfig.embed_model = Nova2MultimodalEmbedding('amazon.nova-2-multimodal-embeddings-v1:0')
GraphRAGConfig.embed_dimensions = 3072
```

**API Format Differences:**

Standard Bedrock embeddings (Titan, Cohere) use:
```json
{"inputText": "text to embed"}
```

Nova 2 multimodal embeddings require:
```json
{
  "taskType": "SINGLE_EMBEDDING",
  "singleEmbeddingParams": {
    "embeddingDimension": 3072,
    "embeddingPurpose": "TEXT_RETRIEVAL",
    "text": {
      "truncationMode": "END",
      "value": "text to embed"
    }
  }
}
```

**Configuration Parameters:**

| Parameter | Description | Default | Valid Values |
| --------- | ----------- | ------- | ------------ |
| `embed_dimensions` | Vector dimensions | `3072` | `1024`, `3072` |
| `embed_purpose` | Embedding optimization purpose | `TEXT_RETRIEVAL` | `TEXT_RETRIEVAL`, `GENERIC_RETRIEVAL`, `DOCUMENT_RETRIEVAL`, `CLASSIFICATION`, `CLUSTERING` |
| `truncation_mode` | How to handle text exceeding max length | `END` | `END`, `NONE` |

**Advanced Configuration:**

To configure Nova 2 multimodal embeddings with custom parameters:

```python
from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils.bedrock_utils import Nova2MultimodalEmbedding

embedding = Nova2MultimodalEmbedding(
    model_name='amazon.nova-2-multimodal-embeddings-v1:0',
    embed_dimensions=3072,
    embed_purpose='TEXT_RETRIEVAL',
    truncation_mode='END'
)

GraphRAGConfig.embed_model = embedding
GraphRAGConfig.embed_dimensions = 3072
```

**Features:**
- Handles Nova 2's unique API format automatically
- Includes retry logic for transient Bedrock errors
- Custom pickle support for multiprocessing scenarios
- Lazy client initialization using GraphRAGConfig.session
- Empty text validation to prevent API errors

#### Batch writes

The lexical-graph uses microbatching to progress source data through the extract and build stages.

  - In the extract stage a batch of source nodes is processed in parallel by one or more workers, with each worker performing chunking, proposition extraction and topic/statement/fact/entity extraction over its allocated source nodes. For a given batch of source nodes, the extract stage emits a collection of chunks derived from those source nodes.
  - In the build stage, chunks from the extract stage are broken down into smaller *indexable* nodes representing sources, chunks, topics, statements and facts. These indexable nodes are then processed by the graph construction and vector indexing handlers.

The `batch_writes_enabled` configuration parameter determines whether all of the indexable nodes derived from a batch of incoming chunks are written to the graph and vector stores singly, or as a bulk operation. Bulk/batch operations tend to improve the throughput of the build stage, at the expense of some additonal latency with regard to this data becoming available to query.

#### Caching Amazon Bedrock LLM responses

If you're using Amazon Bedrock, you can use the local filesystem to cache and reuse LLM responses. Set `GraphRAGConfig.enable_cache` to `True`. LLM responses will then be saved in clear text to a `cache` directory. Subsequent invocations of the same model with the exact same prompt will return the cached response.

Note that streaming responses from the query engine are _not_ cached.

The `cache` directory can grow very large, particularly if you are caching extraction responses for a very large ingest. The lexical-graph will not manage the size of this directory or delete old entries. If you enable the cache, ensure you clear or prune the cache directory regularly.

### Logging configuration

The `graphrag_toolkit` provides two methods for configuring logging in your application. These methods allow you to set logging levels, apply filters to include or exclude specific modules or messages, and customize logging behavior:

- `set_logging_config`
- `set_advanced_logging_config`

#### set_logging_config

The `set_logging_config` method allows you to configure logging with a basic set of options, such as logging level and module filters. Wildcards are supported for module names, and you can pass either a single string or a list of strings for included or excluded modules. You can optionally provide a `filename` to write log output to a file in addition to stdout. For example:

```python
from graphrag_toolkit.lexical_graph import set_logging_config

set_logging_config(
  logging_level='DEBUG',  # or logging.DEBUG
  debug_include_modules='graphrag_toolkit.lexical_graph.storage',  # single string or list of strings
  debug_exclude_modules=['opensearch', 'boto'],  # single string or list of strings
  filename='output.log'  # optional: also write logs to a file
)
```

#### set_advanced_logging_config

The `set_advanced_logging_config` method provides more advanced logging configuration options, including the ability to specify filters for included and excluded modules or messages based on logging levels. Wildcards are supported for module names and included messages, and you can pass either a single string or a list of strings for modules or messages. This method offers greater flexibility and control over the logging behavior.

##### Parameters

| Parameter           | Type                          | Description                                                                                 | Default Value  |
|---------------------|-------------------------------|---------------------------------------------------------------------------------------------|----------------|
| `logging_level`     | `str` or `int`                | The logging level to apply (e.g., `'DEBUG'`, `'INFO'`, `logging.DEBUG`, etc.).              | `logging.INFO` |
| `included_modules`  | `dict[int, str \| list[str]]` | Modules to include in logging, grouped by logging level. Wildcards are supported.           | `None`         |
| `excluded_modules`  | `dict[int, str \| list[str]]` | Modules to exclude from logging, grouped by logging level. Wildcards are supported.         | `None`         |
| `included_messages` | `dict[int, str \| list[str]]` | Specific messages to include in logging, grouped by logging level. Wildcards are supported. | `None`         |
| `excluded_messages` | `dict[int, str \| list[str]]` | Specific messages to exclude from logging, grouped by logging level.                        | `None`         |
| `filename`          | `str`                         | If provided, log output is also written to this file in addition to stdout.                 | `None`         |

##### Example Usage

Here is an example of how to use `set_advanced_logging_config`:

```python
import logging
from graphrag_toolkit.lexical_graph import set_advanced_logging_config

set_advanced_logging_config(
    logging_level=logging.DEBUG,
    included_modules={
        logging.DEBUG: 'graphrag_toolkit',  # single string or list of strings
        logging.INFO: '*',  # wildcard supported
    },
    excluded_modules={
        logging.DEBUG: ['opensearch', 'boto', 'urllib'],  # single string or list of strings
        logging.INFO: ['opensearch', 'boto', 'urllib'],  # wildcard supported
    },
    excluded_messages={
        logging.WARNING: 'Removing unpickleable private attribute',  # single string or list of strings
    }
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

If no profile or region is set explicitly, the system falls back to environment variables or the default AWS CLI configuration.

See [Using AWS Profiles in `GraphRAGConfig`](./aws-profile.md) for more details on configuring and using AWS named profiles.

#### Resilient clients and SSO token refresh

All boto3 clients created by `GraphRAGConfig` are wrapped in a `ResilientClient` ([`config.py:94`](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/config.py#L94)). On `ExpiredToken`, `RequestExpired`, or `InvalidClientTokenId` errors the client is refreshed automatically and the call is retried.

When an AWS SSO profile is in use, the client wrapper also validates the SSO token age. If the token is more than one hour old, it runs `aws sso login` automatically before retrying. This is relevant for long-running indexing jobs and any environment where SSO sessions can expire mid-run.
