[[Home](./)]

## Indexing

### Topics

  - [Overview](#overview)
    - [Extract](#extract)
    - [Build](#build)
  - [Using the LexicalGraphIndex to construct a graph](#using-the-lexicalgraphindex-to-construct-a-graph)
    - [Continous ingest](#continous-ingest)
    - [Run the extract and build stages separately](#run-the-extract-and-build-stages-separately)
    - [Configuring the extract and build stages](#configuring-the-extract-and-build-stages)
    - [Custom prompts](#custom-prompts)
    - [Batch extraction](#batch-extraction)
    - [Checkpoints](#checkpoints)
  - [Advanced graph construction](#advanced-graph-construction)
    
### Overview

There are two stages to indexing: extract, and build. The lexical-graph uses separate pipelines for each of these stages, plus micro-batching, to provide a continous ingest capability. This means that your graph will start being populated soon after extraction begins.

You can run the extract and build pipelines together, to provide for the continuous ingest described above. Or you can run the two pipelines separately, extracting first to file-based chunks, and then later building a graph from these chunks.

The `LexicalGraphIndex` is a convenience class that allows you to run the extract and build pipelines together or separately. Alternatively, you can build your graph construction application using the underlying pipelines. This gives you more control over the configuration of each stage. We describe these two different approaches in the [Using the LexicalGraphIndex to construct a graph](#using-the-lexicalgraphindex-to-construct-a-graph) and [Advanced graph construction](#advanced-graph-construction) sections below.

Indexing supports [multi-tenancy](multi-tenancy.md), whereby you can store separate lexical graphs in the same backend graph and vector stores.

#### Code examples

The code examples here are formatted to run in a Jupyter notebook. If you’re building an application with a main entry point, put your application logic inside a method, and add an [`if __name__ == '__main__'` block](./faq.md#runtimeerror-please-use-nest_asyncioapply-to-allow-nested-event-loops).

#### Extract

The extraction stage is, by default, a three-step process: 

  1. The source documents are broken down into chunks.
  2. For each chunk, an LLM extracts a set of propositions from the unstructured content. This proposition extraction helps 'clean' the content and improve the subsequent entity/topic/statement/fact extraction by breaking complex sentences into simpler sentences, replacing pronouns with specific names, and replacing acronyms where possible. These propositions are added to the chunk's metadata under the `aws::graph::propositions` key.
  3. Following the proposition extraction, a second LLM call extracts entities, relations, topics, statements and facts from the set of extracted propositions. These details are added to the chunk's metadata under the `aws::graph::topics` key.
  
Only the third step here is mandatory. If your source data has already been chunked, you can omit step 1. If you're willing to trade a reduction in LLM calls and improved performance for a reduction in the quality of the entity/topic/statement/fact extraction, you can omit step 2.

Extraction uses a lightly guided strategy whereby the extraction process is seeded with a list of preferred entity classifications. The LLM is instructed to use an existing classification from the list before creating new ones. Any new classifications introduced by the LLM are then carried forward to subsequent invocations. This approach reduces but doesn't eliminate unwanted variations in entity classification.

The list of `DEFAULT_ENTITY_CLASSIFICATIONS` used to seed the extraction process can be found [here](https://github.com/awslabs/graphrag-toolkit/blob/main/src/graphrag_toolkit/indexing/constants.py). If these classifications are not appropriate to your workload you can replace them (see the [Configuring the extract and build stages](#configuring-the-extract-and-build-stages) and [Advanced graph construction](#advanced-graph-construction) sections below).

Relationship values are currently unguided (though relatively concise).

#### Build

In the build stage, the LlamaIndex chunk nodes emitted from the extract stage are broken down further into a stream of individual source, chunk, topic, statement and fact LlamaIndex nodes. Graph construction and vector indexing handlers process these nodes to build and index the graph content. Each of these nodes has an `aws::graph::index` metadata item containing data that can be used to index the node in a vector store (though only the chunk and statement nodes are actually indexed in the current implementation).

### Using the LexicalGraphIndex to construct a graph

The `LexicalGraphIndex` provides a convenient means of constructing a graph – via either continuous ingest, or separate extract and build stages. When constructing a `LexicalGraphIndex` you must supply a graph store and a vector store (see [Storage Model](./storage-model.md) for more details). In the examples below, the graph store and vector store connection strings are fetched from environment variables.

The `LexicalGraphIndex` constructor has an `extraction_dir` named argument. This is the path to a local directory to which intermediate artefacts (such as [checkpoints](#checkpoints)) will be written. By default, the vaue of `extraction_dir` is set to 'output'.

#### Continous ingest

Use `LexicalGraphIndex.extract_and_build()` to extract and build a graph in a manner that supports continous ingest. 

The extraction stage consumes LlamaIndex nodes – either documents, which will be chunked during extraction, or pre-chunked text nodes. Use a LlamaIndex reader to [load source documents](https://docs.llamaindex.ai/en/stable/understanding/loading/loading/). The example below uses a LlamaIndex `SimpleWebReader` to load several HTML pages.

```python
import os

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

from llama_index.readers.web import SimpleWebPageReader

import nest_asyncio
nest_asyncio.apply()

doc_urls = [
    'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html'
]

docs = SimpleWebPageReader(
    html_to_text=True,
    metadata_fn=lambda url:{'url': url}
).load_data(doc_urls)

graph_store = GraphStoreFactory.for_graph_store(os.environ['GRAPH_STORE'])
vector_store = VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE'])

graph_index = LexicalGraphIndex(
    graph_store, 
    vector_store
)

graph_index.extract_and_build(docs)
```

#### Run the extract and build stages separately

Using the `LexicalGraphIndex` you can perform the extract and build stages separately. This is useful if you want to extract the graph once, and then build it multiple times (in different environments, for example.)

When you run the extract and build stages separately, you can persist the extracted documents to Amazon S3 or to the filesystem at the end of the extract stage, and then consume these same documents in the build stage. Use the graphrag-toolkit's `S3BasedDocss` and `FileBasedDocs` classes to persist and then retrieve JSON-serialized LlamaIndex nodes.

The following example shows how to use a `S3BasedDocs` handler to persist extracted documents to an Amazon S3 bucket at the end of the extract stage:

```python
from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs

from llama_index.readers.web import SimpleWebPageReader

import nest_asyncio
nest_asyncio.apply()

extracted_docs = S3BasedDocs(
    region='us-east-1',
    bucket_name='my-bucket',
    key_prefix='extracted',
    collection_id='12345',
    s3_encryption_key_id='arn:aws:kms:us-east-1:222222222222:key/99169dcb-12ce-4493-942b-1523125d7339'
)

graph_store = GraphStoreFactory.for_graph_store(graph_store_info)
vector_store = VectorStoreFactory.for_vector_store(vector_store_info)

graph_index = LexicalGraphIndex(
    graph_store, 
    vector_store
)

doc_urls = [
    'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html'
]

docs = SimpleWebPageReader(
    html_to_text=True,
    metadata_fn=lambda url:{'url': url}
).load_data(doc_urls)

graph_index.extract(docs, handler=extracted_docs)
```

Following the extract stage, you can then build the graph from the previously extracted documents. Whereas in the extract stage the `S3BasedDocs` object acted as a handler to persist extracted documents, in the build stage the `S3BasedDocs` object acts as a source of LlamaIndex nodes, and is thus passed as the first argument to the `build()` method:

```python
from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs

import nest_asyncio
nest_asyncio.apply()

docs = S3BasedDocs(
    region='us-east-1',
    bucket_name='my-bucket',
    key_prefix='extracted',
    collection_id='12345',
    s3_encryption_key_id='arn:aws:kms:us-east-1:222222222222:key/99169dcb-12ce-4493-942b-1523125d7339'
)

graph_store = GraphStoreFactory.for_graph_store(graph_store_info)
vector_store = VectorStoreFactory.for_vector_store(vector_store_info)

graph_index = LexicalGraphIndex(
    graph_store, 
    vector_store
)

graph_index.build(docs)
```

The `S3BasedDocs` object has the following parameters:

| Parameter  | Description | Mandatory |
| ------------- | ------------- | ------------- |
| `region` | AWS Region in which the S3 bucket is located (e.g. `us-east-1`) | Yes |
| `bucket_name` | Amazon S3 bucket name | Yes |
| `key_prefix` | S3 key prefix | Yes |
| `collection_id` | Id for a particular collection of extracted documents. Optional: if no `collection_id` is supplied, the lexical-graph will create a timestamp value. Extracted documents will be written to `s3://<bucket>/<key_prefix>/<collection_id>/`. | No |
| `s3_encryption_key_id` | KMS key id (Key ID, Key ARN, or Key Alias) to use for object encryption. Optional: if no `s3_encryption_key_id` is supplied, the lexical-graph will encrypt objects in S3 using Amazon S3 managed keys. | No |

If you use Amazon Web Services KMS keys to encrypt objects in S3, the identity under which the lexical-graph runs should include the following IAM policy. Replace `<kms-key-arn>` with the ARN of the KMS key you want to use to encrypt objects:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
            	"kms:GenerateDataKey",
            	"kms:Decrypt"
            ],
            "Resource": [
            	"<kms-key-arn>"
            ],
            "Effect": "Allow"
        }
    ]
}
```

If you want to persist extracted documents to the local filesystem instead of an S3 bucket, use a `FileBasedDocs` object instead:

```python
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs

chunks = FileBasedDocs(
    docs_directory='./extracted/',
    collection_id='12345'
)
```

The `FileBasedChunks` object has the following parameters:

| Parameter  | Description | Mandatory |
| ------------- | ------------- | ------------- |
| `docs_directory` | Root directory for the extracted documents | Yes |
| `collection_id` | Id for a particular collection of extracted documents. Optional: if no `collection_id` is supplied, the lexical-graph will create a timestamp value. Extracted documents will be written to `/<docs_directory>/<collection_id>/`. | No |


#### Configuring the extract and build stages

You can configure the number of workers and batch sizes for the extract and build stages of the `LexicalGraphIndex` using the `GraphRAGConfig` object. See [Configuration](./configuration.md) for more details on using the configuration object. 

Besides configuring the workers and batch sizes, you can also configure the indexing process with regard to chunking, proposition extraction and entity classification, and graph and vector store contents by passing an instance of `IndexingConfig` to the `LexicalGraphIndex` constructor:

```python
from graphrag_toolkit.lexical_graph import LexicalGraphIndex, IndexingConfig, ExtractionConfig

...

graph_index = LexicalGraphIndex(
    graph_store, 
    vector_store,
    indexing_config = IndexingConfig(
      chunking=None,
      extraction=ExtractionConfig(
        enable_proposition_extraction=False
      )
      
    )
)
```

The `IndexingConfig` object has the following parameters:

| Parameter  | Description | Default Value |
| ------------- | ------------- | ------------- |
| `chunking` | A list of node parsers (e.g. LlamaIndex `SentenceSplitter`) to be used for chunking source documents. Set `chunking` to `None` to skip chunking. | `SentenceSplitter` with `chunk_size=256` and `chunk_overlap=20` |
| `extraction` | An `ExtractionConfig` object specifying extraction options | `ExtractionConfig` with default values |
| `batch_config` | Batch configuration to be used if performing [batch extraction](./batch-extraction.md). If `batch_config` is `None`, the toolkit will perform chunk-by-chunk extraction.  | `None` |

The `ExtractionConfig` object has the following parameters:

| Parameter  | Description | Default Value |
| ------------- | ------------- | ------------- |
| `enable_proposition_extraction` | Perform proposition extraction before extracting topics, statements, facts and entities | `True` |
| `preferred_entity_classifications` | Comma-separated list of preferred entity classifications used to seed the entity extraction | `DEFAULT_ENTITY_CLASSIFICATIONS` |
| `infer_entity_classifications` | Determines whether to pre-process documents to identify significant domain entity classifications. Supply either `True` or `False`, or an `InferClassificationsConfig` object. | `False` |
| `extract_propositions_prompt_template` | Prompt used to extract propositions from chunks. If `None`, the [default extract propositions template](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/prompts.py#L29-L72) is used. See [Custom prompts](#custom-prompts) below. | `None` |
| `extract_topics_prompt_template` | Prompt used to extract topics, statements and entities from chunks. If `None`, the [default extract topics template](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/prompts.py#L74-L191) is used. See [Custom prompts](#custom-prompts) below. | `None` |


The `InferClassificationsConfig` object has the following parameters:

| Parameter  | Description | Default Value |
| ------------- | ------------- | ------------- |
| `num_iterations` | Number of times to run the pre-processing over the source documents | 1 |
| `num_samples` | Number of chunks (selected at random) from which classifications are extracted per iteration | 5 |
| `prompt_template` | Prompt used to extract classifications from sampled chunks. If `None`, the [default domain entity classifications template](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/prompts.py#L4-L27) is used. See [Custom prompts](#custom-prompts) below. | `None` |


#### Custom prompts

The extract stage uses up to three LLM prompts:

  - [**Domain entity classifications:**](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/prompts.py#L4-L27) Extracts significant domain entity classifications from a sample of source documents prior to processing the documents. These classificatiosn are then supplied to the extract topics prompt as the list of preferred entity classifications.
  - [**Extract propositions:**](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/prompts.py#L29-L72) Extracts a set of standalone, well-formed propositions from a chunk.
  - [**Extract topics:**](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/prompts.py#L74-L191) Extracts topics, statements and entities and their relations from either a set of propositions, or from the raw chunk text.

Using the `ExtractionConfig` and `InferClassificationsConfig` you can customize one or more of these prompts.

**Domain entity classifications:**

The prompt template should included a `{text_chunks}` placeholder, into which the sampled chunks will be inserted. 

The template should return classifications in the following format:

```
<entity_classifications>
Classification1
Classification2
Classification3
</entity_classifications>
```

**Extract propositions:**

The prompt template should include a `{text}` placeholder, into which the chunk text will be inserted. 

The template should return propositions in the following format:

```
proposition
proposition
proposition
```

**Extract topics:**

The prompt template should include a `{text}` placeholder, into which a set of propositions (or the raw chunk text) will be inserted, a `{preferred_topics}` placeholder, into which a list of topics will be inserted, and a `{preferred_entity_classifications}` placeholder, into which a liist of entity classifications will be inserted. 

The template should return extracted topics, statements, entities and relations in the following format:

```
topic: topic

  entities:
    entity|classification
    entity|classification
  
  proposition: [exact proposition text]      
    entity-attribute relationships:
    entity|RELATIONSHIP|attribute
    entity|RELATIONSHIP|attribute
    
    entity-entity relationships:
    entity|RELATIONSHIP|entity
    entity|RELATIONSHIP|entity
    
  proposition: [exact proposition text]    
    entity-attribute relationships:
    entity|RELATIONSHIP|attribute
    entity|RELATIONSHIP|attribute
    
    entity-entity relationships:
    entity|RELATIONSHIP|entity
    entity|RELATIONSHIP|entity
```


#### Batch extraction

You can use [Amazon Bedrock batch inference](https://docs.aws.amazon.com/bedrock/latest/userguide/batch-inference.html) with the extract stage of the indexing process. See [Batch Extraction](./batch-extraction.md) for more details.


#### Checkpoints

The lexical-graph retries upsert operations and calls to LLMs and embedding models that don't succeed. However, failures can still happen. If an extract or build stage fails partway through, you typically don't want to reprocess chunks that have successfully made their way through the entire graph construction pipeline.

To avoid having to reprocess chunks that have been successfully processed in a previous run, provide a `Checkpoint` instance to the `extract_and_build()`, `extract()` and/or `build()` methods. A checkpoint adds a checkpoint *filter* to steps in the extract and build stages, and a checkpoint *writer* to the end of the build stage. When a chunk is emitted from the build stage, after having been successfully handled by both the graph construction *and* vector indexing handlers, its id will be written to a save point in the graph index `extraction_dir`. If a chunk with the same id is subsequently introduced into either the extract or build stage, it will be filtered out by the checkpoint filter.

The following example passes a checkpoint to the `extract_and_build()` method:

```python
from graphrag_toolkit.lexical_graph.indexing.build import Checkpoint

checkpoint = Checkpoint('my-checkpoint')

...

graph_index.extract_and_build(docs, checkpoint=checkpoint)
```

When you create a `Checkpoint`, you must give it a name. A checkpoint filter will only filter out chunks that were checkpointed by a checkpoint writer with the same name. If you use checkpoints when [running separate extract and build processes](#run-the-extract-and-build-stages-separately), ensure the checkpoints have different names. If you use the same name across separate extract and build processes, the build stage will ignore all the chunks created by the extract stage.

Checkpoints do not provide any transactional guarantees. If a chunk is successfully processed by the graph construction handlers, but then fails in a vector indexing handler, it will not make it to the end of the build pipeline, and so will not be checkpointed. If the build stage is restarted, the chunk will be reprocessed by both the graph construction and vector indexing handlers. For stores that support upserts (e.g. Amazon Neptune Database and Amazon Neptune Analytics) this is not an issue.

The lexical-graph does not clean up checkpoints. If you use checkpoints, periodically clean the checkpoint directory of old checkpoint files. 

### Advanced graph construction

If you want more control over the extract and build stages, then instead of using a `LexicalGraphIndex`, you can use the extract and build pipelines directly: 

```python
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.indexing import sink
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, DEFAULT_ENTITY_CLASSIFICATIONS
from graphrag_toolkit.lexical_graph.indexing.extract import ExtractionPipeline
from graphrag_toolkit.lexical_graph.indexing.extract import LLMPropositionExtractor
from graphrag_toolkit.lexical_graph.indexing.extract import TopicExtractor
from graphrag_toolkit.lexical_graph.indexing.extract import GraphScopedValueStore
from graphrag_toolkit.lexical_graph.indexing.extract import ScopedValueProvider, DEFAULT_SCOPE
from graphrag_toolkit.lexical_graph.indexing.build import Checkpoint
from graphrag_toolkit.lexical_graph.indexing.build import BuildPipeline
from graphrag_toolkit.lexical_graph.indexing.build import VectorIndexing
from graphrag_toolkit.lexical_graph.indexing.build import GraphConstruction


from llama_index.core.node_parser import SentenceSplitter
from llama_index.readers.web import SimpleWebPageReader

import nest_asyncio
nest_asyncio.apply()

checkpoint = Checkpoint('advanced-construction-example', enabled=True)

# Create graph and vector stores
graph_store = GraphStoreFactory.for_graph_store(os.environ['GRAPH_STORE'])
vector_store = VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE'])

# Create extraction pipeline components

# 1. Chunking using SentenceSplitter
splitter = SentenceSplitter(
    chunk_size=256,
    chunk_overlap=20
)

# 2. Proposition extraction
proposition_extractor = LLMPropositionExtractor()

# 3. Topic extraction
entity_classification_provider = ScopedValueProvider(
    label='EntityClassification',
    scoped_value_store=GraphScopedValueStore(graph_store=graph_store),
    initial_scoped_values = { DEFAULT_SCOPE: DEFAULT_ENTITY_CLASSIFICATIONS }
)

topic_extractor = TopicExtractor(
    source_metadata_field=PROPOSITIONS_KEY, # Omit this line if not performing proposition extraction
    entity_classification_provider=entity_classification_provider # Entity classifications saved to graph between LLM invocations
)

# Create extraction pipeline
extraction_pipeline = ExtractionPipeline.create(
    components=[
        splitter, 
        proposition_extractor,
        topic_extractor
    ],
    num_workers=2,
    batch_size=4,
    checkpoint=checkpoint,
    show_progress=True
)

# Create build pipeline components
graph_construction = GraphConstruction.for_graph_store(graph_store)
vector_indexing = VectorIndexing.for_vector_store(vector_store)
        
# Create build pipeline        
build_pipeline = BuildPipeline.create(
    components=[
        graph_construction,
        vector_indexing
    ],
    num_workers=2,
    batch_size=10,
    batch_writes_enabled=True,
    checkpoint=checkpoint,
    show_progress=True   
)

# Load source documents
doc_urls = [
    'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html'
]

docs = SimpleWebPageReader(
    html_to_text=True,
    metadata_fn=lambda url:{'url': url}
).load_data(doc_urls)

# Run the build and extraction stages
docs | extraction_pipeline | build_pipeline | sink 
```
