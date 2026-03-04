# Frequently Asked Questions

## Overview

This document answers common questions about the byokg-rag library and provides guidance on troubleshooting, optimization, and best practices.

## Common Questions

### Which graph store should I choose?

Choose your graph store based on deployment requirements and scale:

**Amazon Neptune Analytics** is best for:
- Production workloads requiring fast analytical queries
- Applications needing native vector search for entity linking
- Serverless deployments without infrastructure management
- Integration with AWS analytics services

**Amazon Neptune Database** is best for:
- Transactional workloads requiring ACID guarantees
- Applications needing high availability with automatic failover
- Workloads requiring read replicas for scaling
- Mixed transactional and analytical queries

**Local Graph Store** is best for:
- Development and prototyping
- Testing with small datasets (< 10,000 nodes)
- Learning and experimentation
- Environments without AWS access

TIP: Start with the local graph store for development, then migrate to Neptune Analytics for production deployments.

### How do I optimize query performance?

Optimize performance through these strategies:

**1. Adjust iteration counts**

Reduce `iterations` and `cypher_iterations` parameters to minimize LLM calls:

```python
context = query_engine.query(
    query="Your question",
    iterations=1,  # Reduce from default of 2
    cypher_iterations=1
)
```

**2. Limit retriever parameters**

Reduce the scope of graph exploration:

```python
triplet_retriever = AgenticRetriever(
    llm_generator=llm_generator,
    graph_traversal=graph_traversal,
    graph_verbalizer=triplet_verbalizer,
    max_num_relations=3,      # Reduce from default of 5
    max_num_entities=2,       # Reduce from default of 3
    max_num_iterations=2,     # Reduce from default of 3
    max_num_triplets=30       # Reduce from default of 50
)
```

**3. Use appropriate indexes**

Choose the fastest index type for your use case:
- Fuzzy string index: Fastest, no external dependencies
- Dense index: Slower but better semantic matching
- Graph-store index: Integrated with Neptune Analytics

**4. Enable direct query linking**

Skip LLM-based entity extraction for simple queries:

```python
query_engine = ByoKGQueryEngine(
    graph_store=graph_store,
    direct_query_linking=True  # Use semantic similarity directly
)
```

**5. Optimize LLM configuration**

Use faster models or reduce token limits:

```python
llm_generator = BedrockGenerator(
    model_name="anthropic.claude-3-haiku-20240307-v1:0",  # Faster model
    max_tokens=2048  # Reduce from default of 4096
)
```

### What LLM models are supported?

The byokg-rag library supports Amazon Bedrock models through the `BedrockGenerator` class. Recommended models:

**Claude 3.5 Sonnet** (Recommended)
- Model ID: `anthropic.claude-3-5-sonnet-20240620-v1:0`
- Best balance of performance and cost
- Strong reasoning capabilities for KGQA

**Claude 3.7 Sonnet** (Latest)
- Model ID: `anthropic.claude-3-7-sonnet-20250219-v1:0`
- Latest capabilities and improvements
- Higher cost than 3.5 Sonnet

**Claude 3 Opus**
- Model ID: `anthropic.claude-3-opus-20240229-v1:0`
- Highest capability for complex reasoning
- Highest cost and latency

**Claude 3 Haiku**
- Model ID: `anthropic.claude-3-haiku-20240307-v1:0`
- Fastest and lowest cost
- Suitable for simple queries

To use a different model:

```python
llm_generator = BedrockGenerator(
    model_name="anthropic.claude-3-haiku-20240307-v1:0",
    region_name="<region>"
)
```

NOTE: Ensure the model is available in your AWS region. Check the [Bedrock model availability](https://docs.aws.amazon.com/bedrock/latest/userguide/models-regions.html) documentation.

### How do I handle authentication errors?

Authentication errors typically indicate IAM permission issues. Follow these steps:

**1. Verify AWS credentials**

Ensure your environment has valid AWS credentials:

```bash
aws sts get-caller-identity
```

**2. Check IAM permissions**

Verify your IAM role or user has the required permissions:

- `bedrock:InvokeModel` for LLM access
- `neptune-graph:ReadDataViaQuery` for Neptune Analytics
- `neptune-db:ReadDataViaQuery` for Neptune Database
- `s3:GetObject` and `s3:PutObject` for S3 operations

**3. Verify resource access**

Ensure your credentials can access the specific resources:

```python
import boto3

# Test Neptune Analytics access
client = boto3.client('neptune-graph', region_name='<region>')
response = client.get_graph(graphIdentifier='<graph-id>')
print(response)

# Test Bedrock access
client = boto3.client('bedrock-runtime', region_name='<region>')
# This will fail if you don't have access
```

**4. Check network connectivity**

For Neptune Database, ensure your application runs in the correct VPC with appropriate security groups.

### Can I use byokg-rag with my existing knowledge graph?

Yes, byokg-rag works with existing knowledge graphs. Requirements:

**Graph Structure**
- Property graph model (nodes and edges with properties)
- Compatible with openCypher query language (for Neptune)

**Data Loading**

For Neptune Analytics:
```python
graph_store = NeptuneAnalyticsGraphStore(
    graph_identifier="<existing-graph-id>",
    region="<region>"
)
```

For Neptune Database:
```python
graph_store = NeptuneDBGraphStore(
    endpoint_url="https://<cluster-endpoint>:8182",
    region="<region>"
)
```

For local development:
```python
graph_store = LocalKGStore()
graph_store.read_from_csv(
    nodes_file="your_nodes.csv",
    edges_file="your_edges.csv"
)
```

**Schema Requirements**

The graph store must provide schema information. Neptune Analytics and Neptune Database automatically expose schema. For custom graph stores, implement the `get_schema()` method.

### How many iterations should I configure?

The optimal iteration count depends on query complexity:

**Simple queries (1 iteration)**
- Direct fact lookup: "What is the capital of France?"
- Single-hop relationships: "Who directed Inception?"

**Moderate queries (2 iterations, default)**
- Two-hop reasoning: "What movies did the director of Inception also direct?"
- Multiple entity queries: "Which actors appeared in both Inception and Interstellar?"

**Complex queries (3-5 iterations)**
- Multi-hop reasoning: "What awards did actors from Christopher Nolan films win?"
- Aggregation queries: "How many Nobel Prize winners worked at the same institution?"

**Trade-offs:**
- More iterations: Better coverage, higher latency, higher cost
- Fewer iterations: Faster responses, lower cost, may miss relevant information

Start with the default (2 iterations) and adjust based on your query complexity and performance requirements.

### What's the difference between KGLinker and CypherKGLinker?

**KGLinker** (Multi-Strategy Retrieval)
- Uses multiple retrieval strategies: agentic, path-based, query-based
- Extracts entities from natural language using LLM
- Combines results from different retrieval approaches
- Best for: Complex queries requiring diverse retrieval strategies

**CypherKGLinker** (Cypher-Focused Retrieval)
- Specializes in generating and executing openCypher queries
- Iteratively refines queries based on results
- Focuses on structured query generation
- Best for: Queries that map well to graph patterns

**Usage:**

Multi-strategy retrieval:
```python
kg_linker = KGLinker(
    llm_generator=llm_generator,
    graph_store=graph_store
)

query_engine = ByoKGQueryEngine(
    graph_store=graph_store,
    kg_linker=kg_linker
)
```

Cypher-focused retrieval:
```python
cypher_linker = CypherKGLinker(
    llm_generator=llm_generator,
    graph_store=graph_store
)

query_engine = ByoKGQueryEngine(
    graph_store=graph_store,
    cypher_kg_linker=cypher_linker
)
```

Combined approach:
```python
query_engine = ByoKGQueryEngine(
    graph_store=graph_store,
    kg_linker=kg_linker,
    cypher_kg_linker=cypher_linker  # Tries Cypher first, falls back to multi-strategy
)
```

## Known Limitations

### Retrieval Strategy Limitations

**Agentic Retrieval**
- Requires multiple LLM calls, increasing latency and cost
- May explore irrelevant paths in very large graphs
- Performance depends on LLM reasoning capabilities

**Scoring-Based Retrieval**
- Requires semantic similarity computation for all candidate triplets
- May be slow for graphs with high-degree nodes (many edges per node)
- Effectiveness depends on embedding quality

**Path-Based Retrieval**
- Requires explicit metapath specification from LLM
- May miss relevant paths not matching specified patterns
- Performance degrades with very long paths (> 5 hops)

**Query-Based Retrieval**
- Requires LLM to generate syntactically correct queries
- May fail on complex graph schemas with many node/edge types
- Query generation quality varies by LLM model

### Graph Store Limitations

**Neptune Analytics**
- Vector search requires embeddings to be loaded as node properties
- Very complex queries may timeout (default: 60 seconds)
- Regional availability varies (check AWS documentation)
- Bulk loading requires S3 staging

**Neptune Database**
- VPC-only access (no public endpoints)
- Schema refresh requires recreating graph store instance
- Concurrent query limits depend on instance size
- Read replicas needed for high query concurrency

**Local Graph Store**
- In-memory only, limited by available RAM
- No persistence across restarts
- No support for complex query languages
- Single-process access only

### Performance Considerations

**Large Graphs (> 1M nodes)**
- Entity linking may be slow without proper indexing
- Consider using graph-store indexes for Neptune Analytics
- Limit exploration depth to avoid excessive traversal

**High Query Volume**
- LLM rate limits may throttle requests
- Consider caching frequently asked questions
- Use read replicas for Neptune Database

**Long-Running Queries**
- Queries with many iterations may timeout
- Reduce iteration counts or exploration parameters
- Consider breaking complex queries into simpler sub-queries

## Troubleshooting

### Query returns empty results

**Possible causes:**
1. Entity linking failed to find relevant entities
2. Graph schema doesn't match query expectations
3. Insufficient iterations for multi-hop reasoning

**Solutions:**
- Enable debug logging to see entity linking results
- Verify graph schema matches your domain
- Increase iteration count for complex queries
- Try direct query linking: `direct_query_linking=True`

### LLM timeout errors

**Possible causes:**
1. Input exceeds token limits
2. Network connectivity issues
3. Bedrock service throttling

**Solutions:**
- Reduce `max_input_tokens` parameter
- Reduce graph context size by limiting retrievers
- Implement exponential backoff retry logic
- Check AWS service health dashboard

### High latency

**Possible causes:**
1. Too many LLM calls (high iteration counts)
2. Large graph traversals
3. Slow entity linking

**Solutions:**
- Reduce iteration counts
- Limit retriever parameters (max_num_relations, max_num_entities)
- Use faster index types (fuzzy string vs. dense)
- Use faster LLM models (Claude Haiku)

### Memory errors with local graph store

**Possible causes:**
1. Graph too large for available RAM
2. Too many triplets retained in context

**Solutions:**
- Use Neptune Analytics or Neptune Database instead
- Reduce `max_num_triplets` parameter
- Filter graph data to relevant subset
- Increase available memory

For additional support, refer to the [example notebooks](../../examples/byokg-rag/) or consult the AWS documentation for Neptune and Bedrock services.
