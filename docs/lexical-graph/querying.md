[[Home](./)]

## Querying

The primary unit of context presented to the LLM is the *statement* — a standalone assertion or proposition extracted from a source chunk. Statements are grouped by topic and source, and that grouping is what the query engine presents to the LLM.

The lexical-graph uses a [traversal-based search](./traversal-based-search.md) strategy that combines similarity search with graph traversal. A [semantic-guided search](./semantic-guided-search.md) approach also exists but is likely to be retired in a future release.

Querying supports [metadata filtering](./metadata-filtering.md) and [multi-tenancy](multi-tenancy.md).

### Topics

- [Factory methods](#factory-methods)
- [Context format](#context-format)
- [Verbose mode](#verbose-mode)
- [Async querying](#async-querying)
- [Managing indexed sources](#managing-indexed-sources)

### Factory methods

Use `LexicalGraphQueryEngine.for_traversal_based_search()` for most workloads. Use `for_semantic_guided_search()` if you specifically need the semantic-guided strategy.

Both factory methods accept `graph_store`, `vector_store`, `tenant_id`, `post_processors`, `filter_config`, and `**kwargs`. The versioning parameter name differs between the two ([`lexical_graph_query_engine.py:67`](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/lexical_graph_query_engine.py#L67)):

| Factory method | versioning parameter |
| --- | --- |
| `for_traversal_based_search` | `versioning` |
| `for_semantic_guided_search` | `enable_versioning` |

You can also construct `LexicalGraphQueryEngine` directly, passing `system_prompt`, `user_prompt`, or a `prompt_provider` kwarg. See [Using Custom Prompt Providers](./prompts.md).

### Context format

The `context_format` kwarg controls how retrieved statements are serialised before being injected into the LLM prompt. Supported values ([`lexical_graph_query_engine.py:408`](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/lexical_graph_query_engine.py#L408)):

| Value | Description | Default for |
| --- | --- | --- |
| `'json'` | JSON array of topic/statement objects | `__init__` direct construction |
| `'yaml'` | YAML representation of the same structure | — |
| `'xml'` | XML representation of the same structure | — |
| `'text'` | Plain text, one topic heading per group | `for_traversal_based_search` |
| `'bedrock_xml'` | Pre-formatted XML produced by a `BedrockContextFormat` post-processor | `for_semantic_guided_search` (hardcoded) |

`for_semantic_guided_search` always uses `'bedrock_xml'` and ignores any `context_format` kwarg you pass. `for_traversal_based_search` defaults to `'text'` but accepts any of the values above.

### Verbose mode

The `verbose` kwarg (default `True`) controls answer length. When `True`, the LLM is instructed to answer fully; when `False`, concisely. This only affects the non-streaming code path ([`lexical_graph_query_engine.py:356`](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/lexical_graph_query_engine.py#L356)).

```python
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store,
    vector_store,
    verbose=False
)
```

### Async querying

`LexicalGraphQueryEngine` does not implement async querying. Calling `await query_engine.aquery(...)` will raise a `NotImplementedError`. Use `query_engine.query(...)` instead ([`lexical_graph_query_engine.py:563`](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/lexical_graph_query_engine.py#L563)).

### Managing indexed sources

`LexicalGraphIndex` exposes three methods for inspecting and managing what has been indexed ([`lexical_graph_index.py:596`](https://github.com/awslabs/graphrag-toolkit/blob/main/lexical-graph/src/graphrag_toolkit/lexical_graph/lexical_graph_index.py#L596)):

#### `get_stats()`

Returns a dict with node counts and two graph connectivity metrics:

```python
stats = graph_index.get_stats()
# {
#   'source': 12, 'chunk': 180, 'topic': 950,
#   'statement': 4200, 'fact': 3100, 'entity': 820,
#   'localConnectivity': 1.23456,
#   'globalConnectivity': 0.98765,
#   ...
# }
```

#### `get_sources(...)`

Queries the graph for source document metadata. Accepts a `source_id` (str), `source_ids` (list), `filter` (`FilterConfig`, dict, or list of dicts), an optional `versioning_config`, and an optional `order_by` field name or list.

```python
sources = graph_index.get_sources(filter={'url': 'https://example.com/page'})
```

#### `delete_sources(...)`

Same filter API as `get_sources`. Removes matching sources from both the graph store and the vector store and returns the list of deleted source IDs.

```python
deleted = graph_index.delete_sources(source_id='chunk::abc123')
```

---

See also:

- [Traversal-Based Search](./traversal-based-search.md)
- [Configuring and Tuning Traversal-Based Search](./traversal-based-search-configuration.md)
- [Metadata Filtering](./metadata-filtering.md)
- [Multi-Tenancy](./multi-tenancy.md)
