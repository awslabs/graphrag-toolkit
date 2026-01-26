[[Home](./)]

## Querying
 
The primary unit of context presented to the LLM by the lexical-graph is the *statement*, which is a standalone assertion or proposition. Source documents are broken into chunks, and from these chunks are extracted statements. In the graphrag-toolkit's [graph model](./graph-model.md), statements are thematically grouped by topic, and supported by facts. At question-answering time, the lexical-graph retrieves groups of statements, and presents them in the context window to the LLM.

The lexical-graph uses a [traversal-based search](./traversal-based-search.md) strategy to perform hybrid top-down and bottom-up similarity and graph-based searches for sets of statements grouped by topic and source. (The lexical-graph also includes a [semantic-guided search](./semantic-guided-search.md) approach which will likely be retired in future versions).

Querying supports [metadata filtering](./metadata-filtering.md) and [multi-tenancy](multi-tenancy.md). Metadata filtering allows you to retrieve a constrained set of sources, topics and statements based on metadata filters and associated values when querying a lexical graph. Multi-tenancy allows you to query different lexical graphs hosted in the same backend graph and vector stores. 

See also:

  - [Traversal-Based Search](./traversal-based-search.md)
  - [Configuring and Tuning Traversal-Based Search](./traversal-based-search-configuration.md)
  - [Metadata Filtering](./metadata-filtering.md)
  - [Multi-Tenancy](./multi-tenancy.md)
