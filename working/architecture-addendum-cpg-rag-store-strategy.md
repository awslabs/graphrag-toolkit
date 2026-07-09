# Architecture Addendum: Store-Agnostic CPG-RAG Write/Read Strategy

**Status:** Draft for client alignment · **Relates to:** ADR-0003, ADR-0004,
`graphrag_com_redrawn_architecture.md`

## Principle

CPG-RAG composes the toolkit's `GraphStore` and `VectorStore` abstractions and never binds a
concrete backend. The **configured store topology determines the write and read strategy**
(Strategy pattern; OCP/DIP). Think *both*, not one.

## Topology → strategy

| Configured topology | Where the vector lives | Write strategy | Read strategy |
|---|---|---|---|
| Neptune DB + OpenSearch (external vector) | OpenSearch, keyed by node id | Write `SemanticCodeUnit` node to graph; write embedding to vector store keyed by id | kNN in OpenSearch → node id → Neptune traversal |
| Neptune Analytics (vector-in-graph) | On the graph node | Write `SemanticCodeUnit` node with embedding on the graph | Vector search in-graph → traversal |

## Invariants (hold across both strategies)

- Same logical `SemanticCodeUnit` identity and the same `SUMMARIZES` / `cpgNodeId` linkage to
  the raw Joern node, regardless of where the vector lives.
- Embeddings are entry points; the graph is proof (two-step retrieval).
- Multi-tenancy honoured via the toolkit `TenantId` encoding.

## Guidance (not exclusion)

At hundreds-of-projects scale (new and old), **Neptune DB + OpenSearch is the recommended
default** (ADR-0003). **Neptune Analytics is fully supported** for smaller/isolated
deployments and on-demand analytics (ADR-0004). The client is never locked out of either.

## Resolution flow

```mermaid
flowchart TB
    CFG[Configured toolkit stores] --> RES{Vector-in-graph capable\nand no external VectorStore?}
    RES -- yes --> IG[In-graph write strategy\nembedding on node]
    RES -- no  --> EX[External-vector write strategy\nembedding in vector store, keyed by id]
    IG --> R1[Read: vector search in-graph -> traversal]
    EX --> R2[Read: kNN in vector store -> id -> graph traversal]