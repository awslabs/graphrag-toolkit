# ADR-0004: Store-Agnostic Write/Read Strategy — Support BOTH Neptune Database and Neptune Analytics

- **Status:** Proposed — client decision required
- **Date:** 2026-06-29
- **Stance:** We propose and inform; the client decides (see ADR-0001 §1 Decision stance).
- **Relates to / reconciles:** ADR-0001 (D1, D2), ADR-0003 (store topology), requirements.md R3/R4/R9

---

## 1. Context

ADR-0003 recorded a recommended direction (external vectors on Neptune Database + OpenSearch)
driven by scale. That recommendation stands **as a default**, but it must not become a
hard-coded exclusion of Neptune Analytics. The client must be able to run **both** engines,
and CPG-RAG must not care which is configured.

The toolkit already supports this: `GraphStore` and `VectorStore` are separate, pluggable
abstractions with factories/registries — graph stores include Neptune Database, Neptune
Analytics, and Neo4j; vector stores include OpenSearch Serverless, Neptune Analytics,
pgvector, and S3 Vectors. The correct design leans on that seam rather than choosing a
single store.

## 2. Decision

1. **CPG-RAG depends only on the toolkit's `GraphStore` / `VectorStore` abstractions.**
   It does not know or reference a concrete backend.
2. **The write strategy is polymorphic on the configured store topology** (Strategy pattern
   behind the toolkit's store abstraction — OCP/DIP):
   - **Graph + external vector store** (e.g. Neptune Database + OpenSearch): write the
     `SemanticCodeUnit` node to the graph (summary text + id + `SUMMARIZES`/`cpgNodeId`
     edges), and write its embedding to the vector store keyed by the node id.
     *External-vector strategy.*
   - **Vector-in-graph** (e.g. Neptune Analytics): write the embedding onto the graph
     node/graph directly; no separate vector-store write. *In-graph strategy.*
3. **Both are first-class and selected by configuration, not by code changes.** Adding a
   future backend must not require CPG-RAG changes.
4. **The read/retrieval path adapts symmetrically:**
   - External-vector: kNN in the vector store → node id → graph traversal (two-step;
     vector = entry point, graph = proof).
   - Vector-in-graph: vector search within the graph engine, then traversal.

## 3. Relationship to ADR-0003 (reconciliation)

ADR-0004 supersedes the *tone* of ADR-0003's "direction set," not its analysis:

- **Capability:** both topologies are supported (this ADR).
- **Recommended default at scale:** Neptune Database + OpenSearch, for the reasons in
  ADR-0003 (hundreds of long-lived multi-tenant projects; per-graph single vector index;
  memory pricing for cold data; fixed dimension vs model evolution; non-ACID vector updates
  under delta churn).
- **Neptune Analytics:** fully supported — appropriate for smaller/isolated deployments and
  for on-demand analytics on loaded subgraphs.

So ADR-0003's recommendation is guidance for choosing the default; ADR-0004 guarantees the
client is never locked out of either engine.

## 4. Consequences

- CPG-RAG stays store-agnostic and toolkit-conformant; new `GraphStore` / `VectorStore`
  implementations work without CPG-RAG changes.
- Each backend's constraints still apply where selected (ADR-0003): NA = one vector
  index/graph, fixed dimension, non-ACID vector updates; OpenSearch = many
  indexes/dimensions, per-tenant index suffix.
- The overlay/embedding component (R4) must not assume where the vector lives — it asks the
  resolved write strategy.
- Testing must cover both strategies (external-vector and in-graph) against their respective
  store contracts.

## 5. Design detail to confirm (for design.md, not decided here)

- **Capability detection:** how CPG-RAG determines "external vector store present" vs
  "vector-in-graph" from the configured toolkit stores. This selects the strategy at runtime.
- **Dual-role NA:** when Neptune Analytics is configured as *both* graph and vector store,
  CPG-RAG uses the in-graph strategy and performs no external vector write.
- **Id contract parity:** the shared node-id linkage (external-vector) and the on-node
  embedding (in-graph) must expose the same logical `SemanticCodeUnit` identity so retrieval
  is uniform across strategies.

## 6. Open questions for the client

1. Which deployments target which topology (default Neptune DB + OpenSearch at scale;
   Neptune Analytics where)?
2. Are both strategies required in the same environment simultaneously, or per-environment
   selection?
3. Any deployment that must switch topology later (migration path between strategies)?