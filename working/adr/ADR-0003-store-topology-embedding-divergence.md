# ADR-0003: Graph + Vector Store Topology for CPG-RAG (Neptune Database vs Neptune Analytics)

- **Status:** Proposed — client decision required
- **Date:** 2026-06-29
- **Stance:** We propose and inform; the client decides (see ADR-0001 §1 Decision stance).
- **Relates to:** ADR-0001 (D5), ADR-0002, `requirements.md` R4/R8/NFRs, `working/neptune_vs_neptune_analytics_cpg_rag_architecture.md`

---

## 1. Context

The client anticipates using **both Amazon Neptune Database and Neptune Analytics**. This introduces a real CPG-RAG complication the client raised: **the embedding approach differs between the two engines**, so the retrieval/embedding design cannot be written as if "vectors live in the graph" in both cases.

CPG-RAG needs **multiple embedding types, potentially at different dimensions** — lexical chunk, statement, method summary, endpoint summary, source-to-sink slice, and (Phase 2) finding summary — and **per-tenant separation** (ADR-0002).

This ADR records the decision to be made about **where embeddings live** and **what role each engine plays**. We present options, a recommendation, and consequences; the client decides.

## 2. Constraints (from the storage architecture doc and AWS docs it cites)

- **Neptune Database** — durable graph truth; it is **not** the vector store. The toolkit separates `GraphStore` from `VectorStore`; the paired vector surface is typically **OpenSearch Serverless**.
- **OpenSearch Serverless** — supports **many indexes, multiple/arbitrary dimensions, metadata filtering**, and **per-tenant index naming** (`format_index_name` → `chunk_<tenant>`). ACID-style index operations; frequent delta re-embeds are fine.
- **Neptune Analytics** — memory-optimised, supports **vector-in-graph** search, but: **one vector index per graph, fixed dimension set at graph creation, and vector updates are not ACID**. Loaded from Neptune/S3/snapshot.

## 3. The divergence, stated precisely

- **Embeddings in OpenSearch (Neptune DB path):** multiple embedding types/dimensions coexist; per-tenant separation via index-name suffix; delta re-embedding is safe. Fits CPG-RAG directly.
- **Embeddings in Neptune Analytics (vector-in-graph):** a single graph can hold **only one vector index at one fixed dimension**, so chunk + method-summary + source-to-sink embeddings (different models/dims) **cannot** coexist in one NA graph; per-tenant separation means **separate NA graphs** (not index suffixes); and the **non-ACID** vector updates clash with the delta re-embedding loop (R5).

Therefore the CPG-RAG embedding design **must not assume vectors live in the graph**.

## 4. Options

### Option A — Neptune DB + OpenSearch only (no Neptune Analytics)
- **Pros:** simplest; fits multi-embedding, multi-tenant CPG-RAG; lowest cost; consistent with "embeddings are entry points, graph is proof."
- **Cons:** no in-memory graph algorithms / vector-in-graph analytics.

### Option B — Role split (recommended if "use both" is required)
- **Neptune DB + OpenSearch** = durable graph truth + operational multi-embedding retrieval (the CPG-RAG hot path).
- **Neptune Analytics** = **on-demand analytical** engine, loaded from Neptune/S3 for graph algorithms (centrality, community, blast-radius, similar source-to-sink), optionally with a **single-purpose, single-dimension** vector index for a specific analysis. NA is **not** the primary vector home and **not** on the delta re-embedding path.
- **Pros:** keeps the operational path flexible and correct; still unlocks analytics where NA is genuinely valuable.
- **Cons:** two engines to operate; NA is memory-optimised (cost); requires deliberate load/snapshot boundaries.

### Option C — Neptune Analytics as the unified graph+vector store
- **Pros:** one engine for graph + vectors.
- **Cons:** viable **only** if a single embedding type at a single dimension per graph is acceptable and non-atomic vector updates are tolerable — which conflicts with multi-embedding CPG-RAG and the delta loop. Not recommended as the operational vector home.

## 5. Recommendation (for client approval)

For the **Phase 1 operational retrieval path**, keep embeddings in **OpenSearch Serverless with Neptune Database** (Option A, or Option B if analytics are needed). Treat **Neptune Analytics as optional, analytical, and on-demand** (Option B) — never the operational vector home for multi-embedding CPG-RAG, and never on the delta re-embedding path. Consequently, the `SemanticCodeUnit` overlay embeddings (R4) target OpenSearch, keyed by `graph_node_id`, while Neptune holds the proof relationships.

## 6. Consequences

- The overlay/embedding component (R4) writes to OpenSearch, not into the graph; the design must not depend on vector-in-graph traversal for the operational path.
- Per-tenant vectors are clean in OpenSearch (index-name suffix); in NA, per-tenant means separate graphs — a further reason NA is not the operational vector home.
- If the client mandates a vector-in-graph analysis in NA, it is scoped to one embedding type/dimension on a **loaded snapshot**, isolated from the live delta pipeline.
- Cost: NA is memory-optimised/capacity-priced; Option B should gate NA usage to specific analytical jobs.

## 7. Open questions for the client

1. Does any required query genuinely need vector-in-graph traversal, or is vector-first-then-graph sufficient?
2. How many embedding types/dimensions are in Phase 1?
3. Is Neptune Analytics in scope for Phase 1 at all, or Phase 2 analytics only?
4. What is the cost ceiling for Neptune Analytics (memory-optimised capacity)?

- **Direction set (pending client sign-off):** The client currently stores vectors
  in-graph (Neo4j node property). We recommend a deliberate design shift to
  **external vectors on Neptune Database + OpenSearch** (Option B; embeddings in
  OpenSearch). Rationale — Neptune Analytics does not scale to hundreds of
  long-lived, multi-tenant projects (new and old):
  (a) per-graph single vector index + fixed dimension forces ~one NA graph per tenant;
  (b) memory-optimised/capacity pricing penalises mostly-cold retained old projects;
  (c) fixed dimension blocks embedding-model evolution across project vintages;
  (d) non-ACID vector updates are risky under continuous per-repo delta re-embedding.
  Neptune Analytics is retained for on-demand analytics on loaded subgraphs only.
  Vector-in-graph traversal is not required: CPG-RAG uses vector-as-entry-point then
  graph-as-proof (two-step), not vector inside traversal.
