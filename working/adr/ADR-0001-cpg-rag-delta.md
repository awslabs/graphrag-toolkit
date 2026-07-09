# ADR-0001: CPG-RAG on the GraphRAG Toolkit — Delta Analysis and Architectural Approach

- **Status:** Proposed (for client approval)
- **Date:** 2026-06-29
- **Owners:** Solution architecture
- **Related:** `working/graphrag_com_redrawn_architecture.md`, `working/federated_semantics_graph_architecture.md`, `working/neptune_vs_neptune_analytics_cpg_rag_architecture.md`, `READMORE.md`

---

## 1. Framing (read this first)

The AWS GraphRAG Toolkit (`lexical-graph`, plus the `document-graph` and `codeproperty-graph` packages layered on it) is a **general-purpose, modular, SOLID platform**. It was **not built for this client**. This record is therefore a **delta analysis**: it separates what the toolkit already provides and does correctly from the **net-new components the client's CPG-RAG use case requires**.

Two consequences follow directly and constrain every decision below:

1. **We do not fork or specialize the toolkit for the client.** The core stays general-purpose. Client-specific composition lives above it.
2. **We reuse the toolkit's existing abstractions rather than inventing parallel ones.** Where the toolkit already defines a seam (model providers, graph/vector stores, transformer providers), the delta consumes that seam. Anything that bypasses an existing seam is treated as non-conformant and is part of the delta to correct — not a shortcut to add.

### Decision stance (applies to all ADRs in this set)

We advise; the client decides. Every item recorded as a "decision" is one of:

- **(a) an engineering conformance decision** about how *we* build on the toolkit (SOLID conformance, dependency direction, no vendor coupling) — ours to own and hold the line on; or
- **(b) a proposed decision the client must approve**, always presented with options, a recommendation, and consequences.

An ADR that records options without a final selection is still a decision record: it fixes *the decision that must be made* and its trade-offs, pending client approval. Nothing in these ADRs commits the client to a course of action. Where we lack information to choose, we say so and list the open question rather than assume.

This is an architecture record, not an implementation plan. No code changes are proposed here. Implementation is gated on the design document that follows this ADR.

---

## 2. Toolkit baseline (what already exists and is conformant)

Verified against the source, not assumed:

| Capability | Where | SOLID property it already satisfies |
|---|---|---|
| Model abstraction for generation | `GraphRAGConfig.extraction_llm` / `response_llm` typed as LlamaIndex `LLM`; `LLMType = Union[LLM, str]`; resolved via `to_llm` | DIP — callers depend on the `LLM` abstraction; instances are injectable |
| Model abstraction for embeddings | `GraphRAGConfig.embed_model` typed as `BaseEmbedding`; `EmbeddingType = Union[BaseEmbedding, str]`; resolved via `to_embedding_model` | DIP — callers depend on `BaseEmbedding`; instances are injectable |
| Store separation | distinct `GraphStore` and `VectorStore`; Neptune = graph truth, OpenSearch = vectors, S3 = artifacts | SRP — relationship truth, similarity, and raw artifacts are separate concerns |
| Provider extensibility | factory + registry patterns: `graph_store_factory`, `vector_store_factory`, reader/transformer/schema/extract provider factories and registries | OCP — new providers register without modifying consumers |
| Structured ETL to graph | `document-graph` pipeline: extract → ingest → transform → build (constructors → Cypher) → load; operates on generic record dicts | SRP / strategy pattern per stage |
| CPG domain layer | `codeproperty-graph`: `CPGNode/CPGEdge.from_joern`, `Manifest`, `GraphDiff`, `DeltaIngestor` (skip-or-replace, pluggable `write_fn`, tenant purge), `ManifestManager`, `tenant_ops` | SRP — CPG semantics isolated from graph infrastructure |
| Layering | `codeproperty-graph` → `document-graph` → `lexical-graph` (foundation) | Clean one-directional dependencies |

**Conclusion of the baseline:** the platform already embodies the model-provider abstraction and the enrich-before-load ETL the client needs. The client's local Mistral 7B is, architecturally, a LlamaIndex `LLM`/`BaseEmbedding` implementation injected through the seam that already exists. It is not a new capability of the platform.

---

## 3. Decision drivers

- Preserve the toolkit's SOLID, modular character; no client-specific logic in the core.
- Depend on abstractions, never on vendor SDKs, at every layer (DIP).
- One-directional dependencies: client/domain packages depend on the foundation, never the reverse.
- No in-place mutation of a live graph; no read-modify-write; enrichment happens on data, before the graph store.
- Embeddings are entry points; the graph is proof (per the storage architecture doc).
- Correctness of the delta/idempotency contract over convenience.

---

## 4. Decisions

### D1 — CPG-RAG is a separate package that depends on the toolkit
Client CPG-RAG composition (overlay building, identity resolution, cross-graph linking, multi-graph planning) lives in its own package that **depends on** `lexical-graph` / `document-graph` / `codeproperty-graph`. The toolkit core remains general-purpose. Dependency direction is strictly domain → foundation. This mirrors the existing layering (`codeproperty-graph` already sits on `document-graph` on `lexical-graph`) and keeps release cadence and client specifics isolated.

### D2 — All inference and embedding flow through the toolkit's existing model seam
Every model call — CPG summarisation (generation) and vectorisation (embedding) — depends on the toolkit's `LLM` / `BaseEmbedding` abstraction resolved by `GraphRAGConfig`. **Local Mistral 7B is injected as a LlamaIndex provider through that seam**, not reached by a vendor-specific code path. The current `document-graph` enricher plugins (`LLMEnricherPlugin` binding the OpenAI SDK, `BedrockEnricherPlugin` binding `boto3` bedrock-runtime, each duplicating the transform loop) **embed the inference backend inside a transformer**, which couples a transform concern to a vendor and violates DIP. They are recorded as **non-conformant** and are re-expressed as a single model-agnostic enrichment transformer that consumes an injected `LLM`. This is the conformant design, not an add-on.

### D3 — Enrich Joern CPG JSON before Neptune; retire the Neo4j read-modify-write
The client's current flow (Joern JSON → load Neo4j → read → enrich → write back to Neo4j) mutates a live graph on every run. The conformant flow enriches the **records** (`document-graph` transformers driven by the injected model) and then delta-loads the finished graph to Neptune via `codeproperty-graph.DeltaIngestor`. No graph-database round-trip for enrichment. This is already the platform's intended shape (`document-graph` enriches records; `DeltaIngestor` does skip-or-replace) — the delta is only wiring it for the CPG domain.

### D4 — The delta/idempotency contract keys on (code signature + enrichment recipe version)
`DeltaIngestor` today keys the skip/replace decision on METHOD `full_name:hash` only. That is correct for code change but unsound once enrichment is part of the output: improving the summariser (model/prompt/version) while code is unchanged would incorrectly SKIP and leave stale overlay. The manifest signature must therefore incorporate the **enrichment recipe version** so any change to enrichment inputs forces re-ingest. This is a correctness property of the contract, not a patch.

### D5 — The net-new delta is a set of SRP-bounded components
The following do not exist in any package today and constitute the client delta. Each is a component with a single responsibility and an explicit boundary:
- **Semantic Code Overlay** — `SemanticCodeUnit` (method / endpoint / source-to-sink summaries) as a first-class graph shape, built by an overlay-builder; only summaries are embedded, never raw AST/edges.
- **Canonical Identity Spine** — the bounded context that application/service/repo/API/control/owner identities resolve into.
- **Cross-Graph Linker** — writes explicit canonical-identity edges at ingest, replacing fuzzy entity-extraction correlation.
- **Multi-Graph Query Planner + Provenance** — federated retrieval across lexical, domain, CPG, and evidence graphs with source/graph-path provenance.

### D6 — Generic seams are broadened upstream; client specifics stay downstream
Where a gap is genuinely general-purpose (for example, broadening `to_llm` / `to_embedding_model` string resolution beyond the Bedrock default so any registered provider resolves), the enhancement is contributed **upstream to the toolkit via extension (OCP)** — additive, behaviour-preserving. Anything client- or CPG-specific stays in the downstream CPG-RAG package. The test for placement: *would a non-client user of the toolkit want this?* If yes, upstream; if no, downstream.

---

## 5. What we explicitly are NOT doing

- Not forking or specialising the toolkit for the client.
- Not introducing a parallel model abstraction — the `LLM`/`BaseEmbedding` seam is the abstraction.
- Not leaving vendor SDK bindings inside transformers.
- Not mutating a live graph in place or reading-modifying-writing for enrichment.
- Not embedding raw AST nodes, identifiers, literals, or edges.
- Not putting client orchestration state (Cosmos DB / MongoDB API) on the synchronous retrieval path (tracked separately).

---

## 6. Delta summary (Have / Non-conformant / Missing)

| Component | Status | Action |
|---|---|---|
| Model seam (`LLM` / `BaseEmbedding` via `GraphRAGConfig`) | Have, conformant | Reuse; inject local Mistral 7B provider |
| `document-graph` ETL + transformer registry | Have, conformant | Reuse |
| `document-graph` enrichers (OpenAI/Bedrock-coupled) | Have, **non-conformant** | Re-express as model-agnostic enricher on the injected `LLM` (D2) |
| `codeproperty-graph` `from_joern` + `DeltaIngestor` | Have, conformant | Reuse; extend manifest signature (D4) |
| Semantic Code Overlay (`SemanticCodeUnit`) | Missing | Build in CPG-RAG package (D5) |
| Canonical identity spine + resolver | Missing | Build (D5) |
| Cross-graph linker | Missing | Build (D5) |
| Multi-graph query planner + provenance | Missing | Build (D5) |
| Provider resolution beyond Bedrock default | Enhancement | Upstream via OCP (D6) |

---

## 7. Consequences

**Positive:** the toolkit stays general-purpose and SOLID; the client sees a precise, defensible delta rather than a rewrite; local Mistral 7B, Bedrock, and any future backend are configuration choices behind one seam; the enrich-before-Neptune flow removes the Neo4j round-trip; dependencies stay one-directional.

**Negative / cost:** the non-conformant enrichers must be reworked before they carry production load (D2); the delta components (D5) are real engineering, not configuration; the manifest-signature change (D4) requires a migration of existing manifests.

**Neutral:** client orchestration (MongoDB API / A2A agents) is out of scope for this ADR and is addressed as a separate integration decision.

---

## 8. Next steps (gated)

1. **SPEC (requirements)** — acceptance criteria for D2–D5, phased.
2. **Design document (for approval)** — deep on the buildable first phase (model-agnostic enricher on the injected `LLM` + Semantic Code Overlay + manifest-signature contract), with the identity spine, linker, and planner at outline depth.
3. **Implementation** — only after the design is approved.
