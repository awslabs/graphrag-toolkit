# CPG-RAG Delta — Requirements Specification

- **Status:** Draft for approval
- **Date:** 2026-06-29
- **Derives from:** `working/adr/ADR-0001-cpg-rag-delta.md` (decisions D1–D6)
- **Companion (next):** `working/specs/cpg-rag-missing-pieces/design.md`

This SPEC states *what* the CPG-RAG delta must satisfy and *how we will know it is satisfied*. It does not prescribe implementation; that is the design document. Every requirement traces to an ADR decision and carries testable acceptance criteria.

---

## 1. Scope

**In scope:** the delta between the general-purpose GraphRAG Toolkit and a client CPG-RAG capability — model-agnostic enrichment, local-model injection, enrich-before-Neptune CPG ingestion, semantic code overlay, delta-contract correctness, canonical identity spine, cross-graph linking, and multi-graph retrieval with provenance.

**Out of scope:** the toolkit's existing, conformant capabilities (reused as-is); client agent orchestration state (MongoDB API / A2A) on the retrieval hot path; evidence-graph (SBOM/CVE/findings) beyond interface stubs — deferred to a later phase.

**Non-goals:** forking or specialising the toolkit core; introducing a parallel model abstraction; embedding raw AST/edges; mutating a live graph in place.

---

## 2. Definitions

- **Model seam** — the toolkit's `LLM` / `BaseEmbedding` abstraction resolved via `GraphRAGConfig` (`LLMType`, `EmbeddingType`, `to_llm`, `to_embedding_model`).
- **Enricher** — a `document-graph` `TransformerProvider` that adds derived fields to records.
- **Semantic Code Overlay** — higher-level code-behaviour summaries (`SemanticCodeUnit`) built above raw Joern CPG nodes.
- **Delta ingest** — `codeproperty-graph.DeltaIngestor` skip-or-replace loading keyed on a manifest signature.
- **Enrichment recipe** — the identity of everything that determines enrichment output (model id, prompt version, parameters, overlay builder version).

---

## 3. Phasing

- **Phase 1 (approvable and buildable now):** R1, R2, R3, R4, R5, R9. Conformant enrichment + local model injection + enrich-before-Neptune + semantic code overlay + correct delta contract, packaged correctly.
- **Phase 2 (requires client identity/source input):** R6, R7, R8. Identity spine, cross-graph linker, multi-graph planner + provenance.

---

## 4. Functional requirements

### R1 — Model-agnostic enrichment (traces to D2)
As a pipeline author, I want CPG/document enrichment to depend on the toolkit model seam, so that no transformer is coupled to a vendor SDK.

Acceptance criteria:
- The enrichment transformer SHALL accept an injected `LLM` (and, where it vectorises, a `BaseEmbedding`) and SHALL NOT import a vendor SDK (`openai`, `boto3`) directly.
- WHEN the injected model changes, THEN enrichment behaviour SHALL change with no edit to the enricher class.
- The existing OpenAI- and Bedrock-coupled enricher plugins SHALL be superseded by a single model-agnostic enricher; duplicated transform loops SHALL be removed.
- Unit tests SHALL verify enrichment with a stubbed `LLM` (no network), asserting the derived fields and error handling.

### R2 — Local model provider injection (traces to D2)
As an operator, I want to run enrichment against a self-hosted Mistral 7B, so that enrichment can run without a cloud model dependency.

Acceptance criteria:
- A local Mistral 7B SHALL be usable purely as an injected `LLM` implementation via the existing seam, with no CPG-specific code path.
- Selection between local Mistral 7B, Bedrock, and any other provider SHALL be configuration/injection only.
- WHEN no provider is explicitly configured, THEN the toolkit default SHALL remain unchanged (no regression to existing users).

### R3 — Enrich-before-Neptune CPG pipeline (traces to D3)
As an architect, I want Joern CPG JSON enriched before loading to Neptune, so that the Neo4j read-modify-write step is retired.

Acceptance criteria:
- The pipeline SHALL consume Joern node/edge JSON, enrich records via R1, and load the finished graph to Neptune via `DeltaIngestor`.
- No stage SHALL read-modify-write a live graph database for enrichment.
- The flow SHALL reuse `codeproperty-graph.from_joern` and the `document-graph` transform pipeline without duplicating their logic.
- An end-to-end test SHALL demonstrate: Joern JSON in → enriched, typed graph in Neptune out, with zero graph-DB round-trip during enrichment.

### R4 — Semantic Code Overlay (traces to D5)
As a retrieval designer, I want method/endpoint/source-to-sink summaries as first-class graph nodes with embeddings, so that retrieval has meaningful entry points.

Acceptance criteria:
- A `SemanticCodeUnit` node type SHALL represent method, endpoint, and source-to-sink summaries, linked to the raw CPG node it summarises (e.g. `SUMMARIZES → Method`).
- Only summaries SHALL be embedded; raw AST nodes, identifiers, literals, and edges SHALL NOT be embedded as primary units.
- Summary embeddings SHALL be written to the vector store (OpenSearch), and each SHALL carry a stable pointer back to its graph node.
- Each `SemanticCodeUnit` SHALL carry provenance metadata (repo, commit, file, method, cpg node id).

### R5 — Delta idempotency contract with enrichment recipe version (traces to D4)
As an operator, I want re-ingest to trigger when either code or enrichment changes, so that overlay never goes stale.

Acceptance criteria:
- The manifest signature SHALL be a function of BOTH the method-signature set AND the enrichment recipe version.
- WHEN code is unchanged but the enrichment recipe changes, THEN the delta decision SHALL be INGEST (not SKIP).
- WHEN both code and recipe are unchanged, THEN the decision SHALL be SKIP with zero writes to Neptune and the vector store.
- A migration path SHALL exist for manifests created before the signature change.

### R6 — Canonical identity spine (traces to D5, Phase 2)
As an integrator, I want a canonical identity for applications/services/repos/APIs/controls/owners, so that graphs join on stable identity rather than fuzzy names.

Acceptance criteria:
- A canonical identity model SHALL exist as its own bounded context with a resolver mapping aliases to canonical ids.
- Resolution SHALL be deterministic and auditable (input alias → canonical id → source of authority).
- Lexical and code entities SHALL be able to resolve to a canonical id.

### R7 — Cross-graph linker (traces to D5, Phase 2)
As a retrieval designer, I want explicit canonical-identity edges written at ingest, so that cross-graph links are proof, not guesses.

Acceptance criteria:
- Cross-graph edges SHALL be written from canonical identities at ingest time, replacing runtime fuzzy correlation.
- A link SHALL record its basis (resolved identity vs candidate) so consumers can distinguish proof from candidate.

### R8 — Multi-graph query planner + provenance (traces to D5, Phase 2)
As a user, I want questions answered across lexical, domain, CPG, and evidence graphs with provenance, so that answers are grounded and traceable.

Acceptance criteria:
- The planner SHALL classify intent and route to the appropriate index/graph, using vectors as entry points and graph traversal as proof.
- Every answer SHALL return provenance: source artifact, graph path, and explicit gaps where evidence is missing.
- The planner SHALL compose existing retrievers rather than reimplementing store access.

### R9 — Packaging and dependency direction (traces to D1, D6)
As a maintainer, I want client CPG-RAG isolated from the toolkit core, so that the toolkit stays general-purpose.

Acceptance criteria:
- CPG-RAG composition SHALL live in a separate package depending on the toolkit; dependencies SHALL be one-directional (domain → foundation).
- Enhancements that are genuinely generic SHALL be contributed upstream additively (OCP), preserving existing behaviour and defaults.
- No client-specific logic SHALL be introduced into `lexical-graph`.

---

## 5. Non-functional requirements

- **SOLID conformance:** components single-responsibility, depend on abstractions, extend via registration not modification. Reviewed against the toolkit's existing provider/factory/registry patterns.
- **No vendor coupling:** no vendor SDK imports outside a provider adapter.
- **Multi-tenancy:** all writes SHALL respect the toolkit's tenant label encoding; delta replace SHALL purge the superseded tenant.
- **Embeddings as entry points:** vectors locate; the graph proves. No requirement shall depend on vector similarity as proof.
- **Provenance & auditability:** overlay, links, and answers SHALL be traceable to source.
- **Security:** retrieval SHALL be able to honour source permissions (Confluence, repo) where required; secrets via a secret store, never in code.
- **Observability & cost:** SKIP on unchanged input SHALL incur zero model, Neptune, and vector-store cost; ingest SHALL emit a delta summary.

---

## 6. Definition of done (per phase)

- **Phase 1:** R1–R5 and R9 met with automated tests; a documented Joern-JSON → enriched-Neptune run using an injected local Mistral 7B provider; no vendor SDK imports in transformers; delta SKIP/INGEST correct across code and recipe changes.
- **Phase 2:** R6–R8 met with tests; a single question traverses lexical + CPG + identity and returns a grounded answer with provenance and gaps.

---

## 7. Open questions for the client

1. Joern scope: languages, and which layers are exported (AST/CFG/CDG/REACHING_DEF/CALL)? Are file/line and commit SHA always present?
2. Local Mistral 7B serving: which runtime (Ollama / vLLM / TGI) and endpoint contract, for the injected provider?
3. Which semantic units are required first: method, endpoint, source-to-sink, or authorization-pattern summaries?
4. Identity authority (Phase 2): what is the canonical application/repo/API source, and what alias sources exist?
5. Retrieval permissions: must Confluence/repo permissions be enforced at retrieval time?
