# Migration Mapping: Local / Hybrid → AWS

**Status:** Draft for client alignment · **Relates to:** ADR-0002, ADR-0003, ADR-0004,
ADR-0005, ADR-0006, ADR-0007

## Purpose
A component-by-component map from the client's current stack to AWS targets, with the
decision and status for each. This ties the ADRs into one migration picture.

## Mapping
| Component | Current (client) | AWS target | Decision / status |
|---|---|---|---|
| Code extraction | Joern → nodes.json / edges.json | Unchanged (external) | Preserved (ADR-0006) |
| Graph store | Neo4j (self-hosted) | Neptune Database (default) or Neptune Analytics | Planned; store-agnostic (ADR-0003/0004) |
| Enrichment (summaries) | In Neo4j (read-modify-write) | Build-time transform using injected model | Re-homed; enricher R1 rework; per-node vs traversal fork |
| Generation model | Local Ollama (Mistral 7B) | AWS-hosted Ollama (self-managed) or Bedrock LLM | Decision (ADR-0007) |
| Embedding model | Local Ollama embedding | AWS-hosted Ollama (self-managed) or Bedrock embedding | Decision (ADR-0007); confirm model+dimension |
| Vectors | Vector-in-graph (Neo4j) | OpenSearch (Neptune DB) or vector-in-graph (Neptune Analytics) | Store-agnostic write strategy (ADR-0004) |
| Vectorization ownership | Client pipeline | Toolkit embeds at build+query | No BYO-vector (ADR-0005) |
| Semantic overlay | Client "compiler" (what to summarise) | CPG-RAG overlay builder (SemanticCodeUnit) | Net-new, reuses client logic (R4) |
| Tenant separation | Two Neo4j DBs per project | Logical tenants via TenantId | tenant-per-repo recommended (ADR-0002) |
| Orchestration store | Azure Cosmos DB (MongoDB API) | DynamoDB (AWS-native) or keep cross-cloud | Open decision (earlier) |

## What is preserved
- Joern extraction (unchanged).
- Enrichment *logic* and model *choice* (Mistral generation + Ollama embedding).
- Vector size / embedding model semantics.

## What changes
- Storage: Neo4j → Neptune; vectors → OpenSearch or in-graph.
- Enrichment *location*: from in-graph read-modify-write to a build-time transform.
- Vectorization *owner*: from the client's pipeline to the toolkit (build + query).
- Model *hosting location*: from local to AWS.

## The effort variables (size these before committing)
1. **Enrichment: per-node vs traversal-based.** Per-node → pre-load transform (light).
   Traversal-based → post-load overlay pass (real re-engineering).
2. **Model hosting: self-managed-in-AWS vs Bedrock-native.** (a) preserves the model,
   self-managed; (b) fully managed, requires re-embed.
3. **Orchestration store: DynamoDB vs cross-cloud Cosmos.** Cross-cloud adds latency/egress.

## Sequencing (suggested)
1. Stand up Neptune + (OpenSearch or NA) + in-AWS model hosting.
2. Build path: Joern JSON → document-graph/codeproperty-graph → Neptune, tenant-per-repo, delta.
3. Overlay + embedding at build (toolkit owns vectorization).
4. Retrieval + provenance; join to the lexical (Confluence) graph via identity (Phase 2).
5. Resolve the Cosmos/orchestration decision.