# Domain Graph Migration Architecture: Portable JSON Boundary and AWS Neptune Runtime

## 1\. Purpose

This document describes a practical transition architecture for moving existing client graph workloads into an AWS-native GraphRAG runtime using Amazon Neptune, Neptune Analytics, and/or OpenSearch.

The schema of the JSON files is illustrative and must be updated by the client. This document, once factual and accurate, will serve as the development requirements for the Code Property Graph AWS Solution that will be developed to augment the existing solution.

The new solution will be based on the existing solution accelerator from AWS Labs: [https://github.com/awslabs/graphrag-toolkit](https://github.com/awslabs/graphrag-toolkit)

*Note: The AWS solution assumes that the required artifacts are available and in the correct format.*

The client currently operates separate graph processes, including:

1. A **lexical graph** based on Confluence/document content.  
2. A **domain graph of type CPG** based on Code Property Graph output, currently persisted into Neo4j with vector-in-graph enrichment.

The target architecture does not require immediate ownership of the client’s extraction processes. Instead, it introduces a **portable artifact boundary** between client-owned extraction/enrichment and AWS-owned build/runtime.

## 2\. Core Architectural Principle

**Caution: Extraction can remain portable. Build and runtime become target-specific.**

When the target graph runtime becomes AWS Neptune, the graph build and query runtime must execute in an AWS-reachable environment. However, extraction does not always need to move into AWS if the extracted artifacts can be persisted and shipped.

| Concern | Architectural Position |
| :---- | :---- |
| Extraction | Can remain outside AWS |
| Enrichment | Can remain outside AWS if outputs are portable |
| Embedding generation | Can remain outside AWS if vectors are included in the artifact |
| Build/load | Must run in AWS or from an AWS-reachable environment |
| Query/runtime | Must run in AWS or from an AWS-reachable environment |
| Model access | Required wherever build/query depends on the model |

## 3\. Graph Domains

The client has two distinct graph domains. Each follows a different migration strategy:

| Graph Domain | Source | Current Pattern | Target Pattern |
| :---- | :---- | :---- | :---- |
| Lexical graph | Confluence, documents, pages, chunks | Existing document graph pipeline | **Runs entirely in AWS EKS** using graphrag-toolkit native document-graph pipeline (extract + build + query). No portable artifact needed. |
| CPG domain graph | Joern / Code Property Graph / Neo4j | CPG \+ vector-in-graph in Neo4j | Portable JSON artifact shipped to S3; loaded by custom Domain Graph Module in AWS EKS |

**Key architectural distinction:**
- **Lexical**: The graphrag-toolkit already has a production lexical/document-graph pipeline. It makes no sense to customize the toolkit for a proprietary extraction process. Extract, build, and query all run natively in AWS EKS.
- **CPG**: The client's Joern extraction process is proprietary. The toolkit does not own CPG extraction. The client ships a portable JSON artifact; Deloitte builds a custom Domain Graph Module to load it.

Both domains share the same AWS EKS cluster, same Neptune instance, same OpenSearch collection, and same query layer. The graphrag-toolkit is deployed as **one container image with three runtime personalities** (Extract, Build, Query) — see architecture.md section 10.

These graph domains are handled side-by-side, not collapsed into a single extraction process.

## 4\. CPG Domain Graph: Target Boundary

For the CPG graph, the client should update the current process so that all outputs currently written directly into Neo4j are instead written to a **portable JSON domain graph artifact**.

### Current State

Repo checkout

  ↓

Joern / CPG extraction

  ↓

Code slicing

  ↓

Optional summaries

  ↓

Embedding generation

  ↓

Neo4j graph \+ vector-in-graph

### Target State

Repo checkout

  ↓

Joern / CPG extraction

  ↓

Code slicing

  ↓

Optional summaries

  ↓

Embedding generation

  ↓

Portable JSON Domain Graph Artifact

  ↓

AWS Domain Graph Module

  ↓

Neptune Analytics

or

Neptune Database \+ OpenSearch

## 5\. Transformation Required from the Client

The client does not need to redesign the entire CPG process. The main change is the persistence boundary.

| Current Process | Target Process |
| :---- | :---- |
| Write CPG nodes to Neo4j | Write CPG nodes to `nodes.jsonl` |
| Write CPG edges to Neo4j | Write CPG edges to `edges.jsonl` |
| Store vectors as Neo4j node properties | Write vectors to `vectors.jsonl` |
| Store summaries on Neo4j nodes | Write summaries to `summaries.jsonl` |
| Store code snippets in Neo4j | Write code slices to `code_slices.jsonl` |
| Use Neo4j as the handoff boundary | Use portable JSON as the handoff boundary |

The orchestration can remain largely the same. The database-specific writer changes.

## 6\. CPG Portable JSON Artifact

The CPG artifact must include enough information for AWS to understand and reconstruct the graph and vector enrichment without participating in extraction.

| File | Purpose |
| :---- | :---- |
| `manifest.json` | Describes artifact type, schema version, repo, commit, embedding model, dimensions, and vector strategy |
| `nodes.jsonl` | Contains CPG nodes with stable identifiers |
| `edges.jsonl` | Contains CPG relationships between stable node IDs |
| `vectors.jsonl` | Contains precomputed vectors and embedding metadata |
| `summaries.jsonl` | Contains method/file/slice summaries, if available |
| `code_slices.jsonl` | Contains code snippets, line ranges, and evidence slices |
| `findings.jsonl` | Contains scanner findings or security findings, if available |
| `lineage.jsonl` | Contains analysis run, repo, commit, and source artifact metadata |

## 7\. Required CPG Artifact Metadata

The artifact must avoid Neo4j internal IDs and use stable portable identifiers.

| Required Field | Purpose |
| :---- | :---- |
| `repo_id` | Identifies the repository |
| `commit_sha` | Anchors graph to a specific code version |
| `file_path` | Connects nodes to source files |
| `node_type` | Identifies CPG node type, such as `Method`, `File`, `Call`, `TypeDecl` |
| `cpg_node_id` | Stable cross-system node identifier |
| `line_start` | Source-code evidence start line |
| `line_end` | Source-code evidence end line |
| `code_hash` | Detects code changes |
| `analysis_run_id` | Identifies the extraction/enrichment run |
| `embedding_model` | Identifies vector model used |
| `embedding_dimensions` | Required for vector index compatibility |
| `similarity_function` | Cosine, dot product, or other similarity function |
| `embedding_target` | Describes what was embedded: raw code, summary, slice, finding, etc. |

## 8\. Example CPG Artifact Records

### Example Node Record

| {  "cpg\_node\_id": "payments-api:abc123:Method:validateToken:sha256001",  "node\_type": "Method",  "labels": \["Method"\],  "repo\_id": "payments-api",  "commit\_sha": "abc123",  "file\_path": "src/auth/token.py",  "fully\_qualified\_name": "auth.TokenService.validateToken",  "name": "validateToken",  "line\_start": 42,  "line\_end": 91,  "code\_hash": "sha256001",  "properties": {    "language": "python",    "visibility": "public"  }} |
| :---- |

### Example Edge Record

| {  "edge\_id": "edge:001",  "source\_cpg\_node\_id": "payments-api:abc123:File:src/auth/token.py",  "target\_cpg\_node\_id": "payments-api:abc123:Method:validateToken:sha256001",  "edge\_type": "CONTAINS",  "properties": {    "analysis\_run\_id": "run-20260704-001"  }} |
| :---- |

### Example Vector Record

| {  "cpg\_node\_id": "payments-api:abc123:Method:validateToken:sha256001",  "embedding\_target": "method\_summary",  "embedding\_model": "client-embedding-model",  "embedding\_dimensions": 1024,  "similarity\_function": "cosine",  "embedding\_text\_hash": "sha256:abc123",  "vector": \[0.012, \-0.044, 0.087\]} |
| :---- |

## 9\. AWS Responsibility for the CPG Domain Graph

The AWS-side responsibility begins at the portable JSON boundary.

| AWS Component | Responsibility |
| :---- | :---- |
| S3 landing zone | Receives portable JSON artifact |
| Domain Graph Module | Validates, normalizes, and loads graph artifacts |
| Neptune Analytics | Optional native graph \+ vector target |
| Neptune Database | Relationship graph target |
| OpenSearch | Vector sidecar when using Neptune Database |
| Query service | Executes graph/vector retrieval |
| Evidence assembler | Builds grounded evidence packages |
| Response LLM | Generates final answer from evidence |

## 10\. Vector Strategy Depends on Neptune Target

The handling of precomputed vectors depends on the selected AWS target.

| Target | Graph Location | Vector Location | Join Pattern |
| :---- | :---- | :---- | :---- |
| Neptune Analytics | Neptune Analytics | Neptune Analytics | Native graph \+ vector |
| Neptune Database | Neptune Database | OpenSearch | Join by stable `cpg_node_id` |
| Hybrid future mode | Neptune / OpenSearch / S3 | Depends on workload | Join by artifact identity |

For CPG, vectors should be provided in the portable JSON artifact. AWS should not need to regenerate vectors during build unless the artifact is incomplete or incompatible.

## 11\. Build-Time and Query-Time Model Dependency

If the portable JSON artifact contains precomputed vectors, the AWS build stage does not need access to the client’s embedding model.

However, query-time semantic retrieval still needs a compatible embedding model to vectorize the user question.

| Stage | Requires Model? | Explanation |
| :---- | ----: | :---- |
| CPG JSON validation | No | Schema validation only |
| CPG graph load | No | Deterministic graph load |
| CPG vector load | No | Uses precomputed vectors |
| CPG graph traversal | No | Graph engine handles traversal |
| CPG semantic query | Yes | User question must be embedded |
| CPG final answer | Yes | Response LLM generates answer |
| CPG summarization/enrichment in AWS | Yes | Only if AWS generates new summaries |

## 12\. Lexical Graph — Runs Entirely in AWS (Toolkit Native)

The lexical graph uses the graphrag-toolkit's **native document-graph pipeline**. There is no portable JSON artifact for the lexical domain — the toolkit handles extraction, chunking, embedding, build, and query natively.

It makes no architectural sense to customize the AWS graphrag-toolkit for a proprietary lexical extraction process when the toolkit already handles Confluence/document content out of the box.

### Lexical Graph in AWS EKS

| Stage | Runs in | Personality | Notes |
| :---- | :---- | :---- | :---- |
| Confluence content access | AWS EKS | Extract | API access or content export |
| Lexical extraction + chunking | AWS EKS | Extract | Toolkit native pipeline |
| Embedding generation | AWS EKS | Extract | Uses toolkit embedding seam; model in AWS |
| Summarization | AWS EKS | Extract | Uses toolkit generation seam; model in AWS |
| Build into Neptune + OpenSearch | AWS EKS | Build | Toolkit native build |
| Query / retrieval | AWS EKS | Query | Toolkit native query |

### Client responsibility for lexical domain
- Make Confluence content accessible to AWS EKS (API credentials, network access, or content export to S3)
- Deploy embedding and generation models in AWS EKS
- No extraction code to write — the toolkit does this

## 13\. Lexical Graph Target Pattern

Entirely in AWS EKS (graphrag-toolkit native):

```
Confluence content (API or export)
  ↓
graphrag-toolkit Extract personality (chunking, summarization, embedding)
  ↓
graphrag-toolkit Build personality (graph + vectors → Neptune + OpenSearch)
  ↓
graphrag-toolkit Query personality (retrieval, evidence assembly, answer generation)
```

No cross-cloud boundary. No portable artifact. The toolkit handles this end-to-end.

## 14\. Practical Transition Architecture

This architecture is feasible but operationally more complex than a fully cloud-native implementation.

| Benefit | Cost |
| :---- | :---- |
| Client keeps existing extraction/enrichment logic | Requires portable artifact contract |
| AWS does not need to own Joern extraction | Requires stable ID design |
| Build can be deterministic if vectors are precomputed | Requires vector compatibility management |
| Supports phased migration | Requires artifact shipping and validation |
| Preserves existing client investment | Requires target-specific AWS loaders |
| Avoids immediate CPG-RAG overclaiming | Requires later retrieval/evidence layer work |

## 15\. Recommended Phase Breakdown

| Phase | Name | Scope |
| :---- | :---- | :---- |
| Phase 1 | Portable Artifact Boundary | Client emits portable JSON for CPG and/or lexical extracted artifacts |
| Phase 2 | AWS Domain Graph Loader | AWS validates and loads graph/vector artifacts |
| Phase 3 | AWS Graph Runtime | Neptune/Neptune Analytics/OpenSearch become runtime targets |
| Phase 4 | Retrieval Adapter | Query services retrieve graph/vector evidence |
| Phase 5 | CPG-RAG / Lexical-RAG Semantics | Add graph-aware retrieval, evidence assembly, and LLM answer generation |

## 16\. Scope Clarification

This should not initially be positioned as a full CPG-RAG rebuild.

A better Phase 1 position is:

We are introducing a Domain Graph Module that can ingest portable graph artifacts. The first supported domain graph type is CPG. The client remains responsible for extraction and enrichment. AWS becomes responsible for loading, indexing, querying, and operationalizing the graph.

## 17\. Key Architectural Statement

The migration is not from Joern to AWS.

The migration is from:

Neo4j as the integration boundary

to:

Portable JSON as the integration boundary

Then AWS becomes the target-specific build and runtime environment.

## 18\. Final Position

This architecture is technically viable but not necessarily optimal. It is a pragmatic transition pattern.

The client must update existing Neo4j-oriented processes to produce portable JSON artifacts containing graph structure, stable identities, lineage, and precomputed vectors. AWS then consumes those artifacts through a Domain Graph Module and loads them into Neptune, Neptune Analytics, and/or OpenSearch.

The main dependency created by moving to Neptune is that build and runtime become AWS-specific. Model reachability is only required where models are used: extraction, embedding generation, query embedding, planning, summarization, or final answer generation.

In short:

| Principle | Statement |
| :---- | :---- |
| Extraction | Portable |
| Artifact | Portable JSON |
| Build | AWS-specific |
| Runtime | AWS-specific |
| Vectors | Precomputed where possible |
| Query | Model-dependent |
| Traversal | Graph-dependent |
| Optimization | Future phase |

