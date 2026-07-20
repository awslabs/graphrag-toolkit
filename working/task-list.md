# Task List — CPG-RAG Migration (Neo4j → Amazon Neptune)
## graphrag-toolkit / Cross-Cloud Architecture Implementation

- **Date:** 2026-07-07
- **Reference:** architecture.md, ADR-0010

---

## Ownership Legend

| Owner | Scope |
|-------|-------|
| **Client** | All Azure EKS processes, AWS EKS provisioning/configuration/deployment, architecture approval |
| **AWS** | AWS infrastructure (Neptune, OpenSearch, S3, IAM, VPC) |
| **Deloitte** | graphrag-toolkit customization, containers/Helm charts, integration advisory |

---

## Phase 1: Architecture & Approval

| # | Task | Owner | Depends On | Status |
|---|------|-------|------------|--------|
| 1.1 | Review architecture document (architecture.md) | Client | — | ☐ |
| 1.2 | Review ADR-0010 (cross-cloud seam — S3 artifact) | Client | — | ☐ |
| 1.3 | Decision: Confirm embedding model and vector dimension | Client | — | ☐ |
| 1.4 | Decision: Enrichment strategy — per-node or traversal-based? | Client | — | ☐ |
| 1.5 | Decision: Tenant granularity (per-repo, per-repo+version, per-project) | Client | — | ☐ |
| 1.6 | Decision: Expected volume (repos/projects) for throughput sizing | Client | — | ☐ |
| 1.7 | Decision: Build trigger preference (S3 event-driven or scheduled/manual) | Client | — | ☐ |
| 1.8 | Decision: AWS EKS cluster config (instance types, scaling, namespaces) | Client | — | ☐ |
| 1.9 | Decision: Timeline for migrating extraction from Azure EKS to AWS EKS | Client | — | ☐ |
| 1.10 | Sign-off on architecture | Client | 1.1–1.9 | ☐ |

---

## Phase 2: Azure EKS — Extraction & Enrichment Pipeline

| # | Task | Owner | Depends On | Status |
|---|------|-------|------------|--------|
| 2.1 | Confirm Joern extraction pipeline outputs (nodes.json, edges.json) | Client | 1.10 | ☐ |
| 2.2 | Implement per-node enrichment (generation model → summary text) | Client | 2.1 | ☐ |
| 2.3 | Define and validate the S3 artifact schema (section 6 contract) | Client + Deloitte | 2.1 | ☐ |
| 2.4 | Implement manifest generation (repo, commit, method_signatures, recipe version) | Client | 2.3 | ☐ |
| 2.5 | Implement artifact bundle writer (nodes + edges + semantic_units + manifest) | Client | 2.3, 2.4 | ☐ |
| 2.6 | Configure Azure workload-identity federation (OIDC → AWS IAM role) | Client | 3.2 | ☐ |
| 2.7 | Implement S3 upload from Azure EKS (IAM/SigV4 authenticated) | Client | 2.5, 2.6 | ☐ |
| 2.8 | Test artifact upload — validate schema, checksums, manifest integrity | Client | 2.7 | ☐ |
| 2.9 | Document Azure EKS pipeline runbook (trigger, retry, monitoring) | Client | 2.8 | ☐ |

---

## Phase 3: AWS Infrastructure Provisioning

| # | Task | Owner | Depends On | Status |
|---|------|-------|------------|--------|
| 3.1 | Provision VPC (private subnets, NAT gateway, route tables) | AWS | 1.10 | ☐ |
| 3.2 | Create IAM role for Azure workload-identity federation (OIDC trust, S3 PutObject only) | AWS | 1.10 | ☐ |
| 3.3 | Create S3 artifact bucket with versioning, lifecycle, KMS encryption | AWS | 3.1 | ☐ |
| 3.4 | Create S3 gateway endpoint (VPC, for EKS pod access — no NAT needed) | AWS | 3.1 | ☐ |
| 3.5 | Provision Neptune DB cluster (engine >= 1.4.6.x, serverless, private) | AWS | 3.1 | ☐ |
| 3.6 | Enable Neptune IAM DB authentication | AWS | 3.5 | ☐ |
| 3.7 | Configure Neptune security group (allow only EKS pod subnets) | AWS | 3.5, 4.1 | ☐ |
| 3.8 | Create Neptune cluster parameter group | AWS | 3.5 | ☐ |
| 3.9 | Create Neptune DB subnet group (private subnets) | AWS | 3.1, 3.5 | ☐ |
| 3.10 | Provision OpenSearch Serverless collection (VECTORSEARCH type, VPC endpoint) | AWS | 3.1 | ☐ |
| 3.11 | Configure OpenSearch VPC endpoint (private access from EKS) | AWS | 3.10 | ☐ |
| 3.12 | Configure OpenSearch encryption policy | AWS | 3.10 | ☐ |
| 3.13 | Configure OpenSearch data-access policy (EKS pod IAM roles via IRSA) | AWS | 3.10, 4.3 | ☐ |
| 3.14 | Create IAM roles for EKS pods — IRSA (build role, query role) | AWS | 3.5, 3.10 | ☐ |
| 3.15 | Enable CloudTrail logging for Neptune and OpenSearch access | AWS | 3.5, 3.10 | ☐ |
| 3.16 | Enable Neptune audit logs | AWS | 3.5 | ☐ |
| 3.17 | Enable S3 access logging on artifact bucket | AWS | 3.3 | ☐ |
| 3.18 | Validate Neptune private endpoint connectivity from VPC test instance | AWS | 3.5, 3.7 | ☐ |
| 3.19 | Validate OpenSearch VPC endpoint connectivity | AWS | 3.11, 3.13 | ☐ |
| 3.20 | Document infrastructure — endpoints, ARNs, IRSA role ARNs, VPC config | AWS | 3.18, 3.19 | ☐ |

---

## Phase 4: AWS EKS — Client Cluster Setup

| # | Task | Owner | Depends On | Status |
|---|------|-------|------------|--------|
| 4.1 | Provision AWS EKS cluster (private subnets, managed node groups) | Client | 3.1 | ☐ |
| 4.2 | Configure EKS node instance types and scaling policy (per decision 1.8) | Client | 4.1 | ☐ |
| 4.3 | Configure IRSA (IAM Roles for Service Accounts) on EKS cluster | Client | 4.1, 3.14 | ☐ |
| 4.4 | Create Kubernetes namespaces for toolkit workloads | Client | 4.1 | ☐ |
| 4.5 | Validate EKS pod → Neptune private endpoint connectivity | Client | 4.1, 3.18 | ☐ |
| 4.6 | Validate EKS pod → OpenSearch VPC endpoint connectivity | Client | 4.1, 3.19 | ☐ |
| 4.7 | Validate EKS pod → S3 (via gateway endpoint) connectivity | Client | 4.1, 3.4 | ☐ |
| 4.8 | Deploy embedding model to AWS EKS (e.g. Ollama + nomic-embed) | Client | 4.1, 1.3 | ☐ |
| 4.9 | Validate embedding model service endpoint from within EKS | Client | 4.8 | ☐ |
| 4.10 | Document EKS cluster config — endpoints, namespaces, IRSA mappings | Client | 4.1–4.9 | ☐ |

---

## Phase 5: graphrag-toolkit Customization (Deloitte delivers)

| # | Task | Owner | Depends On | Status |
|---|------|-------|------------|--------|
| 5.1 | Implement S3 artifact reader (ingest nodes/edges/semantic_units from contract) | Deloitte | 2.3 | ☐ |
| 5.2 | Implement CPG-to-graph compiler (Joern nodes/edges → Neptune graph model) | Deloitte | 5.1 | ☐ |
| 5.3 | Implement SemanticCodeUnit overlay (summary text → graph overlay nodes) | Deloitte | 5.2 | ☐ |
| 5.4 | Configure embedding model provider (client's model via toolkit seam) | Deloitte | 1.3, 4.8 | ☐ |
| 5.5 | Validate embed_dimensions matches client model output | Deloitte | 5.4 | ☐ |
| 5.6 | Implement tenant derivation (repo identity → TenantId, <= 25 chars) | Deloitte | 1.5 | ☐ |
| 5.7 | Implement delta/skip-or-replace logic (manifest signature comparison) | Deloitte | 5.1, 2.4 | ☐ |
| 5.8 | Implement tenant purge (remove prior tenant on re-ingest) | Deloitte | 5.6 | ☐ |
| 5.9 | Configure Neptune Database writer (neptunedata API, SigV4, private endpoint) | Deloitte | 3.20 | ☐ |
| 5.10 | Configure OpenSearch Serverless writer (vector store, SigV4, VPC endpoint) | Deloitte | 3.20 | ☐ |
| 5.11 | Implement provenance preservation (repo, commit, file, method, timestamp) | Deloitte | 5.2 | ☐ |
| 5.12 | Implement query/retrieval pipeline (graph traversal + vector search) | Deloitte | 5.9, 5.10 | ☐ |
| 5.13 | Implement traversal-based enrichment pass (if decision 1.4 = traversal) | Deloitte | 5.9, 1.4 | ☐ |
| 5.14 | Build agent integration (expose retrieval for downstream agents) | Deloitte | 5.12 | ☐ |
| 5.15 | Pin toolkit version; implement adapter layer for bespoke isolation | Deloitte | 5.1–5.14 | ☐ |
| 5.16 | Write contract tests (artifact schema validation, build output validation) | Deloitte | 5.1, 5.2 | ☐ |
| 5.17 | Package as containers + Helm charts for EKS deployment | Deloitte | 5.1–5.16 | ☐ |
| 5.18 | Document customization — configuration, model seam, tenant rules, Helm values | Deloitte | 5.17 | ☐ |

---

## Phase 6: AWS EKS Deployment (Client deploys Deloitte deliverables)

| # | Task | Owner | Depends On | Status |
|---|------|-------|------------|--------|
| 6.1 | Deploy build pipeline containers/Helm chart to AWS EKS | Client | 5.17, 4.10 | ☐ |
| 6.2 | Configure build pipeline environment (endpoints, IRSA, region, Helm values) | Client | 6.1, 3.20 | ☐ |
| 6.3 | Deploy query/retrieval service containers/Helm chart to AWS EKS | Client | 5.17, 4.10 | ☐ |
| 6.4 | Configure query service environment (endpoints, IRSA, Helm values) | Client | 6.3, 3.20 | ☐ |
| 6.5 | Deploy agent service (if separate from retrieval) | Client | 5.17, 6.3 | ☐ |
| 6.6 | Configure S3 event trigger for build (new artifact → start build job) | Client | 6.1, 3.3 | ☐ |
| 6.7 | Configure monitoring & alerting (CloudWatch, build failures, latency) | Client | 6.1, 6.3 | ☐ |
| 6.8 | Configure auto-scaling (HPA for query service pods) | Client | 6.3 | ☐ |
| 6.9 | Document deployment runbook (deploy, rollback, Helm upgrades) | Client | 6.1–6.8 | ☐ |

---

## Phase 7: Integration Testing

| # | Task | Owner | Depends On | Status |
|---|------|-------|------------|--------|
| 7.1 | End-to-end test: Azure EKS extract → S3 upload → AWS EKS build → Neptune/OpenSearch | Client + Deloitte | 6.2, 2.8 | ☐ |
| 7.2 | Validate graph correctness (nodes, edges, labels, properties) | Deloitte | 7.1 | ☐ |
| 7.3 | Validate vector index (embedding dimensions, similarity search) | Deloitte | 7.1 | ☐ |
| 7.4 | Validate delta/idempotency (re-run same repo = skip; changed repo = replace) | Deloitte | 7.1 | ☐ |
| 7.5 | Validate multi-tenant isolation (two repos in same cluster, no bleed) | Deloitte | 7.1 | ☐ |
| 7.6 | Validate query/retrieval accuracy (known queries, expected results) | Deloitte + Client | 7.2, 7.3 | ☐ |
| 7.7 | Performance test: build at expected volume (decision 1.6 sizing) | Client + Deloitte | 7.1, 1.6 | ☐ |
| 7.8 | Security validation: no public endpoints, IRSA working, SG correct | Client + AWS | 7.1 | ☐ |
| 7.9 | Validate private connectivity: EKS pods → Neptune, OpenSearch, S3 (no internet) | Client | 7.1 | ☐ |
| 7.10 | Sign-off on integration test results | Client | 7.1–7.9 | ☐ |

---

## Phase 8: Production Cutover & Migration

| # | Task | Owner | Depends On | Status |
|---|------|-------|------------|--------|
| 8.1 | Migrate production workloads (Azure EKS pipelines point to S3 bucket) | Client | 7.10 | ☐ |
| 8.2 | Decommission Neo4j (after validation period) | Client | 8.1 | ☐ |
| 8.3 | Monitor production build/query (first 2 weeks) | Client + Deloitte | 8.1 | ☐ |
| 8.4 | Migrate extraction/enrichment from Azure EKS to AWS EKS (per decision 1.9) | Client | 8.3 | ☐ |
| 8.5 | Revoke Azure workload-identity federation trust (after Azure EKS decommission) | AWS + Client | 8.4 | ☐ |
| 8.6 | Final security audit (private only, no stale credentials, IRSA verified) | Client + AWS | 8.4, 8.5 | ☐ |
| 8.7 | Document end-state architecture (all workloads in AWS EKS) | Deloitte | 8.6 | ☐ |
| 8.8 | Close-out: confirm migration complete, archive interim config | Client | 8.6, 8.7 | ☐ |

---

## Summary — Ownership Matrix

| Phase | Client | AWS | Deloitte |
|-------|--------|-----|----------|
| 1. Architecture & Approval | Decisions, sign-off | — | Advisory |
| 2. Azure EKS Pipeline | Build & operate | — | Schema co-design |
| 3. AWS Infrastructure | — | Provision & configure | — |
| 4. AWS EKS Cluster | Provision, configure, operate | — | — |
| 5. Toolkit Customization | — | — | Build & deliver |
| 6. AWS EKS Deployment | Deploy & operate | — | Support |
| 7. Integration Testing | Execute & validate | Infra validation | Build validation |
| 8. Production & Migration | Operate, migrate, cutover | Revoke access, audit | Document |

---

## Dependencies & Critical Path

```
1.10 (Architecture sign-off)
  ├── 2.x (Azure EKS pipeline) ───── 2.8 (artifact upload tested)
  ├── 3.x (AWS infra) ────────────── 3.20 (infra documented)
  └── 4.x (AWS EKS cluster) ──────── 4.10 (EKS ready)
        │
  5.x (toolkit customization) ────── 5.17 (containers/Helm delivered)
        │
  6.x (AWS EKS deployment) ───────── 6.2 (build deployed)
        │
  7.x (Integration testing) ──────── 7.10 (sign-off)
        │
  8.x (Production cutover & migration)
```

- Phases 2, 3, 4 can run in **parallel** after architecture sign-off (1.10).
- Phase 5 (Deloitte) can start in parallel once artifact schema is agreed (2.3).
- Phase 6 requires outputs from phases 3, 4, and 5.
- Phase 7 requires all prior phases complete.
- Phase 8 is sequential after successful integration testing.
