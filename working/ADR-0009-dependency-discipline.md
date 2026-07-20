# ADR-0009: Dependency Discipline — Insulating the Bespoke and Proprietary Layers from Toolkit Churn

- **Status:** Accepted principle (engineering-conformance — ours to hold, not a client decision; ADR-0001 §1)
- **Date:** 2026-06-29
- **Scope:** Cross-cutting design principle governing how CPG-RAG (bespoke) and the Master
  Evidence Graph (proprietary, Kanjani AI Research) depend on the graphrag-toolkit.

## 1. Context
The CPG-RAG layer is bespoke and forever-maintained; the Master Evidence Graph sits above it
as proprietary IP. Both depend on the graphrag-toolkit, which evolves independently.
Uncontrolled coupling produces two failures: unbounded maintenance cost as the toolkit
changes, and erosion of the IP boundary between the toolkit (AWS/open), the bespoke CPG-RAG,
and the proprietary MEG.

## 2. Principle
The bespoke and proprietary layers MUST be insulated from toolkit churn. Dependency direction
is strictly one-way: **MEG → CPG-RAG → graphrag-toolkit.** Never the reverse.

## 3. Rules
1. **Depend only on stable/public toolkit contracts** — `GraphStore`/`VectorStore`, the
   document-graph public API (`PipelineExecutor`, constructors), `codeproperty-graph`
   `DeltaIngestor`, the `BaseEmbedding`/`LLM` seam, `TenantId`. Do not reach into private or
   internal modules.
2. **Pin exact toolkit versions** (`==`, never `>=`). Upgrade deliberately, never transitively.
3. **Wrap every toolkit touch-point behind an adapter** (anti-corruption layer). A toolkit
   change lands in one adapter, not across CPG-RAG/MEG.
4. **Maintain a contract/compatibility test suite** that runs against each candidate toolkit
   version before upgrade — schema-validation at the boundary, so breakage is caught early.
5. **Push genuinely-generic capability upstream** (ADR-0001 D6) to shrink the bespoke surface;
   keep only domain (CPG) and proprietary (MEG) logic downstream.
6. **Enforce the IP boundary via the dependency direction** — proprietary MEG logic never
   couples into the toolkit or into the CPG-RAG package's public surface.

## 4. Consequences
- **Cost:** upfront adapter, version-pinning, and contract-test discipline; upgrades are
  deliberate work, not automatic.
- **Benefit:** maintenance against toolkit change is bounded and predictable; the IP boundary
  is structurally protected; generic improvements accrue upstream to everyone while
  proprietary value stays isolated.

## 5. Relationship to other ADRs
Operationalises ADR-0001 (D1 packaging/dependency direction, D6 upstream-generic). Applies to
every bespoke component named in ADR-0002 through ADR-0008 and to the MEG layer above them.