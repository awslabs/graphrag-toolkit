# ADR-0002: Tenant Strategy and Logical Separation for CPG

- **Status:** Proposed — client decision required
- **Date:** 2026-06-29
- **Stance:** We propose and inform; the client decides (see ADR-0001 §1 Decision stance).
- **Relates to:** ADR-0001 (D1, D5), `requirements.md` R6/R9, `lexical-graph/.../tenant_id.py`, `docs-site/.../lexical-graph/multi-tenancy.mdx`

---

## 1. Context

The client currently runs **two Neo4j databases per Project**, where a **Project = one or many repositories**. We are proposing to replace physical database separation with **logical separation inside a shared Neptune (and/or Neptune Analytics) store**, using the toolkit's existing `TenantId` mechanism.

This ADR records the decision to be made about **tenant granularity** and how Project / repo / the client's two-database split map onto `TenantId`. We do not select for the client; we present options, constraints, a recommendation, and open questions.

> Open question carried forward: **what do the client's two Neo4j databases per Project actually represent** (e.g. raw vs enriched CPG, two domains, two environments)? The mapping below changes with the answer, so this must be confirmed. We do not assume.

## 2. Constraints (tooling-imposed, non-negotiable)

From `tenant_id.py` (verified in source):

- A `TenantId` value is **1–25 characters, lowercase letters/numbers/periods, periods not at start or end**. No uppercase, hyphens, slashes, or `@`.
- The **default tenant** is `None` (renders as `default_`).
- **Label encoding:** `format_label` produces `` `{label}{value}__` `` — the tenant value is concatenated onto the node label with a trailing `__` (e.g. label `Method`, tenant `proj1` → `` `Methodproj1__` ``).
- **Vector index naming:** `format_index_name` produces `{index_name}_{value}` (e.g. `chunk_proj1`) — i.e. one index name per tenant.
- **Id encoding:** `format_id` produces `{prefix}:{value}:{id}` (non-default) vs `{prefix}::{id}` (default).

From `codeproperty-graph.DeltaIngestor` (verified in source):

- On a **changed** ingest, it writes under a **new tenant id** and then **purges the previous tenant** (`delete_tenant`). Tenant therefore behaves as a **versioned snapshot boundary**, and *replace is whole-tenant*.

**Consequence of combining these:** tenant granularity must be compatible with "replace/purge the entire tenant on change." Any identifier that is not already ≤25 lowercase-alnum-plus-period characters (most repo/project names, e.g. `github.com/org/Repo-Name`) must be **derived** into a valid tenant id — which ties this decision to the canonical identity spine (ADR-0001 D5 / requirements R6).

## 3. Options

### Option A — Tenant per Project (coarse)
All repos of a Project share one tenant.
- **Pros:** trivial project-scoped retrieval (one tenant = one project); fewer tenants.
- **Cons:** collides with `DeltaIngestor` purge semantics — a change in *one* repo would purge the *whole* project's CPG unless the write path is changed to not purge. Higher blast radius.

### Option B — Tenant per repo (or repo+version) — *recommended*
Each repo (optionally repo+version) is its own tenant; a Project is a **set** of repo-tenants grouped by a naming convention (e.g. shared prefix within the 25-char budget).
- **Pros:** aligns with `DeltaIngestor` per-repo skip/replace + purge; change blast radius is one repo; matches the existing design.
- **Cons:** project-scoped retrieval must fan out across the project's tenant set (the multi-graph planner must be tenant-group aware); requires a grouping convention and a derivation into valid tenant ids.

### Option C — Tenant per Project, repo as label/property, versioning without tenant purge
Keep Project as the tenant; encode repo as a label/property; handle versioning via the toolkit's versioned-updates instead of tenant purge.
- **Pros:** project-scoped retrieval trivial; no tenant fan-out.
- **Cons:** diverges from `codeproperty-graph`'s delta design (would not use tenant purge); more custom work; reintroduces in-place update concerns we set out to avoid.

## 4. Recommendation (for client approval)

Adopt **Option B** (tenant per repo, optionally repo+version), with the **canonical identity spine deriving tenant ids** deterministically from canonical repo identity (normalise + short hash to satisfy the 25-char lowercase constraint). Group repos into a Project via a naming/registry convention so the planner can scope retrieval to a project's tenant set. This preserves the delta/purge design and bounds blast radius to a single repo.

On the two-database split: **if** the two Neo4j DBs are "raw" and "enriched" CPG, that split **disappears** in the target — enrichment happens before load, so a single enriched graph (raw CPG + `SemanticCodeUnit` overlay) lives under the repo tenant. We would inform the client that two DBs collapse to one tenant-scoped enriched graph. If the two DBs are two distinct domains, they map to two tenants (or two graph shapes) — pending the clarification above.

## 5. Consequences

- Retrieval must be **tenant-group aware** to answer project-level questions (feeds the multi-graph planner, R8).
- The **canonical identity spine (R6) becomes a prerequisite** for tenant-id derivation, not just cross-graph linking.
- Delta purge blast radius is bounded to one repo (Option B).
- Vector indexes are per-tenant via `format_index_name` — see ADR-0003 for the store-topology interaction.

## 6. Open questions for the client

1. What do the two Neo4j databases per Project represent?
2. Is per-repo (or per-repo-per-version) tenant granularity acceptable, given tenant churn from delta purge?
3. What is the Project→repo grouping convention (prefix/registry)?
4. Are cross-project retrieval questions in scope (affects planner tenant scoping)?
