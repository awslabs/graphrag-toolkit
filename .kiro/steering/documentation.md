# Documentation Standards

## Overview

This document defines documentation standards for the GraphRAG Toolkit — a collection of Python tools for building graph-enhanced Generative AI applications on AWS. It covers the two main packages: **lexical-graph** and **byokg-rag**.

## Scope

These guidelines apply to:
- Root-level `README.md` and all per-package `README.md` files
- Everything under `docs/lexical-graph/` and `docs/byokg-rag/`
- Inline code comments and docstrings
- Example notebooks and scripts under `examples/`

---

## Writing Style

- Use plain, precise English. Avoid marketing language.
- Write in the active voice and present tense.
- Address the reader as "you".
- Do not use emojis. Use plain-text callouts instead:
  - `NOTE:` for important context
  - `WARNING:` for potential data loss or breaking behaviour
  - `TIP:` for optional but useful guidance
- Keep sentences short. Prefer bullet lists over long paragraphs for multi-step procedures.
- Define acronyms on first use (e.g., "GraphRAG (Graph Retrieval-Augmented Generation)").

---

## Audience Segmentation

Tailor content to three reader types and state any assumed knowledge up front.

| Audience | Focus |
|---|---|
| Application developers | Installation, API usage, query composition, examples |
| ML/Data engineers | Indexing pipelines, extraction config, storage backends |
| DevOps / cloud engineers | Deployment, AWS service configuration, security, IAM |

---

## Repository Structure Conventions

```
graphrag-toolkit-aws/
├── README.md                    # Repo-level overview and navigation
├── CHANGELOG.md                 # Version history
├── CONTRIBUTING.md              # Contributor guide
├── docs/
│   ├── README.md                # Docs index
│   ├── lexical-graph/           # Lexical-graph reference docs
│   └── byokg-rag/               # BYOKG-RAG reference docs
├── lexical-graph/               # Package source + package-level README
├── byokg-rag/                   # Package source + package-level README
├── lexical-graph-contrib/       # Community contributions (e.g. FalkorDB)
└── examples/                    # Runnable notebooks and scripts
    ├── lexical-graph/
    ├── lexical-graph-local-dev/
    ├── lexical-graph-hybrid-dev/
    └── byokg-rag/
```

---

## README Requirements

### Root `README.md`

Must contain, in order:

1. **One-sentence project description** — what it is and who it is for.
2. **Package table** — name, brief purpose, link to package folder.
3. **Quick navigation** — links to `docs/`, `examples/`, `CONTRIBUTING.md`, `CHANGELOG.md`.
4. **Security and License** — link to `security.md` and state the licence (Apache-2.0).
5. **External resources** — blog posts, videos, papers; include the medium (e.g., `[Blog]`, `[Video]`, `[Paper]`).

### Per-Package `README.md`

Each package folder (`lexical-graph/`, `byokg-rag/`) must contain a README with:

1. **Overview** — what the package does and the problem it solves.
2. **Prerequisites** — Python version (`>=3.10`), AWS services required, IAM permissions needed.
3. **Installation** — exact pip/uv commands; note any optional extras.
4. **Quick start** — minimal working code snippet runnable in under five minutes.
5. **Configuration reference** — link to detailed config docs in `docs/`.
6. **Links** — point to the relevant `docs/<package>/` folder and examples.

---

## `docs/` Content Standards

### `docs/lexical-graph/`

Maintain one file per logical topic. Required files and their scope:

| File | Content |
|---|---|
| `overview.md` | Architecture diagram, design goals, comparison to pure vector RAG |
| `graph-model.md` | Node types, relationship types, property schema |
| `storage-model.md` | Logical storage layers (graph + vector) |
| `indexing.md` | Extraction pipeline stages, configuration options |
| `querying.md` | Query engine, composition of retrieval strategies |
| `configuration.md` | All `GraphRAGConfig` fields with types, defaults, and examples |
| `readers.md` | Supported document readers and how to add new ones |
| `prompts.md` | Prompt templates, customisation, system vs user roles |
| `metadata-filtering.md` | Filter syntax and supported operators |
| `semantic-guided-search.md` | How semantic-guided traversal works |
| `traversal-based-search.md` | Traversal strategy, depth/breadth controls |
| `traversal-based-search-configuration.md` | All traversal parameters |
| `batch-extraction.md` | Batch mode overview and use cases |
| `configuring-batch-extraction.md` | Step-by-step batch configuration |
| `multi-tenancy.md` | Tenant ID model, isolation guarantees |
| `versioned-updates.md` | How to update an existing graph incrementally |
| `hybrid-deployment.md` | Local extraction + cloud query patterns |
| `security.md` | IAM roles, encryption, network isolation |
| `aws-profile.md` | Configuring named AWS profiles |
| `faq.md` | Common questions and known limitations |
| Graph store files | One file per backend: Neptune DB, Neptune Analytics, Neo4j, FalkorDB |
| Vector store files | One file per backend: Neptune Analytics, OpenSearch Serverless, PostgreSQL, S3 Vectors |

### `docs/byokg-rag/`

Required files:

| File | Content |
|---|---|
| `overview.md` | Architecture, KGQA approach, how LLMs and the graph interact |
| `indexing.md` | Dense index, fuzzy string index, graph-store index |
| `querying.md` | Query engine, entity linking, graph traversal, reranking, verbalisation |
| `graph-stores.md` | Supported graph stores and connection setup |
| `configuration.md` | All configuration parameters |
| `faq.md` | Common questions and known limitations |

---

## Backend-Specific Doc Pages

Graph store and vector store pages must each include:

1. **Service summary** — what the AWS service is and when to choose it.
2. **Prerequisites** — required AWS resources and IAM permissions.
3. **Installation** — any extra packages (`pip install ...`).
4. **Connection setup** — code snippet showing how to instantiate the store class.
5. **Configuration options** — table of constructor parameters with types and defaults.
6. **Limitations** — known constraints (e.g., query complexity, regional availability).
7. **See also** — links to relevant AWS service documentation.

---

## Code Examples

### Inline Code Blocks

- Always specify the language identifier (` ```python `, ` ```bash `, ` ```json `).
- Keep examples self-contained and runnable; import every symbol used.
- Show realistic but minimal data — avoid multi-hundred-line samples in docs.
- Include the expected output or a description of what the snippet produces.

### `examples/` Directory

- Each example must have its own `README.md` explaining what it demonstrates and how to run it.
- Notebooks (`.ipynb`) must be cleared of output before committing (cell outputs may contain credentials or large data).
- Example scripts must work against the latest released package version, not dev branches.
- Specify which AWS region the example was tested in.

---

## AWS-Specific Documentation

- Always state the minimum IAM permissions required for a feature. Provide a minimal IAM policy JSON snippet where practical.
- Name the AWS services used and link to their AWS documentation.
- Explicitly note which features require specific service tiers (e.g., Neptune Analytics vs Neptune DB).
- Do not hardcode AWS account IDs, region names, or ARNs in docs. Use placeholders like `<account-id>`, `<region>`, `<cluster-endpoint>`.
- Document any VPC or network requirements (e.g., notebook instances inside a VPC to reach Neptune).

---

## Python API Documentation

### Docstrings

- Use [Google-style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) for all public classes and functions.
- Every public symbol in `__init__.py` exports must have a docstring.
- Include `Args:`, `Returns:`, and `Raises:` sections where applicable.
- Document parameter types using the type annotation in the signature; the docstring body need not repeat them.

### Config Classes

`GraphRAGConfig` and other config dataclasses must document every field with:
- Purpose
- Type and valid values
- Default value
- Example

---

## CHANGELOG Standards

Follow [Keep a Changelog](https://keepachangelog.com/) conventions:

- Group entries under `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`.
- Each entry references the feature or fix concisely in one line.
- New releases go at the top; unreleased changes live under `[Unreleased]`.
- Include the package version and release date for each release block.

---

## Maintenance

- Update docs in the **same PR** as the corresponding code change.
- Validate all internal Markdown links before merging (`[text](path)` links must resolve).
- Replace or remove screenshots whenever the referenced UI or output changes.
- Run a link-check pass before each release.
- The `faq.md` files are living documents — add entries whenever a question recurs in issues or discussions.
- Version-pin any external documentation links where possible to avoid link rot.
