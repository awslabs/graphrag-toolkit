# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Code Property Graph — domain layer for Joern CPG delta ingestion.

Built on document-graph for typed property graph primitives.
Adds CPG-specific models, delta comparison, manifest management,
and tenant lifecycle for incremental code analysis.

Extended with:
- Vector loading (precomputed embeddings → OpenSearch)
- Summary overlay (LLM summaries → Neptune node properties)
- Code slice storage (source evidence → Neptune node properties)
- Artifact validation (schema compliance before Build)
- Full pipeline orchestration (ingest_artifact)

v0.4.2: GraphLoader now uses toolkit's GraphBatchClient + NeptuneDatabaseClient
(boto3, 32-pool, 600s timeout, UNWIND dedup, retry cascade).

Supported Joern frontends: Java, JavaScript/TypeScript, Python, C/C++,
Go, PHP, Ruby, Kotlin, Swift.

Full CPG schema: 20 node types, 14+ edge types.
See schema.py for complete enumeration.
"""

__version__ = "0.4.2"

from .models import CPGNode, CPGEdge, Manifest, VectorRecord, SummaryRecord, CodeSliceRecord
from .schema import NodeType, EdgeType, DELTA_RELEVANT_TYPES, SUPPORTED_LANGUAGES, joern_export_command
from .graph_diff import GraphDiff
from .manifest_manager import ManifestManager, ManifestConflictError
from .delta_ingestor import DeltaIngestor
from .tenant_ops import delete_tenant, delete_domain, list_domains
from .artifact_validator import ArtifactValidator, ValidationResult
from .graph_loader import GraphLoader
from .vector_loader import VectorLoader, LoadResult
from .summary_overlay import SummaryOverlay
from .code_slice_store import CodeSliceStore
from .graphson_converter import GraphSONConverter
from .artifact_reader import ArtifactReader, ArtifactContents

__all__ = [
    # Models
    "CPGNode", "CPGEdge", "Manifest",
    "VectorRecord", "SummaryRecord", "CodeSliceRecord",
    # Schema
    "NodeType", "EdgeType", "DELTA_RELEVANT_TYPES", "SUPPORTED_LANGUAGES",
    "joern_export_command",
    # Pipeline
    "GraphDiff", "ManifestManager", "ManifestConflictError", "DeltaIngestor",
    "delete_tenant", "delete_domain", "list_domains",
    # Artifact pipeline
    "ArtifactValidator", "ValidationResult",
    "GraphLoader", "VectorLoader", "LoadResult",
    "SummaryOverlay", "CodeSliceStore",
    # Conversion + I/O
    "GraphSONConverter", "ArtifactReader", "ArtifactContents",
]
