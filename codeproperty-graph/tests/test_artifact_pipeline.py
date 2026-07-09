# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for codeproperty-graph extended modules (artifact pipeline)."""

import pytest
from graphrag_toolkit.codeproperty_graph.models import (
    CPGNode, CPGEdge, VectorRecord, SummaryRecord, CodeSliceRecord,
)
from graphrag_toolkit.codeproperty_graph.artifact_validator import ArtifactValidator, ValidationResult


# === from_artifact() factory tests ===

class TestCPGNodeFromArtifact:
    def test_basic_method_node(self):
        raw = {
            "cpg_node_id": "payments-api:abc123:Method:auth.TokenService.validateToken:sha256001",
            "node_type": "METHOD",
            "labels": ["METHOD"],
            "repo_id": "payments-api",
            "commit_sha": "abc123",
            "file_path": "src/auth/token.py",
            "fully_qualified_name": "auth.TokenService.validateToken",
            "name": "validateToken",
            "line_start": 42,
            "line_end": 91,
            "code_hash": "sha256001",
            "properties": {"language": "python", "visibility": "public"},
        }
        node = CPGNode.from_artifact(raw)
        assert node.id == "payments-api:abc123:Method:auth.TokenService.validateToken:sha256001"
        assert node.node_type == "METHOD"
        assert node.full_name == "auth.TokenService.validateToken"
        assert node.hash == "sha256001"
        assert node.filename == "src/auth/token.py"
        assert node.name == "validateToken"
        assert node.line_number == 42
        assert node.line_number_end == 91
        assert node.properties == {"language": "python", "visibility": "public"}

    def test_file_node(self):
        raw = {
            "cpg_node_id": "payments-api:abc123:File:src/auth/token.py",
            "node_type": "FILE",
            "file_path": "src/auth/token.py",
            "name": "token.py",
            "properties": {},
        }
        node = CPGNode.from_artifact(raw)
        assert node.node_type == "FILE"
        assert node.filename == "src/auth/token.py"
        assert node.line_number is None

    def test_stable_id_uses_full_name(self):
        raw = {
            "cpg_node_id": "repo:commit:Method:pkg.Class.method:hash",
            "node_type": "METHOD",
            "fully_qualified_name": "pkg.Class.method",
            "properties": {},
        }
        node = CPGNode.from_artifact(raw)
        assert node.stable_id == "pkg.Class.method"


class TestCPGEdgeFromArtifact:
    def test_contains_edge(self):
        raw = {
            "edge_id": "edge:001",
            "source_cpg_node_id": "payments-api:abc123:File:src/auth/token.py",
            "target_cpg_node_id": "payments-api:abc123:Method:auth.TokenService.validateToken:sha256001",
            "edge_type": "CONTAINS",
            "properties": {"analysis_run_id": "run-20260704-001"},
        }
        edge = CPGEdge.from_artifact(raw)
        assert edge.source_id == "payments-api:abc123:File:src/auth/token.py"
        assert edge.target_id == "payments-api:abc123:Method:auth.TokenService.validateToken:sha256001"
        assert edge.edge_type == "CONTAINS"
        assert edge.properties == {"analysis_run_id": "run-20260704-001"}

    def test_edge_key(self):
        raw = {
            "source_cpg_node_id": "a",
            "target_cpg_node_id": "b",
            "edge_type": "AST",
            "properties": {},
        }
        edge = CPGEdge.from_artifact(raw)
        assert edge.key == "a->AST->b"


class TestVectorRecord:
    def test_from_artifact(self):
        raw = {
            "cpg_node_id": "payments-api:abc123:Method:validateToken:sha256001",
            "embedding_target": "method_summary",
            "embedding_model": "nomic-embed-text",
            "embedding_dimensions": 768,
            "similarity_function": "cosine",
            "embedding_text_hash": "sha256:9f3a2b",
            "vector": [0.012, -0.044, 0.087],
        }
        rec = VectorRecord.from_artifact(raw)
        assert rec.cpg_node_id == "payments-api:abc123:Method:validateToken:sha256001"
        assert rec.embedding_target == "method_summary"
        assert rec.embedding_model == "nomic-embed-text"
        assert rec.embedding_dimensions == 768
        assert rec.similarity_function == "cosine"
        assert rec.vector == [0.012, -0.044, 0.087]


class TestSummaryRecord:
    def test_from_artifact(self):
        raw = {
            "cpg_node_id": "payments-api:abc123:Method:validateToken:sha256001",
            "summary_type": "method_summary",
            "text": "Validates a JWT token by checking the signature.",
            "generation_model": "mistral-7b",
            "generation_prompt_version": "v2.1",
        }
        rec = SummaryRecord.from_artifact(raw)
        assert rec.cpg_node_id == "payments-api:abc123:Method:validateToken:sha256001"
        assert rec.summary_type == "method_summary"
        assert rec.text == "Validates a JWT token by checking the signature."
        assert rec.generation_model == "mistral-7b"
        assert rec.generation_prompt_version == "v2.1"


class TestCodeSliceRecord:
    def test_from_artifact(self):
        raw = {
            "cpg_node_id": "payments-api:abc123:Method:validateToken:sha256001",
            "slice_type": "full_method",
            "code": "def validateToken(self, token: str) -> dict:\n    pass",
            "language": "python",
            "line_start": 42,
            "line_end": 53,
            "file_path": "src/auth/token.py",
        }
        rec = CodeSliceRecord.from_artifact(raw)
        assert rec.cpg_node_id == "payments-api:abc123:Method:validateToken:sha256001"
        assert rec.slice_type == "full_method"
        assert rec.language == "python"
        assert rec.line_start == 42
        assert rec.line_end == 53


# === ArtifactValidator tests ===

class TestArtifactValidator:
    def _valid_manifest(self):
        return {
            "artifact_type": "cpg",
            "schema_version": "1.0",
            "repo_id": "payments-api",
            "commit_sha": "abc123def456",
            "embedding_model": "nomic-embed-text",
            "embedding_dimensions": 768,
            "similarity_function": "cosine",
            "counts": {"nodes": 100, "vectors": 50, "summaries": 30},
        }

    def test_valid_manifest_passes(self):
        validator = ArtifactValidator()
        result = validator.validate(manifest=self._valid_manifest())
        assert result.valid
        assert not result.errors

    def test_missing_required_field(self):
        manifest = self._valid_manifest()
        del manifest["repo_id"]
        validator = ArtifactValidator()
        result = validator.validate(manifest=manifest)
        assert not result.valid
        assert any("repo_id" in e for e in result.errors)

    def test_wrong_artifact_type(self):
        manifest = self._valid_manifest()
        manifest["artifact_type"] = "lexical"
        validator = ArtifactValidator()
        result = validator.validate(manifest=manifest)
        assert not result.valid
        assert any("artifact_type" in e for e in result.errors)

    def test_zero_counts_rejected(self):
        manifest = self._valid_manifest()
        manifest["counts"]["nodes"] = 0
        validator = ArtifactValidator()
        result = validator.validate(manifest=manifest)
        assert not result.valid

    def test_vector_dimension_mismatch(self):
        manifest = self._valid_manifest()
        vectors = [
            {
                "cpg_node_id": "x",
                "embedding_target": "method_summary",
                "vector": [0.1, 0.2, 0.3],
                "embedding_dimensions": 512,  # mismatch with manifest's 768
            }
        ]
        validator = ArtifactValidator()
        result = validator.validate(manifest=manifest, sample_records={"vectors": vectors})
        # Should produce a warning or error about dimension mismatch
        assert result.warnings or not result.valid

    def test_valid_with_samples(self):
        manifest = self._valid_manifest()
        nodes = [{"cpg_node_id": "x", "node_type": "METHOD", "properties": {}}]
        vectors = [
            {
                "cpg_node_id": "x",
                "embedding_target": "method_summary",
                "vector": [0.1] * 768,
                "embedding_dimensions": 768,
            }
        ]
        summaries = [
            {"cpg_node_id": "x", "summary_type": "method_summary", "text": "Does stuff."}
        ]
        validator = ArtifactValidator()
        result = validator.validate(
            manifest=manifest,
            sample_records={"nodes": nodes, "vectors": vectors, "summaries": summaries},
        )
        assert result.valid


# === DeltaIngestor._derive_tenant_id tests ===

class TestDeriveTenantId:
    def test_simple_repo(self):
        from graphrag_toolkit.codeproperty_graph.delta_ingestor import DeltaIngestor
        assert DeltaIngestor._derive_tenant_id("payments-api") == "payments.api"

    def test_slash_repo(self):
        from graphrag_toolkit.codeproperty_graph.delta_ingestor import DeltaIngestor
        assert DeltaIngestor._derive_tenant_id("org/payments-api") == "org.payments.api"

    def test_truncation(self):
        from graphrag_toolkit.codeproperty_graph.delta_ingestor import DeltaIngestor
        long_name = "a" * 50
        result = DeltaIngestor._derive_tenant_id(long_name)
        assert len(result) <= 25

    def test_empty_fallback(self):
        from graphrag_toolkit.codeproperty_graph.delta_ingestor import DeltaIngestor
        assert DeltaIngestor._derive_tenant_id("") == "default"
