# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Validates a CPG artifact before Build processes it."""

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

REQUIRED_MANIFEST_FIELDS = {
    "artifact_type",
    "schema_version",
    "repo_id",
    "commit_sha",
    "embedding_model",
    "embedding_dimensions",
    "similarity_function",
    "counts",
}

# Recommended fields (warn if missing, don't fail)
RECOMMENDED_MANIFEST_FIELDS = {
    "apm_id",              # Client's primary scoping key (apmId)
    "app_name",            # Application name (appName)
    "source_code_repo_uri",  # sourceCodeRepoUri — Git/BitBucket URL
    "language",            # Programming language
    "extraction_tool",     # joern
}

REQUIRED_NODE_FIELDS = {"cpg_node_id", "node_type", "properties"}
REQUIRED_VECTOR_FIELDS = {"cpg_node_id", "embedding_target", "vector", "embedding_dimensions"}
REQUIRED_SUMMARY_FIELDS = {"cpg_node_id", "summary_type", "text"}


@dataclass
class ValidationResult:
    """Result of artifact validation."""

    valid: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class ArtifactValidator:
    """Validates CPG artifact manifests and sample records.

    Designed to fail fast on critical errors (manifest structure)
    but collect all warnings from record-level checks.
    """

    def validate(
        self,
        manifest: dict[str, Any],
        sample_records: dict[str, list[dict[str, Any]]] | None = None,
    ) -> ValidationResult:
        """Validate a CPG artifact manifest and optional sample records.

        Args:
            manifest: The artifact manifest dictionary.
            sample_records: Optional dict with keys 'nodes', 'vectors', 'summaries',
                each mapping to a list of sample JSONL records.

        Returns:
            ValidationResult with valid flag, errors, and warnings.
        """
        result = ValidationResult()

        # --- Manifest-level validation (fail fast on critical errors) ---
        self._validate_manifest(manifest, result)
        if result.errors:
            result.valid = False
            logger.error("Manifest validation failed: %s", result.errors)
            return result

        # --- Record-level validation (collect all warnings) ---
        if sample_records:
            expected_dimensions = manifest.get("embedding_dimensions")
            self._validate_node_records(sample_records.get("nodes", []), result)
            self._validate_vector_records(
                sample_records.get("vectors", []), expected_dimensions, result
            )
            self._validate_summary_records(sample_records.get("summaries", []), result)

        if result.errors:
            result.valid = False

        return result

    def _validate_manifest(self, manifest: dict[str, Any], result: ValidationResult) -> None:
        """Validate required manifest fields and their values."""
        missing = REQUIRED_MANIFEST_FIELDS - set(manifest.keys())
        if missing:
            result.errors.append(f"Missing required manifest fields: {sorted(missing)}")
            return

        # Check recommended fields (warn, don't fail)
        # Also check camelCase variants (apmId, appName, sourceCodeRepoUri)
        for field in RECOMMENDED_MANIFEST_FIELDS:
            camel = field.replace("_", "")  # rough camelCase check
            if field not in manifest and camel not in manifest:
                # Try common variants
                variants = {
                    "apm_id": ["apmId", "apm_id"],
                    "app_name": ["appName", "app_name", "repo_id"],
                    "source_code_repo_uri": ["sourceCodeRepoUri", "source_code_repo_uri", "repo_url"],
                    "language": ["language"],
                    "extraction_tool": ["extraction_tool", "extractionTool"],
                }
                found = any(v in manifest for v in variants.get(field, []))
                if not found:
                    result.warnings.append(f"Recommended manifest field missing: '{field}' (client schema conformance)")

        # artifact_type must be 'cpg'
        if manifest["artifact_type"] != "cpg":
            result.errors.append(
                f"Invalid artifact_type: expected 'cpg', got '{manifest['artifact_type']}'"
            )

        # Validate counts
        counts = manifest.get("counts")
        if not isinstance(counts, dict):
            result.errors.append("'counts' must be a dictionary")
            return

        for count_field in ("nodes", "vectors", "summaries"):
            value = counts.get(count_field)
            if value is None:
                result.errors.append(f"'counts.{count_field}' is missing")
            elif not isinstance(value, int) or value <= 0:
                result.errors.append(
                    f"'counts.{count_field}' must be a positive integer, got {value!r}"
                )

    def _validate_node_records(
        self, records: list[dict[str, Any]], result: ValidationResult
    ) -> None:
        """Validate sample node records."""
        for i, record in enumerate(records):
            missing = REQUIRED_NODE_FIELDS - set(record.keys())
            if missing:
                result.warnings.append(
                    f"Node record [{i}]: missing fields {sorted(missing)}"
                )

    def _validate_vector_records(
        self,
        records: list[dict[str, Any]],
        expected_dimensions: int | None,
        result: ValidationResult,
    ) -> None:
        """Validate sample vector records and embedding dimensions."""
        for i, record in enumerate(records):
            missing = REQUIRED_VECTOR_FIELDS - set(record.keys())
            if missing:
                result.warnings.append(
                    f"Vector record [{i}]: missing fields {sorted(missing)}"
                )
                continue

            # Check embedding_dimensions match manifest
            record_dims = record.get("embedding_dimensions")
            if expected_dimensions is not None and record_dims != expected_dimensions:
                result.errors.append(
                    f"Vector record [{i}]: embedding_dimensions mismatch — "
                    f"record has {record_dims}, manifest specifies {expected_dimensions}"
                )

            # Check vector length matches declared dimensions
            vector = record.get("vector")
            if isinstance(vector, list) and expected_dimensions is not None:
                if len(vector) != expected_dimensions:
                    result.warnings.append(
                        f"Vector record [{i}]: vector length {len(vector)} "
                        f"does not match embedding_dimensions {expected_dimensions}"
                    )

    def _validate_summary_records(
        self, records: list[dict[str, Any]], result: ValidationResult
    ) -> None:
        """Validate sample summary records."""
        for i, record in enumerate(records):
            missing = REQUIRED_SUMMARY_FIELDS - set(record.keys())
            if missing:
                result.warnings.append(
                    f"Summary record [{i}]: missing fields {sorted(missing)}"
                )
