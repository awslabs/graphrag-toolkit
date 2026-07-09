# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Artifact Reader — reads CPG artifact JSONL files from S3 or local filesystem.

Reads the standard CPG artifact layout:
    <prefix>/<repo_id>/<job_id>/
        manifest.json
        nodes.jsonl
        edges.jsonl
        vectors.jsonl
        summaries.jsonl
        code_slices.jsonl
        lineage.jsonl (optional)
        findings.jsonl (optional)
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import boto3

logger = logging.getLogger(__name__)


@dataclass
class ArtifactContents:
    """Parsed contents of a CPG artifact."""
    manifest: dict = field(default_factory=dict)
    nodes: list[dict] = field(default_factory=list)
    edges: list[dict] = field(default_factory=list)
    vectors: list[dict] = field(default_factory=list)
    summaries: list[dict] = field(default_factory=list)
    code_slices: list[dict] = field(default_factory=list)
    lineage: list[dict] = field(default_factory=list)
    findings: list[dict] = field(default_factory=list)


class ArtifactReader:
    """Reads a CPG artifact from S3 or local filesystem.

    Usage (S3):
        reader = ArtifactReader(region="us-east-1")
        artifact = reader.read_s3("s3://bucket/cpg-exports/payments-api/job-001/")

    Usage (local):
        artifact = ArtifactReader.read_local("/path/to/artifact/")
    """

    def __init__(self, region: str = "us-east-1"):
        self._s3 = boto3.client("s3", region_name=region)

    def read_s3(self, s3_uri: str) -> ArtifactContents:
        """Read a CPG artifact from an S3 prefix.

        Args:
            s3_uri: S3 URI prefix (e.g., "s3://bucket/cpg-exports/repo/job/")

        Returns:
            ArtifactContents with all parsed files.
        """
        bucket, prefix = self._parse_s3_uri(s3_uri)
        if not prefix.endswith("/"):
            prefix += "/"

        artifact = ArtifactContents()

        # Read manifest
        artifact.manifest = self._read_s3_json(bucket, f"{prefix}manifest.json")

        # Read JSONL files
        artifact.nodes = self._read_s3_jsonl(bucket, f"{prefix}nodes.jsonl")
        artifact.edges = self._read_s3_jsonl(bucket, f"{prefix}edges.jsonl")
        artifact.vectors = self._read_s3_jsonl(bucket, f"{prefix}vectors.jsonl")
        artifact.summaries = self._read_s3_jsonl(bucket, f"{prefix}summaries.jsonl")
        artifact.code_slices = self._read_s3_jsonl(bucket, f"{prefix}code_slices.jsonl")

        # Optional files
        artifact.lineage = self._read_s3_jsonl(bucket, f"{prefix}lineage.jsonl", required=False)
        artifact.findings = self._read_s3_jsonl(bucket, f"{prefix}findings.jsonl", required=False)

        logger.info(
            f"Artifact read from s3://{bucket}/{prefix}: "
            f"nodes={len(artifact.nodes)}, edges={len(artifact.edges)}, "
            f"vectors={len(artifact.vectors)}, summaries={len(artifact.summaries)}"
        )
        return artifact

    @staticmethod
    def read_local(path: str) -> ArtifactContents:
        """Read a CPG artifact from a local directory.

        Args:
            path: Local directory path containing the artifact files.

        Returns:
            ArtifactContents with all parsed files.
        """
        directory = Path(path)
        artifact = ArtifactContents()

        # Read manifest
        manifest_path = directory / "manifest.json"
        if manifest_path.exists():
            with open(manifest_path) as f:
                artifact.manifest = json.load(f)

        # Read JSONL files
        artifact.nodes = ArtifactReader._read_local_jsonl(directory / "nodes.jsonl")
        artifact.edges = ArtifactReader._read_local_jsonl(directory / "edges.jsonl")
        artifact.vectors = ArtifactReader._read_local_jsonl(directory / "vectors.jsonl")
        artifact.summaries = ArtifactReader._read_local_jsonl(directory / "summaries.jsonl")
        artifact.code_slices = ArtifactReader._read_local_jsonl(directory / "code_slices.jsonl")
        artifact.lineage = ArtifactReader._read_local_jsonl(directory / "lineage.jsonl")
        artifact.findings = ArtifactReader._read_local_jsonl(directory / "findings.jsonl")

        logger.info(
            f"Artifact read from {path}: "
            f"nodes={len(artifact.nodes)}, edges={len(artifact.edges)}, "
            f"vectors={len(artifact.vectors)}, summaries={len(artifact.summaries)}"
        )
        return artifact

    def _read_s3_json(self, bucket: str, key: str) -> dict:
        """Read a single JSON file from S3."""
        try:
            resp = self._s3.get_object(Bucket=bucket, Key=key)
            return json.loads(resp["Body"].read())
        except self._s3.exceptions.NoSuchKey:
            logger.warning(f"File not found: s3://{bucket}/{key}")
            return {}
        except Exception as e:
            logger.error(f"Failed to read s3://{bucket}/{key}: {e}")
            return {}

    def _read_s3_jsonl(self, bucket: str, key: str, required: bool = True) -> list[dict]:
        """Read a JSONL file from S3 (one JSON object per line)."""
        try:
            resp = self._s3.get_object(Bucket=bucket, Key=key)
            body = resp["Body"].read().decode("utf-8")
            records = []
            for line in body.strip().split("\n"):
                line = line.strip()
                if line:
                    records.append(json.loads(line))
            return records
        except self._s3.exceptions.NoSuchKey:
            if required:
                logger.warning(f"Required file not found: s3://{bucket}/{key}")
            return []
        except Exception as e:
            logger.error(f"Failed to read s3://{bucket}/{key}: {e}")
            return []

    @staticmethod
    def _read_local_jsonl(path: Path) -> list[dict]:
        """Read a local JSONL file."""
        if not path.exists():
            return []
        records = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    @staticmethod
    def _parse_s3_uri(uri: str) -> tuple[str, str]:
        """Parse s3://bucket/prefix into (bucket, prefix)."""
        if uri.startswith("s3://"):
            uri = uri[5:]
        parts = uri.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket, prefix
