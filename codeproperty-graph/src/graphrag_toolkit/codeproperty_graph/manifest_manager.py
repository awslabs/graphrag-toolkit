# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Manifest Manager — S3-backed CPG state tracking with optimistic locking.

Provides:
- Manifest read/write with ETag-based optimistic locking
- Change detection (signature comparison)
- Rollback support (maintains previous manifest reference)
"""

import hashlib
import json
import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from .models import Manifest

logger = logging.getLogger(__name__)


class ManifestConflictError(Exception):
    """Raised when a concurrent write conflicts with our manifest update."""
    pass


class ManifestManager:
    """Read/write/compare CPG manifests on S3 with optimistic locking."""

    def __init__(self, bucket: str, prefix: str = "cpg-exports", region: str = "us-east-1"):
        self._bucket = bucket
        self._prefix = prefix
        self._s3 = boto3.client("s3", region_name=region)
        self._etags: dict[str, str] = {}  # repo → last-known ETag

    def compute_signature(self, method_signatures: dict[str, str]) -> str:
        """Compute sha256 signature from method full_name:hash pairs."""
        payload = json.dumps(method_signatures, sort_keys=True)
        return "sha256:" + hashlib.sha256(payload.encode()).hexdigest()

    def get(self, repo: str) -> Optional[Manifest]:
        """Read manifest for a repo from S3. Returns None if not found.

        Stores the ETag for subsequent optimistic locking on put().
        """
        key = self._manifest_key(repo)
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=key)
            self._etags[repo] = resp.get("ETag", "")
            data = json.loads(resp["Body"].read())
            return Manifest(**data)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "AccessDenied", "403"):
                # NoSuchKey = first-time ingest (no manifest yet)
                # AccessDenied = S3/KMS permissions not yet configured (treat as first-time)
                if error_code != "NoSuchKey":
                    logger.warning(f"Manifest read got {error_code} for {repo} — treating as first-time ingest")
                return None
            raise
        except Exception:
            return None

    def put(self, manifest: Manifest) -> None:
        """Write manifest to S3 with optimistic locking.

        If the manifest was previously read (ETag stored), uses If-Match
        to prevent concurrent overwrites. If the ETag doesn't match
        (another writer updated it), raises ManifestConflictError.

        Args:
            manifest: The manifest to write.

        Raises:
            ManifestConflictError: If a concurrent write detected.
        """
        key = self._manifest_key(manifest.repo)
        body = json.dumps({
            "repo": manifest.repo,
            "signature": manifest.signature,
            "job_id": manifest.job_id,
            "tenant_id": manifest.tenant_id,
            "exported_at": manifest.exported_at,
            "nodes_path": manifest.nodes_path,
            "edges_path": manifest.edges_path,
            "node_count": manifest.node_count,
            "edge_count": manifest.edge_count,
            "language": manifest.language,
            "method_signatures": manifest.method_signatures,
        })

        put_kwargs = {
            "Bucket": self._bucket,
            "Key": key,
            "Body": body.encode(),
            "ContentType": "application/json",
        }

        # Optimistic locking: if we have a previous ETag, require it matches
        etag = self._etags.get(manifest.repo)
        if etag:
            put_kwargs["IfMatch"] = etag

        try:
            resp = self._s3.put_object(**put_kwargs)
            # Update stored ETag for next operation
            self._etags[manifest.repo] = resp.get("ETag", "")
            logger.info(f"Manifest written: s3://{self._bucket}/{key}")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("PreconditionFailed", "412"):
                raise ManifestConflictError(
                    f"Concurrent manifest update detected for {manifest.repo}. "
                    f"Another process wrote a newer version. Retry the ingest."
                ) from e
            if error_code in ("AccessDenied", "403"):
                logger.warning(f"Manifest write AccessDenied for {manifest.repo} — S3/KMS permissions not configured. Ingest succeeded but manifest not persisted (delta detection disabled until fixed).")
                return
            raise

    def get_previous(self, repo: str) -> Optional[Manifest]:
        """Read the previous manifest (for rollback).

        The previous manifest is stored at <prefix>/<repo>/manifest.previous.json.
        """
        key = f"{self._prefix}/{repo}/manifest.previous.json"
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=key)
            data = json.loads(resp["Body"].read())
            return Manifest(**data)
        except Exception:
            return None

    def put_with_history(self, manifest: Manifest) -> None:
        """Write manifest to S3, preserving current as previous (for rollback).

        1. Copies current manifest.json → manifest.previous.json
        2. Writes new manifest.json (with optimistic lock)
        """
        key = self._manifest_key(manifest.repo)
        prev_key = f"{self._prefix}/{manifest.repo}/manifest.previous.json"

        # Copy current to previous (if it exists)
        try:
            self._s3.copy_object(
                Bucket=self._bucket,
                CopySource={"Bucket": self._bucket, "Key": key},
                Key=prev_key,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchKey":
                logger.warning(f"Failed to archive previous manifest: {e}")

        # Write new manifest with lock
        self.put(manifest)

    def has_changes(self, repo: str, current_signatures: dict[str, str]) -> tuple[bool, Optional[Manifest]]:
        """Check if current export differs from stored manifest.

        Returns:
            (has_changes: bool, previous_manifest: Optional[Manifest])
        """
        previous = self.get(repo)
        if not previous:
            return True, None
        new_sig = self.compute_signature(current_signatures)
        return new_sig != previous.signature, previous

    def _manifest_key(self, repo: str) -> str:
        """Get the S3 key for a repo's manifest."""
        return f"{self._prefix}/{repo}/manifest.json"
