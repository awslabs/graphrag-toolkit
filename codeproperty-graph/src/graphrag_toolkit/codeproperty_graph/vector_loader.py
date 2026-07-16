# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Vector Loader — loads CPG embedding vectors into OpenSearch via a VectorStoreProtocol.

Reads VectorRecord dicts (from vectors.jsonl or in-memory list) and transforms
them into the format expected by the vector store, grouping by embedding_target
and generating compound document IDs for multi-vector-per-node support.

Document ID scheme: f"{cpg_node_id}::{embedding_target}"
    Allows a single CPG node to have multiple embeddings (e.g. code, docstring, signature).
"""

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class VectorStoreProtocol(Protocol):
    """Protocol for vector store backends (OpenSearch, etc.).

    Each document dict passed to put_vectors has:
        id: str          — compound key (cpg_node_id::embedding_target)
        vector: list[float]
        text: str        — the source text that was embedded
        metadata: dict   — arbitrary metadata (node_type, filename, etc.)
    """

    async def put_vectors(self, documents: list[dict]) -> int:
        """Bulk-index vector documents. Returns count of documents indexed."""
        ...


@dataclass
class VectorRecord:
    """A single embedding vector produced by the CPG embedding stage.

    Fields align with vectors.jsonl output format:
        cpg_node_id: The source CPG node ID this vector represents
        embedding_target: What was embedded (e.g. 'code', 'docstring', 'signature')
        vector: The embedding vector (list of floats)
        text: The source text that was embedded
        metadata: Additional context (node_type, filename, line_number, etc.)
        embedding_model: Model identifier that produced this vector
        embedding_dimensions: Declared dimensionality of the vector
    """

    cpg_node_id: str
    embedding_target: str
    vector: list[float]
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)
    embedding_model: str = ""
    embedding_dimensions: int = 0

    @property
    def document_id(self) -> str:
        """Compound key for OpenSearch: allows multiple vectors per CPG node."""
        return f"{self.cpg_node_id}::{self.embedding_target}"

    @classmethod
    def from_dict(cls, raw: dict) -> "VectorRecord":
        """Create a VectorRecord from a raw dict (e.g. parsed JSONL line)."""
        return cls(
            cpg_node_id=raw["cpg_node_id"],
            embedding_target=raw["embedding_target"],
            vector=raw["vector"],
            text=raw.get("text", ""),
            metadata=raw.get("metadata", {}),
            embedding_model=raw.get("embedding_model", ""),
            embedding_dimensions=raw.get("embedding_dimensions", 0),
        )


@dataclass
class LoadResult:
    """Result of a vector load operation."""

    total_loaded: int = 0
    by_target: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


class VectorLoader:
    """Loads CPG embedding vectors into a vector store (OpenSearch).

    Usage:
        loader = VectorLoader(
            vector_store=opensearch_store,
            expected_dimensions=1024,
            batch_size=500,
        )
        result = await loader.load(records)
        # or
        result = await loader.load_from_file("s3://bucket/vectors.jsonl")

    Args:
        vector_store: Any object implementing VectorStoreProtocol
        expected_dimensions: Expected embedding dimensions (for validation)
        batch_size: Number of documents per bulk indexing call
    """

    def __init__(
        self,
        vector_store: VectorStoreProtocol,
        expected_dimensions: int = 0,
        batch_size: int = 500,
        graph_store=None,
        tenant_id: str = "",
    ):
        self._store = vector_store
        self._expected_dimensions = expected_dimensions
        self._batch_size = batch_size
        self._graph_store = graph_store
        self._tenant_id = tenant_id

    async def load(self, records: list[dict | VectorRecord]) -> LoadResult:
        """Load vector records into the vector store.

        Args:
            records: List of VectorRecord instances or raw dicts (vectors.jsonl format).

        Returns:
            LoadResult with counts and any validation errors.
        """
        result = LoadResult()
        grouped: dict[str, list[dict]] = defaultdict(list)

        for raw in records:
            record = raw if isinstance(raw, VectorRecord) else VectorRecord.from_dict(raw)

            # Validate dimensions
            error = self._validate(record)
            if error:
                result.errors.append(error)
                continue

            # Transform to vector store document format
            doc = {
                "id": record.document_id,
                "vector": record.vector,
                "text": record.text,
                "metadata": {
                    **record.metadata,
                    "cpg_node_id": record.cpg_node_id,
                    "embedding_target": record.embedding_target,
                    "embedding_model": record.embedding_model,
                    "tenant_id": self._tenant_id,
                },
            }
            grouped[record.embedding_target].append(doc)

        # Bulk-index by embedding_target group
        for target, documents in grouped.items():
            count = await self._bulk_index(documents)
            result.by_target[target] = count
            result.total_loaded += count
            logger.info(f"Loaded {count} vectors for target '{target}'")

        # Also write embedding as a node property on Neptune (client schema conformance)
        if self._graph_store and records:
            written = 0
            for raw in records:
                record = raw if isinstance(raw, VectorRecord) else VectorRecord.from_dict(raw)
                try:
                    await self._graph_store.update_node_properties(
                        record.cpg_node_id,
                        {"embedding": record.vector, "embeddedTime": record.metadata.get("embedded_time", "")},
                    )
                    written += 1
                except Exception as e:
                    logger.debug(f"Failed to write embedding property for {record.cpg_node_id}: {e}")
            if written:
                logger.info(f"Wrote embedding property to {written} Neptune nodes")

        if result.errors:
            logger.warning(f"Encountered {len(result.errors)} validation errors during load")

        return result

    async def load_from_file(self, path: str) -> LoadResult:
        """Load vectors from a JSONL file (local path or S3 URI).

        Args:
            path: Local filesystem path or S3 URI (s3://bucket/key) to vectors.jsonl.

        Returns:
            LoadResult with counts and any validation errors.
        """
        records = await self._read_jsonl(path)
        logger.info(f"Read {len(records)} vector records from {path}")
        return await self.load(records)

    def _validate(self, record: VectorRecord) -> str | None:
        """Validate a vector record. Returns error string or None if valid."""
        actual_dims = len(record.vector)

        # Check declared dimensions match actual vector length
        if record.embedding_dimensions and actual_dims != record.embedding_dimensions:
            return (
                f"Vector {record.document_id}: declared dimensions "
                f"({record.embedding_dimensions}) != actual ({actual_dims})"
            )

        # Check against expected model dimensions if configured
        if self._expected_dimensions and actual_dims != self._expected_dimensions:
            return (
                f"Vector {record.document_id}: actual dimensions "
                f"({actual_dims}) != expected ({self._expected_dimensions})"
            )

        if not record.vector:
            return f"Vector {record.document_id}: empty vector"

        return None

    async def _bulk_index(self, documents: list[dict]) -> int:
        """Index documents in batches via the vector store protocol."""
        total_indexed = 0

        for i in range(0, len(documents), self._batch_size):
            batch = documents[i : i + self._batch_size]
            try:
                count = await self._store.put_vectors(batch)
                total_indexed += count
            except Exception as e:
                logger.error(f"Bulk index error on batch {i // self._batch_size}: {e}")
                # Continue with remaining batches
                continue

        return total_indexed

    async def _read_jsonl(self, path: str) -> list[dict]:
        """Read vector records from a JSONL file (local or S3).

        Args:
            path: Local path or s3://bucket/key URI.

        Returns:
            List of raw dicts parsed from JSONL lines.
        """
        if path.startswith("s3://"):
            return await self._read_s3_jsonl(path)
        return self._read_local_jsonl(path)

    @staticmethod
    def _read_local_jsonl(path: str) -> list[dict]:
        """Read JSONL from local filesystem."""
        records: list[dict] = []
        file_path = Path(path)

        if not file_path.exists():
            raise FileNotFoundError(f"Vector file not found: {path}")

        with file_path.open("r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping malformed line {line_num} in {path}: {e}")

        return records

    @staticmethod
    async def _read_s3_jsonl(uri: str) -> list[dict]:
        """Read JSONL from S3 using aiobotocore/boto3.

        Args:
            uri: S3 URI in format s3://bucket/key

        Returns:
            List of raw dicts parsed from JSONL lines.
        """
        import aiobotocore.session  # type: ignore[import-untyped]

        # Parse s3://bucket/key
        parts = uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        session = aiobotocore.session.get_session()
        records: list[dict] = []

        async with session.create_client("s3") as client:
            response = await client.get_object(Bucket=bucket, Key=key)
            async with response["Body"] as stream:
                content = await stream.read()

        for line_num, line in enumerate(content.decode("utf-8").splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping malformed line {line_num} in {uri}: {e}")

        return records
