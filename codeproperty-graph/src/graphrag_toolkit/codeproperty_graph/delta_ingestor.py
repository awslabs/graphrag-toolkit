# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Delta Ingestor — orchestrates skip-or-replace CPG ingestion.

Extended to handle the full CPG artifact pipeline:
  1. Delta check (method signatures)
  2. Validate artifact (manifest + sample records)
  3. Load graph (nodes + edges → Neptune)
  4. Load vectors (vectors.jsonl → OpenSearch)
  5. Apply summaries (summaries.jsonl → Neptune node properties)
  6. Store code slices (code_slices.jsonl → Neptune node properties)
  7. Purge old tenant
  8. Update manifest
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from .graph_diff import GraphDiff
from .manifest_manager import ManifestManager
from .models import Manifest
from .tenant_ops import delete_tenant, delete_domain
from .artifact_validator import ArtifactValidator
from .graph_loader import GraphLoader
from .vector_loader import VectorLoader
from .summary_overlay import SummaryOverlay
from .code_slice_store import CodeSliceStore
from .artifact_reader import ArtifactReader

logger = logging.getLogger(__name__)


class DeltaIngestor:
    """Orchestrates CPG delta ingestion: compare → validate → load → enrich → purge → manifest.

    Usage (full artifact pipeline):
        ingestor = DeltaIngestor(bucket="<your-artifacts-bucket>")
        result = await ingestor.ingest_artifact(
            manifest=manifest_dict,
            nodes_data=[...],
            edges_data=[...],
            vectors_data=[...],
            summaries_data=[...],
            code_slices_data=[...],
            graph_store=neptune_store,
            vector_store=opensearch_store,
        )

    Usage (legacy — graph only):
        result = await ingestor.ingest(
            repo="amigo-core",
            job_id="uuid",
            nodes_data=[...],
            edges_data=[...],
            graph_store=neptune_store,
            write_fn=my_write_function,
        )
    """

    def __init__(self, bucket: str, prefix: str = "cpg-exports", region: str = "us-east-1"):
        self._manifest_mgr = ManifestManager(bucket, prefix, region)

    async def ingest_artifact(
        self,
        manifest: dict,
        nodes_data: list[dict],
        edges_data: list[dict],
        vectors_data: list[dict],
        summaries_data: list[dict],
        code_slices_data: list[dict],
        graph_store: Any,
        vector_store: Any,
        tenant_id: Optional[str] = None,
        validate: bool = True,
        batch_size: int = 500,
    ) -> dict:
        """Execute the full CPG artifact ingestion pipeline.

        Args:
            manifest: Parsed manifest.json dict
            nodes_data: Parsed records from nodes.jsonl
            edges_data: Parsed records from edges.jsonl
            vectors_data: Parsed records from vectors.jsonl
            summaries_data: Parsed records from summaries.jsonl
            code_slices_data: Parsed records from code_slices.jsonl
            graph_store: Neptune graph store (supports write_nodes, write_edges, update_node_properties, execute_query)
            vector_store: OpenSearch vector store (supports put_vectors)
            tenant_id: Override tenant ID (derived from repo if not provided)
            validate: Whether to validate the artifact before ingestion (default True)

        Returns:
            Dict with status, counts, delta summary, and any validation warnings.
        """
        repo = manifest.get("repo_id", "")
        job_id = manifest.get("analysis_run_id", "")
        tenant_id = tenant_id or self._derive_tenant_id(repo)

        # Step 1: Validate artifact
        if validate:
            validator = ArtifactValidator()
            validation = validator.validate(
                manifest=manifest,
                sample_records={
                    "nodes": nodes_data[:10],
                    "vectors": vectors_data[:10],
                    "summaries": summaries_data[:10],
                },
            )
            if not validation.valid:
                logger.error(f"Artifact validation failed for {repo}: {validation.errors}")
                return {
                    "status": "REJECTED",
                    "reason": "validation_failed",
                    "errors": validation.errors,
                    "warnings": validation.warnings,
                }
            if validation.warnings:
                logger.warning(f"Artifact validation warnings for {repo}: {validation.warnings}")

        # Step 2: Delta check
        method_sigs = {
            n.get("fully_qualified_name", n.get("full_name", "")): n.get("code_hash", n.get("hash", ""))
            for n in nodes_data
            if n.get("node_type") == "METHOD" and (n.get("fully_qualified_name") or n.get("full_name"))
        }

        changed, previous = self._manifest_mgr.has_changes(repo, method_sigs)

        if not changed:
            logger.info(f"Delta check: no changes for {repo}, skipping ingest")
            return {
                "status": "SKIPPED",
                "reason": "no_changes",
                "tenant_id": previous.tenant_id,
                "previous_job_id": previous.job_id,
            }

        if previous:
            diff = GraphDiff.compare(previous.method_signatures, method_sigs)
            logger.info(f"Delta: {diff.summary} for {repo}")

        # Step 3: Load graph (nodes + edges → Neptune)
        graph_loader = GraphLoader(graph_store=graph_store, tenant_id=tenant_id, batch_size=batch_size)
        graph_result = await graph_loader.load(nodes_data, edges_data)
        logger.info(f"Graph loaded for {repo}: {graph_result}")

        # Step 4: Load vectors → OpenSearch (enriched with summary text for full-text search)
        # Join summary text into vector records so OpenSearch indexes both vector + text
        summary_by_node = {}
        for s in summaries_data:
            node_id = s.get("cpg_node_id", "")
            summary_by_node.setdefault(node_id, {})[s.get("summary_type", "")] = s.get("text", "")

        enriched_vectors = []
        for v in vectors_data:
            enriched = dict(v)
            node_id = v.get("cpg_node_id", "")
            target = v.get("embedding_target", "")
            # If this vector's embedding_target matches a summary_type, include the text
            if node_id in summary_by_node and target in summary_by_node[node_id]:
                enriched["text"] = summary_by_node[node_id][target]
            enriched_vectors.append(enriched)

        vector_loader = VectorLoader(
            vector_store=vector_store,
            expected_dimensions=manifest.get("embedding_dimensions", 0),
            tenant_id=tenant_id,
        )
        vector_result = await vector_loader.load(enriched_vectors)
        logger.info(f"Vectors loaded for {repo}: {vector_result.total_loaded} vectors")

        # Step 5: Apply summaries → Neptune node properties
        summary_overlay = SummaryOverlay(graph_store=graph_store)
        summaries_applied = await summary_overlay.apply(summaries_data)
        logger.info(f"Summaries applied for {repo}: {summaries_applied}")

        # Step 6: Store code slices → Neptune node properties
        slice_store = CodeSliceStore(graph_store=graph_store)
        slices_stored = await slice_store.store(code_slices_data)
        logger.info(f"Code slices stored for {repo}: {slices_stored}")

        # Step 7: Purge old tenant (domain-scoped — only CPG nodes)
        if previous and previous.tenant_id != tenant_id:
            try:
                await delete_domain(previous.tenant_id, "cpg", graph_store)
                logger.info(f"Purged old CPG domain for tenant {previous.tenant_id}")
            except Exception as e:
                logger.warning(f"Failed to purge old CPG domain {previous.tenant_id}: {e}")

        # Step 8: Update manifest
        new_manifest = Manifest(
            repo=repo,
            signature=self._manifest_mgr.compute_signature(method_sigs),
            job_id=job_id,
            tenant_id=tenant_id,
            exported_at=datetime.now(timezone.utc).isoformat(),
            nodes_path=manifest.get("nodes_path", ""),
            edges_path=manifest.get("edges_path", ""),
            node_count=graph_result.get("nodes_written", 0),
            edge_count=graph_result.get("edges_written", 0),
            language=manifest.get("language", ""),
            method_signatures=method_sigs,
        )
        self._manifest_mgr.put(new_manifest)

        return {
            "status": "INGESTED",
            "tenant_id": tenant_id,
            "repo": repo,
            "job_id": job_id,
            "nodes_written": graph_result.get("nodes_written", 0),
            "edges_written": graph_result.get("edges_written", 0),
            "vectors_loaded": vector_result.total_loaded,
            "summaries_applied": summaries_applied,
            "code_slices_stored": slices_stored,
            "delta": GraphDiff.compare(
                previous.method_signatures if previous else {}, method_sigs
            ).summary,
            "warnings": validation.warnings if validate else [],
        }

    async def ingest(
        self,
        repo: str,
        job_id: str,
        tenant_id: str,
        nodes_data: list[dict],
        edges_data: list[dict],
        nodes_path: str,
        edges_path: str,
        graph_store: Any,
        write_fn=None,
    ) -> dict:
        """Execute delta-aware ingestion (legacy — graph only, no vectors/summaries).

        Args:
            repo: Repository name
            job_id: Current job UUID
            tenant_id: Derived tenant for this job
            nodes_data: Parsed node records from Joern export
            edges_data: Parsed edge records from Joern export
            nodes_path: S3 URI to nodes.json
            edges_path: S3 URI to edges.json
            graph_store: Neptune graph store instance
            write_fn: async callable(nodes_data, edges_data, tenant_id, graph_store) → dict

        Returns:
            Dict with status, nodes_written, etc.
        """
        # Extract method signatures from nodes
        method_sigs = {
            n["full_name"]: n.get("hash", "")
            for n in nodes_data
            if n.get("node_type") == "METHOD" and n.get("full_name")
        }

        # Check manifest
        changed, previous = self._manifest_mgr.has_changes(repo, method_sigs)

        if not changed:
            logger.info(f"Delta check: no changes for {repo}, skipping ingest")
            return {
                "status": "SKIPPED",
                "reason": "no_changes",
                "tenant_id": previous.tenant_id,
                "previous_job_id": previous.job_id,
            }

        # Log diff if previous exists
        if previous:
            diff = GraphDiff.compare(previous.method_signatures, method_sigs)
            logger.info(f"Delta: {diff.summary} for {repo}")

        # Perform full ingest
        if write_fn:
            result = await write_fn(nodes_data, edges_data, tenant_id, graph_store)
        else:
            result = {"nodes_written": len(nodes_data), "edges_written": len(edges_data)}

        # Purge old tenant
        if previous and previous.tenant_id != tenant_id:
            try:
                await delete_tenant(previous.tenant_id, graph_store)
            except Exception:
                pass  # logged inside delete_tenant

        # Update manifest
        new_manifest = Manifest(
            repo=repo,
            signature=self._manifest_mgr.compute_signature(method_sigs),
            job_id=job_id,
            tenant_id=tenant_id,
            exported_at=datetime.now(timezone.utc).isoformat(),
            nodes_path=nodes_path,
            edges_path=edges_path,
            method_signatures=method_sigs,
        )
        self._manifest_mgr.put(new_manifest)

        result["status"] = "INGESTED"
        result["tenant_id"] = tenant_id
        result["delta"] = GraphDiff.compare(
            previous.method_signatures if previous else {}, method_sigs
        ).summary

        return result

    async def rollback(self, repo: str, graph_store: Any, vector_store: Any) -> dict:
        """Rollback to the previous artifact version.

        Reads the previous manifest, fetches the previous artifact from S3,
        and re-ingests it (replacing the current bad data).

        Args:
            repo: Repository name to rollback
            graph_store: Neptune graph store
            vector_store: OpenSearch vector store

        Returns:
            Dict with rollback status and details.
        """
        previous = self._manifest_mgr.get_previous(repo)
        if not previous:
            return {
                "status": "FAILED",
                "reason": "no_previous_manifest",
                "message": f"No previous manifest found for {repo}. Cannot rollback.",
            }

        # Determine S3 URI of previous artifact
        s3_prefix = f"s3://{self._manifest_mgr._bucket}/{self._manifest_mgr._prefix}/{repo}/{previous.job_id}/"

        # Read previous artifact from S3
        reader = ArtifactReader(region=self._manifest_mgr._s3.meta.region_name)
        try:
            artifact = reader.read_s3(s3_prefix)
        except Exception as e:
            return {
                "status": "FAILED",
                "reason": "artifact_read_failed",
                "message": f"Failed to read previous artifact from {s3_prefix}: {e}",
            }

        if not artifact.manifest:
            artifact.manifest = {
                "repo_id": previous.repo,
                "analysis_run_id": previous.job_id,
                "embedding_dimensions": 0,
            }

        # Re-ingest the previous artifact (this triggers domain-scoped delete + reload)
        logger.info(f"Rolling back {repo} to job_id={previous.job_id}")
        result = await self.ingest_artifact(
            manifest=artifact.manifest,
            nodes_data=artifact.nodes,
            edges_data=artifact.edges,
            vectors_data=artifact.vectors,
            summaries_data=artifact.summaries,
            code_slices_data=artifact.code_slices,
            graph_store=graph_store,
            vector_store=vector_store,
            tenant_id=previous.tenant_id,
            validate=False,  # Previous artifact was already validated
        )

        result["rollback_from"] = self._manifest_mgr.get(repo).job_id if self._manifest_mgr.get(repo) else "unknown"
        result["rollback_to"] = previous.job_id
        return result

    @staticmethod
    def _derive_tenant_id(repo: str) -> str:
        """Derive a valid tenant_id from a repository name.

        Tenant IDs must be 1-25 lowercase chars (letters, numbers, periods — not at start/end).
        """
        # Lowercase, replace non-alphanumeric with periods, trim to 25 chars
        tid = repo.lower().replace("/", ".").replace("-", ".").replace("_", ".")
        # Remove leading/trailing periods
        tid = tid.strip(".")
        # Collapse consecutive periods
        while ".." in tid:
            tid = tid.replace("..", ".")
        # Truncate to 25 chars
        tid = tid[:25].rstrip(".")
        # Ensure at least 1 char
        return tid or "default"
