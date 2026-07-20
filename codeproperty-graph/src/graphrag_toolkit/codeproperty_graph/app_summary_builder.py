# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""App Summary Builder — creates :AppSummary node per application.

Client schema conformance: Each application has a top-level AppSummary node with:
  - appName, apmId
  - shortDescription, longDescription (LLM-generated or derived)
  - totalMethods, totalFiles
  - language
  - embedding (for semantic search over app descriptions)
  - summarizedTime, embeddedTime

Linked: (:AppSummary)-[:HAS_METADATA]->(:Metadata)
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class AppSummaryBuilder:
    """Creates an :AppSummary node from the manifest and node statistics.

    Usage:
        builder = AppSummaryBuilder(graph_store=neptune_adapter)
        await builder.build(manifest=manifest, nodes_data=nodes_data)
    """

    def __init__(self, graph_store: Any):
        self._graph_store = graph_store

    async def build(self, manifest: dict, nodes_data: list[dict]) -> dict:
        """Create the AppSummary + Metadata nodes and link them.

        Args:
            manifest: The artifact manifest dict.
            nodes_data: All node records (to compute stats).

        Returns:
            Dict with app_summary_id and metadata_id.
        """
        app_name = manifest.get("app_name", manifest.get("appName", manifest.get("repo_id", "")))
        apm_id = manifest.get("apm_id", manifest.get("apmId", ""))
        language = manifest.get("language", "")
        counts = manifest.get("counts", {})

        # Compute stats from nodes
        total_methods = counts.get("methods", len([n for n in nodes_data if n.get("node_type") == "METHOD"]))
        total_files = counts.get("files", len([n for n in nodes_data if n.get("node_type") == "FILE"]))
        total_type_decls = counts.get("type_decls", len([n for n in nodes_data if n.get("node_type") == "TYPE_DECL"]))

        # IDs
        app_summary_id = f"{app_name}::AppSummary"
        metadata_id = f"{app_name}::Metadata"
        now = datetime.now(timezone.utc).isoformat()

        # Build AppSummary node
        app_summary_node = {
            "id": app_summary_id,
            "label": "AppSummary",
            "properties": {
                "cpg_node_id": app_summary_id,
                "appName": app_name,
                "apmId": apm_id,
                "shortDescription": f"{app_name} — {language} application with {total_methods} methods across {total_files} files.",
                "longDescription": (
                    f"Application '{app_name}' (APM: {apm_id}) is a {language} codebase "
                    f"containing {total_files} source files, {total_type_decls} classes/types, "
                    f"and {total_methods} methods. Analyzed by {manifest.get('extraction_tool', 'joern')} "
                    f"at commit {manifest.get('commit_sha', 'unknown')[:8]}."
                ),
                "language": language,
                "totalMethods": total_methods,
                "totalFiles": total_files,
                "totalTypeDecls": total_type_decls,
                "summarizedTime": now,
                "domain": "cpg",
            },
        }

        # Build Metadata node (client schema: stores extraction metadata)
        metadata_node = {
            "id": metadata_id,
            "label": "Metadata",
            "properties": {
                "cpg_node_id": metadata_id,
                "appName": app_name,
                "apmId": apm_id,
                "sourceCodeRepoUri": manifest.get("source_code_repo_uri", manifest.get("sourceCodeRepoUri", "")),
                "language": language,
                "cpgGeneratedTime": manifest.get("timestamp", now),
                "cpgMode": "source",
                "extractionTool": manifest.get("extraction_tool", "joern"),
                "extractionVersion": manifest.get("extraction_version", ""),
                "commitSha": manifest.get("commit_sha", ""),
                "branch": manifest.get("branch", ""),
                "lastModifiedTime": now,
                "domain": "cpg",
            },
        }

        # HAS_METADATA edge
        has_metadata_edge = {
            "source_id": app_summary_id,
            "target_id": metadata_id,
            "edge_type": "HAS_METADATA",
            "properties": {},
        }

        # Write to Neptune
        try:
            await self._graph_store.write_nodes([app_summary_node, metadata_node])
            await self._graph_store.write_edges([has_metadata_edge])
            logger.info(f"AppSummary + Metadata created for {app_name} (apmId={apm_id})")
        except Exception as e:
            logger.warning(f"Failed to create AppSummary for {app_name}: {e}")

        return {
            "app_summary_id": app_summary_id,
            "metadata_id": metadata_id,
            "app_name": app_name,
            "apm_id": apm_id,
            "total_methods": total_methods,
            "total_files": total_files,
        }
