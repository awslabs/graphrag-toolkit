# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""GraphSON → JSONL Converter — transforms Joern GraphSON exports to portable CPG JSONL.

Joern's export produces GraphSON format (JSON with nodes[] and edges[] arrays).
This converter reads that format and outputs our portable JSONL schema with
stable cpg_node_id identifiers (replacing Joern's internal numeric IDs).

Usage:
    converter = GraphSONConverter(repo_id="payments-api", commit_sha="abc123")
    nodes, edges = converter.convert_file("path/to/joern-export.json")
    converter.write_jsonl(nodes, "output/nodes.jsonl")
    converter.write_jsonl(edges, "output/edges.jsonl")
"""

import json
import logging
from pathlib import Path
from typing import IO

from .models import CPGNode, CPGEdge

logger = logging.getLogger(__name__)


class GraphSONConverter:
    """Converts Joern GraphSON exports to portable CPG JSONL format.

    The conversion:
    1. Reads GraphSON (JSON array or {nodes:[], edges:[]})
    2. Parses each record via CPGNode.from_joern() / CPGEdge.from_joern()
    3. Generates stable cpg_node_id (replacing numeric IDs)
    4. Outputs JSONL with portable identifiers

    The cpg_node_id format: <repo_id>:<commit_sha>:<node_type>:<full_name>:<hash>
    """

    def __init__(self, repo_id: str, commit_sha: str = ""):
        self._repo_id = repo_id
        self._commit_sha = commit_sha
        self._id_map: dict[str, str] = {}  # joern_id → cpg_node_id

    def convert_file(self, path: str) -> tuple[list[dict], list[dict]]:
        """Convert a GraphSON file to portable JSONL records.

        Accepts either:
        - A single JSON file with {"nodes": [...], "edges": [...]}
        - Separate nodes.json and edges.json (pass nodes path, infers edges)

        Args:
            path: Path to the GraphSON file.

        Returns:
            Tuple of (node_records, edge_records) ready for JSONL output.
        """
        path = Path(path)
        with open(path) as f:
            data = json.load(f)

        # Handle TinkerPop GraphSON format: {"@type": "tinker:graph", "@value": {"vertices": [...], "edges": [...]}}
        if isinstance(data, dict) and data.get("@type") == "tinker:graph":
            graph_value = data.get("@value", {})
            raw_nodes = [self._unwrap_vertex(v) for v in graph_value.get("vertices", [])]
            raw_edges = [self._unwrap_edge(e) for e in graph_value.get("edges", [])]
        elif isinstance(data, dict):
            raw_nodes = data.get("nodes", data.get("vertices", []))
            raw_edges = data.get("edges", [])
        elif isinstance(data, list):
            # Heuristic: if items have "src"/"dst", they're edges; otherwise nodes
            if data and ("src" in data[0] or "dst" in data[0]):
                raw_nodes = []
                raw_edges = data
            else:
                raw_nodes = data
                raw_edges = []
                # Try to find edges file alongside
                edges_path = path.parent / "edges.json"
                if edges_path.exists():
                    with open(edges_path) as f:
                        raw_edges = json.load(f)
        else:
            raw_nodes, raw_edges = [], []

        nodes = self._convert_nodes(raw_nodes)
        edges = self._convert_edges(raw_edges)

        logger.info(
            f"GraphSON converted: {len(nodes)} nodes, {len(edges)} edges "
            f"(repo={self._repo_id}, commit={self._commit_sha})"
        )
        return nodes, edges

    def convert_nodes_stream(self, stream: IO) -> list[dict]:
        """Convert a stream of GraphSON nodes (JSON array)."""
        raw_nodes = json.load(stream)
        if isinstance(raw_nodes, dict):
            raw_nodes = raw_nodes.get("nodes", raw_nodes.get("vertices", []))
        return self._convert_nodes(raw_nodes)

    def convert_edges_stream(self, stream: IO) -> list[dict]:
        """Convert a stream of GraphSON edges (JSON array)."""
        raw_edges = json.load(stream)
        if isinstance(raw_edges, dict):
            raw_edges = raw_edges.get("edges", [])
        return self._convert_edges(raw_edges)

    @staticmethod
    def _unwrap_value(val):
        """Unwrap a TinkerPop typed value: {"@type": "g:Int64", "@value": 123} → 123."""
        if isinstance(val, dict) and "@value" in val:
            inner = val["@value"]
            # Handle nested lists: {"@type": "g:List", "@value": ["foo"]}
            if isinstance(inner, list):
                return inner[0] if len(inner) == 1 else inner
            return inner
        return val

    def _unwrap_vertex(self, vertex: dict) -> dict:
        """Convert a TinkerPop g:Vertex to a flat Joern-style dict for from_joern().

        TinkerPop format:
            {"@type": "g:Vertex", "id": {"@type": "g:Int64", "@value": 123},
             "label": "METHOD", "properties": {"FULL_NAME": {"@type": "g:VertexProperty", "@value": {...}}}}

        Joern format (what from_joern expects):
            {"id": "123", "label": "METHOD", "properties": {"FULL_NAME": "pkg.Foo.bar"}}
        """
        raw_id = self._unwrap_value(vertex.get("id", ""))
        label = vertex.get("label", "UNKNOWN")

        # Unwrap properties from TinkerPop VertexProperty format
        props = {}
        raw_props = vertex.get("properties", {})
        for key, prop_val in raw_props.items():
            if isinstance(prop_val, dict):
                # {"@type": "g:VertexProperty", "@value": {"@type": "g:List", "@value": ["actual_value"]}}
                inner = prop_val.get("@value", prop_val)
                if isinstance(inner, dict) and "@value" in inner:
                    inner = inner["@value"]
                if isinstance(inner, list):
                    val = inner[0] if len(inner) == 1 else inner
                else:
                    val = inner
                # Final unwrap if value is still a typed wrapper (e.g., {"@type": "g:Int32", "@value": 4})
                if isinstance(val, dict) and "@value" in val:
                    val = val["@value"]
                props[key] = val
            else:
                props[key] = prop_val

        return {"id": str(raw_id), "label": label, "properties": props}

    def _unwrap_edge(self, edge: dict) -> dict:
        """Convert a TinkerPop g:Edge to a flat Joern-style dict for from_joern().

        TinkerPop format:
            {"@type": "g:Edge", "id": {...}, "inV": {"@type": "g:Int64", "@value": 456},
             "outV": {"@type": "g:Int64", "@value": 123}, "label": "AST", "properties": {}}

        Joern format:
            {"src": "123", "dst": "456", "label": "AST", "properties": {}}
        """
        src = str(self._unwrap_value(edge.get("outV", "")))
        dst = str(self._unwrap_value(edge.get("inV", "")))
        label = edge.get("label", "UNKNOWN")

        # Unwrap edge properties if any
        props = {}
        for key, val in edge.get("properties", {}).items():
            props[key] = self._unwrap_value(val) if isinstance(val, dict) else val

        return {"src": src, "dst": dst, "label": label, "properties": props}

    def _convert_nodes(self, raw_nodes: list[dict]) -> list[dict]:
        """Convert raw Joern nodes to portable JSONL records."""
        records = []
        for raw in raw_nodes:
            node = CPGNode.from_joern(raw)
            cpg_node_id = self._make_cpg_node_id(node)
            self._id_map[node.id] = cpg_node_id

            records.append({
                "cpg_node_id": cpg_node_id,
                "node_type": node.node_type,
                "labels": [node.node_type],
                "repo_id": self._repo_id,
                "commit_sha": self._commit_sha,
                "file_path": node.filename,
                "fully_qualified_name": node.full_name,
                "name": node.name,
                "line_start": node.line_number,
                "line_end": node.line_number_end,
                "code_hash": node.hash,
                "properties": node.properties,
            })
        return records

    def _convert_edges(self, raw_edges: list[dict]) -> list[dict]:
        """Convert raw Joern edges to portable JSONL records."""
        records = []
        for raw in raw_edges:
            edge = CPGEdge.from_joern(raw)
            source_cpg_id = self._id_map.get(edge.source_id, edge.source_id)
            target_cpg_id = self._id_map.get(edge.target_id, edge.target_id)

            records.append({
                "source_cpg_node_id": source_cpg_id,
                "target_cpg_node_id": target_cpg_id,
                "edge_type": edge.edge_type,
                "properties": edge.properties,
            })
        return records

    def _make_cpg_node_id(self, node: CPGNode) -> str:
        """Generate a stable cpg_node_id for a Joern node.

        Format: <repo_id>:<commit_sha>:<node_type>:<full_name_or_id>:<hash>
        """
        identity = node.full_name or node.name or node.id
        hash_part = node.hash or ""
        parts = [self._repo_id, self._commit_sha, node.node_type, identity]
        if hash_part:
            parts.append(hash_part)
        return ":".join(parts)

    @staticmethod
    def write_jsonl(records: list[dict], output_path: str) -> int:
        """Write records to a JSONL file (one JSON object per line).

        Args:
            records: List of dicts to write.
            output_path: Path to output file.

        Returns:
            Number of records written.
        """
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return len(records)
