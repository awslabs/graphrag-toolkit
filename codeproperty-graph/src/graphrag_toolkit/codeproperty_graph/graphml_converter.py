# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""GraphML → JSONL Converter — transforms Joern GraphML exports to portable CPG JSONL.

GraphML is an XML format with <node> and <edge> elements. Each has <data> children
with key/value properties. Joern exports GraphML via:
    joern-export --repr cpg14 --format graphml --out output/ <cpg.bin>

Usage:
    converter = GraphMLConverter(repo_id="payments-api", commit_sha="abc123")
    nodes, edges = converter.convert_file("path/to/export.graphml")
    GraphMLConverter.write_jsonl(nodes, "output/nodes.jsonl")
    GraphMLConverter.write_jsonl(edges, "output/edges.jsonl")
"""

import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import IO

from .models import CPGNode, CPGEdge

logger = logging.getLogger(__name__)

# GraphML namespace
GRAPHML_NS = "http://graphml.graphstruct.org/xmlns"
NS = {"g": GRAPHML_NS}


class GraphMLConverter:
    """Converts Joern GraphML exports to portable CPG JSONL format.

    Handles the standard GraphML format:
    <graphml>
      <key id="k0" for="node" attr.name="FULL_NAME" attr.type="string"/>
      ...
      <graph>
        <node id="123">
          <data key="k0">com.example.Main.main</data>
          <data key="labelV">METHOD</data>
        </node>
        <edge source="123" target="456">
          <data key="labelE">AST</data>
        </edge>
      </graph>
    </graphml>
    """

    def __init__(self, repo_id: str, commit_sha: str = ""):
        self._repo_id = repo_id
        self._commit_sha = commit_sha
        self._id_map: dict[str, str] = {}  # graphml_id → cpg_node_id

    def convert_file(self, path: str) -> tuple[list[dict], list[dict]]:
        """Convert a GraphML file to portable JSONL records.

        Args:
            path: Path to the GraphML file.

        Returns:
            Tuple of (node_records, edge_records) ready for JSONL output.
        """
        tree = ET.parse(path)
        root = tree.getroot()

        # Auto-detect namespace
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0] + "}"

        # Parse key definitions (maps key id → attribute name)
        key_map = {}
        for key_elem in root.iter(f"{ns}key"):
            key_id = key_elem.get("id", "")
            attr_name = key_elem.get("attr.name", "")
            if key_id and attr_name:
                key_map[key_id] = attr_name

        # Find the graph element
        graph = root.find(f"{ns}graph")
        if graph is None:
            # Try without namespace
            graph = root.find("graph")
        if graph is None:
            logger.warning(f"No <graph> element found in {path}")
            return [], []

        # Parse nodes
        raw_nodes = []
        for node_elem in graph.iter(f"{ns}node"):
            node_id = node_elem.get("id", "")
            props = {}
            for data_elem in node_elem.iter(f"{ns}data"):
                key_id = data_elem.get("key", "")
                attr_name = key_map.get(key_id, key_id)
                value = data_elem.text or ""
                props[attr_name] = value
            raw_nodes.append({"id": node_id, "label": props.pop("labelV", props.pop("label", "UNKNOWN")), "properties": props})

        # Parse edges
        raw_edges = []
        for edge_elem in graph.iter(f"{ns}edge"):
            source = edge_elem.get("source", "")
            target = edge_elem.get("target", "")
            props = {}
            for data_elem in edge_elem.iter(f"{ns}data"):
                key_id = data_elem.get("key", "")
                attr_name = key_map.get(key_id, key_id)
                value = data_elem.text or ""
                props[attr_name] = value
            label = props.pop("labelE", props.pop("label", "UNKNOWN"))
            raw_edges.append({"src": source, "dst": target, "label": label, "properties": props})

        # Convert using the same logic as GraphSON converter
        nodes = self._convert_nodes(raw_nodes)
        edges = self._convert_edges(raw_edges)

        logger.info(
            f"GraphML converted: {len(nodes)} nodes, {len(edges)} edges "
            f"(repo={self._repo_id}, commit={self._commit_sha})"
        )
        return nodes, edges

    def _convert_nodes(self, raw_nodes: list[dict]) -> list[dict]:
        """Convert raw nodes to portable JSONL records."""
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
        """Convert raw edges to portable JSONL records."""
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
        """Generate a stable cpg_node_id."""
        identity = node.full_name or node.name or node.id
        hash_part = node.hash or ""
        parts = [self._repo_id, self._commit_sha, node.node_type, identity]
        if hash_part:
            parts.append(hash_part)
        return ":".join(parts)

    @staticmethod
    def write_jsonl(records: list[dict], output_path: str) -> int:
        """Write records to a JSONL file."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return len(records)
