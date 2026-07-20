"""Pipeline Executor — schema-driven orchestration of ingest → transform → build.

Reads an ETLSchema (from JSON/mapping file) and executes the full pipeline:
1. Read source data (via graphrag-toolkit readers)
2. Ingest: column/row/field prep (pandas)
3. Transform: rows → typed Nodes/Edges
4. Build: Cypher generation + write to Neptune via GraphStore
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from document_graph.model_elements import Node, Edge
from document_graph.graph_build.cypher_builder import node_to_cypher, edge_to_cypher

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""
    nodes_created: int = 0
    edges_created: int = 0
    records_processed: int = 0
    errors: list = field(default_factory=list)


class PipelineExecutor:
    """Schema-driven pipeline executor.

    Reads a mapping/schema JSON and orchestrates:
      source → ingest (DataFrame ops) → transform (row→node) → build (Cypher→Neptune)

    Usage:
        from document_graph.pipeline import PipelineExecutor

        executor = PipelineExecutor(graph_store, tenant_id='scim')
        result = executor.run_from_schema('mapping.json', source_df=df)
    """

    def __init__(self, graph_store, tenant_id: str = "default"):
        self._store = graph_store
        self._tenant_id = tenant_id

    def run_from_schema(self, schema_path: str, source_df: pd.DataFrame) -> PipelineResult:
        """Execute the full pipeline from a schema file.

        Args:
            schema_path: Path to mapping.json / ETL schema file
            source_df: Source data as pandas DataFrame
        """
        with open(schema_path) as f:
            schema = json.load(f)

        result = PipelineResult(records_processed=len(source_df))

        # Extract graph definition from schema
        graph_def = schema.get("graph", {})
        node_defs = graph_def.get("nodes", [])
        edge_defs = graph_def.get("edges", [])
        mappings = schema.get("mappings", schema.get("properties", []))

        # Build property map: csv_column → (node_type, property_name)
        property_map = {}
        for m in mappings:
            csv_col = m.get("csv_property_name", "")
            prop_name = m.get("arrow_property_name", csv_col)
            parents = m.get("parents", [m.get("name", "")])
            map_type = m.get("type", "property")
            if csv_col:
                property_map[csv_col] = {
                    "property_name": prop_name,
                    "parents": parents,
                    "type": map_type,
                }

        # Build node type definitions
        node_types = {}
        for node_def in node_defs:
            for label, config in node_def.items():
                id_field = config.get("property", {}).get("csv_map", "id")
                node_types[label] = {"id_field": id_field, "properties": {}}

        # Map properties to their node types
        for csv_col, mapping in property_map.items():
            if mapping["type"] == "property":
                for parent in mapping["parents"]:
                    if parent in node_types:
                        node_types[parent]["properties"][csv_col] = mapping["property_name"]

        # Build edge definitions
        edge_types = []
        for edge_def in edge_defs:
            for source_label, rels in edge_def.items():
                for rel_type, target_label in rels.items():
                    edge_types.append({
                        "source": source_label,
                        "target": target_label,
                        "type": rel_type,
                    })

        # Execute: create nodes
        nodes = []
        for _, row in source_df.iterrows():
            for label, type_def in node_types.items():
                id_col = type_def["id_field"]
                if id_col in row and pd.notna(row[id_col]):
                    props = {}
                    for csv_col, prop_name in type_def["properties"].items():
                        if csv_col in row and pd.notna(row[csv_col]):
                            props[prop_name] = str(row[csv_col])
                    props["id"] = str(row[id_col])
                    props[id_col] = str(row[id_col])

                    node = Node(id=str(row[id_col]), labels=[label], properties=props)
                    nodes.append(node)

        # Deduplicate nodes by id+label
        seen = set()
        unique_nodes = []
        for n in nodes:
            key = (n.id, tuple(n.labels))
            if key not in seen:
                seen.add(key)
                unique_nodes.append(n)

        # Write nodes
        for node in unique_nodes:
            cypher, params = node_to_cypher(node, tenant_id=self._tenant_id)
            try:
                self._store.execute_query(cypher, params)
                result.nodes_created += 1
            except Exception as e:
                result.errors.append(f"Node {node.id}: {e}")

        # Write edges (based on shared fields in the data)
        for edge_def in edge_types:
            src_label = edge_def["source"]
            tgt_label = edge_def["target"]
            rel_type = edge_def["type"]

            src_id_field = node_types.get(src_label, {}).get("id_field", "")
            tgt_id_field = node_types.get(tgt_label, {}).get("id_field", "")

            if src_id_field in source_df.columns and tgt_id_field in source_df.columns:
                for _, row in source_df.iterrows():
                    src_id = str(row[src_id_field]) if pd.notna(row.get(src_id_field)) else None
                    tgt_id = str(row[tgt_id_field]) if pd.notna(row.get(tgt_id_field)) else None
                    if src_id and tgt_id:
                        edge = Edge(
                            id=f"{src_id}-{rel_type}-{tgt_id}",
                            source_id=src_id,
                            target_id=tgt_id,
                            label=rel_type,
                        )
                        cypher, params = edge_to_cypher(edge, tenant_id=self._tenant_id)
                        try:
                            self._store.execute_query(cypher, params)
                            result.edges_created += 1
                        except Exception as e:
                            result.errors.append(f"Edge {src_id}->{tgt_id}: {e}")

        logger.info("Pipeline complete: %d nodes, %d edges, %d errors",
                    result.nodes_created, result.edges_created, len(result.errors))
        return result

    def run_from_dataframe(self, df: pd.DataFrame, node_label: str,
                           id_field: str = "id",
                           edge_field: Optional[str] = None,
                           edge_type: str = "RELATED_TO") -> PipelineResult:
        """Simple pipeline: DataFrame → typed nodes (+ optional edges).

        For when you don't have a full schema, just a label and DataFrame.
        """
        from document_graph.transform.transformer_provider_config import TransformerProviderConfig
        from document_graph.transform.graph_transformers.row_to_node import RowToNodeTransformer

        result = PipelineResult(records_processed=len(df))
        records = df.to_dict("records")

        # Transform
        config = TransformerProviderConfig(name="r2n", args={"type": node_label})
        nodes_data = RowToNodeTransformer(config).transform(records)

        # Build + write nodes
        for n in nodes_data:
            node = Node(
                id=n.get(id_field, n.get("_id", "")),
                labels=[n.get("node_type", node_label)],
                properties=n,
            )
            cypher, params = node_to_cypher(node, tenant_id=self._tenant_id)
            try:
                self._store.execute_query(cypher, params)
                result.nodes_created += 1
            except Exception as e:
                result.errors.append(str(e))

        # Optional: infer edges
        if edge_field and edge_field in df.columns:
            from document_graph.transform.graph_transformers.infer_edges import EdgeInferencer
            config = TransformerProviderConfig(name="edges", args={
                "source_field": edge_field, "edge_type": edge_type
            })
            edges_data = EdgeInferencer(config).transform(records)
            for ed in edges_data:
                edge = Edge(
                    id=ed.get("id", ""),
                    source_id=ed.get("source_id", ""),
                    target_id=ed.get("target_id", ""),
                    label=ed.get("edge_type", edge_type),
                )
                cypher, params = edge_to_cypher(edge, tenant_id=self._tenant_id)
                try:
                    self._store.execute_query(cypher, params)
                    result.edges_created += 1
                except Exception as e:
                    result.errors.append(str(e))

        return result
