"""Tests for document-graph v3."""

import pytest
from document_graph import NodeModel, EdgeModel, Node, Edge
from document_graph.graph_build import node_to_cypher, edge_to_cypher, batch_nodes_to_cypher
from document_graph.query import DocumentGraphQueryEngine


class TestModel:
    def test_node_model_importable(self):
        assert NodeModel is not None

    def test_edge_model_importable(self):
        assert EdgeModel is not None


class TestModelElements:
    def test_node_creation(self):
        n = Node(id="u1", labels=["User"], properties={"name": "Alice", "email": "a@b.com"})
        assert n.id == "u1"
        assert "User" in n.labels
        assert n.properties["name"] == "Alice"

    def test_edge_creation(self):
        e = Edge(id="e1", source_id="u1", target_id="a1", label="OWNS")
        assert e.label == "OWNS"
        assert e.source_id == "u1"
        assert e.target_id == "a1"


class TestCypherBuilder:
    def test_node_to_cypher(self):
        n = Node(id="u1", labels=["User"], properties={"name": "Alice"})
        query, params = node_to_cypher(n)
        assert "MERGE" in query
        assert "User" in query
        assert params["id_val"] == "u1"

    def test_node_to_cypher_with_tenant(self):
        n = Node(id="u1", labels=["User"], properties={"name": "Alice"})
        query, params = node_to_cypher(n, tenant_id="scim")
        assert "__User__scim__" in query

    def test_edge_to_cypher(self):
        e = Edge(id="e1", source_id="u1", target_id="a1", label="OWNS")
        query, params = edge_to_cypher(e)
        assert "MERGE" in query
        assert "OWNS" in query
        assert params["src_id"] == "u1"
        assert params["tgt_id"] == "a1"

    def test_batch_nodes(self):
        nodes = [
            Node(id=f"u{i}", labels=["User"], properties={"name": f"User{i}"})
            for i in range(5)
        ]
        results = batch_nodes_to_cypher(nodes, tenant_id="test")
        assert len(results) == 5
        assert all("MERGE" in q for q, _ in results)


class TestQueryEngine:
    def test_init(self):
        class MockStore:
            def query(self, cypher, params=None):
                return [{"n": "result"}]

        engine = DocumentGraphQueryEngine(MockStore(), tenant_id="scim")
        assert engine._tenant_id == "scim"

    def test_get_nodes(self):
        class MockStore:
            def execute_query(self, cypher, params=None):
                assert "__User__scim__" in cypher
                return [{"n": {"name": "Alice"}}]

        engine = DocumentGraphQueryEngine(MockStore(), tenant_id="scim")
        result = engine.get_nodes("User", limit=10)
        assert len(result) == 1

    def test_find_by_property(self):
        class MockStore:
            def execute_query(self, cypher, params=None):
                assert params["val"] == "alice@test.com"
                return [{"n": {"email": "alice@test.com"}}]

        engine = DocumentGraphQueryEngine(MockStore(), tenant_id="scim")
        result = engine.find_by_property("User", "email", "alice@test.com")
        assert len(result) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Extended tests — Schema Providers, Transformers, ETLSchema, Graph Build, Discovery
# ─────────────────────────────────────────────────────────────────────────────

import json
import csv
import tempfile
import uuid
from pathlib import Path
from pydantic import ValidationError

from document_graph.graph_build import node_to_cypher, edge_to_cypher, batch_nodes_to_cypher
from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig
from document_graph.schema.providers.csv_schema_provider import CSVSchemaProvider
from document_graph.schema.providers.json_schema_provider import JSONSchemaProvider
from document_graph.schema.static_schema_provider import StaticSchemaProvider
from document_graph.schema.providers.schema_provider_factory import SchemaProviderFactory
from document_graph.schema.etl_schema_model import (
    ETLSchema, ExtractConfig, TransformConfig, LoadConfig,
    ChunkingConfig, NormalizeConfig, EntityExtractionConfig,
    MetadataMapping, NodeDefinition, RelationshipDefinition,
)
from document_graph.transform.transformer_provider_config import TransformerProviderConfig
from document_graph.transform.normalizers.normalize_whitespace_provider import NormalizeWhitespaceProvider
from document_graph.transform.normalizers.normalize_nulls_provider import NormalizeNullsProvider
from document_graph.transform.normalizers.normalize_case_provider import NormalizeCaseProvider
from document_graph.transform.field_transformers.json_flattener import JSONFlattenerProvider
from document_graph.transform.field_transformers.uuid_generator import UuidGeneratorTransformer
from document_graph.transform.graph_transformers.row_to_node import RowToNodeTransformer
from document_graph.transform.graph_transformers.infer_edges import EdgeInferencer
from document_graph.transform.filter_transformers.row_filter import RowFilterProvider
from document_graph.transform.truncators.length_truncator import LengthTruncator
from document_graph.schema.discovery.csv_discovery_provider import CSVSchemaDiscoveryProvider


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_etl_schema():
    """A valid minimal ETLSchema."""
    return ETLSchema(
        schema_id="test-schema",
        description="Test",
        extract=ExtractConfig(source_type="file"),
        transform=TransformConfig(
            chunking=ChunkingConfig(strategy="fixed_length"),
            metadata_mapping=MetadataMapping(),
            entity_extraction=EntityExtractionConfig(method="ner"),
            normalize=NormalizeConfig(),
        ),
        load=LoadConfig(
            document_node=NodeDefinition(type="Doc", fields=["title"]),
            section_node=NodeDefinition(type="Sec", fields=["text"]),
            relationships=[RelationshipDefinition(type="has", source="d", target="s")],
        ),
    )


@pytest.fixture
def tmp_csv(tmp_path):
    """Create a temp CSV file for testing."""
    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text("id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com\n")
    return csv_file


@pytest.fixture
def tmp_json(tmp_path):
    """Create a temp JSON file for testing."""
    json_file = tmp_path / "test_data.json"
    data = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    json_file.write_text(json.dumps(data))
    return json_file


def _make_config(name="test", **args):
    """Helper to build a TransformerProviderConfig."""
    return TransformerProviderConfig(name=name, args=args)


# ─── ETLSchema Model Tests ──────────────────────────────────────────────────

class TestETLSchemaModel:
    def test_valid_construction(self, sample_etl_schema):
        assert sample_etl_schema.schema_id == "test-schema"
        assert sample_etl_schema.extract.source_type == "file"
        assert sample_etl_schema.transform.chunking.strategy == "fixed_length"

    def test_missing_schema_id_raises(self):
        with pytest.raises(ValidationError):
            ETLSchema(
                extract=ExtractConfig(source_type="file"),
                transform=TransformConfig(
                    chunking=ChunkingConfig(strategy="x"),
                    metadata_mapping=MetadataMapping(),
                    entity_extraction=EntityExtractionConfig(method="ner"),
                    normalize=NormalizeConfig(),
                ),
                load=LoadConfig(
                    document_node=NodeDefinition(type="D", fields=[]),
                    section_node=NodeDefinition(type="S", fields=[]),
                    relationships=[],
                ),
            )

    def test_missing_extract_raises(self):
        with pytest.raises(ValidationError):
            ETLSchema(
                schema_id="x",
                transform=TransformConfig(
                    chunking=ChunkingConfig(strategy="x"),
                    metadata_mapping=MetadataMapping(),
                    entity_extraction=EntityExtractionConfig(method="ner"),
                    normalize=NormalizeConfig(),
                ),
                load=LoadConfig(
                    document_node=NodeDefinition(type="D", fields=[]),
                    section_node=NodeDefinition(type="S", fields=[]),
                    relationships=[],
                ),
            )

    def test_model_dump_roundtrip(self, sample_etl_schema):
        dumped = sample_etl_schema.model_dump()
        reconstructed = ETLSchema(**dumped)
        assert reconstructed.schema_id == sample_etl_schema.schema_id
        assert reconstructed.extract.source_type == "file"
        assert reconstructed.load.document_node.type == "Doc"

    def test_extra_fields_allowed(self):
        schema = ETLSchema(
            schema_id="extra",
            extract=ExtractConfig(source_type="api", custom_field="custom_val"),
            transform=TransformConfig(
                chunking=ChunkingConfig(strategy="x"),
                metadata_mapping=MetadataMapping(),
                entity_extraction=EntityExtractionConfig(method="ner"),
                normalize=NormalizeConfig(),
            ),
            load=LoadConfig(
                document_node=NodeDefinition(type="D", fields=[]),
                section_node=NodeDefinition(type="S", fields=[]),
                relationships=[],
            ),
        )
        assert schema.extract.custom_field == "custom_val"


# ─── Schema Providers Tests ─────────────────────────────────────────────────

class TestSchemaProviders:
    def test_csv_schema_provider(self, tmp_csv):
        config = SchemaProviderConfig(type="csv", schema_id="csv-test", connection_config={"path": str(tmp_csv)})
        provider = CSVSchemaProvider(config)
        schema = provider.load_schema()
        assert isinstance(schema, ETLSchema)
        assert "id" in schema.load.document_node.fields
        assert "name" in schema.load.document_node.fields

    def test_csv_schema_provider_missing_path(self):
        config = SchemaProviderConfig(type="csv", connection_config={})
        with pytest.raises(ValueError):
            CSVSchemaProvider(config)

    def test_csv_schema_provider_file_not_found(self, tmp_path):
        config = SchemaProviderConfig(type="csv", connection_config={"path": str(tmp_path / "nope.csv")})
        with pytest.raises(FileNotFoundError):
            CSVSchemaProvider(config)

    def test_json_schema_provider(self, tmp_json):
        config = SchemaProviderConfig(type="json", schema_id="json-test", connection_config={"path": str(tmp_json)})
        provider = JSONSchemaProvider(config)
        schema = provider.load_schema()
        assert isinstance(schema, ETLSchema)
        assert "id" in schema.load.document_node.fields

    def test_json_schema_provider_missing_path(self):
        config = SchemaProviderConfig(type="json", connection_config={})
        with pytest.raises(ValueError):
            JSONSchemaProvider(config)

    def test_static_schema_provider(self):
        provider = StaticSchemaProvider({})
        schema = provider.load_schema()
        assert schema.schema_id == "static-default"
        assert schema.extract.source_type == "s3"
        assert schema.load.document_node.type == "DocumentNode"

    def test_static_schema_from_config(self):
        provider = StaticSchemaProvider.from_config({"type": "static"})
        assert provider.get_schema_id() == "static-default"

    def test_factory_creates_csv_provider(self, tmp_csv):
        config = {"type": "csv", "schema_id": "factory-csv", "connection_config": {"path": str(tmp_csv)}}
        provider = SchemaProviderFactory.create(config)
        assert isinstance(provider, CSVSchemaProvider)

    def test_factory_creates_json_provider(self, tmp_json):
        config = {"type": "json", "schema_id": "factory-json", "connection_config": {"path": str(tmp_json)}}
        provider = SchemaProviderFactory.create(config)
        assert isinstance(provider, JSONSchemaProvider)

    def test_factory_invalid_type_raises(self):
        with pytest.raises(Exception):
            SchemaProviderFactory.create({"type": "unknown_xyz", "connection_config": {}})


# ─── Transformer Tests ───────────────────────────────────────────────────────

class TestNormalizers:
    def test_whitespace_normalizer(self):
        cfg = _make_config("ws", fields=["text"])
        t = NormalizeWhitespaceProvider(cfg)
        result = t.transform([{"text": "  hello   world  \n\t end  "}])
        assert result[0]["text"] == "hello world end"

    def test_whitespace_preserves_non_string(self):
        cfg = _make_config("ws", fields=["val"])
        t = NormalizeWhitespaceProvider(cfg)
        result = t.transform([{"val": 42}])
        assert result[0]["val"] == 42

    def test_nulls_normalizer(self):
        cfg = _make_config("nulls", fields=["a", "b"])
        t = NormalizeNullsProvider(cfg)
        result = t.transform([{"a": "N/A", "b": "hello", "c": "null"}])
        assert result[0]["a"] is None
        assert result[0]["b"] == "hello"
        assert result[0]["c"] == "null"  # not in fields list

    def test_nulls_custom_null_like(self):
        cfg = _make_config("nulls", fields=["x"], null_like=["missing"])
        t = NormalizeNullsProvider(cfg)
        result = t.transform([{"x": "missing"}])
        assert result[0]["x"] is None

    def test_case_normalizer_lower(self):
        cfg = _make_config("case", mode="lower")
        t = NormalizeCaseProvider(cfg)
        result = t.transform([{"name": "ALICE", "age": 30}])
        assert result[0]["name"] == "alice"
        assert result[0]["age"] == 30

    def test_case_normalizer_upper(self):
        cfg = _make_config("case", mode="upper")
        t = NormalizeCaseProvider(cfg)
        result = t.transform([{"name": "alice"}])
        assert result[0]["name"] == "ALICE"


class TestFieldTransformers:
    def test_json_flattener(self):
        cfg = _make_config("jf", field="data")
        t = JSONFlattenerProvider(cfg)
        records = [{"id": "1", "data": '{"key": "val", "nested": {"a": 1}}'}]
        result = t.transform(records)
        assert "datakey" in result[0]
        assert result[0]["datakey"] == "val"

    def test_json_flattener_invalid_json(self):
        cfg = _make_config("jf", field="data")
        t = JSONFlattenerProvider(cfg)
        records = [{"id": "1", "data": "not json"}]
        result = t.transform(records)
        # Should return record without crashing
        assert result[0]["id"] == "1"

    def test_uuid_generator(self):
        cfg = _make_config("uuid", target_field="my_id")
        t = UuidGeneratorTransformer(cfg)
        records = [{"name": "Alice"}, {"name": "Bob"}]
        result = t.transform(records)
        assert "my_id" in result[0]
        assert "my_id" in result[1]
        # Should be valid UUIDs
        uuid.UUID(result[0]["my_id"])
        uuid.UUID(result[1]["my_id"])
        # Should be unique
        assert result[0]["my_id"] != result[1]["my_id"]


class TestGraphTransformers:
    def test_row_to_node(self):
        cfg = _make_config("rtn", type="Person")
        t = RowToNodeTransformer(cfg)
        records = [{"id": "p1", "name": "Alice"}]
        result = t.transform(records)
        assert result[0]["node_type"] == "Person"
        assert result[0]["graph_element"] == "node"
        assert result[0]["id"] == "p1"

    def test_row_to_node_auto_id(self):
        cfg = _make_config("rtn", type="Row")
        t = RowToNodeTransformer(cfg)
        records = [{"name": "Alice"}]
        result = t.transform(records)
        assert result[0]["id"] == "row_0"

    def test_row_to_node_auto_content(self):
        cfg = _make_config("rtn", type="Row")
        t = RowToNodeTransformer(cfg)
        records = [{"name": "Alice", "role": "admin"}]
        result = t.transform(records)
        assert "name: Alice" in result[0]["content"]

    def test_infer_edges(self):
        cfg = _make_config("ie", source_field="project", edge_type="WORKS_ON")
        t = EdgeInferencer(cfg)
        records = [
            {"id": "r1", "project": "A"},
            {"id": "r2", "project": "A"},
            {"id": "r3", "project": "B"},
        ]
        result = t.transform(records)
        edges = [r for r in result if r.get("edge_type") == "edge"]
        assert len(edges) == 1
        assert edges[0]["source_id"] == "r1"
        assert edges[0]["target_id"] == "r2"
        assert edges[0]["relationship"] == "WORKS_ON"

    def test_infer_edges_no_shared_field(self):
        cfg = _make_config("ie", source_field="project")
        t = EdgeInferencer(cfg)
        records = [{"id": "r1", "project": "A"}, {"id": "r2", "project": "B"}]
        result = t.transform(records)
        edges = [r for r in result if r.get("edge_type") == "edge"]
        assert len(edges) == 0


class TestFilterTransformers:
    def test_row_filter_eq(self):
        cfg = _make_config("rf", conditions=[{"field": "role", "operator": "eq", "value": "admin"}])
        t = RowFilterProvider(cfg)
        records = [{"role": "admin"}, {"role": "user"}]
        result = t.transform(records)
        assert len(result) == 1
        assert result[0]["role"] == "admin"

    def test_row_filter_not_null(self):
        cfg = _make_config("rf", conditions=[{"field": "email", "operator": "not_null"}])
        t = RowFilterProvider(cfg)
        records = [{"email": "a@b.com"}, {"email": None}]
        result = t.transform(records)
        assert len(result) == 1

    def test_row_filter_no_conditions_passes_all(self):
        cfg = _make_config("rf", conditions=[])
        t = RowFilterProvider(cfg)
        records = [{"a": 1}, {"a": 2}]
        assert len(t.transform(records)) == 2

    def test_row_filter_unsupported_op_raises(self):
        cfg = _make_config("rf", conditions=[{"field": "x", "operator": "regex", "value": ".*"}])
        t = RowFilterProvider(cfg)
        with pytest.raises(ValueError, match="Unsupported operator"):
            t.transform([{"x": "hello"}])


class TestTruncators:
    def test_length_truncator(self):
        cfg = _make_config("lt", max_length=5, fields=["text"])
        t = LengthTruncator(cfg)
        result = t.transform({"text": "abcdefghij", "other": "keep"})
        assert result["text"] == "abcde"
        assert result["other"] == "keep"

    def test_length_truncator_short_field_unchanged(self):
        cfg = _make_config("lt", max_length=100, fields=["text"])
        t = LengthTruncator(cfg)
        result = t.transform({"text": "short"})
        assert result["text"] == "short"

    def test_length_truncator_missing_field_ignored(self):
        cfg = _make_config("lt", max_length=5, fields=["missing"])
        t = LengthTruncator(cfg)
        result = t.transform({"text": "hello"})
        assert result["text"] == "hello"


# ─── Graph Build Edge Cases ──────────────────────────────────────────────────

class TestCypherBuilderEdgeCases:
    def test_node_special_chars_in_properties(self):
        n = Node(id="n1", labels=["User"], properties={"bio": "It's a \"quote\" & <tag>"})
        query, params = node_to_cypher(n)
        assert params["props"]["bio"] == "It's a \"quote\" & <tag>"
        assert "MERGE" in query

    def test_node_none_value_in_properties(self):
        n = Node(id="n1", labels=["User"], properties={"name": None})
        query, params = node_to_cypher(n)
        assert params["props"]["name"] is None

    def test_node_empty_labels_uses_default(self):
        n = Node(id="n1", labels=[], properties={"x": 1})
        query, params = node_to_cypher(n)
        assert "Node" in query

    def test_node_multiple_labels(self):
        n = Node(id="n1", labels=["Person", "Employee"], properties={})
        query, params = node_to_cypher(n)
        assert "Person:Employee" in query

    def test_edge_with_properties(self):
        e = Edge(id="e1", source_id="a", target_id="b", label="KNOWS", properties={"since": 2020})
        query, params = edge_to_cypher(e)
        assert params["props"]["since"] == 2020

    def test_edge_empty_properties(self):
        e = Edge(id="e1", source_id="a", target_id="b", label="KNOWS")
        query, params = edge_to_cypher(e)
        assert params["props"] == {}

    def test_node_tenant_multi_label(self):
        n = Node(id="n1", labels=["Person", "Admin"], properties={})
        query, params = node_to_cypher(n, tenant_id="t1")
        assert "__Person__t1__" in query
        assert "__Admin__t1__" in query


# ─── Schema Discovery Tests ─────────────────────────────────────────────────

class TestCSVSchemaDiscovery:
    def test_discover_from_temp_csv(self, tmp_csv):
        discovery = CSVSchemaDiscoveryProvider(source=tmp_csv)
        schema = discovery.discover_schema()
        assert isinstance(schema, ETLSchema)
        assert "id" in schema.load.document_node.fields
        assert "name" in schema.load.document_node.fields
        assert "email" in schema.load.document_node.fields
        assert schema.schema_id == "discovered-test_data"

    def test_discover_from_project_csv(self):
        csv_path = Path(__file__).parent.parent / "examples" / "cloud" / "notebooks" / "data" / "users.csv"
        if not csv_path.exists():
            pytest.skip("users.csv not found in expected location")
        discovery = CSVSchemaDiscoveryProvider(source=csv_path)
        schema = discovery.discover_schema()
        assert "id" in schema.load.document_node.fields
        assert "name" in schema.load.document_node.fields
        assert "email" in schema.load.document_node.fields
        assert "role" in schema.load.document_node.fields
        assert "account_id" in schema.load.document_node.fields

    def test_discover_file_not_found(self, tmp_path):
        discovery = CSVSchemaDiscoveryProvider(source=tmp_path / "nonexistent.csv")
        with pytest.raises(FileNotFoundError):
            discovery.discover_schema()


# ─── TransformerProviderConfig Tests ─────────────────────────────────────────

class TestTransformerProviderConfig:
    def test_basic_construction(self):
        cfg = TransformerProviderConfig(name="test", type="normalizer", args={"x": 1})
        assert cfg.name == "test"
        assert cfg.type == "normalizer"
        assert cfg.args["x"] == 1

    def test_parameters_syncs_to_args(self):
        cfg = TransformerProviderConfig(name="test", parameters={"y": 2})
        assert cfg.args["y"] == 2
