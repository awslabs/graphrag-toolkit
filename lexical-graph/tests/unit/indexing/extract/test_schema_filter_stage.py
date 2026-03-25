# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from llama_index.core.schema import TextNode
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_schema import ExtractionSchema, EntityTypeConfig
from graphrag_toolkit.lexical_graph.indexing.extract.stages.schema_filter_stage import SchemaFilter, SchemaFilterStage
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.model import (
    TopicCollection, Topic, Statement, Fact, Entity, Relation,
)


def make_topic_node(topics_data):
    """Create a TextNode with topics metadata."""
    node = TextNode(text='test')
    node.metadata[TOPICS_KEY] = topics_data
    return node


def make_topics_with_entities(entities, facts=None):
    """Create a TopicCollection dict with given entities and optional facts."""
    stmts = []
    if facts:
        stmts = [Statement(value='test stmt', facts=facts)]
    tc = TopicCollection(topics=[
        Topic(value='test topic', entities=entities, statements=stmts)
    ])
    return tc.model_dump()


class TestSchemaFilterStage:
    """Tests for SchemaFilterStage."""

    def test_is_extraction_stage(self):
        """Verify SchemaFilterStage is an ExtractionStage."""
        stage = SchemaFilterStage(schema=ExtractionSchema())
        assert isinstance(stage, ExtractionStage)

    def test_input_keys(self):
        """Verify input_keys returns TOPICS_KEY."""
        stage = SchemaFilterStage(schema=ExtractionSchema())
        assert stage.input_keys() == [TOPICS_KEY]

    def test_output_keys(self):
        """Verify output_keys returns TOPICS_KEY."""
        stage = SchemaFilterStage(schema=ExtractionSchema())
        assert stage.output_keys() == [TOPICS_KEY]

    def test_stage_type(self):
        """Verify stage_type is 'filter'."""
        stage = SchemaFilterStage(schema=ExtractionSchema())
        assert stage.stage_type == 'filter'

    def test_as_transform_returns_schema_filter(self):
        """Verify as_transform returns a SchemaFilter."""
        stage = SchemaFilterStage(schema=ExtractionSchema())
        result = stage.as_transform()
        assert isinstance(result, SchemaFilter)


class TestSchemaFilter:
    """Tests for SchemaFilter TransformComponent."""

    def test_non_strict_passes_through(self):
        """Verify non-strict mode passes all data through unchanged."""
        schema = ExtractionSchema(
            entity_types={'Person': EntityTypeConfig()},
            strict=False,
        )
        entities = [
            Entity(value='John', classification='Person'),
            Entity(value='Acme', classification='Company'),
        ]
        node = make_topic_node(make_topics_with_entities(entities))
        sf = SchemaFilter(extraction_schema=schema)
        result = sf([node])
        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc.topics[0].entities) == 2

    def test_strict_filters_entities(self):
        """Verify strict mode removes entities not in schema."""
        schema = ExtractionSchema(
            entity_types={'Person': EntityTypeConfig()},
            strict=True,
        )
        entities = [
            Entity(value='John', classification='Person'),
            Entity(value='Acme', classification='Company'),
        ]
        node = make_topic_node(make_topics_with_entities(entities))
        sf = SchemaFilter(extraction_schema=schema)
        result = sf([node])
        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc.topics[0].entities) == 1
        assert tc.topics[0].entities[0].value == 'John'

    def test_strict_filters_relationships(self):
        """Verify strict mode removes facts with disallowed relationship types."""
        schema = ExtractionSchema(
            entity_types={
                'Person': EntityTypeConfig(),
                'Company': EntityTypeConfig(),
            },
            relationship_types=['WORKS_FOR'],
            strict=True,
        )
        facts = [
            Fact(
                subject=Entity(value='John', classification='Person'),
                predicate=Relation(value='WORKS_FOR'),
                object=Entity(value='Acme', classification='Company'),
            ),
            Fact(
                subject=Entity(value='John', classification='Person'),
                predicate=Relation(value='LIVES_IN'),
                object=Entity(value='NYC', classification='Location'),
            ),
        ]
        entities = [
            Entity(value='John', classification='Person'),
            Entity(value='Acme', classification='Company'),
            Entity(value='NYC', classification='Location'),
        ]
        node = make_topic_node(make_topics_with_entities(entities, facts))
        sf = SchemaFilter(extraction_schema=schema)
        result = sf([node])
        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc.topics[0].statements[0].facts) == 1
        assert tc.topics[0].statements[0].facts[0].predicate.value == 'WORKS_FOR'

    def test_strict_entity_alias_matching(self):
        """Verify strict mode matches entity aliases."""
        schema = ExtractionSchema(
            entity_types={
                'Person': EntityTypeConfig(aliases=['Individual']),
            },
            strict=True,
        )
        entities = [
            Entity(value='John', classification='Individual'),
            Entity(value='Acme', classification='Company'),
        ]
        node = make_topic_node(make_topics_with_entities(entities))
        sf = SchemaFilter(extraction_schema=schema)
        result = sf([node])
        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc.topics[0].entities) == 1
        assert tc.topics[0].entities[0].value == 'John'

    def test_strict_case_insensitive_entity_matching(self):
        """Verify entity type matching is case-insensitive."""
        schema = ExtractionSchema(
            entity_types={'Person': EntityTypeConfig()},
            strict=True,
        )
        entities = [
            Entity(value='John', classification='person'),
            Entity(value='Jane', classification='PERSON'),
        ]
        node = make_topic_node(make_topics_with_entities(entities))
        sf = SchemaFilter(extraction_schema=schema)
        result = sf([node])
        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc.topics[0].entities) == 2

    def test_node_without_topics_unchanged(self):
        """Verify nodes without topics metadata are unchanged."""
        schema = ExtractionSchema(entity_types={'Person': EntityTypeConfig()}, strict=True)
        node = TextNode(text='test')
        sf = SchemaFilter(extraction_schema=schema)
        result = sf([node])
        assert TOPICS_KEY not in result[0].metadata

    def test_no_relationship_types_skips_fact_filtering(self):
        """Verify no relationship_types means facts are not filtered."""
        schema = ExtractionSchema(
            entity_types={'Person': EntityTypeConfig(), 'Company': EntityTypeConfig()},
            relationship_types=[],
            strict=True,
        )
        facts = [
            Fact(
                subject=Entity(value='John', classification='Person'),
                predicate=Relation(value='WORKS_FOR'),
                object=Entity(value='Acme', classification='Company'),
            ),
        ]
        entities = [
            Entity(value='John', classification='Person'),
            Entity(value='Acme', classification='Company'),
        ]
        node = make_topic_node(make_topics_with_entities(entities, facts))
        sf = SchemaFilter(extraction_schema=schema)
        result = sf([node])
        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc.topics[0].statements[0].facts) == 1
