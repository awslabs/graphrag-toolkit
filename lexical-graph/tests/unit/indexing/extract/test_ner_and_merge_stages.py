# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import patch, MagicMock
from llama_index.core.schema import TextNode
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.stages.ner_extraction_stage import (
    NERExtractionStage, NERTransform, PRE_EXTRACTED_ENTITIES_KEY,
)
from graphrag_toolkit.lexical_graph.indexing.extract.stages.entity_merge_stage import (
    EntityMergeStage, EntityMergeTransform,
)
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.model import (
    TopicCollection, Topic, Entity,
)


class TestNERExtractionStage:
    """Tests for NERExtractionStage."""

    def test_is_extraction_stage(self):
        stage = NERExtractionStage()
        assert isinstance(stage, ExtractionStage)

    def test_input_keys_empty(self):
        stage = NERExtractionStage()
        assert stage.input_keys() == []

    def test_output_keys(self):
        stage = NERExtractionStage()
        assert stage.output_keys() == [PRE_EXTRACTED_ENTITIES_KEY]

    def test_stage_type(self):
        stage = NERExtractionStage()
        assert stage.stage_type == 'local'

    def test_as_transform_returns_ner_transform(self):
        stage = NERExtractionStage(model_name='test-model', entity_labels=['Person'], threshold=0.7)
        transform = stage.as_transform()
        assert isinstance(transform, NERTransform)
        assert transform.model_name == 'test-model'
        assert transform.entity_labels == ['Person']
        assert transform.threshold == 0.7


class TestEntityMergeStage:
    """Tests for EntityMergeStage."""

    def test_is_extraction_stage(self):
        stage = EntityMergeStage()
        assert isinstance(stage, ExtractionStage)

    def test_input_keys(self):
        stage = EntityMergeStage()
        assert PRE_EXTRACTED_ENTITIES_KEY in stage.input_keys()
        assert TOPICS_KEY in stage.input_keys()

    def test_output_keys(self):
        stage = EntityMergeStage()
        assert stage.output_keys() == [TOPICS_KEY]

    def test_stage_type(self):
        stage = EntityMergeStage()
        assert stage.stage_type == 'transform'


class TestEntityMergeTransform:
    """Tests for EntityMergeTransform."""

    def _make_node(self, ner_entities, topic_entities):
        tc = TopicCollection(topics=[
            Topic(value='test', entities=topic_entities, statements=[])
        ])
        node = TextNode(text='test')
        node.metadata[PRE_EXTRACTED_ENTITIES_KEY] = ner_entities
        node.metadata[TOPICS_KEY] = tc.model_dump()
        return node

    def test_merges_new_entities(self):
        """Verify NER entities not in topics are added."""
        ner = [{'value': 'Acme', 'classification': 'Company'}]
        existing = [Entity(value='John', classification='Person')]
        node = self._make_node(ner, existing)

        transform = EntityMergeTransform()
        result = transform([node])

        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        names = {e.value for e in tc.topics[0].entities}
        assert 'John' in names
        assert 'Acme' in names

    def test_skips_duplicate_entities(self):
        """Verify NER entities already in topics are not duplicated."""
        ner = [{'value': 'John', 'classification': 'Person'}]
        existing = [Entity(value='John', classification='Person')]
        node = self._make_node(ner, existing)

        transform = EntityMergeTransform()
        result = transform([node])

        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc.topics[0].entities) == 1

    def test_case_insensitive_dedup(self):
        """Verify deduplication is case-insensitive."""
        ner = [{'value': 'john', 'classification': 'Person'}]
        existing = [Entity(value='John', classification='Person')]
        node = self._make_node(ner, existing)

        transform = EntityMergeTransform()
        result = transform([node])

        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc.topics[0].entities) == 1

    def test_no_ner_entities_unchanged(self):
        """Verify node without NER entities is unchanged."""
        existing = [Entity(value='John', classification='Person')]
        tc = TopicCollection(topics=[Topic(value='test', entities=existing, statements=[])])
        node = TextNode(text='test')
        node.metadata[TOPICS_KEY] = tc.model_dump()

        transform = EntityMergeTransform()
        result = transform([node])

        tc2 = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc2.topics[0].entities) == 1

    def test_no_topics_unchanged(self):
        """Verify node without topics is unchanged."""
        node = TextNode(text='test')
        node.metadata[PRE_EXTRACTED_ENTITIES_KEY] = [{'value': 'X', 'classification': 'Y'}]

        transform = EntityMergeTransform()
        result = transform([node])
        assert TOPICS_KEY not in result[0].metadata

    def test_fuzzy_dedup_blocks_similar(self):
        """Verify fuzzy matching blocks similar entity names."""
        ner = [{'value': 'DataBridge', 'classification': 'Company'}]
        existing = [Entity(value='DataBridge AI', classification='Company')]
        node = self._make_node(ner, existing)

        transform = EntityMergeTransform(fuzzy_threshold=0.7)
        result = transform([node])

        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc.topics[0].entities) == 1

    def test_fuzzy_dedup_allows_different(self):
        """Verify fuzzy matching allows sufficiently different names."""
        ner = [{'value': 'Acme Corp', 'classification': 'Company'}]
        existing = [Entity(value='DataBridge AI', classification='Company')]
        node = self._make_node(ner, existing)

        transform = EntityMergeTransform(fuzzy_threshold=0.7)
        result = transform([node])

        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc.topics[0].entities) == 2

    def test_no_fuzzy_threshold_exact_match(self):
        """Verify None threshold uses exact matching (similar names not deduped)."""
        ner = [{'value': 'DataBridge', 'classification': 'Company'}]
        existing = [Entity(value='DataBridge AI', classification='Company')]
        node = self._make_node(ner, existing)

        transform = EntityMergeTransform(fuzzy_threshold=None)
        result = transform([node])

        tc = TopicCollection(**result[0].metadata[TOPICS_KEY])
        assert len(tc.topics[0].entities) == 2
