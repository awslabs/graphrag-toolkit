# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/processors/update_chunk_metadata."""

import pytest
from graphrag_toolkit.core.types import QueryBundle

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.model import (
    Chunk,
    EntityContexts,
    SearchResult,
    SearchResultCollection,
    Source,
    Statement,
    Topic,
    Versioning,
)
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.processors.update_chunk_metadata import (
    UpdateChunkMetadata,
)


def _collection(chunks):
    versioning = Versioning(valid_from=0, valid_to=9999999999)
    source = Source(sourceId='doc1', metadata={}, versioning=versioning)
    topic = Topic(
        topic='T',
        topicId='t1',
        chunks=chunks,
        statements=[Statement(statement='s1', score=0.5)],
    )
    return SearchResultCollection(
        results=[SearchResult(source=source, topics=[topic])],
        entity_contexts=EntityContexts(contexts=[], keywords=[]),
    )


@pytest.fixture
def processor():
    return UpdateChunkMetadata(ProcessorArgs(debug_results=[]), FilterConfig())


class TestUpdateChunkMetadata:
    def test_moves_metadata_value_to_chunk_value(self, processor):
        chunk = Chunk(
            chunkId='c1',
            value=None,
            metadata={'value': 'hoisted text', 'other': 'keep'},
        )
        processed = processor._process_results(_collection([chunk]), QueryBundle('q'))
        out = processed.results[0].topics[0].chunks[0]
        assert out.value == 'hoisted text'
        assert 'value' not in out.metadata
        assert out.metadata.get('other') == 'keep'

    def test_strips_chunk_id_from_metadata(self, processor):
        chunk = Chunk(chunkId='c1', metadata={'chunkId': 'shadow', 'k': 'v'})
        processed = processor._process_results(_collection([chunk]), QueryBundle('q'))
        out = processed.results[0].topics[0].chunks[0]
        assert 'chunkId' not in out.metadata

    def test_missing_value_leaves_chunk_value_unchanged(self, processor):
        chunk = Chunk(chunkId='c1', value='already set', metadata={'other': 'x'})
        processed = processor._process_results(_collection([chunk]), QueryBundle('q'))
        out = processed.results[0].topics[0].chunks[0]
        assert out.value == 'already set'
