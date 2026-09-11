# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for `VectorIndexing`.

Rewritten against the real API. The earlier version of this file called
`index_document_chunks`, `reindex_existing_chunks` and `delete_indexed_chunks`,
none of which exist - `VectorIndexing` is a `NodeHandler` whose entry point is
`accept` - and it depended on a `mock_opensearch_store` fixture that does not
exist, so every test errored at setup. Nothing noticed, because pytest's default
`norecursedirs` contains `build`, so this whole directory was skipped unless its
path was named explicitly.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from graphrag_toolkit.lexical_graph.indexing.build.vector_indexing import VectorIndexing
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore

from llama_index.core.schema import TextNode

@pytest.fixture
def mock_vector_store():
    '''
    A vector store reporting one index per embedding index name. `VectorStore` is
    a pydantic model, so its `__init__` is patched to allow a mock in the field.
    '''
    with patch.object(VectorStore, '__init__', return_value=None):
        store = MagicMock(spec=VectorStore)

        indexes = []
        for index_name in ('chunk', 'statement'):
            index = MagicMock()
            index.index_name = index_name
            indexes.append(index)

        store.all_indexes.return_value = indexes
        return store

def indexed_node(index_name:str, node_id:str='n-1', text:str='some text', metadata=None) -> TextNode:
    node = TextNode(text=text, id_=node_id)
    node.metadata = dict(metadata or {})
    node.metadata[INDEX_KEY] = {'index': index_name}
    return node

def added_to(store:MagicMock, index_name:str) -> list:
    index = next(i for i in store.all_indexes.return_value if i.index_name == index_name)
    return [call.args[0] for call in index.add_embeddings.call_args_list]

class TestVectorIndexingInitialization:
    """Tests for VectorIndexing initialization."""

    def test_initialization(self, mock_vector_store):
        """Verify VectorIndexing holds the store it was given."""
        indexing = VectorIndexing(vector_store=mock_vector_store)

        assert indexing.vector_store is mock_vector_store

    def test_for_vector_store_adopts_an_existing_store(self, mock_vector_store):
        """Verify a caller who already has a store does not pay for the factory."""
        indexing = VectorIndexing.for_vector_store(mock_vector_store)

        assert indexing.vector_store is mock_vector_store

class TestVectorIndexingOperations:
    """Tests for the accept() indexing path."""

    def test_node_is_added_to_the_index_named_in_its_metadata(self, mock_vector_store):
        """Verify routing is by `INDEX_KEY`, not by node type."""
        indexing = VectorIndexing(vector_store=mock_vector_store)

        list(indexing.accept(
            [indexed_node('statement', node_id='s-1')],
            batch_writes_enabled=False,
            batch_write_size=10,
        ))

        assert len(added_to(mock_vector_store, 'statement')) == 1
        assert added_to(mock_vector_store, 'chunk') == []

    def test_node_without_index_metadata_is_yielded_but_not_indexed(self, mock_vector_store):
        """Verify unrelated nodes pass through untouched."""
        indexing = VectorIndexing(vector_store=mock_vector_store)

        node = TextNode(text='some text', id_='n-1')
        node.metadata = {}

        results = list(indexing.accept([node], batch_writes_enabled=False, batch_write_size=10))

        assert results == [node]
        assert added_to(mock_vector_store, 'chunk') == []

    def test_unknown_index_name_is_ignored(self, mock_vector_store):
        """Verify an index name outside the known set indexes nothing.

        Guarded rather than left to `get_index`, which would raise - a node
        labelled with something unexpected should not fail the whole build.
        """
        indexing = VectorIndexing(vector_store=mock_vector_store)

        results = list(indexing.accept(
            [indexed_node('something_else')],
            batch_writes_enabled=False,
            batch_write_size=10,
        ))

        assert len(results) == 1
        assert added_to(mock_vector_store, 'chunk') == []

    def test_batching_defers_writes_and_yields_after_apply(self, mock_vector_store):
        """Verify batched nodes are written and yielded once, at the end."""
        indexing = VectorIndexing(vector_store=mock_vector_store)

        nodes = [indexed_node('chunk', node_id=f'c-{i}') for i in range(3)]

        results = list(indexing.accept(nodes, batch_writes_enabled=True, batch_write_size=2))

        assert [node.node_id for node in results] == ['c-0', 'c-1', 'c-2']
        assert [len(batch) for batch in added_to(mock_vector_store, 'chunk')] == [2, 1]

    def test_indexing_error_is_logged_and_reraised(self, mock_vector_store):
        """Verify a failing index write is not swallowed."""
        indexing = VectorIndexing(vector_store=mock_vector_store)

        index = next(i for i in mock_vector_store.all_indexes.return_value if i.index_name == 'chunk')
        index.add_embeddings.side_effect = RuntimeError('index unavailable')

        with pytest.raises(RuntimeError, match='index unavailable'):
            list(indexing.accept(
                [indexed_node('chunk')],
                batch_writes_enabled=False,
                batch_write_size=10,
            ))

class TestIndexableTransformations:
    """Tests for the transformations applied before indexing."""

    def test_json_content_is_rewritten_as_yaml(self, mock_vector_store):
        """Verify JSON node content is indexed as YAML.

        Embedded text is what the retriever's similarity is computed over, so the
        substitution is behaviour, not formatting.
        """
        indexing = VectorIndexing(vector_store=mock_vector_store)

        node = indexed_node('chunk', text=json.dumps({'value': 'some statement'}))

        list(indexing.accept([node], batch_writes_enabled=False, batch_write_size=10))

        (indexed,) = added_to(mock_vector_store, 'chunk')[0]
        assert indexed.get_content() == 'value: some statement\n'
        # The original node is not mutated - only the copy that gets indexed.
        assert node.get_content() == json.dumps({'value': 'some statement'})

    def test_non_json_content_is_left_alone(self, mock_vector_store):
        """Verify plain text is indexed as written."""
        indexing = VectorIndexing(vector_store=mock_vector_store)

        list(indexing.accept(
            [indexed_node('chunk', text='just prose')],
            batch_writes_enabled=False,
            batch_write_size=10,
        ))

        (indexed,) = added_to(mock_vector_store, 'chunk')[0]
        assert indexed.get_content() == 'just prose'

    def test_datetime_source_metadata_is_normalized(self, mock_vector_store):
        """Verify a `_date`-suffixed source field is formatted before indexing."""
        indexing = VectorIndexing(vector_store=mock_vector_store)

        node = indexed_node('chunk', metadata={'source': {'metadata': {'publish_date': '2024-01-15'}}})

        list(indexing.accept([node], batch_writes_enabled=False, batch_write_size=10))

        (indexed,) = added_to(mock_vector_store, 'chunk')[0]
        assert indexed.metadata['source']['metadata']['publish_date'].startswith('2024-01-15')

    def test_unparseable_datetime_source_metadata_is_dropped(self, mock_vector_store):
        """Verify a `_date` field that cannot be parsed is removed, not indexed raw.

        Indexing the raw value would put an unparseable date into the embedded
        text and into any metadata filter built over it.
        """
        indexing = VectorIndexing(vector_store=mock_vector_store)

        node = indexed_node('chunk', metadata={'source': {'metadata': {'publish_date': 'not a date'}}})

        list(indexing.accept([node], batch_writes_enabled=False, batch_write_size=10))

        (indexed,) = added_to(mock_vector_store, 'chunk')[0]
        assert 'publish_date' not in indexed.metadata['source']['metadata']

    def test_non_datetime_source_metadata_is_untouched(self, mock_vector_store):
        """Verify only `_date`-suffixed keys are rewritten."""
        indexing = VectorIndexing(vector_store=mock_vector_store)

        node = indexed_node('chunk', metadata={'source': {'metadata': {'title': 'A title'}}})

        list(indexing.accept([node], batch_writes_enabled=False, batch_write_size=10))

        (indexed,) = added_to(mock_vector_store, 'chunk')[0]
        assert indexed.metadata['source']['metadata'] == {'title': 'A title'}
