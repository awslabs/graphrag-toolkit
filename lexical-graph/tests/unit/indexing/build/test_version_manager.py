# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for `VersionManager`.

Rewritten against the real API. `VersionManager` versions *source documents* on a
`valid_from`/`valid_to` timeline; it has nothing to do with semantic versioning of
the library. The earlier version of this file tested `get_version`,
`increment_major`, `compare` and `is_compatible` - none of which exist - by
assigning each a `Mock` and then asserting the mock's own return value, so every
test passed on a class it never called. It also constructed `VersionManager()` and
`VersionManager(version=...)`, while the real class is a pydantic `NodeHandler`
with required `graph_store` and `vector_store` fields, so setup raised anyway.
Nothing noticed, because pytest's default `norecursedirs` contains `build`, so
this whole directory was skipped unless its path was named explicitly.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from graphrag_toolkit.lexical_graph.errors import IndexError as GraphRAGIndexError
from graphrag_toolkit.lexical_graph.indexing.build.version_manager import VersionManager
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
from graphrag_toolkit.lexical_graph.storage.vector import DummyVectorIndex, VectorStore
from graphrag_toolkit.lexical_graph.versioning import (
    TIMESTAMP_UPPER_BOUND,
    VALID_TO,
    VERSION_INDEPENDENT_ID_FIELDS,
)

from llama_index.core.schema import TextNode

def vector_index(index_name:str) -> MagicMock:
    index = MagicMock()
    index.index_name = index_name
    index.underlying_index_name.return_value = f'underlying_{index_name}'
    # An empty list means "nothing failed"; the default MagicMock return is truthy
    # and would send `_update_vector_store_versions` into its retry sleeps.
    index.update_versioning.return_value = []
    return index

@pytest.fixture
def mock_vector_store():
    '''
    A vector store reporting a single chunk index. `VectorStore` is a pydantic
    model, so its `__init__` is patched to allow a mock into the field.
    '''
    with patch.object(VectorStore, '__init__', return_value=None):
        store = MagicMock(spec=VectorStore)
        store.all_indexes.return_value = [vector_index('chunk')]
        return store

@pytest.fixture
def version_manager(mock_neptune_store, mock_vector_store):
    return VersionManager(
        graph_store=mock_neptune_store,
        vector_store=mock_vector_store,
        show_progress=False,
    )

def source_node(source_id:str='source-1', valid_from:int=200, id_fields=None, versioning=None) -> TextNode:
    node = TextNode(text='', id_=source_id)
    node.metadata = {
        'source': {
            'sourceId': source_id,
            'metadata': {'url': 'https://example.com/doc'},
            'versioning': versioning if versioning is not None else {'valid_from': valid_from, 'id_fields': id_fields},
        },
        INDEX_KEY: {'index': 'source'},
    }
    return node

class TestVersionManagerInitialization:
    """Tests for VersionManager initialization."""

    def test_initialization(self, mock_neptune_store, mock_vector_store):
        """Verify the manager holds the two stores it was given."""
        manager = VersionManager(graph_store=mock_neptune_store, vector_store=mock_vector_store)

        assert manager.graph_store is mock_neptune_store
        assert manager.vector_store is mock_vector_store

    def test_both_stores_are_required(self, mock_neptune_store):
        """Verify neither store defaults.

        Versioning has to read the graph and write the vector indexes, so a
        manager missing either would fail partway through a build rather than at
        construction.
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            VersionManager(graph_store=mock_neptune_store)

    def test_for_graph_and_vector_store_adopts_existing_stores(self, mock_neptune_store, mock_vector_store):
        """Verify a caller who already has stores does not go through the factories."""
        manager = VersionManager.for_graph_and_vector_store(mock_neptune_store, mock_vector_store)

        assert manager.graph_store is mock_neptune_store
        assert manager.vector_store is mock_vector_store

class TestGetUpdates:
    """Tests for the timeline arithmetic in `_get_updates`."""

    def test_first_version_of_a_source_is_open_ended(self, version_manager):
        """Verify a source with no history is valid until the upper bound."""
        (source_node_result, adjustments) = version_manager._get_updates(
            {'source_id': 's1', 'valid_from': 100, 'valid_to': None}, []
        )

        assert source_node_result['valid_to'] == TIMESTAMP_UPPER_BOUND
        assert adjustments == []

    def test_newer_version_archives_the_previous_latest(self, version_manager):
        """Verify the incoming version closes off the one it supersedes."""
        (new_node, adjustments) = version_manager._get_updates(
            {'source_id': 's2', 'valid_from': 200, 'valid_to': None},
            [{'source_id': 's1', 'valid_from': 100, 'valid_to': TIMESTAMP_UPPER_BOUND}],
        )

        assert new_node['valid_to'] == TIMESTAMP_UPPER_BOUND
        assert adjustments == [{'source_id': 's1', 'valid_from': 100, 'valid_to': 200}]

    def test_earliest_version_is_closed_by_the_existing_earliest(self, version_manager):
        """Verify back-dated content is inserted below the timeline, not on top of it.

        A document arriving out of order must not claim to be current, and must
        not disturb the version that already is.
        """
        (new_node, adjustments) = version_manager._get_updates(
            {'source_id': 's0', 'valid_from': 50, 'valid_to': None},
            [{'source_id': 's1', 'valid_from': 100, 'valid_to': TIMESTAMP_UPPER_BOUND}],
        )

        assert new_node['valid_to'] == 100
        assert adjustments == []

    def test_same_valid_from_reuses_the_existing_valid_to(self, version_manager):
        """Verify a re-ingest of the same version occupies the same interval."""
        (new_node, adjustments) = version_manager._get_updates(
            {'source_id': 's1-again', 'valid_from': 100, 'valid_to': None},
            [{'source_id': 's1', 'valid_from': 100, 'valid_to': 300}],
        )

        assert new_node['valid_to'] == 300
        assert adjustments == []

    def test_version_inserted_between_two_existing_versions(self, version_manager):
        """Verify a mid-timeline insert takes the interval up to the next version."""
        (new_node, adjustments) = version_manager._get_updates(
            {'source_id': 's-mid', 'valid_from': 150, 'valid_to': None},
            [
                {'source_id': 's1', 'valid_from': 100, 'valid_to': 200},
                {'source_id': 's2', 'valid_from': 200, 'valid_to': TIMESTAMP_UPPER_BOUND},
            ],
        )

        assert new_node['valid_to'] == 200
        assert adjustments == [{'source_id': 's1', 'valid_from': 100, 'valid_to': 150}]

    def test_existing_nodes_are_sorted_before_use(self, version_manager):
        """Verify the result does not depend on the order the query returned rows in."""
        existing = [
            {'source_id': 's2', 'valid_from': 200, 'valid_to': TIMESTAMP_UPPER_BOUND},
            {'source_id': 's1', 'valid_from': 100, 'valid_to': 200},
        ]

        (ascending, _) = version_manager._get_updates(
            {'source_id': 's3', 'valid_from': 300, 'valid_to': None}, list(reversed(existing))
        )
        (descending, _) = version_manager._get_updates(
            {'source_id': 's3', 'valid_from': 300, 'valid_to': None}, existing
        )

        assert ascending == descending

class TestGetExistingSourceNodes:
    """Tests for the lookup of source nodes already in the graph."""

    def test_no_id_fields_queries_nothing(self, version_manager):
        """Verify versioning is opt-in via `id_fields`."""
        assert version_manager._get_existing_source_nodes(None, source_node()) == []
        assert not version_manager.graph_store.execute_query.called

    def test_id_field_absent_from_the_node_queries_nothing(self, version_manager):
        """Verify a declared id field with no value does not match everything.

        Without the value there are no other filter criteria, and querying on the
        id-fields marker alone would return every versioned source in the graph.
        """
        node = source_node(id_fields=['url'])
        node.metadata['source']['metadata'] = {}

        assert version_manager._get_existing_source_nodes(['url'], node) == []
        assert not version_manager.graph_store.execute_query.called

    def test_query_filters_on_the_id_field_and_its_marker(self, version_manager):
        """Verify both the field value and the id-fields marker are in the query."""
        version_manager.graph_store.execute_query = Mock(return_value=[
            {'result': {'source_id': 's1', 'valid_from': 100, 'valid_to': TIMESTAMP_UPPER_BOUND}}
        ])

        results = version_manager._get_existing_source_nodes(['url'], source_node(id_fields=['url']))

        (cypher, parameters) = version_manager.graph_store.execute_query.call_args.args
        assert '__Source__' in cypher
        assert VERSION_INDEPENDENT_ID_FIELDS in cypher
        assert parameters == {'versionIndependentIdFields': 'url'}
        assert results == [{'source_id': 's1', 'valid_from': 100, 'valid_to': TIMESTAMP_UPPER_BOUND}]

    def test_multiple_id_fields_are_joined_into_one_marker(self, version_manager):
        """Verify the marker is the semicolon-joined field list."""
        version_manager.graph_store.execute_query = Mock(return_value=[])

        version_manager._get_existing_source_nodes(
            ['url', 'title'],
            source_node(),
        )

        (_, parameters) = version_manager.graph_store.execute_query.call_args.args
        assert parameters['versionIndependentIdFields'] == 'url;title'

class TestGetNodeIds:
    """Tests for resolving the graph nodes belonging to a source."""

    @pytest.mark.parametrize(
        ('index_name', 'id_field'),
        [('chunk', 'chunkId'), ('topic', 'topicId'), ('statement', 'statementId')],
    )
    def test_each_index_has_its_own_traversal(self, version_manager, index_name, id_field):
        """Verify the query collects the ids of the index it was asked about."""
        version_manager.graph_store.execute_query = Mock(return_value=[
            {'result': {'sourceId': 's1', 'nodeIds': ['n1', 'n2']}}
        ])

        result = version_manager._get_node_ids(vector_index(index_name), ['s1'])

        (cypher, parameters) = version_manager.graph_store.execute_query.call_args.args
        assert id_field in cypher
        assert parameters == {'sourceIds': ['s1']}
        assert result == {'s1': ['n1', 'n2']}

    def test_unknown_index_name_raises(self, version_manager):
        """Verify an index with no traversal is an error, not a silent no-op."""
        with pytest.raises(ValueError, match='Invalid index name: fact'):
            version_manager._get_node_ids(vector_index('fact'), ['s1'])

    def test_dummy_index_returns_empty(self, version_manager):
        """Verify a store without a real index is skipped without querying.

        Returns an empty *list* where every other path returns a dict; callers
        only iterate `.items()` on a non-empty result, so it happens to work.
        Pinned so a change to either side is deliberate.
        """
        assert version_manager._get_node_ids(DummyVectorIndex(index_name='chunk'), ['s1']) == []
        assert not version_manager.graph_store.execute_query.called

class TestSetSourceNodeVersionInfo:
    """Tests for closing off a source node in the graph."""

    def test_valid_to_and_id_fields_are_bound_not_interpolated(self, version_manager):
        """Verify the values travel as parameters."""
        version_manager._set_source_node_version_info('s1', 500, ['url', 'title'])

        (cypher, properties) = version_manager.graph_store.execute_query_with_retry.call_args.args
        assert VALID_TO in cypher
        assert properties == {
            'sourceId': 's1',
            'versioningTimestamp': 500,
            'versionIndependentIdFields': 'url;title',
        }

class TestUpdateVectorStoreVersions:
    """Tests for propagating version info into a vector index."""

    def test_node_ids_are_written_in_batches_of_100(self, version_manager):
        """Verify a large id list is chunked rather than sent whole."""
        index = vector_index('chunk')

        version_manager._update_vector_store_versions('s1', [f'n{i}' for i in range(250)], 500, index)

        batches = [call.args[1] for call in index.update_versioning.call_args_list]
        assert [len(batch) for batch in batches] == [100, 100, 50]
        assert all(call.args[0] == 500 for call in index.update_versioning.call_args_list)

    def test_transient_failure_is_retried(self, version_manager):
        """Verify a batch reporting failed ids is attempted again."""
        index = vector_index('chunk')
        index.update_versioning.side_effect = [['n1'], []]

        with patch('graphrag_toolkit.lexical_graph.indexing.build.version_manager.time.sleep'):
            version_manager._update_vector_store_versions('s1', ['n1', 'n2'], 500, index)

        assert index.update_versioning.call_count == 2

    def test_persistent_failure_raises(self, version_manager):
        """Verify ids that never succeed are surfaced, not dropped.

        A silently unversioned chunk would keep being returned by current-version
        retrieval after its source had been superseded.
        """
        index = vector_index('chunk')
        index.update_versioning.return_value = ['n1']

        with patch('graphrag_toolkit.lexical_graph.indexing.build.version_manager.time.sleep'):
            with pytest.raises(GraphRAGIndexError, match='Failed to update valid_to version info'):
                version_manager._update_vector_store_versions('s1', ['n1'], 500, index)

        assert index.update_versioning.call_count == 5

class TestAccept:
    """Tests for the accept() entry point."""

    def test_source_node_gets_its_version_window_written_into_metadata(self, version_manager):
        """Verify downstream builders see the resolved window, not the raw input."""
        node = source_node(valid_from=200)

        results = list(version_manager.accept([node]))

        assert results == [node]
        versioning = node.metadata['source']['versioning']
        assert versioning['valid_from'] == 200
        assert versioning['valid_to'] == TIMESTAMP_UPPER_BOUND
        assert versioning['prev_versions'] == []

    def test_superseded_source_is_closed_in_both_stores(self, version_manager):
        """Verify an archived version is updated in the graph and the vector index."""
        version_manager.graph_store.execute_query = Mock(side_effect=[
            [{'result': {'source_id': 's-old', 'valid_from': 100, 'valid_to': TIMESTAMP_UPPER_BOUND}}],
            [{'result': {'sourceId': 's-old', 'nodeIds': ['c1', 'c2']}}],
        ])

        node = source_node(source_id='s-new', valid_from=200, id_fields=['url'])

        list(version_manager.accept([node]))

        assert node.metadata['source']['versioning']['prev_versions'] == ['s-old']

        (index,) = version_manager.vector_store.all_indexes.return_value
        assert index.update_versioning.call_args.args == (200, ['c1', 'c2'])

        (_, properties) = version_manager.graph_store.execute_query_with_retry.call_args.args
        assert properties == {
            'sourceId': 's-old',
            'versioningTimestamp': 200,
            'versionIndependentIdFields': 'url',
        }

    def test_non_source_node_inherits_its_sources_window(self, version_manager):
        """Verify a chunk is stamped with the window resolved for its source."""
        source = source_node(source_id='s1', valid_from=200)

        chunk = TextNode(text='some text', id_='c1')
        chunk.metadata = {
            'source': {'sourceId': 's1', 'versioning': {}},
            INDEX_KEY: {'index': 'chunk'},
        }

        list(version_manager.accept([source, chunk]))

        assert chunk.metadata['source']['versioning'] == {
            'valid_from': 200,
            'valid_to': TIMESTAMP_UPPER_BOUND,
        }

    def test_non_source_node_seen_before_its_source_is_left_alone(self, version_manager):
        """Verify an out-of-order chunk is yielded rather than stamped or dropped.

        Source nodes precede their chunks in the pipeline, so this should not
        happen; if it does, the chunk keeps whatever window it arrived with.
        """
        chunk = TextNode(text='some text', id_='c1')
        chunk.metadata = {
            'source': {'sourceId': 'unseen', 'versioning': {}},
            INDEX_KEY: {'index': 'chunk'},
        }

        results = list(version_manager.accept([chunk]))

        assert results == [chunk]
        assert chunk.metadata['source']['versioning'] == {}

    def test_node_without_index_metadata_is_passed_through(self, version_manager):
        """Verify unrelated nodes are neither versioned nor lost."""
        node = TextNode(text='some text', id_='n1')
        node.metadata = {}

        assert list(version_manager.accept([node])) == [node]
        assert not version_manager.graph_store.execute_query_with_retry.called

    def test_source_without_valid_from_falls_back_to_the_extract_timestamp_key(self, version_manager):
        """Verify the documented fallback is a literal string, not a timestamp.

        `versioning.get('valid_from', 'extract_timestamp')` yields the *name* of
        the field rather than a value, so a source built without a `valid_from`
        ends up with a string where every comparison expects an int. Pinned as
        current behaviour, not endorsed.
        """
        node = source_node(versioning={})

        list(version_manager.accept([node]))

        assert node.metadata['source']['versioning']['valid_from'] == 'extract_timestamp'
