# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
A collection keeps the source id width it was first written with.

Every chunk, topic and statement id is built from the source id, so the same
document written at two widths becomes two unrelated sets of nodes. A graph
records its width on first write; a graph written before the record existed
has its width read back from a sample of stored source ids.
"""

import pytest

from unittest.mock import Mock, patch

from llama_index.core.schema import TextNode, NodeRelationship, RelatedNodeInfo

from graphrag_toolkit.lexical_graph import TenantId, GraphRAGConfig
from graphrag_toolkit.lexical_graph.config import SourceIdWidth
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.indexing.source_id_width import (
    SourceIdWidthMismatchError,
    SourceIdWidthGuard,
    graph_source_id_width,
    recorded_source_id_width,
    sampled_source_id_width,
    record_graph_source_id_width,
    resolve_source_id_width,
)

LEGACY, FULL = SourceIdWidth.LEGACY, SourceIdWidth.FULL

LEGACY_ID = 'aws::5eb63bbb:d41d'
FULL_ID = 'aws::5eb63bbbe01eeed093cb22bb8f5acdc3:d41d'


pytestmark = pytest.mark.usefixtures('isolated_source_id_width')


class TestResolveSourceIdWidth:

    def test_a_new_collection_takes_the_default(self):
        assert resolve_source_id_width([], configured=None, default=FULL) is FULL

    def test_a_new_collection_takes_an_explicit_setting(self):
        assert resolve_source_id_width([], configured=LEGACY, default=FULL) is LEGACY

    def test_an_existing_legacy_collection_stays_legacy(self):
        assert resolve_source_id_width([LEGACY], configured=None, default=FULL) is LEGACY

    def test_unknown_widths_are_ignored(self):
        assert resolve_source_id_width([None, LEGACY], configured=None, default=FULL) is LEGACY

    def test_mixed_widths_raise(self):
        with pytest.raises(SourceIdWidthMismatchError):
            resolve_source_id_width([LEGACY, FULL], configured=None, default=FULL)

    def test_a_setting_that_contradicts_the_collection_raises(self):
        # Silently using the stored width would give the caller ids they did
        # not ask for; silently using the setting would duplicate the collection.
        with pytest.raises(SourceIdWidthMismatchError):
            resolve_source_id_width([LEGACY], configured=FULL, default=FULL)

    def test_the_error_names_both_widths(self):
        with pytest.raises(SourceIdWidthMismatchError, match=r'LEGACY \(8\).*FULL \(32\)'):
            resolve_source_id_width([LEGACY, FULL], configured=None, default=FULL)


def graph_store(record=None, source_id=None, written=None, source_ids=None):
    """A graph store answering the three queries the width needs."""
    store = Mock()
    store.node_id = Mock(side_effect=lambda name: name)

    def execute_query(cypher, parameters={}, **kwargs):
        if 'MERGE' in cypher:
            store.recorded = parameters
            return [{'width': written if written is not None else parameters['width']}]
        if '__SYS_Config__' in cypher:
            return [{'width': record}] if record is not None else []
        if '__Source__' in cypher:
            ids = source_ids if source_ids is not None else ([source_id] if source_id else [])
            return [{'sourceId': i} for i in ids]
        raise AssertionError(f'unexpected query: {cypher}')

    store.execute_query = Mock(side_effect=execute_query)
    return store


class TestGraphSourceIdWidth:

    def test_an_empty_graph_has_no_width(self):
        assert graph_source_id_width(graph_store()) is None

    def test_the_record_is_authoritative(self):
        store = graph_store(record=8, source_id=FULL_ID)

        assert graph_source_id_width(store) is LEGACY
        assert store.execute_query.call_count == 1

    def test_a_record_a_store_returns_as_text_still_parses(self):
        assert graph_source_id_width(graph_store(record='32')) is FULL

    def test_a_graph_without_a_record_is_read_from_a_stored_source_id(self):
        assert graph_source_id_width(graph_store(source_id=LEGACY_ID)) is LEGACY

    def test_stored_ids_the_generator_did_not_write_are_skipped(self):
        store = graph_store(source_ids=['aws:custom-id', LEGACY_ID])

        assert graph_source_id_width(store) is LEGACY

    def test_a_graph_already_holding_two_widths_raises(self):
        store = graph_store(source_ids=[LEGACY_ID, FULL_ID])

        with pytest.raises(SourceIdWidthMismatchError, match='already holds'):
            graph_source_id_width(store)

    def test_a_graph_holding_only_foreign_ids_has_no_width(self):
        assert graph_source_id_width(graph_store(source_ids=['aws:custom-id'])) is None

    def test_recording_writes_the_digest_length(self):
        store = graph_store()

        record_graph_source_id_width(store, TenantId(), FULL)

        assert store.recorded['width'] == 32

    def test_the_record_is_scoped_to_the_tenant(self):
        store = graph_store()

        record_graph_source_id_width(store, TenantId('tenant1'), FULL)

        assert 'tenant1' in store.recorded['configId']

    def test_a_record_written_concurrently_at_another_width_raises(self):
        # MERGE sets the width only on create, so a losing writer reads back
        # the winner's width rather than overwriting it.
        with pytest.raises(SourceIdWidthMismatchError):
            record_graph_source_id_width(graph_store(written=8), TenantId(), FULL)


def source_document(source_id):
    node = TextNode(text='chunk')
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
    return SourceDocument(nodes=[node])


def guard(store, configured=None):
    return SourceIdWidthGuard(graph_store=store, tenant_id=TenantId(), configured=configured)


class TestSourceIdWidthGuard:

    def _run(self, store, *source_ids, configured=None):
        return list(guard(store, configured)([source_document(s) for s in source_ids]))

    def test_documents_at_the_graph_width_pass_through(self):
        docs = self._run(graph_store(record=8), LEGACY_ID, LEGACY_ID)

        assert [d.source_id() for d in docs] == [LEGACY_ID, LEGACY_ID]

    def test_a_document_at_another_width_stops_the_build(self):
        # The case where an S3 collection extracted at one width is built into
        # a graph written at another.
        with pytest.raises(SourceIdWidthMismatchError):
            self._run(graph_store(record=8), FULL_ID)

    def test_the_first_document_into_an_empty_graph_records_its_width(self):
        store = graph_store()

        self._run(store, FULL_ID)

        assert store.recorded['width'] == 32

    def test_a_graph_that_already_has_a_record_is_not_rewritten(self):
        store = graph_store(record=32)

        self._run(store, FULL_ID)

        queries = [call[0][0] for call in store.execute_query.call_args_list]
        assert not any('MERGE' in q for q in queries)

    def test_the_error_names_the_document_and_both_widths(self):
        with pytest.raises(SourceIdWidthMismatchError) as raised:
            self._run(graph_store(record=8), FULL_ID)

        assert FULL_ID in str(raised.value)
        assert 'SOURCE_ID_WIDTH=LEGACY' in str(raised.value)

    def test_mixed_widths_in_one_build_stop_it(self):
        with pytest.raises(SourceIdWidthMismatchError):
            self._run(graph_store(), FULL_ID, LEGACY_ID)

    def test_documents_with_foreign_source_ids_pass_through(self):
        docs = self._run(graph_store(record=8), 'aws:custom-id', '0f8fad5b-d9cb-469f-a165-70867728950e')

        assert len(docs) == 2

    def test_text_nodes_are_checked_and_passed_through_unchanged(self):
        nodes = [source_document(LEGACY_ID).nodes[0], source_document(LEGACY_ID).nodes[0]]

        out = guard(graph_store(record=8))(nodes)

        assert out == nodes

    def test_a_text_node_at_another_width_stops_the_build(self):
        nodes = [source_document(FULL_ID).nodes[0]]

        with pytest.raises(SourceIdWidthMismatchError):
            guard(graph_store(record=8))(nodes)

    def test_a_sized_input_stays_sized(self):
        # BuildPipeline reads len(inputs) to report batch totals.
        out = guard(graph_store(record=8))(
            [source_document(LEGACY_ID)] * 3)

        assert len(out) == 3

    def test_an_unsized_input_stays_lazy(self):
        docs = iter([source_document(LEGACY_ID)])

        out = guard(graph_store(record=8))(docs)

        assert not hasattr(out, '__len__')


class TestConfiguredWidth:

    def test_the_default_is_the_full_digest(self):
        assert GraphRAGConfig.source_id_width is FULL

    def test_an_unset_width_is_not_an_explicit_setting(self):
        assert GraphRAGConfig.source_id_width_setting is None

    def test_the_environment_is_an_explicit_setting(self, monkeypatch):
        monkeypatch.setenv('SOURCE_ID_WIDTH', 'legacy')

        assert GraphRAGConfig.source_id_width_setting is LEGACY
        assert GraphRAGConfig.source_id_width is LEGACY

    def test_the_setter_is_an_explicit_setting(self):
        GraphRAGConfig.source_id_width = LEGACY

        assert GraphRAGConfig.source_id_width_setting is LEGACY


class TestLexicalGraphIndexWiring:
    """The index resolves the width before extraction and guards the build."""

    @staticmethod
    def _index(store):
        from graphrag_toolkit.lexical_graph.lexical_graph_index import LexicalGraphIndex

        module = 'graphrag_toolkit.lexical_graph.lexical_graph_index'
        with (
            patch(f'{module}.GraphStoreFactory.for_graph_store', return_value=store),
            patch(f'{module}.MultiTenantGraphStore.wrap', return_value=store),
            patch(f'{module}.VectorStoreFactory.for_vector_store', return_value=Mock()),
            patch(f'{module}.MultiTenantVectorStore.wrap', return_value=Mock()),
            patch.object(LexicalGraphIndex, '_configure_extraction_pipeline', return_value=([], [])),
        ):
            return LexicalGraphIndex(graph_store='dummy://', vector_store='dummy://')

    def test_extraction_uses_the_width_the_graph_was_written_at(self):
        index = self._index(graph_store(source_id=LEGACY_ID))
        module = 'graphrag_toolkit.lexical_graph.lexical_graph_index'

        with patch(f'{module}.ExtractionPipeline.create') as create, \
             patch(f'{module}.BuildPipeline.create'):
            index.extract([])

        assert create.call_args.kwargs['source_id_width'] is LEGACY

    def test_building_documents_at_another_width_raises(self):
        from pipe import Pipe
        index = self._index(graph_store(record=8))
        module = 'graphrag_toolkit.lexical_graph.lexical_graph_index'

        with patch(f'{module}.BuildPipeline.create', return_value=Pipe(list)), \
             patch(f'{module}.GraphConstruction.for_graph_store'), \
             patch(f'{module}.VectorIndexing.for_vector_store'):
            with pytest.raises(SourceIdWidthMismatchError):
                index.build([source_document(FULL_ID)])


class TestASampledWidthIsRecorded:
    """
    A collection written before the record existed is sampled once, then read
    from its record. Leaving it unrecorded makes the sampling window permanent
    for exactly the collections the window was a concession to.
    """

    def test_a_sampled_width_is_written_to_the_record(self):
        store = graph_store(source_id=LEGACY_ID)

        list(guard(store)([source_document(LEGACY_ID)]))

        assert store.recorded['width'] == LEGACY.value

    def test_the_record_is_written_once_for_a_batch(self):
        store = graph_store(source_id=LEGACY_ID)

        list(guard(store)([source_document(LEGACY_ID) for _ in range(3)]))

        merges = [c for c in store.execute_query.call_args_list if 'MERGE' in c.args[0]]
        assert len(merges) == 1

    def test_a_collection_that_already_has_a_record_is_not_rewritten(self):
        store = graph_store(record=8, source_id=LEGACY_ID)

        list(guard(store)([source_document(LEGACY_ID)]))

        assert not [c for c in store.execute_query.call_args_list if 'MERGE' in c.args[0]]


class TestTheGuardHonoursAnExplicitWidth:
    """
    A build takes documents it did not extract, so an empty collection would
    otherwise be stamped with whatever width they carry, and the next run would
    then fail against the setting the operator never changed.
    """

    def test_documents_contradicting_the_setting_stop_an_empty_collection(self):
        with pytest.raises(SourceIdWidthMismatchError) as e:
            list(guard(graph_store(), configured=FULL)([source_document(LEGACY_ID)]))

        assert 'SOURCE_ID_WIDTH' in str(e.value)

    def test_nothing_is_recorded_when_the_documents_are_refused(self):
        store = graph_store()

        with pytest.raises(SourceIdWidthMismatchError):
            list(guard(store, configured=FULL)([source_document(LEGACY_ID)]))

        assert not [c for c in store.execute_query.call_args_list if 'MERGE' in c.args[0]]

    def test_documents_matching_the_setting_are_recorded(self):
        store = graph_store()

        list(guard(store, configured=LEGACY)([source_document(LEGACY_ID)]))

        assert store.recorded['width'] == LEGACY.value

    def test_an_unset_setting_leaves_the_documents_to_decide(self):
        store = graph_store()

        list(guard(store, configured=None)([source_document(LEGACY_ID)]))

        assert store.recorded['width'] == LEGACY.value


class TestReadingTheRecord:

    def test_every_record_is_read_not_just_the_first(self):
        store = Mock()
        store.node_id = Mock(side_effect=lambda name: name)
        store.execute_query = Mock(return_value=[{'width': 8}, {'width': 32}])

        with pytest.raises(SourceIdWidthMismatchError):
            recorded_source_id_width(store)

    def test_records_that_agree_resolve(self):
        store = Mock()
        store.node_id = Mock(side_effect=lambda name: name)
        store.execute_query = Mock(return_value=[{'width': 8}, {'width': 8}])

        assert recorded_source_id_width(store) is LEGACY

    def test_a_null_source_id_in_the_sample_is_skipped(self):
        assert sampled_source_id_width(
            graph_store(source_ids=[None, LEGACY_ID])) is LEGACY

    def test_a_sample_of_only_null_ids_has_no_width(self):
        assert sampled_source_id_width(graph_store(source_ids=[None])) is None

    def test_a_record_column_that_comes_back_null_does_not_block_recording(self):
        record_graph_source_id_width(graph_store(written=None), TenantId(), FULL)
