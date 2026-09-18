# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Two distinct documents whose source ids collide are indistinguishable downstream.

The pair below was found by hashing sequentially numbered documents until two
shared the first eight characters of their md5 digest, which took 30,059 of them.
That is the problem in one number: the default width discriminates on 32 bits, so
a corpus reaches even odds of a collision far below the scale anyone designs for.

`IdRewriter` passes `''` for the metadata component when a node carries no
metadata, which makes that component constant and leaves only the text digest to
separate two documents. That is the case measured in the collision spike and the
case these tests use.
"""

import pytest

from unittest.mock import Mock

from graphrag_toolkit.lexical_graph.config import SourceIdWidth
from graphrag_toolkit.lexical_graph.indexing.build.source_graph_builder import SourceGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore

# md5(TEXT_A) and md5(TEXT_B) agree on their first eight hex characters, a4439cdb.
TEXT_A = 'document 27347 body text'
TEXT_B = 'document 30059 body text'

COLLIDING_SOURCE_ID = 'aws::a4439cdb:d41d'

NO_METADATA = ''

# Pinned rather than read from config, so the facts below hold whatever a run is
# configured to use. LEGACY is what every existing graph was written with; FULL is
# the whole digest.
LEGACY_WIDTH = SourceIdWidth.LEGACY
WIDENED = SourceIdWidth.FULL


def source_id_at(text, width, metadata_str=NO_METADATA):
    """The id this text would get at an explicit width."""
    return IdGenerator(source_id_width=width).create_source_id(text, metadata_str)


def source_id_as_configured(text, metadata_str=NO_METADATA):
    """The id this text gets at whatever width the run is configured to use."""
    return IdGenerator().create_source_id(text, metadata_str)


class TestCollidingPair:
    """
    Preconditions. These are arithmetic about md5, not statements about any
    configuration.
    """

    def test_they_share_one_source_id_at_the_legacy_width(self):
        assert (source_id_at(TEXT_A, LEGACY_WIDTH)
                == source_id_at(TEXT_B, LEGACY_WIDTH)
                == COLLIDING_SOURCE_ID)

    def test_a_wider_text_digest_separates_them(self):
        assert source_id_at(TEXT_A, WIDENED) != source_id_at(TEXT_B, WIDENED)

    def test_metadata_separates_them_only_when_it_differs(self):
        # The second component is a digest of the metadata, so it discriminates
        # only across documents whose metadata is not identical. A corpus loaded
        # without metadata, or with the same metadata throughout, gets no help
        # from it however wide it is.
        shared = 'file_path:corpus.txt'
        assert (source_id_at(TEXT_A, LEGACY_WIDTH, shared)
                == source_id_at(TEXT_B, LEGACY_WIDTH, shared))
        assert (source_id_at(TEXT_A, LEGACY_WIDTH, 'file_path:a.txt')
                != source_id_at(TEXT_B, LEGACY_WIDTH, 'file_path:b.txt'))


class TestCollisionConsequences:
    """
    What the shared id costs at the storage layer. These pass today: they
    characterise the damage rather than assert the fix.
    """

    def test_one_prefix_reads_back_as_one_document_holding_both(
        self, download_source_prefix, chunk_node
    ):
        # The S3 prefix is the bare source id, so a shared id is a shared prefix
        # and each document's object lands beside the other's.
        doc = download_source_prefix({
            f'{COLLIDING_SOURCE_ID}-aaaaa.jsonl': [chunk_node('a1', COLLIDING_SOURCE_ID)],
            f'{COLLIDING_SOURCE_ID}-bbbbb.jsonl': [chunk_node('b1', COLLIDING_SOURCE_ID)],
        })

        # Two documents went in; one comes out, carrying a chunk from each.
        assert {n.node_id for n in doc.nodes} == {'a1', 'b1'}
        assert doc.source_id() == COLLIDING_SOURCE_ID

    def test_nothing_reports_the_collision(self, download_source_prefix, chunk_node):
        # No error, no warning, no marker. A reader cannot tell this document
        # from one that genuinely had two chunks, which is what makes the
        # failure silent rather than something a run surfaces.
        doc = download_source_prefix({
            f'{COLLIDING_SOURCE_ID}-aaaaa.jsonl': [chunk_node('a1', COLLIDING_SOURCE_ID)],
            f'{COLLIDING_SOURCE_ID}-bbbbb.jsonl': [chunk_node('b1', COLLIDING_SOURCE_ID)],
        })

        assert len(doc.nodes) == 2


class TestCollisionReachesTheGraph:
    """
    The graph half of the defect. `SourceGraphBuilder` MERGEs on the source id,
    so two documents carrying one id bind one merge key.

    Against a mock, so these observe what the builder sends, not what a store
    does with it.
    """

    @staticmethod
    def _graph_client():
        client = Mock(spec=GraphStore)
        client.node_id = Mock(side_effect=lambda field: field)
        client.property_assigment_fn = Mock(side_effect=lambda key, value: (lambda x: x))
        client.execute_query_with_retry = Mock()
        return client

    @staticmethod
    def _source_node(text):
        """A source node carrying the id `text` gets in a graph written at the legacy width."""
        node = Mock()
        node.metadata = {
            'source': {
                'sourceId': source_id_at(text, LEGACY_WIDTH),
                'metadata': {'file_path': f'{text}.txt'},
            }
        }
        return node

    def _merge_calls(self, *texts):
        client = self._graph_client()
        for text in texts:
            SourceGraphBuilder().build(self._source_node(text), client)
        return client.execute_query_with_retry.call_args_list

    def test_both_documents_merge_on_one_source_id(self):
        # The builder binds the id it is given, unchanged, so two documents that
        # collide bind one key. Graphs written before the default widened still
        # carry the legacy width, so they still merge these two.
        calls = self._merge_calls(TEXT_A, TEXT_B)

        assert len(calls) == 2
        bound = [call[0][1]['params'][0]['sourceId'] for call in calls]
        assert bound[0] == bound[1]

    def test_the_merge_key_is_the_source_id(self):
        query = self._merge_calls(TEXT_A)[0][0][0]

        assert 'MERGE (source:`__Source__`{sourceId: params.sourceId})' in query

    def test_their_differing_metadata_lands_on_the_one_node(self):
        # Both builds set metadata under one key, and the query overwrites on
        # match. Which document's metadata survives is the store's to decide.
        calls = self._merge_calls(TEXT_A, TEXT_B)

        paths = [call[0][1]['params'][0]['file_path'] for call in calls]
        assert paths[0] != paths[1]
        assert 'ON MATCH SET' in calls[1][0][0]


class TestSourceIdUniqueness:
    """
    Two distinct documents must be distinguishable by id alone, because
    every downstream identity derives from it: the S3 prefix above, the
    `__Source__` node the graph MERGEs on, and every chunk, topic, statement
    and fact id.

    These read the configured width rather than a pinned one, so they assert
    that the default is wide enough rather than anything about a given width.
    """

    def test_distinct_documents_get_distinct_source_ids(self):
        assert source_id_as_configured(TEXT_A) != source_id_as_configured(TEXT_B)

    def test_distinct_documents_get_distinct_chunk_id_prefixes(self):
        generator = IdGenerator()

        chunk_a = generator.create_chunk_id(source_id_as_configured(TEXT_A), TEXT_A, NO_METADATA)
        chunk_b = generator.create_chunk_id(source_id_as_configured(TEXT_B), TEXT_B, NO_METADATA)

        assert chunk_a.rsplit(':', 1)[0] != chunk_b.rsplit(':', 1)[0]


# ---------------------------------------------------------------------------
# Detection. A collision is two different documents behind one source id. The
# document's identity is the hash llama_index computes over its text and
# metadata, which every chunk carries on its SOURCE relationship, so the guard
# needs no new field on any node.
# ---------------------------------------------------------------------------

from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import Document, NodeRelationship, RelatedNodeInfo, TextNode

from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.indexing.extract.id_rewriter import IdRewriter
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.indexing.source_id_collision import (
    DOCUMENT_HASH_PROPERTY,
    SourceIdCollisionError,
    SourceIdCollisionGuard,
)


def document(text, file_path):
    return Document(text=text, metadata={'file_path': file_path})


def source_document(text, file_path, source_id=None, document_hash=None):
    """A document's chunk as the build sees it: id and hash on the SOURCE relationship."""
    doc = document(text, file_path)
    node = TextNode(text=text)
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(
        node_id=source_id or source_id_at(text, LEGACY_WIDTH, f'file_path:{file_path}'),
        metadata=dict(doc.metadata),
        hash=document_hash or doc.hash,
    )
    return SourceDocument(nodes=[node])


def graph_with(rows):
    """A graph store whose hash lookup returns the given rows."""
    store = Mock()
    store.node_id = Mock(side_effect=lambda name: name)
    store.execute_query = Mock(return_value=rows)
    return store


def guard(store=None, **kwargs):
    return SourceIdCollisionGuard(graph_store=store or graph_with([]), tenant_id=TenantId(), **kwargs)


class TestTheHashTravelsWithTheChunks:

    def test_the_rewriter_keeps_the_document_hash_on_every_chunk(self):
        doc = document('sentence one. ' * 200, 'a.txt')
        rewriter = IdRewriter(inner=SentenceSplitter(chunk_size=128, chunk_overlap=10),
                              id_generator=IdGenerator(source_id_width=LEGACY_WIDTH))

        chunks = rewriter.handle_source_docs([SourceDocument(nodes=[doc])])[0].nodes

        assert len(chunks) > 1
        assert {c.relationships[NodeRelationship.SOURCE].hash for c in chunks} == {doc.hash}

    def test_the_two_colliding_documents_have_different_hashes(self):
        assert document(TEXT_A, 'a.txt').hash != document(TEXT_B, 'b.txt').hash


class TestCollisionWithinOneRun:

    def test_two_documents_on_one_id_raise_naming_both(self):
        shared = 'file_path:corpus.txt'
        a = source_document(TEXT_A, 'corpus.txt', source_id=source_id_at(TEXT_A, LEGACY_WIDTH, shared))
        b = source_document(TEXT_B, 'corpus.txt', source_id=source_id_at(TEXT_B, LEGACY_WIDTH, shared))
        assert a.source_id() == b.source_id()

        with pytest.raises(SourceIdCollisionError) as raised:
            list(guard()([a, b]))

        message = str(raised.value)
        assert a.source_id() in message
        assert a.nodes[0].relationships[NodeRelationship.SOURCE].hash in message
        assert b.nodes[0].relationships[NodeRelationship.SOURCE].hash in message

    def test_the_same_document_twice_is_not_a_collision(self):
        a = source_document(TEXT_A, 'a.txt')

        out = list(guard()([a, source_document(TEXT_A, 'a.txt')]))

        assert len(out) == 2

    def test_one_document_whose_chunks_disagree_is_a_collision(self, download_source_prefix, chunk_node):
        # The storage read merges two colliding documents' objects into one
        # SourceDocument. Their chunks still carry their own document's hash.
        chunk_a = chunk_node('a1', COLLIDING_SOURCE_ID)
        chunk_a.relationships[NodeRelationship.SOURCE].hash = document(TEXT_A, 'a.txt').hash
        chunk_b = chunk_node('b1', COLLIDING_SOURCE_ID)
        chunk_b.relationships[NodeRelationship.SOURCE].hash = document(TEXT_B, 'b.txt').hash
        merged = download_source_prefix({
            f'{COLLIDING_SOURCE_ID}-aaaaa.jsonl': [chunk_a],
            f'{COLLIDING_SOURCE_ID}-bbbbb.jsonl': [chunk_b],
        })

        with pytest.raises(SourceIdCollisionError, match=COLLIDING_SOURCE_ID):
            list(guard()([merged]))

    def test_a_chunk_without_a_hash_is_not_checked(self, chunk_node):
        # Nodes that predate the hash, or were built by hand, carry none.
        doc = SourceDocument(nodes=[chunk_node('a1', COLLIDING_SOURCE_ID)])

        assert len(list(guard()([doc]))) == 1


class TestCollisionAgainstTheGraph:

    def test_a_different_document_already_in_the_graph_raises(self):
        a = source_document(TEXT_A, 'a.txt')
        stored = graph_with([{'sourceId': a.source_id(), DOCUMENT_HASH_PROPERTY: document(TEXT_B, 'b.txt').hash}])

        with pytest.raises(SourceIdCollisionError) as raised:
            list(guard(stored)([a]))

        assert 'already in the graph' in str(raised.value)
        assert a.source_id() in str(raised.value)

    def test_the_error_names_the_stored_document_when_the_graph_knows_it(self):
        a = source_document(TEXT_A, 'a.txt')
        stored = graph_with([{'sourceId': a.source_id(), DOCUMENT_HASH_PROPERTY: document(TEXT_B, 'b.txt').hash, 'filePath': 'b.txt'}])

        with pytest.raises(SourceIdCollisionError, match='b.txt already in the graph'):
            list(guard(stored)([a]))

    def test_the_same_document_already_in_the_graph_is_a_re_ingest(self):
        a = source_document(TEXT_A, 'a.txt')
        stored = graph_with([{'sourceId': a.source_id(), DOCUMENT_HASH_PROPERTY: a.nodes[0].relationships[NodeRelationship.SOURCE].hash}])

        assert len(list(guard(stored)([a]))) == 1

    def test_a_source_written_before_the_hash_existed_is_accepted(self):
        # Graphs written before this change carry no hash on the source node.
        a = source_document(TEXT_A, 'a.txt')
        stored = graph_with([{'sourceId': a.source_id(), DOCUMENT_HASH_PROPERTY: None}])

        assert len(list(guard(stored)([a]))) == 1

    def test_lookups_are_batched(self):
        store = graph_with([])
        docs = [source_document(f'document {i}', f'{i}.txt') for i in range(250)]

        list(guard(store, lookup_batch_size=100)(docs))

        assert store.execute_query.call_count == 3
        sizes = [len(call.args[1]['sourceIds']) for call in store.execute_query.call_args_list]
        assert sizes == [100, 100, 50]

    def test_the_lookup_binds_the_ids_rather_than_inlining_them(self):
        store = graph_with([])
        a = source_document(TEXT_A, 'a.txt')

        list(guard(store)([a]))

        query, params = store.execute_query.call_args.args
        assert a.source_id() not in query
        assert params['sourceIds'] == [a.source_id()]


class TestGuardShape:

    def test_a_sized_input_stays_sized(self):
        docs = [source_document(f'document {i}', f'{i}.txt') for i in range(3)]

        out = guard()(docs)

        assert len(out) == 3

    def test_an_unsized_input_stays_lazy(self):
        out = guard()(iter([source_document(TEXT_A, 'a.txt')]))

        assert not hasattr(out, '__len__')

    def test_bare_nodes_pass_through_unchanged(self):
        nodes = source_document(TEXT_A, 'a.txt').nodes

        assert guard()(nodes) == nodes


class TestLexicalGraphIndexWiring:
    """build() refuses a document that collides with one already in the graph."""

    def test_building_a_colliding_document_raises(self):
        from unittest.mock import patch
        from pipe import Pipe
        from graphrag_toolkit.lexical_graph.lexical_graph_index import LexicalGraphIndex

        a = source_document(TEXT_A, 'a.txt')
        store = Mock()
        store.node_id = Mock(side_effect=lambda name: name)

        def execute_query(cypher, parameters={}, **kwargs):
            if 'documentHash' in cypher:
                return [{'sourceId': a.source_id(), 'documentHash': document(TEXT_B, 'b.txt').hash}]
            if '__SYS_Config__' in cypher:
                return [{'width': 8}]
            return []
        store.execute_query = Mock(side_effect=execute_query)

        module = 'graphrag_toolkit.lexical_graph.lexical_graph_index'
        with (
            patch(f'{module}.GraphStoreFactory.for_graph_store', return_value=store),
            patch(f'{module}.MultiTenantGraphStore.wrap', return_value=store),
            patch(f'{module}.VectorStoreFactory.for_vector_store', return_value=Mock()),
            patch(f'{module}.MultiTenantVectorStore.wrap', return_value=Mock()),
            patch.object(LexicalGraphIndex, '_configure_extraction_pipeline', return_value=([], [])),
        ):
            index = LexicalGraphIndex(graph_store='dummy://', vector_store='dummy://')

        with patch(f'{module}.BuildPipeline.create', return_value=Pipe(list)), \
             patch(f'{module}.GraphConstruction.for_graph_store'), \
             patch(f'{module}.VectorIndexing.for_vector_store'):
            with pytest.raises(SourceIdCollisionError):
                index.build([a])
