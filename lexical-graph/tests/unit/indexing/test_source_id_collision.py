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

from typing import Dict, List

import pytest

from unittest.mock import Mock

from llama_index.core.bridge.pydantic import Field

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
# source id is a prefix of the source hash, so the hash is what separates them:
# it is built from the same text and metadata string, undivided by truncation.
# ---------------------------------------------------------------------------

from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import Document, NodeRelationship, RelatedNodeInfo, TextNode

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.build.graph_construction import GraphConstruction
from graphrag_toolkit.lexical_graph.indexing.constants import SOURCE_HASH_PROPERTY
from graphrag_toolkit.lexical_graph.indexing.extract.id_rewriter import IdRewriter
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.indexing.source_id_collision import (
    SourceIdClaims,
    SourceIdCollisionError,
)
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.versioning import VALID_FROM, add_versioning_info


def document(text, file_path):
    return Document(text=text, metadata={'file_path': file_path})


def rewrite(*documents, width=LEGACY_WIDTH):
    """The chunks a build sees, with the ids and hashes IdRewriter gives them."""
    rewriter = IdRewriter(inner=SentenceSplitter(chunk_size=128, chunk_overlap=10),
                          id_generator=IdGenerator(source_id_width=width))
    docs = rewriter.handle_source_docs([SourceDocument(nodes=[d]) for d in documents])
    return [sd.nodes for sd in docs]


def source_hash_of(chunks):
    hashes = {c.relationships[NodeRelationship.SOURCE].hash for c in chunks}
    assert len(hashes) == 1
    return hashes.pop()


def source_node(source_id, source_hash, file_path='a.txt'):
    """A source node as SourceNodeBuilder builds it."""
    node = TextNode(id_=source_id, text='')
    node.metadata = {
        'source': {
            'sourceId': source_id,
            'metadata': {'file_path': file_path},
            SOURCE_HASH_PROPERTY: source_hash,
        },
        INDEX_KEY: {'index': 'source', 'key': source_id},
    }
    return node


def graph_returning(rows):
    store = Mock()
    store.node_id = Mock(side_effect=lambda name: name)
    store.execute_query_with_retry = Mock(return_value=rows)
    return store


class RecordingGraphStore(DummyGraphStore):
    """A real GraphStore, so GraphConstruction accepts it and the query goes
    through the store's own retry wrapper."""
    rows:List[Dict] = Field(default_factory=list)
    queries:List = Field(default_factory=list)

    def _execute_query(self, cypher, parameters={}, correlation_id=None):
        self.queries.append((cypher, parameters))
        return list(self.rows)


class TestTheHashIsTheIdUntruncated:
    """
    The source id keeps a prefix of each digest; the hash keeps both whole. That
    is what makes the hash able to separate two documents the id cannot, and what
    makes it agree with the id on which documents are the same document.
    """

    def test_the_id_is_built_from_the_hash(self):
        generator = IdGenerator(source_id_width=LEGACY_WIDTH)
        text_digest, metadata_digest = generator.create_source_hash(TEXT_A, NO_METADATA).split(':')

        source_id = generator.create_source_id(TEXT_A, NO_METADATA)

        assert source_id == f'aws::{text_digest[:LEGACY_WIDTH]}:{metadata_digest[:4]}'

    def test_it_separates_two_documents_that_share_an_id(self):
        generator = IdGenerator(source_id_width=LEGACY_WIDTH)

        assert generator.create_source_id(TEXT_A, NO_METADATA) == generator.create_source_id(TEXT_B, NO_METADATA)
        assert generator.create_source_hash(TEXT_A, NO_METADATA) != generator.create_source_hash(TEXT_B, NO_METADATA)


class TestTheHashAgreesWithTheIdOnDocumentIdentity:
    """
    A document whose source id is unchanged must have an unchanged hash, or a
    re-ingest of that document reads as a collision. The id excludes the
    versioning keys and sorts the rest, so the hash has to do the same. The hash
    llama_index puts on a document does neither, which is why it is replaced.
    """

    def test_a_new_valid_from_changes_neither_the_id_nor_the_hash(self):
        # add_versioning_info(..., valid_from=...) is how an effective date is
        # corrected on a document that has not otherwise changed.
        plain = document(TEXT_A, 'a.txt')
        dated = document(TEXT_A, 'a.txt')
        add_versioning_info(dated.metadata, valid_from=20240101)
        assert VALID_FROM in dated.metadata

        [plain_chunks], [dated_chunks] = rewrite(plain), rewrite(dated)

        assert plain_chunks[0].relationships[NodeRelationship.SOURCE].node_id == \
               dated_chunks[0].relationships[NodeRelationship.SOURCE].node_id
        assert source_hash_of(plain_chunks) == source_hash_of(dated_chunks)

    def test_reordered_metadata_changes_neither_the_id_nor_the_hash(self):
        first = Document(text=TEXT_A, metadata={'file_path': 'a.txt', 'author': 'bob'})
        second = Document(text=TEXT_A, metadata={'author': 'bob', 'file_path': 'a.txt'})

        [first_chunks], [second_chunks] = rewrite(first), rewrite(second)

        assert first_chunks[0].relationships[NodeRelationship.SOURCE].node_id == \
               second_chunks[0].relationships[NodeRelationship.SOURCE].node_id
        assert source_hash_of(first_chunks) == source_hash_of(second_chunks)

    def test_the_hash_llama_index_computes_disagrees_in_both_cases(self):
        # Document.hash covers the whole metadata dict in insertion order, so it
        # is finer than the id. Both documents above would read as collisions.
        dated = document(TEXT_A, 'a.txt')
        add_versioning_info(dated.metadata, valid_from=20240101)
        assert document(TEXT_A, 'a.txt').hash != dated.hash

        first = Document(text=TEXT_A, metadata={'file_path': 'a.txt', 'author': 'bob'})
        second = Document(text=TEXT_A, metadata={'author': 'bob', 'file_path': 'a.txt'})
        assert first.hash != second.hash


class TestTheHashTravelsWithTheChunks:

    def test_every_chunk_carries_its_document_hash(self):
        doc = document('sentence one. ' * 200, 'a.txt')
        expected = IdGenerator(source_id_width=LEGACY_WIDTH).create_source_hash(
            doc.text, f'file_path:{doc.metadata["file_path"]}')

        [chunks] = rewrite(doc)

        assert len(chunks) > 1
        assert source_hash_of(chunks) == expected

    def test_two_colliding_documents_carry_different_hashes(self):
        [chunks_a], [chunks_b] = rewrite(document(TEXT_A, 'corpus.txt')), rewrite(document(TEXT_B, 'corpus.txt'))

        assert chunks_a[0].relationships[NodeRelationship.SOURCE].node_id == \
               chunks_b[0].relationships[NodeRelationship.SOURCE].node_id
        assert source_hash_of(chunks_a) != source_hash_of(chunks_b)


class TestTwoClaimsInOneBatch:
    """
    Two documents that collide can both be in one batch. Comparing only the last
    claim against the graph is not enough: whether the earlier one is reported
    then depends on which of the two the graph happens to hold.
    """

    def test_they_raise_naming_both_documents(self):
        claims = SourceIdClaims()
        claims.add(source_node(COLLIDING_SOURCE_ID, 'hash-a', 'a.txt'))

        with pytest.raises(SourceIdCollisionError) as raised:
            claims.add(source_node(COLLIDING_SOURCE_ID, 'hash-b', 'b.txt'))

        message = str(raised.value)
        assert COLLIDING_SOURCE_ID in message
        assert 'a.txt (hash hash-a)' in message
        assert 'b.txt (hash hash-b)' in message

    def test_the_owner_arriving_last_does_not_hide_the_other(self):
        # The graph already holds a.txt, and this batch offers b.txt and then
        # a.txt. Reading the hash back finds a.txt, which is what the last claim
        # says, so nothing about b.txt is left to notice.
        claims = SourceIdClaims()
        claims.add(source_node(COLLIDING_SOURCE_ID, 'hash-b', 'b.txt'))

        with pytest.raises(SourceIdCollisionError, match='b.txt'):
            claims.add(source_node(COLLIDING_SOURCE_ID, 'hash-a', 'a.txt'))

    def test_the_same_document_twice_is_not_a_collision(self):
        claims = SourceIdClaims()
        claims.add(source_node(COLLIDING_SOURCE_ID, 'hash-a', 'a.txt'))
        claims.add(source_node(COLLIDING_SOURCE_ID, 'hash-a', 'a.txt'))

        assert len(claims) == 1

    def test_two_documents_that_do_not_collide_both_claim(self):
        claims = SourceIdClaims()
        claims.add(source_node('aws::a4439cdb:d41d', 'hash-a', 'a.txt'))
        claims.add(source_node('aws::b5540dec:d41d', 'hash-b', 'b.txt'))

        assert len(claims) == 2


class TestClaimsAgainstTheGraph:
    """
    The check reads the hash back after the write. The write keeps the hash it
    finds, so whatever the graph holds afterwards is the owner's, whichever build
    got there first.
    """

    @staticmethod
    def _claims(*nodes):
        claims = SourceIdClaims()
        for node in nodes:
            claims.add(node)
        return claims

    def test_a_different_hash_in_the_graph_raises_naming_both_documents(self):
        node = source_node(COLLIDING_SOURCE_ID, 'hash-a', 'a.txt')
        store = graph_returning([
            {'sourceId': COLLIDING_SOURCE_ID, 'sourceHash': 'hash-b', 'name': 'b.txt'}
        ])

        with pytest.raises(SourceIdCollisionError) as raised:
            self._claims(node).verify(store)

        message = str(raised.value)
        assert COLLIDING_SOURCE_ID in message
        assert 'a.txt (hash hash-a)' in message
        assert 'b.txt already in the graph (hash hash-b)' in message

    def test_the_same_hash_is_a_re_ingest(self):
        node = source_node(COLLIDING_SOURCE_ID, 'hash-a')
        store = graph_returning([
            {'sourceId': COLLIDING_SOURCE_ID, 'sourceHash': 'hash-a', 'name': 'a.txt'}
        ])

        self._claims(node).verify(store)

    def test_a_source_written_before_the_hash_existed_is_accepted(self):
        node = source_node(COLLIDING_SOURCE_ID, 'hash-a')
        store = graph_returning([
            {'sourceId': COLLIDING_SOURCE_ID, 'sourceHash': None, 'name': 'old.txt'}
        ])

        self._claims(node).verify(store)

    def test_an_unnamed_stored_document_still_raises(self):
        # None of the name keys is present, so the id and the hash are all the
        # error can offer.
        node = source_node(COLLIDING_SOURCE_ID, 'hash-a')
        store = graph_returning([
            {'sourceId': COLLIDING_SOURCE_ID, 'sourceHash': 'hash-b', 'name': None}
        ])

        with pytest.raises(SourceIdCollisionError, match='a document already in the graph'):
            self._claims(node).verify(store)

    def test_a_source_node_without_a_hash_is_not_looked_up(self):
        node = source_node(COLLIDING_SOURCE_ID, 'hash-a')
        del node.metadata['source'][SOURCE_HASH_PROPERTY]
        store = graph_returning([])

        self._claims(node).verify(store)

        store.execute_query_with_retry.assert_not_called()

    def test_nothing_claimed_reads_nothing(self):
        store = graph_returning([])

        self._claims().verify(store)

        store.execute_query_with_retry.assert_not_called()

    def test_one_read_covers_every_id_in_the_batch(self):
        nodes = [source_node(f'aws::{i:08x}:d41d', f'hash-{i}') for i in range(250)]
        store = graph_returning([])

        self._claims(*nodes).verify(store)

        assert store.execute_query_with_retry.call_count == 1
        assert len(store.execute_query_with_retry.call_args.args[1]['sourceIds']) == 250

    def test_the_read_binds_the_ids_rather_than_inlining_them(self):
        store = graph_returning([])

        self._claims(source_node(COLLIDING_SOURCE_ID, 'hash-a')).verify(store)

        query, params = store.execute_query_with_retry.call_args.args
        assert COLLIDING_SOURCE_ID not in query
        assert params['sourceIds'] == [COLLIDING_SOURCE_ID]

    def test_the_read_retries(self):
        # A transient failure on this read would otherwise abort a build with a
        # GraphQueryError that looks unrelated to anything the build did.
        store = graph_returning([])

        self._claims(source_node(COLLIDING_SOURCE_ID, 'hash-a')).verify(store)

        store.execute_query.assert_not_called()
        store.execute_query_with_retry.assert_called_once()


class TestGraphConstructionWiring:
    """
    The check runs in graph construction, so it covers a directly wired
    BuildPipeline as well as LexicalGraphIndex.
    """

    @staticmethod
    def _construct(*nodes, store):
        construction = GraphConstruction(graph_client=store, builders=[SourceGraphBuilder()])
        return list(construction.accept(list(nodes), batch_writes_enabled=False, batch_write_size=10))

    def test_a_colliding_source_node_raises(self):
        store = RecordingGraphStore(rows=[
            {'sourceId': COLLIDING_SOURCE_ID, 'sourceHash': 'hash-b', 'name': 'b.txt'}
        ])

        with pytest.raises(SourceIdCollisionError):
            self._construct(source_node(COLLIDING_SOURCE_ID, 'hash-a'), store=store)

    def test_two_colliding_source_nodes_in_one_batch_raise_without_a_read(self):
        store = RecordingGraphStore(rows=[])

        with pytest.raises(SourceIdCollisionError, match='both in this build'):
            self._construct(
                source_node(COLLIDING_SOURCE_ID, 'hash-a', 'a.txt'),
                source_node(COLLIDING_SOURCE_ID, 'hash-b', 'b.txt'),
                store=store,
            )

        assert not [q for q, _ in store.queries if '$sourceIds' in q]

    def test_a_matching_source_node_passes_through(self):
        store = RecordingGraphStore(rows=[
            {'sourceId': COLLIDING_SOURCE_ID, 'sourceHash': 'hash-a', 'name': 'a.txt'}
        ])

        assert len(self._construct(source_node(COLLIDING_SOURCE_ID, 'hash-a'), store=store)) == 1
        assert [q for q, _ in store.queries if '$sourceIds' in q]

    def test_the_check_can_be_turned_off(self):
        store = RecordingGraphStore(rows=[
            {'sourceId': COLLIDING_SOURCE_ID, 'sourceHash': 'hash-b', 'name': 'b.txt'}
        ])
        GraphRAGConfig.detect_source_id_collisions = False
        try:
            assert len(self._construct(source_node(COLLIDING_SOURCE_ID, 'hash-a'), store=store)) == 1
            # The write still records the hash; only the check is off.
            assert not [q for q, _ in store.queries if '$sourceIds' in q]
        finally:
            GraphRAGConfig.detect_source_id_collisions = True


class TestTheHashReachesTheSourceNode:
    """
    The whole chain, in the shape the extraction pipeline uses it: a first pass
    over the documents, then a splitting pass, then the build.
    """

    def test_a_document_becomes_a_source_node_carrying_its_hash(self):
        from graphrag_toolkit.lexical_graph.indexing.build.build_filters import BuildFilters
        from graphrag_toolkit.lexical_graph.indexing.build.source_node_builder import SourceNodeBuilder
        from graphrag_toolkit.lexical_graph.indexing.extract.docs_to_nodes import DocsToNodes
        from graphrag_toolkit.lexical_graph.metadata import DefaultSourceMetadataFormatter

        generator = IdGenerator(source_id_width=LEGACY_WIDTH)
        doc = document(TEXT_A, 'a.txt')
        [sd] = IdRewriter(id_generator=generator).handle_source_docs([SourceDocument(nodes=[doc])])
        chunks = IdRewriter(inner=DocsToNodes(), id_generator=generator)._parse_nodes(sd.nodes)

        [built] = SourceNodeBuilder(
            id_generator=generator,
            build_filters=BuildFilters(),
            source_metadata_formatter=DefaultSourceMetadataFormatter(),
        ).build_nodes(chunks)

        source_hash = generator.create_source_hash(TEXT_A, 'file_path:a.txt')
        assert built.metadata['source'][SOURCE_HASH_PROPERTY] == source_hash
        # The id is that hash truncated, which is the whole reason it can collide.
        text_digest, metadata_digest = source_hash.split(':')
        assert built.metadata['source']['sourceId'] == f'aws::{text_digest[:LEGACY_WIDTH]}:{metadata_digest[:4]}'
