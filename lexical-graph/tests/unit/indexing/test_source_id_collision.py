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

from graphrag_toolkit.lexical_graph.indexing.build.source_graph_builder import SourceGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore

# md5(TEXT_A) and md5(TEXT_B) agree on their first eight hex characters, a4439cdb.
TEXT_A = 'document 27347 body text'
TEXT_B = 'document 30059 body text'

COLLIDING_SOURCE_ID = 'aws::a4439cdb:d41d'

NO_METADATA = ''

# The width every existing graph was written with, pinned rather than read from
# config so the facts below hold whatever a run is configured to use.
LEGACY_WIDTH = 8

# The width under consideration for the fix. 64 discriminating bits.
CANDIDATE_WIDTH = 16


def source_id_at(text, width, metadata_str=NO_METADATA):
    """The id this text would get at an explicit width."""
    return IdGenerator(source_id_hash_length=width).create_source_id(text, metadata_str)


def source_id_as_configured(text, metadata_str=NO_METADATA):
    """The id this text gets at whatever width the run is configured to use."""
    return IdGenerator().create_source_id(text, metadata_str)


class TestCollidingPair:
    """
    Preconditions. These are arithmetic about md5, not statements about any
    configuration. If one stops holding, the pair needs regenerating.
    """

    def test_the_two_documents_are_different(self):
        # Guards the rest: every assertion below is worthless if these converge.
        assert TEXT_A != TEXT_B

    def test_they_share_one_source_id_at_the_legacy_width(self):
        assert (source_id_at(TEXT_A, LEGACY_WIDTH)
                == source_id_at(TEXT_B, LEGACY_WIDTH)
                == COLLIDING_SOURCE_ID)

    def test_a_wider_text_digest_separates_them(self):
        assert source_id_at(TEXT_A, CANDIDATE_WIDTH) != source_id_at(TEXT_B, CANDIDATE_WIDTH)

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
    does with it. Whether the nodes actually collapse needs a real graph.
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
        """A source node carrying the id the pipeline would derive from `text`."""
        node = Mock()
        node.metadata = {
            'source': {
                'sourceId': source_id_as_configured(text),
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
        # collide bind one key. Fails once the default width is widened, which
        # is the point; update it then rather than deleting it.
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


@pytest.mark.xfail(
    strict=True,
    reason='Source ids discriminate on 32 bits at the default width. Remove this '
           'marker when the default is widened; strict=True fails the run once the '
           'assertions start passing, so the marker cannot outlive the fix.',
)
class TestSourceIdUniqueness:
    """
    RED. Two distinct documents must be distinguishable by id alone, because
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
