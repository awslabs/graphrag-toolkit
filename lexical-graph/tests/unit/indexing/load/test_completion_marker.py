# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
A completion marker records that every chunk for a document reached S3.

Two properties matter. The marker goes down only when every upload succeeded,
so its presence means something. And a prefix holding one reads back exactly as
it did before, because both downloaders list every object under a prefix and
hand it to TextNode.from_json, which accepts a marker as a node with a fresh
uuid and empty text rather than rejecting it.
"""

import json

import pytest
from unittest.mock import Mock, patch

from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode

from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import (
    COMPLETION_MARKER_PREFIX,
    S3ChunkDownloader,
    S3ChunkUploader,
    S3DocDownloader,
    S3DocUploader,
    completion_marker_key,
    completion_marker_name,
    is_complete,
    is_completion_marker,
    node_ids_hash,
)
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

COLLECTION_PREFIX = 'p/c'
SOURCE_ID = 'aws::dead:beef'


def _chunk(node_id, metadata=None, source_id=SOURCE_ID):
    # SourceDocument.source_id() reads the SOURCE relationship, so a chunk
    # without one has no document to belong to.
    node = TextNode(text=f'text for {node_id}', id_=node_id, metadata=metadata or {})
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
    return node


def _doc(node_ids, index_node_ids=(), source_id=SOURCE_ID):
    return SourceDocument(nodes=[
        *(_chunk(i, source_id=source_id) for i in node_ids),
        *(_chunk(i, {INDEX_KEY: 'x'}, source_id=source_id) for i in index_node_ids),
    ])


def _uploader():
    return S3ChunkUploader(
        bucket_name='b', collection_prefix=COLLECTION_PREFIX, num_threads=2
    )


def _doc_uploader():
    return S3DocUploader(
        bucket_name='b', collection_prefix=COLLECTION_PREFIX, num_threads=2
    )


def _upload(uploader, docs, failing_keys=()):
    """
    Run the uploader against a mock S3, returning the objects it wrote.

    failing_keys are matched as fragments, so a marker can be failed without
    reproducing the digest in its name.
    """
    written = {}

    def put_object(**kwargs):
        key = kwargs['Key']
        if any(f in key for f in failing_keys):
            raise RuntimeError(f'upload failed: {key}')
        written[key] = kwargs['Body']

    s3_client = Mock()
    s3_client.put_object.side_effect = put_object

    with patch(
        'graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs.GraphRAGConfig'
    ) as config:
        config.s3 = s3_client
        config.extraction_num_threads_per_worker = 2
        yielded = list(uploader.upload(docs))

    return written, yielded


def _marker_key(node_ids, source_id=SOURCE_ID):
    return completion_marker_key(f'{COLLECTION_PREFIX}/{source_id}', node_ids)


def _markers(written):
    return sorted(k for k in written if is_completion_marker(k))


class TestMarkerIsWrittenOnSuccess:

    def test_a_fully_uploaded_document_gets_a_marker(self):
        written, yielded = _upload(_uploader(), [_doc(['c1', 'c2'])])

        assert _marker_key(['c1', 'c2']) in written
        assert len(yielded) == 1

    def test_the_marker_records_the_chunk_ids_the_count_and_a_hash(self):
        written, _ = _upload(_uploader(), [_doc(['c1', 'c2'])])

        marker = json.loads(written[_marker_key(['c1', 'c2'])])

        assert marker['chunk_ids'] == ['c1', 'c2']
        assert marker['count'] == 2
        assert marker['content_hash'] == node_ids_hash(['c1', 'c2'])

    def test_index_nodes_are_not_counted(self):
        # A chunk carrying an index key is a vector store artefact. The uploader
        # does not write it, so the marker must not claim it.
        written, _ = _upload(_uploader(), [_doc(['c1'], index_node_ids=['v1'])])

        marker = json.loads(written[_marker_key(['c1'])])

        assert marker['chunk_ids'] == ['c1']
        assert marker['count'] == 1

    def test_the_marker_goes_down_after_every_chunk(self):
        written, _ = _upload(_uploader(), [_doc(['c1', 'c2'])])

        keys = list(written)

        assert keys.index(_marker_key(['c1', 'c2'])) == len(keys) - 1


class TestMarkerIsWithheldOnFailure:

    def test_a_failed_chunk_leaves_no_marker(self):
        written, yielded = _upload(
            _uploader(), [_doc(['c1', 'c2'])], failing_keys=['c2.json']
        )

        assert _markers(written) == []
        assert len(yielded) == 1, 'the document is still yielded, as it is today'

    def test_the_chunks_that_did_succeed_are_still_written(self):
        written, _ = _upload(
            _uploader(), [_doc(['c1', 'c2'])], failing_keys=['c2.json']
        )

        assert f'{COLLECTION_PREFIX}/{SOURCE_ID}/c1.json' in written


class TestEdgeCases:

    @pytest.mark.parametrize('uploader', [_uploader, _doc_uploader], ids=['chunks', 'jsonl'])
    def test_a_document_with_nothing_to_write_creates_no_prefix(self, uploader):
        # Whatever is left in the prefix reads back as a document with no nodes,
        # and passes is_complete on one empty set matching another.
        written, yielded = _upload(uploader(), [_doc([], index_node_ids=['v1'])])

        assert written == {}
        assert len(yielded) == 1, 'the document is still yielded'

    def test_several_documents_for_one_source_get_their_own_markers(self):
        # An auto-tuned run emits one source as several SourceDocuments, which
        # share a prefix. A fixed marker name would let the last one written
        # speak for all of them.
        written, _ = _upload(
            _uploader(), [_doc(['c1', 'c2']), _doc(['c3', 'c4'])]
        )

        assert _markers(written) == sorted(
            [_marker_key(['c1', 'c2']), _marker_key(['c3', 'c4'])]
        )

    def test_one_documents_marker_does_not_certify_anothers_truncated_prefix(self):
        # The failure this feature exists to prevent: doc 1 completes, doc 2
        # loses a chunk, and the prefix holds 3 of 4 objects. No marker in it
        # may describe the whole prefix as complete.
        written, _ = _upload(
            _uploader(),
            [_doc(['c1', 'c2']), _doc(['c3', 'c4'])],
            failing_keys=['c4.json'],
        )

        markers = _markers(written)

        assert markers == [_marker_key(['c1', 'c2'])]
        covered = json.loads(written[markers[0]])['chunk_ids']
        assert covered == ['c1', 'c2'], 'it speaks only for its own document'
        assert _marker_key(['c3', 'c4']) not in written

    def test_a_marker_that_fails_to_write_does_not_break_the_stream(self):
        # The chunks survived; the run should too. No marker means a re-stage,
        # which is cheaper than losing the rest of the corpus.
        written, yielded = _upload(
            _uploader(),
            [_doc(['c1']), _doc(['c2'])],
            failing_keys=[COMPLETION_MARKER_PREFIX],
        )

        assert _markers(written) == []
        assert len(yielded) == 2
        assert f'{COLLECTION_PREFIX}/{SOURCE_ID}/c1.json' in written


class TestReadersIgnoreTheMarker:
    """
    Both downloaders list every object under a prefix. TextNode.from_json
    accepts a marker as a node with a generated uuid and empty text, so an
    unfiltered listing turns the marker into a phantom chunk on every read.
    """

    MARKER_BODY = json.dumps({'chunk_ids': ['c1'], 'count': 1, 'content_hash': 'x'})

    def _s3_returning(self, objects):
        s3_client = Mock()
        s3_client.get_paginator.return_value.paginate.return_value = [
            {'Contents': [{'Key': key} for key in objects],
             'CommonPrefixes': [{'Prefix': f'{COLLECTION_PREFIX}/{SOURCE_ID}/'}]}
        ]

        def download_fileobj(bucket, key, stream):
            stream.write(objects[key].encode('UTF-8'))

        s3_client.download_fileobj.side_effect = download_fileobj
        return s3_client

    def test_the_chunk_downloader_skips_it(self):
        objects = {
            f'{COLLECTION_PREFIX}/{SOURCE_ID}/c1.json': TextNode(text='one', id_='c1').to_json(),
            _marker_key(['c1']): self.MARKER_BODY,
        }
        downloader = S3ChunkDownloader(
            key_prefix='p', collection_id='c', bucket_name='b', fn=lambda n: n
        )

        with patch(
            'graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs.GraphRAGConfig'
        ) as config:
            config.s3 = self._s3_returning(objects)
            config.extraction_num_threads_per_worker = 2
            docs = list(downloader.download())

        assert [n.node_id for doc in docs for n in doc.nodes] == ['c1']

    def test_the_doc_downloader_skips_it(self):
        objects = {
            f'{COLLECTION_PREFIX}/{SOURCE_ID}/doc.jsonl': TextNode(text='one', id_='c1').to_json(),
            _marker_key(['c1']): self.MARKER_BODY,
        }
        downloader = S3DocDownloader(
            key_prefix='p', collection_id='c', bucket_name='b', fn=lambda n: n
        )

        doc = downloader._download_doc('prefix', self._s3_returning(objects))

        assert [n.node_id for n in doc.nodes] == ['c1']


class TestEncryption:
    """
    The KMS-versus-managed-keys branch was written out four times and asserted
    nowhere, so dropping the headers altogether kept the suite green. It is one
    branch now, and these are what hold it in place.
    """

    def _upload_capturing(self, encryption_key_id=None):
        captured = []
        uploader = S3ChunkUploader(
            bucket_name='b',
            collection_prefix=COLLECTION_PREFIX,
            s3_encryption_key_id=encryption_key_id,
            num_threads=2,
        )
        s3_client = Mock()
        s3_client.put_object.side_effect = lambda **kwargs: captured.append(kwargs)

        with patch(
            'graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs.GraphRAGConfig'
        ) as config:
            config.s3 = s3_client
            config.extraction_num_threads_per_worker = 2
            list(uploader.upload([_doc(['c1'])]))

        return captured

    def test_managed_keys_when_no_kms_key_is_configured(self):
        captured = self._upload_capturing()

        assert len(captured) == 2, 'one chunk and its marker'
        for kwargs in captured:
            assert kwargs['ServerSideEncryption'] == 'AES256'
            assert 'SSEKMSKeyId' not in kwargs

    def test_a_configured_kms_key_is_used(self):
        key_arn = 'arn:aws:kms:us-east-1:123456789012:key/12345678'

        captured = self._upload_capturing(encryption_key_id=key_arn)

        assert len(captured) == 2, 'one chunk and its marker'
        for kwargs in captured:
            assert kwargs['ServerSideEncryption'] == 'aws:kms'
            assert kwargs['SSEKMSKeyId'] == key_arn

    def test_the_marker_is_encrypted_like_every_other_object(self):
        captured = self._upload_capturing()
        markers = [k for k in captured if is_completion_marker(k['Key'])]

        assert len(markers) == 1
        assert markers[0]['ServerSideEncryption'] == 'AES256'
        assert markers[0]['ContentType'] == 'application/json'


class TestOrdering:
    """
    upload() promises documents come back in the order they went in. A document
    with nothing to write still has to take its turn: yielding it as soon as it
    is seen jumps every document already in flight.
    """

    def test_a_document_with_nothing_to_write_keeps_its_place(self):
        docs = [
            _doc(['a1', 'a2'], source_id='aws::a'),
            _doc([], index_node_ids=['v1'], source_id='aws::b'),
            _doc(['c1', 'c2'], source_id='aws::c'),
        ]

        _, yielded = _upload(_uploader(), docs)

        assert [d.source_id() for d in yielded] == ['aws::a', 'aws::b', 'aws::c']

    def test_nothing_is_lost_or_yielded_twice(self):
        docs = [
            _doc([], index_node_ids=['v1'], source_id='aws::a'),
            _doc(['b1'], source_id='aws::b'),
            _doc([], source_id='aws::c'),
        ]

        _, yielded = _upload(_uploader(), docs)

        assert [id(d) for d in yielded] == [id(d) for d in docs]


class TestMarkerNaming:

    def test_only_the_basename_decides(self):
        # A substring test would drop a chunk whose own id happened to contain
        # the marker prefix somewhere in its path.
        assert is_completion_marker(completion_marker_key('p/c/src', ['c1']))
        assert not is_completion_marker(f'p/c/{COMPLETION_MARKER_PREFIX}x/c1.json')
        assert not is_completion_marker('p/c/src/c1.json')
        # A chunk whose node id opens with the prefix is a chunk, not a marker.
        assert not is_completion_marker(f'p/c/src/{COMPLETION_MARKER_PREFIX}abcde.json')

    def test_the_name_follows_the_chunk_ids(self):
        assert completion_marker_name(['c1', 'c2']) == completion_marker_name(['c2', 'c1'])

    def test_a_chunk_named_like_a_marker_survives_the_round_trip(self):
        """The marker segment keeps the two namespaces apart.

        A chunk is keyed by its node id. While markers sat beside the chunks, a
        node id opening with the marker prefix was written as a chunk and then
        excluded from the reconstructed document with no error.
        """
        node_id = f'{COMPLETION_MARKER_PREFIX}looks-like-a-marker'
        chunk_key = f'{COLLECTION_PREFIX}/src/{node_id}.json'

        assert not is_completion_marker(chunk_key)
        assert is_completion_marker(completion_marker_key(f'{COLLECTION_PREFIX}/src', [node_id]))
        assert completion_marker_name(['c1', 'c2']) != completion_marker_name(['c1', 'c3'])


def _s3_holding(markers):
    """A client serving marker bodies keyed by the marker's own key."""
    s3_client = Mock()

    def download_fileobj(bucket, key, stream):
        stream.write(json.dumps(markers[key]).encode('UTF-8'))

    s3_client.download_fileobj.side_effect = download_fileobj
    return s3_client


class TestAMarkerThatDeclaresNothing:
    """
    A closing marker declares every chunk id stored for its source. One that
    declares an empty list is saying the source holds nothing, which cannot be
    true of a prefix with chunks in it.
    """

    def test_an_empty_declaration_does_not_certify_the_part(self):
        key = _marker_key(['c2'])
        markers = {key: {'chunk_ids': ['c2'], 'final': True, 'source_chunk_ids': []}}

        assert not is_complete(['c2'], [key], 'b', _s3_holding(markers))

    def test_a_declaration_covering_the_prefix_still_certifies(self):
        key = _marker_key(['c1', 'c2'])
        markers = {
            key: {'chunk_ids': ['c1', 'c2'], 'final': True, 'source_chunk_ids': ['c1', 'c2']}
        }

        assert is_complete(['c1', 'c2'], [key], 'b', _s3_holding(markers))

    def test_a_part_that_leaves_its_source_open_still_falls_back(self):
        # No source_chunk_ids at all is the earlier format and the non-final
        # part: the marker speaks for its own chunks.
        key = _marker_key(['c1'])
        markers = {key: {'chunk_ids': ['c1'], 'final': True}}

        assert is_complete(['c1'], [key], 'b', _s3_holding(markers))


class TestASourceWithAFailedChunkStaysUnmarked:
    """
    A source that lost a chunk gets no closing marker, so its prefix reads as
    incomplete and the document is staged again. A later part of the same
    source must not undo that.
    """

    def test_a_later_final_part_does_not_close_a_poisoned_source(self):
        written, yielded = _upload(
            _uploader(),
            [
                _doc(['c1', 'c2']),
                _doc(['c3', 'c4']),
            ],
            failing_keys=['c1.json'],
        )

        closing = [
            json.loads(written[key])
            for key in _markers(written)
            if json.loads(written[key]).get('final')
        ]

        assert closing == [], 'a source that lost a chunk was closed anyway'


class TestASourceOpenAcrossAnUploadBatch:
    """
    S3DocUploader.upload cuts the stream into batches of 1000 documents. A
    source whose parts straddle a cut is still open when the first batch ends,
    and must not be closed until the stream does.
    """

    BATCH = 1000

    def _part(self, source_id, chunk_ids, final_part=True):
        nodes = []
        for chunk_id in chunk_ids:
            node = TextNode(text='chunk text', id_=chunk_id)
            node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
            nodes.append(node)
        return SourceDocument(nodes=nodes, final_part=final_part)

    def _upload_across_the_cut(self):
        filler = [
            self._part(f'filler-{i}', [f'f{i}'])
            for i in range(self.BATCH - 1)
        ]
        split = [
            self._part('split-src', ['c1', 'c2'], final_part=False),
            self._part('split-src', ['c3', 'c4']),
        ]

        written = {}

        def put_object(**kwargs):
            written[kwargs['Key']] = kwargs['Body']

        s3_client = Mock()
        s3_client.put_object.side_effect = put_object

        uploader = S3DocUploader(bucket_name='b', collection_prefix=COLLECTION_PREFIX)
        with patch(
            'graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs.GraphRAGConfig'
        ) as config:
            config.s3 = s3_client
            config.extraction_num_threads_per_worker = 2
            list(uploader.upload(filler + split))

        return written

    def test_the_source_is_closed_once_declaring_every_part(self):
        written = self._upload_across_the_cut()

        closing = [
            json.loads(body)
            for key, body in written.items()
            if is_completion_marker(key)
            and f'{COLLECTION_PREFIX}/split-src/' in key
            and json.loads(body).get('final')
        ]

        assert len(closing) == 1, 'the source was closed more than once'
        assert closing[0]['source_chunk_ids'] == ['c1', 'c2', 'c3', 'c4']

    def test_a_source_left_open_by_the_stream_is_closed_at_the_end(self):
        # A round can finish a source without emitting a final part for it: a
        # resumed run drops the chunks it already extracted. Nothing else ends
        # those, so the end of the stream has to.
        written = {}

        def put_object(**kwargs):
            written[kwargs['Key']] = kwargs['Body']

        s3_client = Mock()
        s3_client.put_object.side_effect = put_object

        uploader = S3DocUploader(bucket_name='b', collection_prefix=COLLECTION_PREFIX)
        with patch(
            'graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs.GraphRAGConfig'
        ) as config:
            config.s3 = s3_client
            config.extraction_num_threads_per_worker = 2
            list(uploader.upload([self._part('open-src', ['c1', 'c2'], final_part=False)]))

        closing = [
            json.loads(body)
            for key, body in written.items()
            if is_completion_marker(key) and json.loads(body).get('final')
        ]

        assert len(closing) == 1
        assert closing[0]['source_chunk_ids'] == ['c1', 'c2']
