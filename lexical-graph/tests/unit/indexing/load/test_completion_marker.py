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
    COMPLETION_MARKER_KEY,
    S3ChunkDownloader,
    S3ChunkUploader,
    S3DocDownloader,
    node_ids_hash,
)
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

COLLECTION_PREFIX = 'p/c'
SOURCE_ID = 'aws::dead:beef'


def _chunk(node_id, metadata=None):
    # SourceDocument.source_id() reads the SOURCE relationship, so a chunk
    # without one has no document to belong to.
    node = TextNode(text=f'text for {node_id}', id_=node_id, metadata=metadata or {})
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=SOURCE_ID)
    return node


def _doc(node_ids, index_node_ids=()):
    return SourceDocument(nodes=[
        *(_chunk(i) for i in node_ids),
        *(_chunk(i, {INDEX_KEY: 'x'}) for i in index_node_ids),
    ])


def _uploader():
    return S3ChunkUploader(
        bucket_name='b', collection_prefix=COLLECTION_PREFIX, num_threads=2
    )


def _upload(uploader, docs, failing_keys=()):
    """Run the uploader against a mock S3, returning the objects it wrote."""
    written = {}

    def put_object(**kwargs):
        key = kwargs['Key']
        if any(key.endswith(f) for f in failing_keys):
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


def _marker_key(source_id=SOURCE_ID):
    return f'{COLLECTION_PREFIX}/{source_id}/{COMPLETION_MARKER_KEY}'


class TestMarkerIsWrittenOnSuccess:

    def test_a_fully_uploaded_document_gets_a_marker(self):
        written, yielded = _upload(_uploader(), [_doc(['c1', 'c2'])])

        assert _marker_key() in written
        assert len(yielded) == 1

    def test_the_marker_records_the_chunk_ids_the_count_and_a_hash(self):
        written, _ = _upload(_uploader(), [_doc(['c1', 'c2'])])

        marker = json.loads(written[_marker_key()])

        assert marker['chunk_ids'] == ['c1', 'c2']
        assert marker['count'] == 2
        assert marker['content_hash'] == node_ids_hash(['c1', 'c2'])

    def test_index_nodes_are_not_counted(self):
        # A chunk carrying an index key is a vector store artefact. The uploader
        # does not write it, so the marker must not claim it.
        written, _ = _upload(_uploader(), [_doc(['c1'], index_node_ids=['v1'])])

        marker = json.loads(written[_marker_key()])

        assert marker['chunk_ids'] == ['c1']
        assert marker['count'] == 1

    def test_the_marker_goes_down_after_every_chunk(self):
        written, _ = _upload(_uploader(), [_doc(['c1', 'c2'])])

        keys = list(written)

        assert keys.index(_marker_key()) == len(keys) - 1


class TestMarkerIsWithheldOnFailure:

    def test_a_failed_chunk_leaves_no_marker(self):
        written, yielded = _upload(
            _uploader(), [_doc(['c1', 'c2'])], failing_keys=['c2.json']
        )

        assert _marker_key() not in written
        assert len(yielded) == 1, 'the document is still yielded, as it is today'

    def test_the_chunks_that_did_succeed_are_still_written(self):
        written, _ = _upload(
            _uploader(), [_doc(['c1', 'c2'])], failing_keys=['c2.json']
        )

        assert f'{COLLECTION_PREFIX}/{SOURCE_ID}/c1.json' in written


class TestEdgeCases:

    def test_a_document_with_nothing_to_write_is_still_marked_complete(self):
        # Every chunk succeeded, of which there were none. Withholding the
        # marker would leave the document looking incomplete forever.
        written, yielded = _upload(_uploader(), [_doc([], index_node_ids=['v1'])])

        marker = json.loads(written[_marker_key()])

        assert marker == {'chunk_ids': [], 'count': 0, 'content_hash': node_ids_hash([])}
        assert len(yielded) == 1

    def test_a_marker_that_fails_to_write_does_not_break_the_stream(self):
        # The chunks survived; the run should too. No marker means a re-stage,
        # which is cheaper than losing the rest of the corpus.
        written, yielded = _upload(
            _uploader(),
            [_doc(['c1']), _doc(['c2'])],
            failing_keys=[COMPLETION_MARKER_KEY],
        )

        assert _marker_key() not in written
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
            _marker_key(): self.MARKER_BODY,
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
            _marker_key(): self.MARKER_BODY,
        }
        downloader = S3DocDownloader(
            key_prefix='p', collection_id='c', bucket_name='b', fn=lambda n: n
        )

        doc = downloader._download_doc('prefix', self._s3_returning(objects))

        assert [n.node_id for n in doc.nodes] == ['c1']
