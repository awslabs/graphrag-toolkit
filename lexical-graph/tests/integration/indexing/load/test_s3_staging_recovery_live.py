# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live checks that a half-written collection reads back as incomplete.

A run killed part way through leaves a source document prefix holding some of
its chunks and no completion marker. Mocked tests can state that shape, but not
that S3 produces it: the listing, the reserved marker segment and the collection
record all have to line up against a real endpoint.

Each test stages a collection, damages it the way an interrupted run would, and
reads it back.

Skipped unless S3_TEST_BUCKET is set. To run locally:

    S3_TEST_BUCKET=my-bucket \\
        pytest tests/integration/indexing/load/test_s3_staging_recovery_live.py

Objects are written under a unique prefix per run and deleted afterwards.
"""

import os
import uuid

import pytest
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import (
    COLLECTION_RECORD_NAME,
    S3BasedDocs,
    is_completion_marker,
)
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument

S3_TEST_BUCKET = os.environ.get('S3_TEST_BUCKET')
REGION = os.environ.get('AWS_REGION', 'us-east-1')

pytestmark = pytest.mark.skipif(
    not S3_TEST_BUCKET,
    reason='set S3_TEST_BUCKET to a writable bucket to run this live test',
)


@pytest.fixture
def key_prefix():
    run_prefix = f'staging-recovery-tests/{uuid.uuid4()}'
    yield run_prefix

    s3_client = GraphRAGConfig.s3
    pages = s3_client.get_paginator('list_objects_v2').paginate(
        Bucket=S3_TEST_BUCKET, Prefix=run_prefix
    )
    keys = [{'Key': o['Key']} for page in pages for o in page.get('Contents', [])]
    if keys:
        s3_client.delete_objects(Bucket=S3_TEST_BUCKET, Delete={'Objects': keys})


def _doc(source_id, num_chunks=3):
    nodes = []
    for i in range(num_chunks):
        node = TextNode(text=f'text for {source_id} chunk {i}', id_=f'{source_id}-chunk-{i}')
        node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
        nodes.append(node)
    return SourceDocument(nodes=nodes)


def _handler(key_prefix, collection_id, for_jsonl):
    return S3BasedDocs(
        region=REGION,
        bucket_name=S3_TEST_BUCKET,
        key_prefix=key_prefix,
        collection_id=collection_id,
        for_jsonl=for_jsonl,
    )


def _stage(key_prefix, collection_id, docs, for_jsonl=False):
    return list(_handler(key_prefix, collection_id, for_jsonl).accept(docs))


def _read_back(key_prefix, collection_id, for_jsonl=False):
    return sorted(
        doc.source_id() for doc in _handler(key_prefix, collection_id, for_jsonl)
    )


def _keys_under(prefix):
    pages = GraphRAGConfig.s3.get_paginator('list_objects_v2').paginate(
        Bucket=S3_TEST_BUCKET, Prefix=prefix
    )
    return [o['Key'] for page in pages for o in page.get('Contents', [])]


def _delete(keys):
    GraphRAGConfig.s3.delete_objects(
        Bucket=S3_TEST_BUCKET, Delete={'Objects': [{'Key': key} for key in keys]}
    )


def _interrupt_before_the_marker(key_prefix, collection_id, source_id):
    """The state a run killed mid-document leaves: chunks written, no marker."""
    markers = [
        key
        for key in _keys_under(f'{key_prefix}/{collection_id}/{source_id}/')
        if is_completion_marker(key)
    ]
    assert markers, 'staging wrote no marker to remove'
    _delete(markers)


@pytest.mark.parametrize('for_jsonl', [False, True], ids=['chunks', 'jsonl'])
class TestAnInterruptedRunReadsBackAsIncomplete:

    def test_a_document_that_never_got_its_marker_is_skipped(self, key_prefix, for_jsonl):
        collection_id = 'interrupted'
        _stage(key_prefix, collection_id, [_doc('src-1'), _doc('src-2')], for_jsonl)

        _interrupt_before_the_marker(key_prefix, collection_id, 'src-2')

        assert _read_back(key_prefix, collection_id, for_jsonl) == ['src-1']

    def test_a_document_missing_one_of_its_chunks_is_skipped(self, key_prefix, for_jsonl):
        collection_id = 'truncated'
        _stage(key_prefix, collection_id, [_doc('src-1'), _doc('src-2')], for_jsonl)

        content = [
            key
            for key in _keys_under(f'{key_prefix}/{collection_id}/src-2/')
            if not is_completion_marker(key)
        ]
        _delete(content[:1])

        assert _read_back(key_prefix, collection_id, for_jsonl) == ['src-1']

    def test_re_staging_the_damaged_document_brings_it_back(self, key_prefix, for_jsonl):
        collection_id = 'restaged'
        _stage(key_prefix, collection_id, [_doc('src-1'), _doc('src-2')], for_jsonl)
        _interrupt_before_the_marker(key_prefix, collection_id, 'src-2')

        _stage(key_prefix, collection_id, [_doc('src-2')], for_jsonl)

        assert _read_back(key_prefix, collection_id, for_jsonl) == ['src-1', 'src-2']

    def test_an_undamaged_collection_reads_back_whole(self, key_prefix, for_jsonl):
        collection_id = 'intact'
        _stage(key_prefix, collection_id, [_doc('src-1'), _doc('src-2')], for_jsonl)

        assert _read_back(key_prefix, collection_id, for_jsonl) == ['src-1', 'src-2']


@pytest.mark.parametrize('for_jsonl', [False, True], ids=['chunks', 'jsonl'])
class TestACollectionStagedBeforeMarkersExisted:
    """No record, so the reader has no grounds to call anything incomplete."""

    def test_every_document_is_read_back_without_a_marker(self, key_prefix, for_jsonl):
        collection_id = 'legacy'
        _stage(key_prefix, collection_id, [_doc('src-1'), _doc('src-2')], for_jsonl)

        _delete([f'{key_prefix}/{collection_id}/{COLLECTION_RECORD_NAME}'])
        for source_id in ('src-1', 'src-2'):
            _interrupt_before_the_marker(key_prefix, collection_id, source_id)

        assert _read_back(key_prefix, collection_id, for_jsonl) == ['src-1', 'src-2']

    def test_staging_into_an_older_collection_leaves_it_unrecorded(self, key_prefix, for_jsonl):
        collection_id = 'appended'
        _stage(key_prefix, collection_id, [_doc('src-1')], for_jsonl)
        _delete([f'{key_prefix}/{collection_id}/{COLLECTION_RECORD_NAME}'])
        _interrupt_before_the_marker(key_prefix, collection_id, 'src-1')

        _stage(key_prefix, collection_id, [_doc('src-2')], for_jsonl)

        assert f'{key_prefix}/{collection_id}/{COLLECTION_RECORD_NAME}' not in _keys_under(
            f'{key_prefix}/{collection_id}/'
        )
        assert _read_back(key_prefix, collection_id, for_jsonl) == ['src-1', 'src-2']

    def test_the_record_is_not_read_back_as_a_document(self, key_prefix, for_jsonl):
        collection_id = 'record-visible'
        _stage(key_prefix, collection_id, [_doc('src-1')], for_jsonl)

        assert f'{key_prefix}/{collection_id}/{COLLECTION_RECORD_NAME}' in _keys_under(
            f'{key_prefix}/{collection_id}/'
        )
        assert _read_back(key_prefix, collection_id, for_jsonl) == ['src-1']
