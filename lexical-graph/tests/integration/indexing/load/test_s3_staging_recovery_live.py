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


def _part(source_id, chunk_ids, final_part=True):
    """One round's worth of a source's chunks, as extraction emits it."""
    nodes = []
    for chunk_id in chunk_ids:
        node = TextNode(text=f'text for {chunk_id}', id_=chunk_id)
        node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
        nodes.append(node)
    return SourceDocument(nodes=nodes, final_part=final_part)


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


def _no_document_objects_under(key_prefix, collection_id):
    """
    Nothing of the document reached the bucket.

    The collection record is written before the first document and stays: an
    empty collection carrying one reads strict and yields nothing.
    """
    record_key = f'{key_prefix}/{collection_id}/{COLLECTION_RECORD_NAME}'
    return [k for k in _keys_under(f'{key_prefix}/') if k != record_key] == []


def _doc_with_ids(source_id, node_ids):
    nodes = []
    for node_id in node_ids:
        node = TextNode(text=f'text for {node_id}', id_=node_id)
        node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
        nodes.append(node)
    return SourceDocument(nodes=nodes)


@pytest.mark.parametrize('for_jsonl', [False, True], ids=['chunks', 'jsonl'])
class TestAnIdThatWouldLeaveTheCollectionPrefix:
    """
    Against a real endpoint, because the escape depends on how S3 treats a key
    segment: botocore sends segments unencoded, so what a separator does to a
    key is a property of the service, not of the string.
    """

    @pytest.mark.parametrize('source_id', ['../escaped', 'a/b'])
    def test_a_source_id_carrying_a_separator_writes_nothing(
        self, key_prefix, for_jsonl, source_id
    ):
        collection_id = 'hostile-source-id'

        with pytest.raises(ValueError, match='source_id'):
            _stage(key_prefix, collection_id, [_doc_with_ids(source_id, ['c1'])], for_jsonl)

        assert _no_document_objects_under(key_prefix, collection_id)

    def test_a_node_id_under_the_marker_segment_writes_nothing(self, key_prefix, for_jsonl):
        collection_id = 'hostile-node-id'

        with pytest.raises(ValueError, match='node_id'):
            _stage(
                key_prefix,
                collection_id,
                [_doc_with_ids('src-1', ['c1', '_markers/x'])],
                for_jsonl,
            )

        assert _no_document_objects_under(key_prefix, collection_id)

    def test_a_generated_id_still_stages_and_reads_back(self, key_prefix, for_jsonl):
        collection_id = 'generated-ids'
        _stage(key_prefix, collection_id, [_doc('src-1')], for_jsonl)

        assert _read_back(key_prefix, collection_id, for_jsonl) == ['src-1']
@pytest.mark.parametrize('for_jsonl', [False, True], ids=['chunks', 'jsonl'])
class TestASourceSplitAcrossRounds:
    """
    Each part of a split source is marked as it arrives, so a prefix holding
    only the earlier parts still has markers accounting for what is present.
    """

    def _stage_both_parts(self, key_prefix, collection_id, for_jsonl):
        """Both rounds through one handler, as one extraction run does."""
        handler = _handler(key_prefix, collection_id, for_jsonl)
        list(handler.accept([_part('src-1', ['c1', 'c2'], final_part=False)]))
        list(handler.accept([_part('src-1', ['c3', 'c4'])]))

    def _objects_for(self, source_prefix, chunk_ids):
        """The objects holding these chunks. A JSONL object is keyed on its
        source, so the chunk ids are in the body rather than the key."""
        found = []
        for key in _keys_under(source_prefix):
            if is_completion_marker(key):
                continue
            body = GraphRAGConfig.s3.get_object(Bucket=S3_TEST_BUCKET, Key=key)['Body'].read().decode('UTF-8')
            if any(chunk_id in key[len(source_prefix):] or chunk_id in body for chunk_id in chunk_ids):
                found.append(key)
        return found

    def test_both_parts_present_reads_back_whole(self, key_prefix, for_jsonl):
        collection_id = 'split-intact'
        self._stage_both_parts(key_prefix, collection_id, for_jsonl)

        assert _read_back(key_prefix, collection_id, for_jsonl) == ['src-1']

    def test_a_source_that_lost_its_opening_part_is_skipped(self, key_prefix, for_jsonl):
        # The part that ended the source landed and declares all four chunks,
        # so the two that are gone cannot pass unnoticed.
        collection_id = 'split-early'
        self._stage_both_parts(key_prefix, collection_id, for_jsonl)

        source_prefix = f'{key_prefix}/{collection_id}/src-1/'
        opening = self._objects_for(source_prefix, ['c1', 'c2'])
        assert opening, 'nothing found for the opening part'
        _delete(opening)

        assert _read_back(key_prefix, collection_id, for_jsonl) == []
