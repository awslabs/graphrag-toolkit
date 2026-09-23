# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live checks that a run's records and the resume read behave against real S3.

Three things here are the service's behaviour rather than ours, and a mocked
client cannot show any of them. The rollup merge writes one object and then
deletes the records it folded in, so a listing has to agree afterwards. The
resume read decides what a collection already holds from one listing of keys,
so the keys a real uploader writes have to be the keys it expects. And the
records live inside the staged collection, so the collection listing has to
keep treating their directory as something other than a source document.

Skipped unless S3_TEST_BUCKET is set. To run locally:

    S3_TEST_BUCKET=my-bucket \\
        pytest tests/integration/indexing/load/test_run_manifest_live.py

Objects are written under a unique prefix per run and deleted afterwards.
"""

import os
import uuid

import pytest
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.extract.resume import (
    plan_resume,
    staged_source_ids,
)
from graphrag_toolkit.lexical_graph.indexing.extract.run_plan import RunPlanStore
from graphrag_toolkit.lexical_graph.indexing.extract.run_manifest import (
    COMPLETE,
    SUBMITTED,
    PartitionRecord,
    RunManifestStore,
)
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import (
    S3BasedDocs,
    is_completion_marker,
    list_collection,
)
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument

S3_TEST_BUCKET = os.environ.get('S3_TEST_BUCKET')
REGION = os.environ.get('AWS_REGION', 'us-east-1')

pytestmark = pytest.mark.skipif(
    not S3_TEST_BUCKET,
    reason='set S3_TEST_BUCKET to a writable bucket to run this live test',
)

COLLECTION_ID = 'collection'
RUN_ID = 'run-1'


@pytest.fixture
def key_prefix():
    run_prefix = f'run-manifest-tests/{uuid.uuid4()}'
    yield run_prefix

    s3_client = GraphRAGConfig.s3
    pages = s3_client.get_paginator('list_objects_v2').paginate(
        Bucket=S3_TEST_BUCKET, Prefix=run_prefix
    )
    keys = [{'Key': o['Key']} for page in pages for o in page.get('Contents', [])]
    if keys:
        s3_client.delete_objects(Bucket=S3_TEST_BUCKET, Delete={'Objects': keys})


@pytest.fixture
def store(key_prefix):
    return RunManifestStore(
        bucket_name=S3_TEST_BUCKET,
        key_prefix=key_prefix,
        collection_id=COLLECTION_ID,
        run_id=RUN_ID,
    )


def _record(partition, state=COMPLETE, attempt=1):
    return PartitionRecord(
        partition_id=partition, attempt=attempt, state=state,
        job_name=f'extract-topics-{RUN_ID}-{partition}-a{attempt}',
        job_arn=f'arn:aws:bedrock:{REGION}:000000000000:model-invocation-job/{partition}',
        output_path='outputs/', input_filename='in.jsonl',
    )


def _doc(source_id, num_chunks=2):
    nodes = []
    for i in range(num_chunks):
        node = TextNode(text=f'text for {source_id} chunk {i}', id_=f'{source_id}-chunk-{i}')
        node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
        nodes.append(node)
    return SourceDocument(nodes=nodes)


def _stage(key_prefix, docs, for_jsonl=False, skip_source_ids=None):
    handler = S3BasedDocs(
        region=REGION, bucket_name=S3_TEST_BUCKET, key_prefix=key_prefix,
        collection_id=COLLECTION_ID, for_jsonl=for_jsonl, skip_source_ids=skip_source_ids,
    )
    return list(handler.accept(docs))


def _keys_under(prefix):
    pages = GraphRAGConfig.s3.get_paginator('list_objects_v2').paginate(
        Bucket=S3_TEST_BUCKET, Prefix=prefix
    )
    return [o['Key'] for page in pages for o in page.get('Contents', [])]


class TestARecordSurvivesARoundTrip:

    def test_a_partition_reads_back_as_it_was_written(self, store):
        store.write(_record('abc', state=SUBMITTED), GraphRAGConfig.s3)

        assert store.read('abc', GraphRAGConfig.s3) == _record('abc', state=SUBMITTED)

    def test_a_partition_no_run_touched_reads_as_nothing(self, store):
        # A missing object has to answer with the code the read treats as
        # 'never started', not raise.
        assert store.read('never-ran', GraphRAGConfig.s3) is None


class TestTheRollup:

    def test_a_merged_record_leaves_the_listing_and_stays_in_the_rollup(self, store):
        store.write(_record('done'), GraphRAGConfig.s3)

        store.merge_rollup(GraphRAGConfig.s3)

        assert store.list_partition_keys(GraphRAGConfig.s3) == []
        assert store.read_rollup(GraphRAGConfig.s3)['done'].state == COMPLETE

    def test_a_partition_still_working_keeps_its_record(self, store):
        store.write(_record('done'), GraphRAGConfig.s3)
        store.write(_record('working', state=SUBMITTED), GraphRAGConfig.s3)

        store.merge_rollup(GraphRAGConfig.s3)

        assert store.list_partition_keys(GraphRAGConfig.s3) == [store.partition_key('working')]

    def test_a_restart_reads_the_rollup_and_what_came_after_it(self, store):
        store.write(_record('first'), GraphRAGConfig.s3)
        store.merge_rollup(GraphRAGConfig.s3)
        store.write(_record('second', state=SUBMITTED), GraphRAGConfig.s3)

        partitions = store.read_partitions(GraphRAGConfig.s3)

        assert sorted(partitions) == ['first', 'second']
        assert partitions['second'].state == SUBMITTED


class TestWhatACollectionAlreadyHolds:

    def test_a_staged_source_is_found_from_the_keys_alone(self, key_prefix):
        _stage(key_prefix, [_doc('src-1'), _doc('src-2')])

        staged = staged_source_ids(S3_TEST_BUCKET, key_prefix, COLLECTION_ID, GraphRAGConfig.s3)

        assert staged == {'src-1', 'src-2'}

    def test_the_jsonl_format_is_not_answered_for(self):
        # Its node ids live inside the objects, so a listing cannot account for
        # them. Nothing is skipped and the run stores its documents again.
        assert staged_source_ids(
            S3_TEST_BUCKET, 'any', COLLECTION_ID, GraphRAGConfig.s3, for_jsonl=True
        ) == set()

    def test_a_source_missing_a_chunk_its_marker_names_is_not_staged(self, key_prefix):
        _stage(key_prefix, [_doc('src-1', num_chunks=3)])

        chunks = [
            key for key in _keys_under(f'{key_prefix}/{COLLECTION_ID}/src-1/')
            if not is_completion_marker(key)
        ]
        GraphRAGConfig.s3.delete_objects(
            Bucket=S3_TEST_BUCKET, Delete={'Objects': [{'Key': chunks[0]}]}
        )

        assert staged_source_ids(
            S3_TEST_BUCKET, key_prefix, COLLECTION_ID, GraphRAGConfig.s3
        ) == set()

    def test_a_source_an_interrupted_run_left_unmarked_is_not_found(self, key_prefix):
        _stage(key_prefix, [_doc('src-1'), _doc('src-2')])

        markers = [
            key for key in _keys_under(f'{key_prefix}/{COLLECTION_ID}/src-2/')
            if is_completion_marker(key)
        ]
        assert markers, 'staging wrote no marker to remove'
        GraphRAGConfig.s3.delete_objects(
            Bucket=S3_TEST_BUCKET, Delete={'Objects': [{'Key': k} for k in markers]}
        )

        staged = staged_source_ids(S3_TEST_BUCKET, key_prefix, COLLECTION_ID, GraphRAGConfig.s3)

        assert staged == {'src-1'}

    def test_the_collection_record_and_the_run_directory_are_not_sources(self, key_prefix, store):
        _stage(key_prefix, [_doc('src-1')])
        store.write(_record('abc'), GraphRAGConfig.s3)

        staged = staged_source_ids(S3_TEST_BUCKET, key_prefix, COLLECTION_ID, GraphRAGConfig.s3)

        assert staged == {'src-1'}

    def test_the_run_directory_is_not_read_back_as_a_source_document(self, key_prefix, store):
        _stage(key_prefix, [_doc('src-1')])
        store.write(_record('abc'), GraphRAGConfig.s3)
        store.merge_rollup(GraphRAGConfig.s3)

        prefixes, _ = list_collection(
            S3_TEST_BUCKET, key_prefix, COLLECTION_ID,
            GraphRAGConfig.s3.get_paginator('list_objects_v2')
        )

        assert prefixes == [f'{key_prefix}/{COLLECTION_ID}/src-1/']


class TestARunResumingItsOwnWork:

    def test_a_source_already_staged_is_not_written_again(self, key_prefix):
        _stage(key_prefix, [_doc('src-1')])
        before = sorted(_keys_under(f'{key_prefix}/{COLLECTION_ID}/src-1/'))

        _stage(key_prefix, [_doc('src-1')], skip_source_ids={'src-1'})

        assert sorted(_keys_under(f'{key_prefix}/{COLLECTION_ID}/src-1/')) == before

    def test_a_source_the_earlier_run_never_reached_is_staged(self, key_prefix):
        _stage(key_prefix, [_doc('src-1')])

        _stage(key_prefix, [_doc('src-1'), _doc('src-2')], skip_source_ids={'src-1'})

        assert _keys_under(f'{key_prefix}/{COLLECTION_ID}/src-2/')

    def test_the_report_says_what_the_restart_will_reuse(self, key_prefix, store):
        _stage(key_prefix, [_doc('src-1')])
        store.write(_record('done'), GraphRAGConfig.s3)
        store.write(_record('started', state=SUBMITTED), GraphRAGConfig.s3)

        report = plan_resume(store, GraphRAGConfig.s3)

        assert report.is_restart
        assert report.partitions_complete == 1
        assert report.partitions_outstanding == 1
        assert report.staged_source_ids == {'src-1'}

    def test_a_run_id_nothing_has_used_reads_as_a_new_run(self, key_prefix):
        fresh = RunManifestStore(
            bucket_name=S3_TEST_BUCKET, key_prefix=key_prefix,
            collection_id=COLLECTION_ID, run_id='never-used',
        )

        report = plan_resume(fresh, GraphRAGConfig.s3)

        assert not report.is_restart

    def test_a_handler_built_for_the_run_stages_only_what_is_missing(self, key_prefix):
        # The whole feature through its operator surface: stage a source, then
        # build a handler for the same run and stage both.
        _stage(key_prefix, [_doc('src-1')])
        before = sorted(_keys_under(f'{key_prefix}/{COLLECTION_ID}/src-1/'))

        plan_store = RunPlanStore(
            bucket_name=S3_TEST_BUCKET,
            key_prefix=key_prefix,
            collection_id=COLLECTION_ID,
        )
        handler = plan_store.staging_handler(RUN_ID, GraphRAGConfig.s3, region=REGION)

        list(handler.accept([_doc('src-1'), _doc('src-2')]))

        assert sorted(_keys_under(f'{key_prefix}/{COLLECTION_ID}/src-1/')) == before
        assert _keys_under(f'{key_prefix}/{COLLECTION_ID}/src-2/')
