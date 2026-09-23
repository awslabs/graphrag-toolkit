# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live checks that a run plan behaves against a real S3 endpoint.

The plan is written once and read on every restart of the same run id. Two
things about that are the service's behaviour rather than ours, and a mocked
client cannot demonstrate either: a conditional write refusing the second
writer, and a missing object answering with the code the read treats as
'no plan'. The plan also lives inside the staged collection, so a real
delimited listing has to agree that its directory is not a source document.

Skipped unless S3_TEST_BUCKET is set. To run locally:

    S3_TEST_BUCKET=my-bucket \\
        pytest tests/integration/indexing/load/test_run_plan_live.py

Objects are written under a unique prefix per run and deleted afterwards.
"""

import os
import uuid

import pytest
from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.extract.run_plan import (
    ALREADY_WRITTEN_CODES,
    RunPlan,
    RunPlanMismatch,
    RunPlanStore,
)
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import (
    COLLECTION_RECORD_NAME,
    S3ChunkUploader,
    list_collection,
)

S3_TEST_BUCKET = os.environ.get('S3_TEST_BUCKET')

pytestmark = pytest.mark.skipif(
    not S3_TEST_BUCKET,
    reason='set S3_TEST_BUCKET to a writable bucket to run this live test',
)

COLLECTION_ID = 'collection'


@pytest.fixture
def key_prefix():
    run_prefix = f'run-plan-tests/{uuid.uuid4()}'
    yield run_prefix

    s3_client = GraphRAGConfig.s3
    pages = s3_client.get_paginator('list_objects_v2').paginate(
        Bucket=S3_TEST_BUCKET, Prefix=run_prefix
    )
    keys = [{'Key': o['Key']} for page in pages for o in page.get('Contents', [])]
    if keys:
        s3_client.delete_objects(Bucket=S3_TEST_BUCKET, Delete={'Objects': keys})


def _store(key_prefix):
    return RunPlanStore(
        bucket_name=S3_TEST_BUCKET,
        key_prefix=key_prefix,
        collection_id=COLLECTION_ID,
    )


def _plan(**overrides):
    fields = {
        'run_id': 'run-1',
        'document_ids': ['aws::doc-a:0000', 'aws::doc-b:0000'],
        'num_workers': 8,
        'batch_size': 4,
        'config': {'aws_region': 'us-east-1'},
    }
    fields.update(overrides)
    return RunPlan(**fields)


class TestARunPlanAgainstRealStorage:
    """
    What the plan needs from S3: an object that is not there reads as no plan,
    and one that round-trips comes back as it went in.
    """

    def test_a_run_that_has_not_started_has_no_plan(self, key_prefix):
        assert _store(key_prefix).read('run-1', GraphRAGConfig.s3) is None

    def test_a_plan_reads_back_as_it_was_written(self, key_prefix):
        store = _store(key_prefix)

        store.resolve(_plan(), GraphRAGConfig.s3)
        recorded = store.read('run-1', GraphRAGConfig.s3)

        assert recorded == _plan()

    def test_a_restart_of_the_same_run_obeys_what_is_stored(self, key_prefix):
        store = _store(key_prefix)
        store.resolve(_plan(num_workers=8), GraphRAGConfig.s3)

        resolved = store.resolve(_plan(num_workers=2), GraphRAGConfig.s3)

        assert resolved.num_workers == 8

    def test_a_restart_under_a_changed_batch_size_is_refused(self, key_prefix):
        store = _store(key_prefix)
        store.resolve(_plan(batch_size=4), GraphRAGConfig.s3)

        with pytest.raises(RunPlanMismatch, match='batch_size'):
            store.resolve(_plan(batch_size=16), GraphRAGConfig.s3)


class TestTwoRunsOfOneIdRacingForThePlan:
    """
    The conditional write is what makes a plan written once. Only the service
    can show that the second writer is refused rather than overwriting.
    """

    def test_the_second_write_of_one_run_id_is_refused(self, key_prefix):
        store = _store(key_prefix)
        s3_client = GraphRAGConfig.s3
        store.write(_plan(), s3_client)

        with pytest.raises(ClientError) as refused:
            store.write(_plan(document_ids=['aws::doc-c:0000']), s3_client)

        assert refused.value.response['Error']['Code'] in ALREADY_WRITTEN_CODES

    def test_the_run_that_loses_the_race_obeys_the_plan_that_won(self, key_prefix):
        store = _store(key_prefix)
        s3_client = GraphRAGConfig.s3
        winner = _plan(document_ids=['aws::doc-a:0000', 'aws::doc-b:0000'], num_workers=16)
        store.write(winner, s3_client)

        # The loser read before the winner wrote, so it still offers its own plan.
        resolved = store.resolve(_plan(num_workers=2), s3_client)

        assert resolved.num_workers == 16

    def test_the_plan_that_won_is_the_one_left_in_the_bucket(self, key_prefix):
        store = _store(key_prefix)
        s3_client = GraphRAGConfig.s3
        store.write(_plan(num_workers=16), s3_client)

        store.resolve(_plan(num_workers=2), s3_client)

        assert store.read('run-1', s3_client).num_workers == 16


class TestThePlanInsideAStagedCollection:
    """
    The plan lives beside the documents it divides, so a delimited listing has
    to ignore its directory and the collection record still has to be written.
    """

    def test_the_run_directory_is_not_read_as_a_source_document(self, key_prefix):
        s3_client = GraphRAGConfig.s3
        _store(key_prefix).resolve(_plan(), s3_client)

        source_doc_prefixes, _ = list_collection(
            S3_TEST_BUCKET, key_prefix, COLLECTION_ID, s3_client.get_paginator('list_objects_v2')
        )

        assert source_doc_prefixes == []

    def test_a_collection_holding_only_a_plan_is_still_recorded(self, key_prefix):
        s3_client = GraphRAGConfig.s3
        _store(key_prefix).resolve(_plan(), s3_client)

        S3ChunkUploader(
            bucket_name=S3_TEST_BUCKET,
            collection_prefix=f'{key_prefix}/{COLLECTION_ID}',
        ).record_collection(key_prefix, COLLECTION_ID, s3_client)

        _, recorded = list_collection(
            S3_TEST_BUCKET, key_prefix, COLLECTION_ID, s3_client.get_paginator('list_objects_v2')
        )
        assert recorded

    def test_a_collection_already_holding_documents_is_left_unrecorded(self, key_prefix):
        s3_client = GraphRAGConfig.s3
        _store(key_prefix).resolve(_plan(), s3_client)
        s3_client.put_object(
            Bucket=S3_TEST_BUCKET,
            Key=f'{key_prefix}/{COLLECTION_ID}/aws::doc-a:0000/chunk.json',
            Body=b'{}',
        )

        S3ChunkUploader(
            bucket_name=S3_TEST_BUCKET,
            collection_prefix=f'{key_prefix}/{COLLECTION_ID}',
        ).record_collection(key_prefix, COLLECTION_ID, s3_client)

        keys = [
            o['Key']
            for page in s3_client.get_paginator('list_objects_v2').paginate(
                Bucket=S3_TEST_BUCKET, Prefix=f'{key_prefix}/{COLLECTION_ID}/'
            )
            for o in page.get('Contents', [])
        ]
        assert not any(key.endswith(COLLECTION_RECORD_NAME) for key in keys)
