# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
A run records what it did with each partition, so a restart knows which jobs it
has already paid for.

A partition names itself by the nodes it holds. The run plan fixes how a
collection divides, so a restart forms the same partitions and asks after them
by the same names, which is what lets a worker process, handed nodes and
nothing else, find the record belonging to the work in front of it.
"""

import json

import pytest
from unittest.mock import Mock

from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph.indexing.extract.run_manifest import (
    COMPLETE,
    MAX_JOB_NAME,
    MAX_KEYS_PER_DELETE,
    SUBMITTED,
    PartitionRecord,
    RunManifestStore,
    batch_job_name,
    partition_id,
)

BUCKET = 'b'
KEY_PREFIX = 'p'
COLLECTION_ID = 'c'
RUN_ID = 'run-1'


def _record(partition='abc', state=SUBMITTED, attempt=1, **kwargs):
    return PartitionRecord(
        partition_id=partition, attempt=attempt, state=state, **kwargs
    )


def _store():
    return RunManifestStore(
        bucket_name=BUCKET, key_prefix=KEY_PREFIX, collection_id=COLLECTION_ID, run_id=RUN_ID
    )


def _s3_holding(objects):
    """A mock S3 holding these keys, and refusing any other as missing."""
    written = {}

    def get_object(Bucket, Key):
        body = written.get(Key, objects.get(Key))
        if body is None:
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
        # The store writes bytes; the fixtures above are written as text.
        raw = body if isinstance(body, bytes) else body.encode('UTF-8')
        return {'Body': Mock(read=lambda: raw)}

    events = []

    def put_object(**kwargs):
        written[kwargs['Key']] = kwargs['Body']
        events.append(('put', kwargs['Key']))

    def paginate(Bucket, Prefix):
        keys = sorted(k for k in {**objects, **written} if k.startswith(Prefix))
        return [{'Contents': [{'Key': key} for key in keys]}]

    s3_client = Mock()
    s3_client.get_object.side_effect = get_object
    s3_client.put_object.side_effect = put_object
    s3_client.get_paginator.return_value = Mock(paginate=paginate)
    s3_client.deleted = []
    s3_client.delete_objects.side_effect = lambda Bucket, Delete: (
        s3_client.deleted.extend(o['Key'] for o in Delete['Objects']),
        events.extend(('delete', o['Key']) for o in Delete['Objects']),
        [written.pop(o['Key'], None) for o in Delete['Objects']],
    )
    s3_client.written = written
    # One ordered log of writes and deletes, so a test can say which came first.
    s3_client.events = events

    return s3_client


class TestAPartitionNamesItself:

    def test_the_same_nodes_give_the_same_name(self):
        assert partition_id(['c1', 'c2'], stage='topic') == partition_id(['c1', 'c2'], stage='topic')

    def test_the_order_the_nodes_arrive_in_does_not_matter(self):
        # Workers receive a partition's nodes in whatever order the pool hands
        # them over, and it is the same partition either way.
        assert partition_id(['c1', 'c2'], stage='topic') == partition_id(['c2', 'c1'], stage='topic')

    def test_two_stages_over_the_same_nodes_are_different_partitions(self):
        # A pipeline runs both extractors over the same nodes. Named by the
        # node ids alone, the topic stage would find the proposition stage's
        # record and serve its output as topics, on a first run as well as a
        # restart.
        assert (partition_id(['c1', 'c2'], stage='topic')
                != partition_id(['c1', 'c2'], stage='proposition'))

    def test_different_nodes_give_different_names(self):
        assert partition_id(['c1', 'c2'], stage='topic') != partition_id(['c1', 'c3'], stage='topic')


class TestAJobNameSaysWhereItCameFrom:

    def test_it_carries_the_run_the_partition_and_the_attempt(self):
        name = batch_job_name('extract-topics', RUN_ID, partition_id(['c1'], stage='topic'), 2)

        assert name.startswith('extract-topics-')
        assert RUN_ID in name
        assert name.endswith('-a2')

    def test_a_long_run_id_still_leaves_a_usable_name(self):
        name = batch_job_name('extract-propositions', 'a' * 200, partition_id(['c1'], stage='topic'), 11)

        assert len(name) <= MAX_JOB_NAME
        assert name.endswith('-a11')

    @pytest.mark.parametrize('run_id', ['run/1', 'run 1', 'run_1', 'RUN.1'])
    def test_characters_bedrock_refuses_are_replaced(self, run_id):
        name = batch_job_name('extract-topics', run_id, partition_id(['c1'], stage='topic'), 1)

        assert name[0].isalnum()
        assert all(c.isalnum() or c == '-' for c in name)

    def test_two_attempts_of_one_partition_get_different_names(self):
        partition = partition_id(['c1'], stage='topic')

        assert (batch_job_name('x', RUN_ID, partition, 1)
                != batch_job_name('x', RUN_ID, partition, 2))


class TestReadingWhatARunRecorded:

    def test_a_partition_with_no_record_reads_as_nothing(self):
        assert _store().read('abc', _s3_holding({})) is None

    def test_a_record_reads_back_as_it_was_written(self):
        store, s3_client = _store(), _s3_holding({})
        store.write(_record(job_name='j', job_arn='arn', output_path='out/'), s3_client)

        assert store.read('abc', s3_client) == _record(
            job_name='j', job_arn='arn', output_path='out/'
        )

    def test_the_last_write_for_a_partition_wins(self):
        # One partition, one writer: the worker holding it. A later state
        # replaces the earlier one rather than contending with it.
        store, s3_client = _store(), _s3_holding({})
        store.write(_record(state=SUBMITTED), s3_client)
        store.write(_record(state=COMPLETE), s3_client)

        assert store.read('abc', s3_client).state == COMPLETE

    def test_a_field_a_later_build_added_is_ignored(self):
        store = _store()
        s3_client = _s3_holding({
            store.partition_key('abc'): json.dumps({
                'partition_id': 'abc', 'attempt': 1,
                'state': COMPLETE, 'something_new': 'ignored',
            })
        })

        assert store.read('abc', s3_client).state == COMPLETE


class TestTheRollup:

    def test_a_restart_reads_the_rollup_and_anything_written_since(self):
        store, s3_client = _store(), _s3_holding({})
        store.write(_record('first', state=COMPLETE), s3_client)
        store.merge_rollup(s3_client)
        store.write(_record('second', state=SUBMITTED), s3_client)

        partitions = store.read_partitions(s3_client)

        assert sorted(partitions) == ['first', 'second']
        assert partitions['first'].state == COMPLETE
        assert partitions['second'].state == SUBMITTED

    def test_only_completed_partitions_are_rolled_up(self):
        store, s3_client = _store(), _s3_holding({})
        store.write(_record('done', state=COMPLETE), s3_client)
        store.write(_record('working', state=SUBMITTED), s3_client)

        assert sorted(store.merge_rollup(s3_client)) == ['done']

    def test_a_partition_still_working_keeps_its_record(self):
        store, s3_client = _store(), _s3_holding({})
        store.write(_record('working', state=SUBMITTED), s3_client)

        store.merge_rollup(s3_client)

        assert store.partition_key('working') not in s3_client.deleted
        assert store.read('working', s3_client).state == SUBMITTED

    def test_a_rolled_up_record_is_removed_only_after_the_rollup_is_written(self):
        store, s3_client = _store(), _s3_holding({})
        store.write(_record('done', state=COMPLETE), s3_client)

        store.merge_rollup(s3_client)

        events = s3_client.events
        rollup_written = events.index(('put', store.rollup_key()))
        record_deleted = events.index(('delete', store.partition_key('done')))
        assert rollup_written < record_deleted, 'the rollup must exist before the record it replaces is gone'
        assert store.read_rollup(s3_client)['done'].state == COMPLETE

    def test_a_run_with_no_rollup_yet_reads_as_empty(self):
        assert _store().read_rollup(_s3_holding({})) == {}


class TestWhereTheRecordsLive:

    def test_they_sit_under_the_run_beside_the_collection(self):
        store = _store()

        assert store.partition_key('abc') == f'{KEY_PREFIX}/{COLLECTION_ID}/_runs/{RUN_ID}/partitions/abc.json'
        assert store.rollup_key() == f'{KEY_PREFIX}/{COLLECTION_ID}/_runs/{RUN_ID}/rollup.json'

    def test_a_run_id_that_would_leave_its_directory_is_refused(self):
        with pytest.raises(ValueError, match='invalid characters'):
            RunManifestStore(
                bucket_name=BUCKET, key_prefix=KEY_PREFIX,
                collection_id=COLLECTION_ID, run_id='../elsewhere'
            )


class TestAPartitionTheRollupAlreadyHolds:
    """
    merge_rollup folds a completed partition's record into the rollup and
    removes the file. A reader that looked only for the file would take that
    partition for one that never ran, and a restart would pay for its job
    again.
    """

    def test_a_rolled_up_partition_still_reads_as_complete(self):
        store, s3_client = _store(), _s3_holding({})
        store.write(_record('done', state=COMPLETE), s3_client)
        store.merge_rollup(s3_client)

        assert _store().read('done', s3_client).state == COMPLETE

    def test_a_partition_in_neither_place_reads_as_nothing(self):
        store, s3_client = _store(), _s3_holding({})
        store.write(_record('done', state=COMPLETE), s3_client)
        store.merge_rollup(s3_client)

        assert _store().read('never-ran', s3_client) is None

    def test_a_record_written_after_the_rollup_was_read_wins(self):
        store, s3_client = _store(), _s3_holding({})
        store.write(_record('p', state=COMPLETE), s3_client)
        store.merge_rollup(s3_client)

        store.read('p', s3_client)
        store.write(_record('p', state=SUBMITTED, attempt=2), s3_client)

        assert store.read('p', s3_client).attempt == 2


class TestRemovingTheRecordsTheRollupTookOver:

    def test_more_partitions_than_one_delete_allows_are_removed_in_batches(self):
        # S3 refuses a delete naming more than a thousand keys, and the refusal
        # would arrive after the rollup was already written.
        store, s3_client = _store(), _s3_holding({})
        for i in range(MAX_KEYS_PER_DELETE + 5):
            store.write(_record(f'p{i:04}', state=COMPLETE), s3_client)

        store.merge_rollup(s3_client)

        assert len(s3_client.deleted) == MAX_KEYS_PER_DELETE + 5
        assert all(
            len(call.kwargs['Delete']['Objects']) <= MAX_KEYS_PER_DELETE
            for call in s3_client.delete_objects.call_args_list
        )
