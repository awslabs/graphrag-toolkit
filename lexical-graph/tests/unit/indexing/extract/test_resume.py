# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
What a restart reads before it spends anything.

The run plan fixes how the collection divides and the partition records say
which of those divisions already ran. What neither answers is which documents
are already in the collection, and re-staging those costs puts for bytes that
are already in place. One listing of the collection answers it, rather than one
call per document, which at a million documents is its own scale problem.
"""

import json

from unittest.mock import Mock

from graphrag_toolkit.lexical_graph.indexing.extract.resume import (
    ResumeReport,
    plan_resume,
    staged_source_ids,
)
from graphrag_toolkit.lexical_graph.indexing.extract import RunPlanStore
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import completion_marker_key
from graphrag_toolkit.lexical_graph.indexing.extract.run_manifest import (
    COMPLETE,
    SUBMITTED,
    PartitionRecord,
)

BUCKET = 'b'
KEY_PREFIX = 'p'
COLLECTION_ID = 'c'
RUN_ID = 'run-1'
REGION = 'us-east-1'


def _s3(keys, marker_bodies=None):
    """A mock S3 that lists these keys and serves these marker bodies."""
    marker_bodies = marker_bodies or {}

    def get_object(Bucket, Key):
        body = json.dumps(marker_bodies.get(Key, {'chunk_ids': []}))
        return {'Body': Mock(read=lambda: body.encode('UTF-8'))}

    s3_client = Mock()
    s3_client.get_paginator.return_value = Mock(
        paginate=lambda Bucket, Prefix: [
            {'Contents': [{'Key': key} for key in keys if key.startswith(Prefix)]}
        ]
    )
    s3_client.download_fileobj.side_effect = (
        lambda bucket, key, stream: stream.write(
            json.dumps(marker_bodies.get(key, {'chunk_ids': []})).encode('UTF-8')
        )
    )
    s3_client.get_object.side_effect = get_object
    return s3_client


def _collection(sources, extra_keys=(), stored=None):
    """
    Keys as the chunk uploader writes them: one object per chunk, and a marker
    naming the chunks it covers. `stored` narrows what actually landed, which
    is what an interrupted run leaves.
    """
    keys = [f'{KEY_PREFIX}/{COLLECTION_ID}/{key}' for key in extra_keys]
    bodies = {}

    for source_id, chunk_ids in sources.items():
        for chunk_id in (stored.get(source_id, chunk_ids) if stored else chunk_ids):
            keys.append(f'{KEY_PREFIX}/{COLLECTION_ID}/{source_id}/{chunk_id}.json')

        marker_key = completion_marker_key(
            f'{KEY_PREFIX}/{COLLECTION_ID}/{source_id}', chunk_ids
        )
        keys.append(marker_key)
        bodies[marker_key] = {'chunk_ids': list(chunk_ids)}

    return _s3(keys, bodies)


def _staged(sources, extra_keys=(), stored=None):
    return staged_source_ids(
        BUCKET, KEY_PREFIX, COLLECTION_ID, _collection(sources, extra_keys, stored)
    )


def _manifest_store(partitions):
    store = Mock()
    store.run_id = RUN_ID
    store.bucket_name = BUCKET
    store.key_prefix = KEY_PREFIX
    store.collection_id = COLLECTION_ID
    store.read_partitions.return_value = partitions
    return store


def _record(partition, state, job_name=None):
    return PartitionRecord(
        partition_id=partition, attempt=1, state=state, job_name=job_name
    )


class TestWhichSourcesAreAlreadyStaged:

    def test_a_source_whose_markers_account_for_its_chunks_counts(self):
        assert _staged({'src-1': ['c1', 'c2']}) == {'src-1'}

    def test_a_source_missing_a_chunk_its_marker_names_does_not(self):
        # What an interrupted run leaves: the marker covers two chunks and one
        # of them never landed. Skipping this source would lose that chunk with
        # nothing to say so.
        assert _staged({'src-1': ['c1', 'c2']}, stored={'src-1': ['c1']}) == set()

    def test_a_source_with_no_marker_does_not(self):
        s3_client = _s3([f'{KEY_PREFIX}/{COLLECTION_ID}/src-1/c1.json'])

        assert staged_source_ids(BUCKET, KEY_PREFIX, COLLECTION_ID, s3_client) == set()

    def test_the_collection_record_and_the_run_directory_are_not_sources(self):
        staged = _staged(
            {'src-1': ['c1']},
            extra_keys=('_staging.json', '_runs/run-1/plan.json', '_runs/run-1/rollup.json'),
        )

        assert staged == {'src-1'}

    def test_several_sources_are_answered_by_one_listing(self):
        s3_client = _collection({'src-1': ['c1'], 'src-2': ['c2'], 'src-3': ['c3']})

        staged = staged_source_ids(BUCKET, KEY_PREFIX, COLLECTION_ID, s3_client)

        assert staged == {'src-1', 'src-2', 'src-3'}
        assert s3_client.get_paginator.call_count == 1, 'one listing, not one call per document'

    def test_the_jsonl_format_is_not_answered_for(self):
        # Its node ids live inside the objects, so a listing cannot account for
        # them and the run stores its documents again.
        s3_client = _collection({'src-1': ['c1']})

        staged = staged_source_ids(
            BUCKET, KEY_PREFIX, COLLECTION_ID, s3_client, for_jsonl=True
        )

        assert staged == set()
        assert not s3_client.get_paginator.called, 'nothing is listed for a format it cannot read'


class TestWhatARestartReportsBeforeItStarts:

    def test_a_run_with_nothing_behind_it_reads_as_new(self):
        report = plan_resume(_manifest_store({}), _s3([]))

        assert not report.is_restart
        assert 'Starting a new run' in report.describe()

    def test_finished_and_unfinished_partitions_are_counted_apart(self):
        store = _manifest_store({
            'done': _record('done', COMPLETE),
            'started': _record('started', SUBMITTED, job_name='extract-topics-run-1-started-a1'),
        })

        report = plan_resume(store, _s3([]))

        assert report.partitions_complete == 1
        assert report.partitions_outstanding == 1
        assert report.outstanding_jobs == ['extract-topics-run-1-started-a1']

    def test_the_staged_sources_come_back_for_the_handler_to_skip(self):
        s3_client = _collection({'src-1': ['c1']})

        report = plan_resume(_manifest_store({}), s3_client)

        assert report.staged_source_ids == {'src-1'}
        assert report.sources_staged == 1

    def test_the_description_says_what_will_be_redone(self):
        store = _manifest_store({'done': _record('done', COMPLETE)})
        s3_client = _collection({'src-1': ['c1']})

        described = plan_resume(store, s3_client).describe()

        assert 'Resuming a run' in described
        assert 'partitions already done: 1' in described
        assert 'sources already staged: 1' in described


class TestTheHandlerARestartStagesThrough:
    """
    The join nothing else covers: the records say what ran, the collection says
    what is stored, and the handler has to be built from both. The pipeline
    cannot do it, because the handler is composed downstream of it.
    """

    def _store(self, s3_client):
        """A real plan store, with only its manifests faked."""
        store = RunPlanStore(
            bucket_name=BUCKET,
            key_prefix=KEY_PREFIX,
            collection_id=COLLECTION_ID,
        )
        store.manifest_store = Mock(return_value=_manifest_store({}))
        return store

    def test_a_source_already_stored_is_skipped_and_the_rest_are_not(self):
        s3_client = _collection({'src-1': ['c1', 'c2'], 'src-2': ['c3']},
                                stored={'src-2': []})

        handler = self._store(s3_client).staging_handler(RUN_ID, s3_client, region=REGION)

        assert handler.skip_source_ids == {'src-1'}

    def test_the_handler_and_the_records_name_one_collection(self):
        # Four ordered steps by hand is four chances to point them at different
        # collections. Derived from the plan store, they cannot disagree.
        s3_client = _collection({'src-1': ['c1']})
        store = self._store(s3_client)

        handler = store.staging_handler(RUN_ID, s3_client, region=REGION)

        assert (handler.bucket_name, handler.key_prefix, handler.collection_id) == (
            BUCKET, KEY_PREFIX, COLLECTION_ID
        )
        store.manifest_store.assert_called_once_with(RUN_ID)

    def test_a_jsonl_handler_is_given_nothing_to_skip(self):
        s3_client = _collection({'src-1': ['c1']})

        handler = self._store(s3_client).staging_handler(RUN_ID, s3_client, region=REGION, for_jsonl=True)

        assert handler.skip_source_ids == set()
        assert handler.for_jsonl is True

    def test_handler_settings_are_passed_through(self):
        s3_client = _collection({'src-1': ['c1']})

        handler = self._store(s3_client).staging_handler(RUN_ID, s3_client, region=REGION, num_threads=3)

        assert handler.num_threads == 3
