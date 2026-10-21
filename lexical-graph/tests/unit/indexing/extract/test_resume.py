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
import logging
import threading
import time

import pytest
from unittest.mock import Mock, PropertyMock, patch

from graphrag_toolkit.lexical_graph.indexing.extract.resume import (
    ResumeReport,
    plan_resume,
    staged_source_ids,
)
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.extract import RunPlanStore
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import S3ChunkUploader, completion_marker_key
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
    """
    A mock S3 that lists these keys and serves these marker bodies: bytes as
    stored, anything else as JSON.
    """
    marker_bodies = marker_bodies or {}

    def body_of(key):
        body = marker_bodies.get(key, {'chunk_ids': []})
        return body if isinstance(body, bytes) else json.dumps(body).encode('UTF-8')

    def get_object(Bucket, Key):
        body = body_of(Key)
        return {'Body': Mock(read=lambda: body)}

    s3_client = Mock()
    s3_client.get_paginator.return_value = Mock(
        paginate=lambda Bucket, Prefix: [
            {'Contents': [{'Key': key} for key in keys if key.startswith(Prefix)]}
        ]
    )
    s3_client.download_fileobj.side_effect = (
        lambda bucket, key, stream: stream.write(body_of(key))
    )
    s3_client.get_object.side_effect = get_object
    return s3_client


def _as_written(chunk_ids) -> bytes:
    """The marker the chunk uploader stores on ending a source with these chunks."""
    s3_client = Mock()
    S3ChunkUploader(bucket_name=BUCKET, collection_prefix=f'{KEY_PREFIX}/{COLLECTION_ID}')._write_completion_marker(
        'root', list(chunk_ids), s3_client, source_chunk_ids=list(chunk_ids)
    )
    return s3_client.put_object.call_args.kwargs['Body']


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

    def test_the_markers_are_read_at_once_rather_than_one_source_at_a_time(self):
        # Each read waits for all three to start, so serial reads never finish.
        s3_client = _collection({'src-1': ['c1'], 'src-2': ['c2'], 'src-3': ['c3']})
        serve = s3_client.download_fileobj.side_effect
        all_started = threading.Barrier(3, timeout=5)

        def download_fileobj(bucket, key, stream):
            all_started.wait()
            serve(bucket, key, stream)

        s3_client.download_fileobj.side_effect = download_fileobj

        staged = staged_source_ids(BUCKET, KEY_PREFIX, COLLECTION_ID, s3_client, num_threads=3)

        assert staged == {'src-1', 'src-2', 'src-3'}

    def test_no_more_markers_are_read_at_once_than_the_thread_count(self):
        s3_client = _collection({f'src-{n}': [f'c{n}'] for n in range(8)})
        serve = s3_client.download_fileobj.side_effect
        lock = threading.Lock()
        running, most = 0, 0

        def download_fileobj(bucket, key, stream):
            nonlocal running, most
            with lock:
                running += 1
                most = max(most, running)
            time.sleep(0.01)
            serve(bucket, key, stream)
            with lock:
                running -= 1

        s3_client.download_fileobj.side_effect = download_fileobj

        staged = staged_source_ids(BUCKET, KEY_PREFIX, COLLECTION_ID, s3_client, num_threads=2)

        assert len(staged) == 8
        assert most == 2

    def test_a_marker_that_cannot_be_read_leaves_its_source_to_be_staged_again(self):
        s3_client = _collection({'src-1': ['c1'], 'src-2': ['c2'], 'src-3': ['c3']})
        serve = s3_client.download_fileobj.side_effect

        def download_fileobj(bucket, key, stream):
            if '/src-2/' in key:
                raise RuntimeError('service unavailable')
            serve(bucket, key, stream)

        s3_client.download_fileobj.side_effect = download_fileobj

        staged = staged_source_ids(BUCKET, KEY_PREFIX, COLLECTION_ID, s3_client, num_threads=2)

        assert staged == {'src-1', 'src-3'}

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

    def test_a_new_run_over_a_populated_collection_is_not_called_a_restart(self):
        # Staging keeps no run id, so the staged sources are the collection's.
        # Only this run's own records make it a restart.
        report = plan_resume(_manifest_store({}), _collection({'src-1': ['c1']}))

        assert not report.is_restart
        assert 'Starting a run over a collection with sources already staged' in report.describe()
        assert 'sources already staged in the collection: 1' in report.describe()

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
        assert 'sources already staged in the collection: 1' in described


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

    def _handler(self, store, s3_client, **handler_kwargs):
        with patch.object(type(GraphRAGConfig), 's3', new_callable=PropertyMock, return_value=s3_client):
            return store.staging_handler(RUN_ID, region=REGION, **handler_kwargs)

    def test_a_source_already_stored_is_skipped_and_the_rest_are_not(self):
        s3_client = _collection({'src-1': ['c1', 'c2'], 'src-2': ['c3']},
                                stored={'src-2': []})

        handler = self._handler(self._store(s3_client), s3_client)

        assert handler.skip_source_ids == {'src-1'}

    def test_the_handler_and_the_records_name_one_collection(self):
        # Four ordered steps by hand is four chances to point them at different
        # collections. Derived from the plan store, they cannot disagree.
        s3_client = _collection({'src-1': ['c1']})
        store = self._store(s3_client)

        handler = self._handler(store, s3_client)

        assert (handler.bucket_name, handler.key_prefix, handler.collection_id) == (
            BUCKET, KEY_PREFIX, COLLECTION_ID
        )
        store.manifest_store.assert_called_once_with(RUN_ID)

    def test_a_jsonl_handler_is_given_nothing_to_skip(self):
        s3_client = _collection({'src-1': ['c1']})

        handler = self._handler(self._store(s3_client), s3_client, for_jsonl=True)

        assert handler.skip_source_ids == set()
        assert handler.for_jsonl is True

    def test_handler_settings_are_passed_through(self):
        s3_client = _collection({'src-1': ['c1']})

        handler = self._handler(self._store(s3_client), s3_client, num_threads=3)

        assert handler.num_threads == 3


class TestWhatARestartTrusts:
    """
    Markers are unsigned and name no run, so a restart takes any marker that
    accounts for the chunks beside it, whoever wrote both. Write access to the
    collection prefix is the boundary; these pin the behaviour inside it.
    """

    PLANTED = 'src-planted'

    def _planted(self, chunk_ids, stored=None, body=None):
        """
        A source no run staged: chunk objects, and a marker as the writer
        stores one unless body says otherwise. stored is what is actually
        under the prefix.
        """
        root = f'{KEY_PREFIX}/{COLLECTION_ID}/{self.PLANTED}'
        marker_key = completion_marker_key(root, chunk_ids)
        keys = [f'{root}/{chunk_id}.json' for chunk_id in (chunk_ids if stored is None else stored)]
        return _s3([*keys, marker_key], {marker_key: _as_written(chunk_ids) if body is None else body})

    def _skipped(self, s3_client, partitions=None):
        """What staging_handler leaves alone, for a run with these partition records."""
        store = RunPlanStore(bucket_name=BUCKET, key_prefix=KEY_PREFIX, collection_id=COLLECTION_ID)
        store.manifest_store = Mock(return_value=_manifest_store(partitions or {}))

        with patch.object(type(GraphRAGConfig), 's3', new_callable=PropertyMock, return_value=s3_client):
            return store.staging_handler(RUN_ID, region=REGION).skip_source_ids

    @pytest.mark.parametrize('partitions', [
        {},
        {'done': _record('done', COMPLETE)},
    ], ids=['a new run id', 'a restart'])
    def test_a_source_no_run_staged_is_skipped_when_its_marker_accounts_for_it(self, partitions):
        # Documented trust, not a defect: a run id in the marker would not stop
        # this, since nothing about the chunks is signed either.
        assert self._skipped(self._planted(['c1', 'c2']), partitions) == {self.PLANTED}

    def test_a_chunk_added_beside_a_marker_is_not_skipped(self):
        assert self._skipped(self._planted(['c1', 'c2'], stored=['c1', 'c2', 'c3'])) == set()

    @pytest.mark.parametrize('body', [
        b'{"chunk_ids": [',
        b'["c1", "c2"]',
        json.dumps({'chunk_ids': 'c1,c2'}).encode('UTF-8'),
        json.dumps({'chunk_ids': ['c1', 'c2'], 'final': 'true'}).encode('UTF-8'),
        b'{}',
        json.dumps({'chunk_ids': [], 'final': True}).encode('UTF-8'),
        json.dumps({'chunk_ids': ['c1', 'c2'], 'count': 99, 'final': True}).encode('UTF-8'),
    ], ids=['not JSON', 'not an object', 'chunk ids not a list', 'final not a boolean',
            'empty object', 'empty chunk ids', 'count disagrees'])
    def test_a_marker_the_writer_could_not_have_written_leaves_its_source_to_be_staged_again(self, body, caplog):
        s3_client = self._planted(['c1', 'c2'], body=body)

        with caplog.at_level(logging.WARNING):
            skipped = self._skipped(s3_client)

        assert skipped == set()
        marker_key = completion_marker_key(f'{KEY_PREFIX}/{COLLECTION_ID}/{self.PLANTED}', ['c1', 'c2'])
        assert any(
            marker_key in record.getMessage() and 'Delete the marker' in record.getMessage()
            for record in caplog.records
        ), 'the warning names the marker and says what clears it'
