# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from contextlib import nullcontext
from types import SimpleNamespace

from benchmarks.scripts.restart_profile import (
    EVERY_DOCUMENT,
    InterruptAfter,
    InterruptedRun,
    profile_name,
    restarts_from_env,
    run_with_restarts,
)


def _factory():
    """A store factory whose context manager yields nothing in particular."""
    return SimpleNamespace(for_graph_store=lambda *a, **k: nullcontext(),
                           for_vector_store=lambda *a, **k: nullcontext())


class _Doc:
    """A document with the one method the staging handler identifies it by."""

    def __init__(self, source_id:str):
        self._source_id = source_id

    def source_id(self):
        return self._source_id

    def __repr__(self):
        return f'_Doc({self._source_id!r})'


def _docs(source_ids) -> list:
    return [_Doc(source_id) for source_id in source_ids]


class _Handler:
    """
    One run's staging handler. It resolves what to skip when it is built and
    never again, and it yields the documents it skipped in place alongside the
    ones it stored, both the way S3BasedDocs does.
    """

    for_jsonl = False

    def __init__(self, collection):
        self.collection = collection
        self.skip_source_ids = set(collection.stored)

    def accept(self, source_documents, **kwargs):
        for doc in source_documents:
            if doc.source_id() not in self.skip_source_ids:
                self.collection.stored.append(doc.source_id())
            yield doc


class _SplittingHandler:
    """
    A handler that yields a source as several documents, the way staging does
    when a source is split across extraction rounds, and yields a document with
    a source id of None for a source nothing was written for.
    """

    for_jsonl = False
    skip_source_ids = frozenset()

    def __init__(self, parts_per_source:int=1, empty:int=0):
        self.parts_per_source = parts_per_source
        self.empty = empty

    def accept(self, source_documents, **kwargs):
        for doc in source_documents:
            for _ in range(self.parts_per_source):
                yield doc
        for _ in range(self.empty):
            yield _Doc(None)


class _Collection:
    """What the runs of one profile stage into, and the handlers they stage through."""

    def __init__(self):
        self.stored = []

    def handler(self) -> _Handler:
        return _Handler(self)


def _extract(collection):
    """An extract that stages every document it is handed, in order."""
    def extract(documents, handler):
        list(handler.accept(documents))
    return extract


class TestHowManyTimesToInterrupt:

    def test_an_unset_profile_is_the_baseline(self, monkeypatch):
        monkeypatch.delenv('BENCHMARK_RESTARTS', raising=False)

        assert restarts_from_env(100) == 0

    def test_a_count_is_taken_as_given(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_RESTARTS', '10')

        assert restarts_from_env(100) == 10

    def test_the_worst_case_interrupts_at_every_document(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_RESTARTS', EVERY_DOCUMENT)

        assert restarts_from_env(100) == 99

    def test_more_interruptions_than_documents_are_capped(self, monkeypatch):
        # A stop after a document the run never reaches would never be met, and
        # the run would not finish.
        monkeypatch.setenv('BENCHMARK_RESTARTS', '500')

        assert restarts_from_env(100) == 99

    def test_something_that_is_neither_is_refused(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_RESTARTS', 'sometimes')

        with pytest.raises(ValueError, match='BENCHMARK_RESTARTS'):
            restarts_from_env(100)

    def test_a_negative_count_is_refused(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_RESTARTS', '-1')

        with pytest.raises(ValueError, match='negative'):
            restarts_from_env(100)


class TestWhatTheProfileIsCalled:

    def test_no_interruption_is_the_baseline(self):
        assert profile_name(0, 100) == 'baseline'

    def test_a_count_names_itself(self):
        assert profile_name(10, 100) == '10-restarts'

    def test_one_per_document_is_the_worst_case(self):
        assert profile_name(99, 100) == EVERY_DOCUMENT

    def test_a_single_interruption_reads_as_one(self):
        assert profile_name(1, 100) == '1-restart'


class TestStoppingARunPartWay:

    def test_it_stops_once_it_has_stored_enough(self):
        collection = _Collection()
        wrapped = InterruptAfter(collection.handler(), after=3)

        with pytest.raises(InterruptedRun):
            list(wrapped.accept(_docs('abcde')))

        assert collection.stored == ['a', 'b', 'c'], 'what it stored before it stopped'
        assert wrapped.stored == 3

    def test_what_it_stored_before_stopping_is_kept(self):
        # The point of the interruption: a restart has to find this and skip it.
        collection = _Collection()

        with pytest.raises(InterruptedRun):
            list(InterruptAfter(collection.handler(), after=2).accept(_docs('abc')))

        assert collection.stored == ['a', 'b']

    def test_documents_a_restart_skips_do_not_count_towards_the_stop(self):
        # The handler yields what it skipped as well as what it stored. Counting
        # everything it yields would stop a restart before it stored anything.
        collection = _Collection()
        collection.stored.extend(['a', 'b', 'c'])
        wrapped = InterruptAfter(collection.handler(), after=2)

        with pytest.raises(InterruptedRun):
            list(wrapped.accept(_docs('abcdef')))

        assert wrapped.stored == 2
        assert collection.stored == ['a', 'b', 'c', 'd', 'e'], 'it stored past the skips'

    def test_a_source_split_across_rounds_counts_once(self):
        # Staging yields a split source as several documents. Counting the
        # documents that arrive would stop the run early and make the per-run
        # counts sum to more than the corpus.
        wrapped = InterruptAfter(_SplittingHandler(parts_per_source=3), after=None)

        list(wrapped.accept(_docs('abcd')))

        assert wrapped.stored == 4, 'four sources, not twelve documents'

    def test_a_source_nothing_was_written_for_is_not_counted(self):
        # Such a document comes back with a source id of None, which is in no
        # skip set and so would otherwise count as stored.
        wrapped = InterruptAfter(_SplittingHandler(empty=3), after=None)

        list(wrapped.accept(_docs('ab')))

        assert wrapped.stored == 2

    def test_a_split_source_does_not_trip_the_stop_early(self):
        wrapped = InterruptAfter(_SplittingHandler(parts_per_source=4), after=2)

        with pytest.raises(InterruptedRun):
            list(wrapped.accept(_docs('abcdef')))

        assert wrapped.stored == 2, 'stopped on the second source, not the second part'

    def test_a_run_with_no_limit_is_counted_but_never_stopped(self):
        collection = _Collection()
        wrapped = InterruptAfter(collection.handler(), after=None)

        assert len(list(wrapped.accept(_docs('abcde')))) == 5
        assert wrapped.stored == 5


class TestARunDrivenToCompletionThroughItsInterruptions:

    def test_the_baseline_runs_once_and_stages_everything(self):
        collection = _Collection()
        docs = _docs('abcdefghij')

        report = run_with_restarts(_extract(collection), collection.handler, docs, restarts=0)

        assert report.runs == 1
        assert report.interruptions == 0
        assert collection.stored == list('abcdefghij')
        assert report.documents_per_run == [10]
        assert report.profile == 'baseline'

    def test_an_interrupted_run_still_stages_every_document(self):
        collection = _Collection()
        docs = _docs('abcdefghij')

        report = run_with_restarts(_extract(collection), collection.handler, docs, restarts=4)

        assert collection.stored == list('abcdefghij'), \
            'the restarts finish what the interruptions left'
        assert report.interruptions == 4
        assert report.runs == 5, 'four interrupted runs and one that completes'
        # Every run stored its own share. A restart that restaged the whole
        # collection, or one stopped by the documents it skipped, would show
        # itself here rather than in the end state, which the last run reaches
        # on its own either way.
        assert report.documents_per_run == [2, 2, 2, 2, 2]
        assert sum(report.documents_per_run) == len(docs), 'nothing stored twice'

    def test_the_worst_case_finishes_too(self):
        collection = _Collection()
        docs = _docs('abcde')

        report = run_with_restarts(_extract(collection), collection.handler, docs, restarts=4)

        assert collection.stored == list('abcde')
        assert report.documents_per_run == [1, 1, 1, 1, 1], 'one document per run'
        assert report.profile == EVERY_DOCUMENT

    def test_first_run_and_restart_time_are_recorded_apart(self):
        # Criterion 3 compares restart time against reprocessing, so the two
        # cannot be reported as one number.
        collection = _Collection()

        report = run_with_restarts(
            _extract(collection), collection.handler, _docs('abcdef'), restarts=2
        )

        assert report.first_run_seconds > 0
        assert report.restart_seconds > 0
        assert report.total_seconds == pytest.approx(
            report.first_run_seconds + report.restart_seconds
        )

    def test_every_run_is_handed_the_whole_document_set(self):
        # A run plan refuses a changed document set as a new run rather than a
        # restart, so a restart cannot be given only what is left.
        seen = []
        collection = _Collection()
        docs = _docs('abcdef')

        def extract(documents, handler):
            seen.append([doc.source_id() for doc in documents])
            list(handler.accept(documents))

        run_with_restarts(extract, collection.handler, docs, restarts=2)

        assert all(run == list('abcdef') for run in seen)
        assert len(seen) == 3

    def test_a_run_that_restages_shows_up_in_the_report(self):
        # The numbers are what says whether a restart resumed or started over,
        # so a run that stages what an earlier one already stored has to be
        # visible in them. Inferring the last run's share from the corpus would
        # absorb the duplicates instead of reporting them.
        collection = _Collection()

        def handler_that_never_skips():
            handler = collection.handler()
            handler.skip_source_ids = set()
            return handler

        report = run_with_restarts(
            _extract(collection), handler_that_never_skips, _docs('abcdef'), restarts=2
        )

        assert report.documents_per_run == [2, 2, 6]
        assert sum(report.documents_per_run) > 6, 'the duplicate staging is on the report'

    def test_more_interruptions_than_documents_is_refused(self):
        # Only restarts_from_env caps this. A direct caller that skipped it
        # would otherwise get a run labelled every-document that is not one.
        collection = _Collection()

        with pytest.raises(ValueError, match='cannot be reached'):
            run_with_restarts(_extract(collection), collection.handler, _docs('ab'), restarts=5)

    def test_the_report_says_which_corpus_and_how_many_stops_were_planned(self, monkeypatch):
        # Two profiles are only comparable if the results say whether the
        # document count is the whole corpus or a slice of it.
        monkeypatch.setenv('BENCHMARK_EXTRACT_DOC_LIMIT', '6')
        collection = _Collection()

        report = run_with_restarts(
            _extract(collection), collection.handler, _docs('abcdef'), restarts=2
        )
        output = report.as_output()

        assert output['extract_doc_limit'] == 6
        assert output['restarts_planned'] == 2
        assert output['documents'] == 6
        assert output['documents_staged_per_run'] == [2, 2, 2]

    def test_each_run_gets_a_handler_of_its_own(self):
        # A staging handler resolves what to skip when it is built. One reused
        # across runs would keep skipping the first run's set and restage
        # everything the runs after it stored.
        built = []
        collection = _Collection()

        def new_handler():
            handler = collection.handler()
            built.append(handler)
            return handler

        run_with_restarts(_extract(collection), new_handler, _docs('abcdef'), restarts=2)

        assert len(built) == 3, 'one handler per run'
        assert [sorted(h.skip_source_ids) for h in built] == [
            [], ['a', 'b'], ['a', 'b', 'c', 'd'],
        ], 'each run skips what the runs before it stored'


class TestHowTheBenchmarkDrivesARestart:
    """
    The wiring in run_benchmark_extract. A restart needs somewhere to read the
    collection back from, and it reports what it cost from the records.
    """

    @staticmethod
    def _wire(monkeypatch, doc_store:str, for_jsonl:bool=False, stages:bool=False):
        """
        Stand every collaborator of run_benchmark_extract up as a fake and
        return what it did, so the wiring below is exercised rather than read.

        With ``stages`` the fake extract stages through the handler it is given,
        which is what drives the restart loop to completion. Without it,
        reaching extraction at all is the failure the refusal tests assert on.
        """
        from benchmarks.scripts import benchmark_extract
        from graphrag_toolkit.lexical_graph.indexing.extract import run_plan

        monkeypatch.setenv('BENCHMARK_DOC_STORE', doc_store)
        monkeypatch.setenv('BENCHMARK_S3_JSONL', 'true' if for_jsonl else 'false')
        monkeypatch.setenv('AWS_REGION_NAME', 'us-west-2')
        monkeypatch.setenv('S3_RESULTS_BUCKET', 'bucket')
        monkeypatch.setenv('S3_RESULTS_PREFIX', 'prefix')
        monkeypatch.setenv('GRAPH_STORE', 'x')
        monkeypatch.setenv('VECTOR_STORE', 'x')
        monkeypatch.delenv('BENCHMARK_EXTRACT_DOC_LIMIT', raising=False)
        monkeypatch.setattr(benchmark_extract, 'sync_benchmark_data_from_s3', lambda *a, **k: None)
        monkeypatch.setattr(benchmark_extract, 'apply_extraction_config', lambda: None)

        loaded = _docs('abcdef')
        collection = _Collection()
        did = SimpleNamespace(
            loaded=loaded,
            collection=collection,
            plan_store_kwargs=None,
            handlers_built=[],
            extract_kwargs=[],
            outputs={},
            s3_client_read_with=[],
        )

        class _Reader:
            def __init__(self, input_dir): pass
            def load_data(self): return loaded

        class _Index:
            def __init__(self, *a, **k): pass

            def extract(self, documents, **kwargs):
                did.extract_kwargs.append(kwargs)
                if not stages:
                    raise AssertionError('must be refused before extracting')
                list(kwargs['handler'].accept(documents))

        class _DocStore:
            """
            The collection, which a run with no restarts stages straight into.
            It skips nothing of its own: only a handler the plan store builds
            carries a skip set.
            """

            collection_id = 'c'
            bucket_name = 'bucket'
            key_prefix = 'prefix/doc-store/ntsb'
            skip_source_ids = frozenset()

            def __init__(self, **kwargs):
                self.for_jsonl = kwargs.get('for_jsonl', False)

            def accept(self, source_documents, **kwargs):
                for doc in source_documents:
                    if doc.source_id() not in collection.stored:
                        collection.stored.append(doc.source_id())
                    yield doc

        class _PlanStore:
            """The plan store the restart branch builds, recording how it was built."""

            def __init__(self, **kwargs):
                did.plan_store_kwargs = kwargs

            def staging_handler(self, run_id, **kwargs):
                did.handlers_built.append((run_id, kwargs))
                return collection.handler()

            def manifest_store(self, run_id):
                def read_partitions(s3_client):
                    did.s3_client_read_with.append(s3_client)
                    return {
                        'p1': SimpleNamespace(job_arn='arn:job-1', attempt=1),
                        'p2': SimpleNamespace(job_arn='arn:job-1', attempt=3),
                        'p3': SimpleNamespace(job_arn='arn:job-2', attempt=1),
                        'p4': SimpleNamespace(job_arn=None, attempt=1),
                    }
                return SimpleNamespace(read_partitions=read_partitions)

        # Imported inside run_benchmark_extract, so it has to be replaced where
        # it is defined rather than on the module that imports it.
        monkeypatch.setattr(run_plan, 'RunPlanStore', _PlanStore)
        monkeypatch.setattr(benchmark_extract, 'SimpleDirectoryReader', _Reader)
        monkeypatch.setattr(benchmark_extract, 'FileBasedDocs',
                            lambda **kw: SimpleNamespace(collection_id='c'))
        monkeypatch.setattr(benchmark_extract, 'S3BasedDocs', _DocStore)
        monkeypatch.setattr(benchmark_extract, 'LexicalGraphIndex', _Index)
        monkeypatch.setattr(benchmark_extract, 'GraphStoreFactory', _factory())
        monkeypatch.setattr(benchmark_extract, 'VectorStoreFactory', _factory())
        monkeypatch.setattr(benchmark_extract, 'GraphRAGConfig', SimpleNamespace(s3='an-s3-client'))
        monkeypatch.setattr(benchmark_extract, '_count_source_docs',
                            lambda docs: len(set(collection.stored)))

        def run():
            benchmark_extract.run_benchmark_extract(
                handler=SimpleNamespace(add_output=did.outputs.__setitem__,
                                        run_assertions=lambda c: None),
                dataset_name='ntsb', data_dir='/tmp', expected_docs=len(loaded),
                use_batch=False,
            )
            return did

        did.run = run

        return run

    def test_a_restart_without_the_s3_doc_store_is_refused(self, monkeypatch):
        # The plan and the partition records live beside the collection, so a
        # file doc store has no collection for a restart to read.
        monkeypatch.setenv('BENCHMARK_RESTARTS', '2')
        run = self._wire(monkeypatch, doc_store='file')

        with pytest.raises(ValueError, match='BENCHMARK_DOC_STORE=s3'):
            run()

    def test_a_restart_of_a_jsonl_collection_is_refused(self, monkeypatch):
        # A resume reads staged source ids from a listing, and JSONL keeps its
        # node ids inside the objects where the listing cannot see them, so
        # every run would restage from the first document and the profile would
        # time duplicate staging while reporting per-run counts that look like
        # progress.
        monkeypatch.setenv('BENCHMARK_RESTARTS', '2')
        run = self._wire(monkeypatch, doc_store='s3', for_jsonl=True)

        with pytest.raises(ValueError, match='BENCHMARK_S3_JSONL'):
            run()

    def test_a_jsonl_run_with_no_restarts_is_left_alone(self, monkeypatch):
        # The refusal is about the restart profile, not about JSONL.
        monkeypatch.delenv('BENCHMARK_RESTARTS', raising=False)
        run = self._wire(monkeypatch, doc_store='s3', for_jsonl=True)

        with pytest.raises(AssertionError, match='must be refused before extracting'):
            run()

    def test_the_restart_branch_wires_the_run_end_to_end(self, monkeypatch):
        # The assertions that follow each cover one join in this branch. They
        # share a run because standing the fakes up is the expensive part and
        # the branch has to be driven to completion either way.
        monkeypatch.setenv('BENCHMARK_RESTARTS', '2')
        did = self._wire(monkeypatch, doc_store='s3', stages=True)()

        # The plan store names the collection being staged into. One pointed at
        # another collection would read one collection and stage into another,
        # and so skip nothing.
        assert did.plan_store_kwargs == {
            'bucket_name': 'bucket',
            'key_prefix': 'prefix/doc-store/ntsb',
            'collection_id': 'c',
        }

        # One handler per run, each built for the same run, and each told
        # whether this collection is JSONL.
        assert len(did.handlers_built) == 3, 'two interrupted runs and one that completes'
        run_ids = {run_id for run_id, _ in did.handlers_built}
        assert len(run_ids) == 1, 'a restart is the same run, not a new one'
        assert all(kwargs['for_jsonl'] is False for _, kwargs in did.handlers_built)
        assert all(kwargs['region'] == 'us-west-2' for _, kwargs in did.handlers_built)

        # Every run carries the run id and the plan store into extract, or the
        # pipeline would divide the documents afresh instead of reading the plan.
        assert len(did.extract_kwargs) == 3
        assert {kwargs['run_id'] for kwargs in did.extract_kwargs} == run_ids
        assert all(kwargs['run_plan_store'] is not None for kwargs in did.extract_kwargs)

        # The run id is published, since it is what names the run to resume.
        assert did.outputs['run_id'] == run_ids.pop()

        # Every number the report carries reaches the results.
        assert did.outputs['restart_profile'] == '2-restarts'
        assert did.outputs['documents'] == 6
        assert did.outputs['runs'] == 3
        assert did.outputs['interruptions'] == 2
        assert did.outputs['documents_staged_per_run'] == [2, 2, 2]
        assert did.outputs['restarts_planned'] == 2

        # The restarts finished the work, and none of it was staged twice.
        assert sorted(did.collection.stored) == list('abcdef')

    def test_the_job_numbers_come_from_the_partition_records(self, monkeypatch):
        # Counting jobs by listing Bedrock needs ListModelInvocationJobs, which
        # operates on no resource and so cannot be scoped to this run's jobs.
        # The records carry the same answer.
        monkeypatch.setenv('BENCHMARK_RESTARTS', '2')
        did = self._wire(monkeypatch, doc_store='s3', stages=True)()

        assert did.outputs['partitions'] == 4
        assert did.outputs['jobs_submitted'] == 2, 'two distinct arns across four partitions'
        assert did.outputs['max_attempt'] == 3
        assert did.s3_client_read_with == ['an-s3-client']

    def test_a_run_with_no_restarts_touches_none_of_the_restart_wiring(self, monkeypatch):
        # A baseline run must not need the plan store, and must stage straight
        # into the doc store rather than through a resuming handler.
        monkeypatch.delenv('BENCHMARK_RESTARTS', raising=False)
        did = self._wire(monkeypatch, doc_store='s3', stages=True)()

        assert did.plan_store_kwargs is None
        assert did.handlers_built == []
        assert len(did.extract_kwargs) == 1
        assert 'run_id' not in did.extract_kwargs[0]
        assert 'run_plan_store' not in did.extract_kwargs[0]
        assert 'restart_profile' not in did.outputs


class TestCountingTheExtractedSources:
    """
    The count the doc-count assertion is made against. A run that records a
    plan keeps it under the collection, where a delimited listing returns it
    beside the source documents.
    """

    @staticmethod
    def _count(prefixes):
        from benchmarks.scripts import benchmark_extract
        from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs

        class _Paginator:
            def paginate(self, **kwargs):
                return [{'CommonPrefixes': [{'Prefix': p} for p in prefixes]}]

        docs = S3BasedDocs.__new__(S3BasedDocs)
        object.__setattr__(docs, '__dict__', {
            'bucket_name': 'bucket', 'key_prefix': 'prefix', 'collection_id': 'c',
        })
        original = benchmark_extract.GraphRAGConfig
        benchmark_extract.GraphRAGConfig = SimpleNamespace(
            s3=SimpleNamespace(get_paginator=lambda name: _Paginator())
        )
        try:
            return benchmark_extract._count_source_docs(docs)
        finally:
            benchmark_extract.GraphRAGConfig = original

    def test_the_run_plan_directory_is_not_a_source_document(self):
        # Without the filter every restart run reports one document too many
        # and the doc-count assertion fails.
        counted = self._count([
            'prefix/c/_runs/',
            'prefix/c/aws::aaa:1111/',
            'prefix/c/aws::bbb:2222/',
        ])

        assert counted == 2

    def test_a_run_without_a_plan_counts_every_prefix(self):
        counted = self._count(['prefix/c/aws::aaa:1111/', 'prefix/c/aws::bbb:2222/'])

        assert counted == 2
