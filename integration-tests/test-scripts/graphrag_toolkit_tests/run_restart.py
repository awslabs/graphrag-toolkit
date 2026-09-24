# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import patch
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase, delete_prefix
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex, GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils import node_batcher
from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import is_completion_marker, list_collection
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.extract.resume import plan_resume
from graphrag_toolkit.lexical_graph.indexing.extract.run_plan import RunPlanMismatch, RunPlanStore
from graphrag_toolkit.lexical_graph import IndexingConfig

from llama_index.core.schema import Document

PIPELINE = 'graphrag_toolkit.lexical_graph.indexing.extract.extraction_pipeline'


class Coordinates:
    """The bucket, collection and run one restart test works in."""

    def __init__(self, bucket_name, key_prefix, collection_id, run_id, region, store):
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix
        self.collection_id = collection_id
        self.run_id = run_id
        self.region = region
        self.store = store

    @property
    def collection_path(self):
        return f'{self.key_prefix}/{self.collection_id}/'

    def docs(self, count):
        return [
            Document(id_=f'test-{i}', text=f'Boggles can live for up to {i+10} years')
            for i in range(0, count)
        ]

    def handler(self):
        """
        Where this run stages, skipping whatever it already holds whole.

        The store builds it, which is the call the feature offers an operator,
        so the resume test exercises that rather than a handler assembled by
        hand.
        """
        return self.store.staging_handler(self.run_id, GraphRAGConfig.s3, region=self.region)

    def reader(self):
        """
        The collection, with nothing skipped.

        Reading it back is one use. The other is staging for a test whose
        subject is the plan rather than the resume: a handler that skipped
        what an earlier run staged would answer for the collection instead of
        the run under test.
        """
        return S3BasedDocs(
            region=self.region,
            bucket_name=self.bucket_name,
            key_prefix=self.key_prefix,
            collection_id=self.collection_id
        )


class RestartTest(IntegrationTestBase):
    """
    Ground shared by the restart tests: a collection of their own, a run id,
    and the extraction settings put back as they were found.

    The suite runs its tests in one process, so a test that left
    extraction_batch_size where it set it would divide every test after it.
    """

    @contextmanager
    def _collection(self, params, name, collection_prefix):
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')

        original_batch_size = GraphRAGConfig.extraction_batch_size
        original_num_workers = GraphRAGConfig.extraction_num_workers

        GraphRAGConfig.extraction_batch_size = 2
        GraphRAGConfig.extraction_num_workers = 2

        bucket_name = os.environ['S3_RESULTS_BUCKET']
        key_prefix = f"{os.environ['S3_RESULTS_PREFIX']}/{name}"
        collection_id = f'{collection_prefix}-{uuid.uuid4().hex[:8]}'
        run_id = f'run{uuid.uuid4().hex[:8]}'

        # The path segment carries a hyphen; the results JSON has always keyed
        # these with an underscore.
        recorded_as = name.replace('-', '_')

        params[f'{recorded_as}.collection_id'] = collection_id
        params[f'{recorded_as}.run_id'] = run_id
        params[f'{recorded_as}.key_prefix'] = key_prefix

        coordinates = Coordinates(
            bucket_name=bucket_name,
            key_prefix=key_prefix,
            collection_id=collection_id,
            run_id=run_id,
            region=os.environ['AWS_REGION_NAME'],
            store=RunPlanStore(
                bucket_name=bucket_name,
                key_prefix=key_prefix,
                collection_id=collection_id
            ),
        )

        try:
            with(
                GraphStoreFactory.for_graph_store(
                    os.environ['GRAPH_STORE'],
                    log_formatting=NonRedactedGraphQueryLogFormatting()
                ) as graph_store,
                VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
            ):
                yield coordinates, graph_store, vector_store
        finally:
            GraphRAGConfig.extraction_batch_size = original_batch_size
            GraphRAGConfig.extraction_num_workers = original_num_workers
            delete_prefix(bucket_name, coordinates.collection_path)


class ExtractWithRunPlan(RestartTest):

    @property
    def description(self):
        return 'Record how a run divides its input, and follow that plan on a second run'

    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):

        with self._collection(params, 'run-plan', 'rp') as (run, graph_store, vector_store):

            run_id = run.run_id
            run_plan_store = run.store
            graph_index = LexicalGraphIndex(graph_store, vector_store)

            def extract(graph_index, docs):
                graph_index.extract(
                    docs,
                    handler=run.reader(),
                    show_progress=True,
                    run_id=run_id,
                    run_plan_store=run_plan_store
                )

            docs = run.docs(4)

            divided_into = []

            def recording_node_batcher(num_batches, nodes):
                divided_into.append(num_batches)
                return node_batcher(num_batches=num_batches, nodes=nodes)

            # The plan object cannot answer this: it is written once and
            # never rewritten, so reading it twice says nothing about what
            # the second run did. What the run divided its input into does.
            with patch(f'{PIPELINE}.node_batcher', side_effect=recording_node_batcher):
                extract(graph_index, docs)

            first_run_divided_into = list(divided_into)
            divided_into.clear()

            plan_1 = run_plan_store.read(run_id, GraphRAGConfig.s3)

            # A restart on a smaller host offers fewer workers. The recorded
            # count is what decides the division, so the plan must not move.
            GraphRAGConfig.extraction_num_workers = 1

            with patch(f'{PIPELINE}.node_batcher', side_effect=recording_node_batcher):
                extract(graph_index, docs)

            second_run_divided_into = list(divided_into)

            plan_2 = run_plan_store.read(run_id, GraphRAGConfig.s3)

            # Both refusals are caught: an exception that escapes _run_test
            # means run_assertions never runs and the test records FAIL with
            # nothing to read.
            added_document = None
            try:
                extract(graph_index, docs + [
                    Document(id_='test-4', text='Boggles can live for up to 14 years')
                ])
            except RunPlanMismatch as e:
                added_document = str(e)

            changed_batch_size = None
            try:
                GraphRAGConfig.extraction_batch_size = 3
                extract(graph_index, docs)
            except RunPlanMismatch as e:
                changed_batch_size = str(e)

            # Restart does not cover auto-tuning for this release, and the
            # refusal is raised before any extraction, so this asks for a
            # batch extractor without paying for one.
            auto_tuned_index = LexicalGraphIndex(
                graph_store,
                vector_store,
                indexing_config=IndexingConfig(
                    batch_config=BatchConfig(
                        region=run.region,
                        bucket_name=run.bucket_name,
                        key_prefix=f'{run.collection_path}batch-inference',
                        role_arn=os.environ['BATCH_INFERENCE_ROLE'],
                        max_batch_size=250,
                        auto_tune=True
                    )
                )
            )

            auto_tune_refused = None
            try:
                auto_tuned_index.extract(
                    docs,
                    handler=run.reader(),
                    run_id=run_id,
                    run_plan_store=run_plan_store
                )
            except ValueError as e:
                auto_tune_refused = str(e)

            staged = run.reader()

            class RunPlanAssertions(unittest.TestCase):

                @classmethod
                def setUpClass(cls):
                    cls._plan_1_document_ids = plan_1.document_ids
                    cls._plan_1_num_workers = plan_1.num_workers
                    cls._plan_1_batch_size = plan_1.batch_size
                    cls._plan_2_num_workers = plan_2.num_workers
                    cls._plan_2_document_ids = plan_2.document_ids
                    cls._first_run_divided_into = first_run_divided_into
                    cls._second_run_divided_into = second_run_divided_into
                    cls._offered_num_workers = 1
                    cls._added_document = added_document
                    cls._changed_batch_size = changed_batch_size
                    cls._auto_tune_refused = auto_tune_refused
                    cls._staged_source_ids = sorted(d.source_id() for d in staged)
                    cls._expected_num_docs = len(docs)

                def test_the_first_run_records_the_documents_it_divided(self):
                    """Run plan names one document id for each document extracted"""

                    self.assertEqual(len(self._plan_1_document_ids), self._expected_num_docs)

                def test_the_first_run_records_the_settings_that_divide_them(self):
                    """Run plan records the batch size and worker count the run started with"""

                    self.assertEqual(self._plan_1_batch_size, 2)
                    self.assertEqual(self._plan_1_num_workers, 2)

                def test_the_second_run_divides_its_input_as_the_first_did(self):
                    """Restart divides into the recorded number of pieces, not the number it offered"""

                    self.assertEqual(
                        self._second_run_divided_into, self._first_run_divided_into
                    )
                    self.assertNotIn(self._offered_num_workers, self._second_run_divided_into)

                def test_the_collection_holds_every_document(self):
                    """Collection holds one source document per input document"""

                    self.assertEqual(len(self._staged_source_ids), self._expected_num_docs)

                def test_a_changed_document_set_is_refused(self):
                    """Restarting a run id with a document added fails, naming the document"""

                    # The ids in the message are the rewritten content
                    # hashes, not the ids the documents arrived with, so
                    # this pins the direction rather than the literal: one
                    # document added, none gone.
                    self.assertIsNotNone(self._added_document)
                    self.assertIn('no longer present: []', self._added_document)
                    self.assertRegex(self._added_document, r"not in the plan: \['aws::[^']+'\]")

                def test_a_changed_batch_size_is_refused(self):
                    """Restarting a run id with a changed batch size fails, naming the setting"""

                    self.assertIsNotNone(self._changed_batch_size)
                    self.assertIn('batch_size', self._changed_batch_size)

                def test_a_run_plan_is_refused_on_the_auto_tuned_path(self):
                    """Asking for a run plan and auto-tuning together fails, naming the path"""

                    self.assertIsNotNone(self._auto_tune_refused)
                    self.assertIn('auto-tuned path', self._auto_tune_refused)

            handler.run_assertions(RunPlanAssertions)


NUM_DOCS = 6


def objects_under(bucket_name, prefix):
    """Every object under a prefix, and when it was last written."""
    pages = GraphRAGConfig.s3.get_paginator('list_objects_v2').paginate(
        Bucket=bucket_name, Prefix=prefix
    )

    return {obj['Key']: obj['LastModified'] for page in pages for obj in page.get('Contents', [])}


class ResumeInterruptedExtraction(RestartTest):

    @property
    def description(self):
        return 'Resume an interrupted extraction, leaving the sources it already stored alone'

    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):

        with self._collection(params, 'run-resume', 'rr') as (run, graph_store, vector_store):

            collection_path = run.collection_path
            run_id = run.run_id
            run_plan_store = run.store

            def staged_source_ids():
                """
                The sources the collection holds, as the reader sees them.

                list_collection delimits on '/' and drops the run directory, so
                this says what a source document prefix is without guessing at
                the shape of an id.
                """
                prefixes, _ = list_collection(
                    run.bucket_name, run.key_prefix, run.collection_id,
                    GraphRAGConfig.s3.get_paginator('list_objects_v2')
                )
                return sorted(prefix[len(collection_path):].strip('/') for prefix in prefixes)

            docs = run.docs(NUM_DOCS)

            graph_index = LexicalGraphIndex(graph_store, vector_store)

            # Stages all six sources. The deletes below construct the state
            # an interrupted run leaves behind.
            graph_index.extract(
                docs,
                handler=run.handler(),
                show_progress=True,
                run_id=run_id,
                run_plan_store=run_plan_store
            )

            after_first_run = objects_under(run.bucket_name, collection_path)

            staged_sources = staged_source_ids()

            # Two shapes of interruption, and they are not the same check.
            # A source with no marker is refused because nothing closes it.
            # A source that keeps its marker but lost a chunk is refused by
            # the comparison the marker exists for, and only this second
            # case reaches it.
            unmarked_source = staged_sources[-1]
            short_source = staged_sources[-2]
            damaged_sources = sorted([unmarked_source, short_source])
            intact_sources = staged_sources[:-2]

            doomed = [
                key for key in after_first_run
                if f'{collection_path}{unmarked_source}/' in key
                and is_completion_marker(key)
            ]
            doomed += [
                key for key in sorted(after_first_run)
                if f'{collection_path}{short_source}/' in key
                and not is_completion_marker(key)
            ][:1]

            GraphRAGConfig.s3.delete_objects(
                Bucket=run.bucket_name,
                Delete={'Objects': [{'Key': key} for key in doomed]}
            )

            report = plan_resume(run_plan_store.manifest_store(run_id), GraphRAGConfig.s3)

            resuming = run.handler()

            graph_index.extract(
                docs,
                handler=resuming,
                show_progress=True,
                run_id=run_id,
                run_plan_store=run_plan_store
            )

            after_restart = objects_under(run.bucket_name, collection_path)

            # The JSONL format keeps its node ids inside the objects, where
            # a listing cannot reach them, so it is answered with nothing.
            jsonl_handler = run_plan_store.staging_handler(
                run_id, GraphRAGConfig.s3, region=run.region, for_jsonl=True
            )

            handler.add_output('resume_report', report.describe())

            class ResumeAssertions(unittest.TestCase):

                @classmethod
                def setUpClass(cls):
                    cls._skipped = sorted(resuming.skip_source_ids or [])
                    cls._intact_sources = intact_sources
                    cls._damaged_sources = damaged_sources
                    cls._unmarked_source = unmarked_source
                    cls._short_source = short_source
                    cls._intact_keys = [
                        key for key in after_first_run
                        if any(f'{collection_path}{s}/' in key for s in intact_sources)
                    ]
                    cls._rewritten_intact_keys = [
                        key for key in cls._intact_keys
                        if after_restart.get(key) != after_first_run[key]
                    ]
                    cls._damaged_markers_after = [
                        key for key in after_restart
                        if any(f'{collection_path}{s}/' in key for s in damaged_sources)
                        and is_completion_marker(key)
                    ]
                    cls._damaged_chunks_before = len([
                        key for key in after_first_run
                        if any(f'{collection_path}{s}/' in key for s in damaged_sources)
                        and not is_completion_marker(key)
                    ])
                    cls._damaged_chunks_after = len([
                        key for key in after_restart
                        if any(f'{collection_path}{s}/' in key for s in damaged_sources)
                        and not is_completion_marker(key)
                    ])
                    cls._sources_after = staged_source_ids()
                    cls._jsonl_skipped = sorted(jsonl_handler.skip_source_ids or [])
                    cls._report = report.describe()

                def test_a_source_stored_whole_is_skipped(self):
                    """Restart skips the sources its earlier attempt stored whole"""

                    self.assertEqual(self._skipped, self._intact_sources)

                def test_a_source_left_unmarked_is_not_skipped(self):
                    """Restart does not skip a source whose marker the interruption removed"""

                    self.assertNotIn(self._unmarked_source, self._skipped)

                def test_a_source_short_of_a_chunk_its_marker_names_is_not_skipped(self):
                    """Restart does not skip a source whose marker names a chunk that is gone"""

                    self.assertNotIn(self._short_source, self._skipped)

                def test_a_skipped_source_is_not_written_again(self):
                    """Objects of a skipped source keep the timestamps the first run gave them"""

                    self.assertEqual(self._rewritten_intact_keys, [])

                def test_the_unfinished_source_is_completed(self):
                    """Restart stores the unfinished sources again, marker included"""

                    self.assertEqual(
                        len(self._damaged_markers_after), len(self._damaged_sources)
                    )

                def test_the_chunk_the_interruption_removed_comes_back(self):
                    """Restart restores every chunk of a source it stages again, not just its marker"""

                    self.assertEqual(self._damaged_chunks_after, self._damaged_chunks_before)

                def test_the_collection_holds_every_source_after_the_restart(self):
                    """Collection holds every input document once the restart finishes"""

                    self.assertEqual(len(self._sources_after), NUM_DOCS)

                def test_the_jsonl_format_skips_nothing(self):
                    """A JSONL handler is given nothing to skip, because a listing cannot prove it"""

                    self.assertEqual(self._jsonl_skipped, [])

                def test_the_report_says_what_the_restart_will_reuse(self):
                    """Resume report names the run as resuming and counts what it will leave alone"""

                    self.assertIn('Resuming a run', self._report)
                    self.assertIn(
                        f'sources already staged: {len(self._intact_sources)}', self._report
                    )

            handler.run_assertions(ResumeAssertions)
