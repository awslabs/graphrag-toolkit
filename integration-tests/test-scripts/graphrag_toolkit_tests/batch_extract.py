# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
import uuid
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase, delete_prefix
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph import GraphRAGConfig, IndexingConfig, ExtractionConfig, BuildConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs, JSONArrayReader
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig, InferClassificationsConfig
from graphrag_toolkit.lexical_graph.indexing.extract.run_manifest import COMPLETE, MAX_JOB_NAME
from graphrag_toolkit.lexical_graph.indexing.extract.run_plan import RunPlanStore
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import RUN_ARTIFACT_DIR

def get_text(data):
    return f"Title: {data.get('title', '')}\nCategory: {data.get('category', '')}\nAuthor: {data.get('author', '')}\nSource: {data.get('source', '')}\nPublished At: {data.get('published_at', '')}\nURL: {data.get('url', '')}\n\n{data.get('body', '')}"

def get_metadata(data):
    metadata = {}
    metadata['title'] = data.get('title', None)
    metadata['author'] = data.get('author', None)
    metadata['source'] = data.get('source', None)
    metadata['published_at'] = data.get('published_at', None)
    metadata['url'] = data.get('url', None)
    metadata['category'] = data.get('category', None)
    return metadata

def apply_extraction_doc_limit(docs):
    """Optionally cap the number of source documents used for extraction.

    Controlled by the BENCHMARK_EXTRACT_DOC_LIMIT environment variable (set
    directly, via .env / .env.testing, or through the build-tests.sh
    --benchmark-extract-doc-limit flag). When it is a positive integer, only
    the first N documents are extracted and the rest are skipped; when it is
    unset, empty, or non-positive, all documents are extracted as normal.

    Capping the input list here — before extract() is called — is worker-count
    agnostic: extraction concurrency (num_workers) operates on whatever
    documents remain in the list, so this behaves identically for single- and
    multi-threaded runs. Downstream assertions and the batch_build.BuildFromS3
    step read len(docs) after capping, so expected counts stay consistent.
    """
    raw_limit = os.environ.get('BENCHMARK_EXTRACT_DOC_LIMIT', '').strip()
    if not raw_limit:
        return docs

    try:
        limit = int(raw_limit)
    except ValueError:
        raise ValueError(
            f"BENCHMARK_EXTRACT_DOC_LIMIT must be an integer, but got '{raw_limit}'"
        )

    if limit <= 0 or limit >= len(docs):
        return docs

    print(f'[benchmark] BENCHMARK_EXTRACT_DOC_LIMIT set: extracting first {limit} of {len(docs)} documents')
    return docs[:limit]

class BatchExtractToS3(IntegrationTestBase):
    
    @property
    def description(self):
        return ('Baseline batch extraction: extract propositions and topics from the local docs '
                'corpus (source-data/corpus-modified.json) using Bedrock batch inference with a fixed '
                'batch_size (100), save to S3; output feeds batch_build.BuildFromS3')
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')
        GraphRAGConfig.extraction_batch_size = 100
        GraphRAGConfig.extraction_num_workers = 2

        s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        s3_results_prefix = os.environ['S3_RESULTS_PREFIX']
        aws_region_name = os.environ['AWS_REGION_NAME']
        batch_inference_role = os.environ['BATCH_INFERENCE_ROLE']
        batch_inference_prefix = f'{s3_results_prefix}/batch-inference'
        extracted_prefix = f'{s3_results_prefix}/extracted'
         
        extracted_docs = S3BasedDocs(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=extracted_prefix
        )
        
        infer_config = InferClassificationsConfig(
            num_samples=5,
            num_iterations=10
        )

        batch_config = BatchConfig(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=batch_inference_prefix,
            role_arn=batch_inference_role,
            max_batch_size=250,
            max_num_concurrent_batches=2
        )
    
        indexing_config = IndexingConfig(
            extraction=ExtractionConfig(
                infer_entity_classifications=infer_config,
            ),   
            build=BuildConfig(
                include_local_entities=True
            ),
            batch_config=batch_config
        )
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        

            reader = JSONArrayReader(text_fn=get_text, metadata_fn=get_metadata)
            docs = reader.load_data('./source-data/corpus-modified.json')
            docs = apply_extraction_doc_limit(docs)

            graph_index = LexicalGraphIndex(
                graph_store, 
                vector_store,
                indexing_config=indexing_config
            )
            
            graph_index.extract(docs, handler=extracted_docs, show_progress=True)
            
            collection_id = extracted_docs.collection_id
            
            params['batch_collection_id'] = collection_id
            params['multihop_expected_num_batch_docs'] = len(docs)
            
            class BatchExtractAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_extracted_docs = len([d for d in extracted_docs])
                    cls._expected_num_docs = len(docs)
            
                def test_extracted_one_doc_for_each_url(self):
                    """Extracted directory in S3 contains one source doc per source URL"""
                    
                    self.assertEqual(self._num_extracted_docs, self._expected_num_docs)
                    
            handler.run_assertions(BatchExtractAssertions)
        
class BatchExtractAutoTuneToS3(IntegrationTestBase):

    @property
    def description(self):
        return ('Like BatchExtractToS3 over the same local corpus, but auto-tunes batch_size '
                '(auto_tune=True; user sets only num_workers + max_batch_size, no fixed batch_size or '
                'max_num_concurrent_batches); writes to the same extracted/ prefix so output feeds '
                'batch_build.BuildFromS3')

    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):

        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')
        # Under auto-tuning, batch_size is a derived value; the user only needs
        # to specify num_workers (here) and max_batch_size (on BatchConfig).
        GraphRAGConfig.extraction_num_workers = 2

        s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        s3_results_prefix = os.environ['S3_RESULTS_PREFIX']
        aws_region_name = os.environ['AWS_REGION_NAME']
        batch_inference_role = os.environ['BATCH_INFERENCE_ROLE']
        batch_inference_prefix = f'{s3_results_prefix}/batch-inference-auto-tune'
        extracted_prefix = f'{s3_results_prefix}/extracted'

        extracted_docs = S3BasedDocs(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=extracted_prefix
        )

        infer_config = InferClassificationsConfig(
            num_samples=5,
            num_iterations=10
        )

        # Under auto-tuning the user specifies only num_workers (above) and
        # max_batch_size; batch_size is derived. max_num_concurrent_batches is
        # not used in auto-tuning mode (num_workers is the unit of concurrency).
        batch_config = BatchConfig(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=batch_inference_prefix,
            role_arn=batch_inference_role,
            max_batch_size=250,
            auto_tune=True
        )

        indexing_config = IndexingConfig(
            extraction=ExtractionConfig(
                infer_entity_classifications=infer_config,
            ),
            build=BuildConfig(
                include_local_entities=True
            ),
            batch_config=batch_config
        )

        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):

            reader = JSONArrayReader(text_fn=get_text, metadata_fn=get_metadata)
            docs = reader.load_data('./source-data/corpus-modified.json')
            docs = apply_extraction_doc_limit(docs)

            graph_index = LexicalGraphIndex(
                graph_store,
                vector_store,
                indexing_config=indexing_config
            )

            graph_index.extract(docs, handler=extracted_docs, show_progress=True)

            collection_id = extracted_docs.collection_id

            # Use the same param keys as BatchExtractToS3 so that batch_build.BuildFromS3
            # picks up this collection and its expected doc count as the next step.
            params['batch_collection_id'] = collection_id
            params['multihop_expected_num_batch_docs'] = len(docs)

            class BatchExtractAutoTuneAssertions(unittest.TestCase):

                @classmethod
                def setUpClass(cls):
                    cls._num_extracted_docs = len([d for d in extracted_docs])
                    cls._expected_num_docs = len(docs)

                def test_extracted_one_doc_for_each_url(self):
                    """Auto-tuned extraction produces one source doc per source URL,
                    equivalent to the fixed-batch_size run over the same corpus."""

                    self.assertEqual(self._num_extracted_docs, self._expected_num_docs)

            handler.run_assertions(BatchExtractAutoTuneAssertions)


class BatchExtractWithRunPlanToS3(IntegrationTestBase):
    """
    Batch extract under a run plan, then restart the same run id.

    The restart is the point: every partition already has a completed job, so
    the second pass downloads what the first paid for instead of submitting
    again.
    """

    @property
    def description(self):
        return 'Batch extract under a run plan, then restart it without paying for the jobs twice'

    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):

        # apply_extraction_doc_limit can leave a partition under Bedrock's
        # 100-record floor, and extraction then runs the non-batch path: no
        # job, no record, and assertions that read like a restart bug rather
        # than the configuration that caused them.
        if os.environ.get('BENCHMARK_EXTRACT_DOC_LIMIT', '').strip():
            print('BENCHMARK_EXTRACT_DOC_LIMIT is set, so there may be no batch job to reuse; skipping test')
            handler.skip()
            return

        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')
        GraphRAGConfig.extraction_batch_size = 100
        GraphRAGConfig.extraction_num_workers = 2

        s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        s3_results_prefix = os.environ['S3_RESULTS_PREFIX']
        aws_region_name = os.environ['AWS_REGION_NAME']
        batch_inference_role = os.environ['BATCH_INFERENCE_ROLE']
        batch_inference_prefix = f'{s3_results_prefix}/batch-inference-run-plan'
        extracted_prefix = f'{s3_results_prefix}/extracted'

        collection_id = f'bp{uuid.uuid4().hex[:8]}'
        run_id = f'run{uuid.uuid4().hex[:8]}'

        run_plan_store = RunPlanStore(
            bucket_name=s3_results_bucket,
            key_prefix=extracted_prefix,
            collection_id=collection_id
        )

        batch_config = BatchConfig(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=batch_inference_prefix,
            role_arn=batch_inference_role,
            max_batch_size=250,
            max_num_concurrent_batches=2
        )

        indexing_config = IndexingConfig(
            extraction=ExtractionConfig(
                infer_entity_classifications=InferClassificationsConfig(
                    num_samples=5,
                    num_iterations=10
                ),
            ),
            build=BuildConfig(
                include_local_entities=True
            ),
            batch_config=batch_config
        )

        def jobs_for_this_run():
            """
            The Bedrock jobs this run has submitted, by the name the records
            give them.

            Paginated: one page holds forty, and a run with more partitions
            than that would compare two truncated counts and call them equal.
            """
            pages = GraphRAGConfig.bedrock.get_paginator('list_model_invocation_jobs').paginate(
                nameContains=run_id
            )
            return sorted(
                job['jobName']
                for page in pages for job in page.get('invocationJobSummaries', [])
            )

        def objects_staged():
            pages = GraphRAGConfig.s3.get_paginator('list_objects_v2').paginate(
                Bucket=s3_results_bucket, Prefix=f'{extracted_prefix}/{collection_id}/'
            )
            return {
                obj['Key']: obj['LastModified']
                for page in pages for obj in page.get('Contents', [])
                if f'/{RUN_ARTIFACT_DIR}/' not in obj['Key']
            }

        try:

            with(
                GraphStoreFactory.for_graph_store(
                    os.environ['GRAPH_STORE'],
                    log_formatting=NonRedactedGraphQueryLogFormatting()
                ) as graph_store,
                VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
            ):

                reader = JSONArrayReader(text_fn=get_text, metadata_fn=get_metadata)
                docs = reader.load_data('./source-data/corpus-modified.json')
                docs = apply_extraction_doc_limit(docs)

                graph_index = LexicalGraphIndex(
                    graph_store,
                    vector_store,
                    indexing_config=indexing_config
                )

                # Submits the Bedrock jobs. Its handler skips nothing, because
                # nothing is stored yet, and the same call builds it either way.
                graph_index.extract(
                    docs,
                    handler=run_plan_store.staging_handler(
                        run_id, GraphRAGConfig.s3, region=aws_region_name
                    ),
                    show_progress=True,
                    run_id=run_id,
                    run_plan_store=run_plan_store
                )

                manifest_store = run_plan_store.manifest_store(run_id)

                records_after_first_run = manifest_store.read_partitions(GraphRAGConfig.s3)
                leftover_partition_keys = manifest_store.list_partition_keys(GraphRAGConfig.s3)
                jobs_after_first_run = jobs_for_this_run()
                staged_after_first_run = objects_staged()

                # The restart. Every partition is complete and every source stored,
                # so this submits nothing and writes nothing.
                graph_index.extract(
                    docs,
                    handler=run_plan_store.staging_handler(
                        run_id, GraphRAGConfig.s3, region=aws_region_name
                    ),
                    show_progress=True,
                    run_id=run_id,
                    run_plan_store=run_plan_store
                )

                jobs_after_restart = jobs_for_this_run()
                staged_after_restart = objects_staged()
                records_after_restart = manifest_store.read_partitions(GraphRAGConfig.s3)

                staged_docs = S3BasedDocs(
                    region=aws_region_name,
                    bucket_name=s3_results_bucket,
                    key_prefix=extracted_prefix,
                    collection_id=collection_id
                )

                # Nothing downstream reads these. They go into the results JSON so
                # the collection and run behind a failure can be found in S3, and
                # they use their own keys because batch_build.BuildFromS3 and the
                # multi-hop queries read what the auto-tuned test wrote.
                params['run_plan_batch_collection_id'] = collection_id
                params['run_plan_batch_run_id'] = run_id

                class BatchRunPlanAssertions(unittest.TestCase):

                    @classmethod
                    def setUpClass(cls):
                        cls._num_extracted_docs = len([d for d in staged_docs])
                        cls._expected_num_docs = len(docs)
                        cls._partition_states = sorted(
                            {r.state for r in records_after_first_run.values()}
                        )
                        cls._num_partitions = len(records_after_first_run)
                        cls._job_names = jobs_after_first_run
                        cls._jobs_after_restart = jobs_after_restart
                        cls._leftover_partition_keys = leftover_partition_keys
                        cls._attempts_after_restart = sorted(
                            {r.attempt for r in records_after_restart.values()}
                        )
                        cls._rewritten_keys = [
                            key for key, modified in staged_after_first_run.items()
                            if staged_after_restart.get(key) != modified
                        ]

                    def test_extracted_one_doc_for_each_source(self):
                        """Extracted collection holds one source doc per input document"""

                        self.assertEqual(self._num_extracted_docs, self._expected_num_docs)

                    def test_every_partition_is_recorded_complete(self):
                        """Each partition the run divided its input into is recorded complete"""

                        self.assertGreater(self._num_partitions, 0)
                        self.assertEqual(self._partition_states, [COMPLETE])

                    def test_a_job_name_says_which_run_it_belongs_to(self):
                        """Every Bedrock job this run submitted carries the run id and an attempt"""

                        self.assertGreater(len(self._job_names), 0)
                        for job_name in self._job_names:
                            self.assertIn(run_id, job_name)
                            self.assertRegex(job_name, r'-a\d+')
                            self.assertLessEqual(len(job_name), MAX_JOB_NAME)

                    def test_the_records_are_rolled_up_when_the_run_ends(self):
                        """Completed partition records are folded into the rollup"""

                        self.assertEqual(self._leftover_partition_keys, [])

                    def test_a_restart_submits_no_new_jobs(self):
                        """Restarting the same run id submits no further Bedrock jobs"""

                        self.assertEqual(self._jobs_after_restart, self._job_names)

                    def test_a_restart_does_not_raise_the_attempt_count(self):
                        """A partition whose job completed is not attempted a second time"""

                        self.assertEqual(self._attempts_after_restart, [1])

                    def test_a_restart_stages_nothing_again(self):
                        """Objects the first run stored keep the timestamps it gave them"""

                        self.assertEqual(self._rewritten_keys, [])

                handler.run_assertions(BatchRunPlanAssertions)

            # Only once the assertions have passed: on the failure path the
            # job's input and output are what there is to diagnose from.
            delete_prefix(s3_results_bucket, f'{batch_inference_prefix}/')

        finally:
            # The staged collection goes on every path. Nothing downstream
            # reads it, and it is the larger of the two.
            delete_prefix(s3_results_bucket, f'{extracted_prefix}/{collection_id}/')
