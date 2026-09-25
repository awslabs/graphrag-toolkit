# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import MagicMock, Mock, patch
from llama_index.core.schema import Document, NodeRelationship, RelatedNodeInfo, TextNode
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_pipeline import (
    PassThroughDecorator,
    ExtractionPipeline,
    _document_id,
    _in_plan_order,
    _json_carriable,
)
from graphrag_toolkit.lexical_graph.indexing.extract.run_plan import RunPlan, RunPlanMismatch
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument


class TestPassThroughDecoratorInitialization:
    """Tests for PassThroughDecorator initialization."""
    
    def test_initialization(self):
        """Verify PassThroughDecorator initializes correctly."""
        decorator = PassThroughDecorator()
        assert decorator is not None


class TestPassThroughDecoratorHandleInputDocs:
    """Tests for handle_input_docs method."""
    
    def test_handle_input_docs_returns_iterable(self):
        """Verify handle_input_docs returns iterable."""
        decorator = PassThroughDecorator()
        docs = [SourceDocument(refNode=Document(text="test"))]
        
        result = decorator.handle_input_docs(docs)
        
        assert hasattr(result, '__iter__')
    
    def test_handle_input_docs_preserves_documents(self):
        """Verify handle_input_docs preserves documents."""
        decorator = PassThroughDecorator()
        docs = [
            SourceDocument(refNode=Document(text="doc1")),
            SourceDocument(refNode=Document(text="doc2"))
        ]
        
        result = list(decorator.handle_input_docs(docs))
        
        assert len(result) == 2
        assert result[0].refNode.text == "doc1"
        assert result[1].refNode.text == "doc2"
    
    def test_handle_input_docs_with_empty_list(self):
        """Verify handle_input_docs handles empty list."""
        decorator = PassThroughDecorator()
        
        result = list(decorator.handle_input_docs([]))
        
        assert result == []


class TestPassThroughDecoratorHandleOutputDoc:
    """Tests for handle_output_doc method."""
    
    def test_handle_output_doc_returns_document(self):
        """Verify handle_output_doc returns document."""
        decorator = PassThroughDecorator()
        doc = SourceDocument(refNode=Document(text="test"))
        
        result = decorator.handle_output_doc(doc)
        
        assert isinstance(result, SourceDocument)
    
    def test_handle_output_doc_preserves_document(self):
        """Verify handle_output_doc preserves document."""
        decorator = PassThroughDecorator()
        doc = SourceDocument(
            refNode=Document(text="original"),
            nodes=[TextNode(text="chunk")]
        )
        
        result = decorator.handle_output_doc(doc)
        
        assert result.refNode.text == "original"
        assert len(result.nodes) == 1
        assert result.nodes[0].text == "chunk"


class TestExtractionPipelineInitialization:
    """Tests for ExtractionPipeline initialization."""
    
    def test_initialization_with_empty_components(self):
        """Verify ExtractionPipeline initializes with empty components."""
        pipeline = ExtractionPipeline(
            components=[],
            decorator=PassThroughDecorator()
        )
        
        assert pipeline is not None
    
    def test_initialization_with_decorator(self):
        """Verify ExtractionPipeline initializes with decorator."""
        decorator = PassThroughDecorator()
        pipeline = ExtractionPipeline(
            components=[],
            decorator=decorator
        )
        
        assert pipeline is not None


class TestExtractionPipelineCreate:
    """Tests for ExtractionPipeline.create factory method."""
    
    def test_create_with_empty_components(self):
        """Verify create works with empty components."""
        pipeline = ExtractionPipeline.create(
            components=[],
            decorator=PassThroughDecorator()
        )
        
        assert pipeline is not None
    

class TestExtractionPipelineExtract:
    """Tests for extract method."""
    
    def test_extract_with_documents(self):
        """Verify extract processes documents."""
        pipeline = ExtractionPipeline(
            components=[],
            decorator=PassThroughDecorator()
        )
        
        docs = [Document(text="test document")]
        result = list(pipeline.extract(docs))
        
        assert isinstance(result, list)
        assert len(result) > 0
    
    def test_extract_with_empty_input(self):
        """Verify extract handles empty input."""
        pipeline = ExtractionPipeline(
            components=[],
            decorator=PassThroughDecorator()
        )
        
        result = list(pipeline.extract([]))
        
        assert isinstance(result, list)
        assert len(result) == 0
    

class TestExtractionPipelineSourceDocumentsConversion:
    """Tests for _source_documents_from_base_nodes method."""
    


class TestExtractionPipelineIntegration:
    """Integration tests for ExtractionPipeline."""
    
    def test_full_pipeline_flow(self):
        """Verify complete pipeline flow."""
        decorator = PassThroughDecorator()
        pipeline = ExtractionPipeline(
            components=[],
            decorator=decorator
        )
        
        docs = [Document(text="Test document for pipeline")]
        result = list(pipeline.extract(docs))
        
        assert len(result) > 0
        assert all(isinstance(sd, SourceDocument) for sd in result)
    
    def test_pipeline_with_multiple_documents(self):
        """Verify pipeline handles multiple documents."""
        pipeline = ExtractionPipeline(
            components=[],
            decorator=PassThroughDecorator()
        )
        
        docs = [
            Document(text=f"Document {i}")
            for i in range(5)
        ]
        
        result = list(pipeline.extract(docs))
        
        assert len(result) >= 5


PIPELINE = 'graphrag_toolkit.lexical_graph.indexing.extract.extraction_pipeline'


class TestPartitionsAndProcessesAreCountedApart:
    """
    The worker count decides how the input is divided and how many processes
    run the pieces. A restart on a smaller host has to divide as the original
    run did while running fewer processes.
    """

    def _pipeline(self, cores, **kwargs):
        with patch(f'{PIPELINE}.multiprocessing.cpu_count', return_value=cores):
            return ExtractionPipeline(components=[], batch_size=8, **kwargs)

    def _divide(self, pipeline, docs):
        """The pieces the fixed-batch path hands to run_pipeline, and its process count."""
        seen = {}

        def capture(pipeline_, node_batches, num_workers=1, **kwargs):
            seen['pieces'] = [[n.node_id for n in batch] for batch in node_batches]
            seen['processes'] = num_workers
            return []

        with patch(f'{PIPELINE}.run_pipeline', side_effect=capture):
            list(pipeline.extract(docs))
        return seen

    def test_a_fresh_run_caps_both_at_the_core_count(self):
        pipeline = self._pipeline(cores=2, num_workers=8)

        assert (pipeline.num_workers, pipeline.num_processes) == (2, 2)

    def test_a_fresh_run_below_the_core_count_is_unchanged(self):
        pipeline = self._pipeline(cores=8, num_workers=4)

        assert (pipeline.num_workers, pipeline.num_processes) == (4, 4)

    def test_a_pinned_partition_count_outlives_a_smaller_host(self):
        pipeline = self._pipeline(cores=2, num_workers=2, partition_workers=8)

        assert (pipeline.num_workers, pipeline.num_processes) == (8, 2)

    def test_a_smaller_host_divides_the_input_as_the_original_run_did(self):
        docs = [Document(text=f'document {i}', id_=f'doc-{i}') for i in range(8)]

        original = self._divide(self._pipeline(cores=8, num_workers=8), docs)
        restarted = self._divide(self._pipeline(cores=2, num_workers=2, partition_workers=8), docs)

        assert restarted['pieces'] == original['pieces']


class TestARunFollowsThePlanItWroteDown:
    """
    A run records how it divided its input so a restart divides it the same
    way, instead of submitting different jobs and paying for the work twice.
    """

    def _store(self, returns=None):
        """A plan store that hands back what it was given, or a recorded plan."""
        store = MagicMock()
        store.resolve.side_effect = lambda plan: returns if returns is not None else plan
        return store

    def _pipeline(self, cores, **kwargs):
        with patch(f'{PIPELINE}.multiprocessing.cpu_count', return_value=cores):
            return ExtractionPipeline(components=[], batch_size=8, **kwargs)

    def _divide(self, pipeline, docs):
        seen = {}

        def capture(pipeline_, node_batches, num_workers=1, **kwargs):
            seen.setdefault('pieces', []).extend([n.node_id for n in batch] for batch in node_batches)
            seen['processes'] = num_workers
            return []

        with patch(f'{PIPELINE}.run_pipeline', side_effect=capture):
            list(pipeline.extract(docs))
        return seen

    def _docs(self, count=8):
        return [Document(text=f'document {i}', id_=f'doc-{i}') for i in range(count)]

    def test_a_run_without_a_run_id_asks_for_no_plan(self):
        store = self._store()
        pipeline = self._pipeline(cores=4, num_workers=4, run_plan_store=store)

        self._divide(pipeline, self._docs())

        assert store.resolve.call_count == 0

    def test_the_plan_names_documents_by_their_source_not_their_first_chunk(self):
        # A collection read back from staging arrives already chunked, where the
        # first chunk's id is not the document's.
        chunk = TextNode(text='a chunk', id_='aws::doc-a:chunk-0')
        chunk.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id='aws::doc-a')

        assert _document_id(SourceDocument(nodes=[chunk])) == 'aws::doc-a'

    def test_a_document_that_is_its_own_node_is_named_by_that_node(self):
        assert _document_id(SourceDocument(nodes=[TextNode(text='a doc', id_='aws::doc-a')])) == 'aws::doc-a'

    def test_the_recorded_batch_size_divides_a_restart(self):
        recorded = RunPlan(run_id='run-1', document_ids=[], num_workers=4, batch_size=2)
        pipeline = self._pipeline(cores=4, num_workers=4, run_id='run-1', run_plan_store=self._store(recorded))
        rounds = []

        def capture(pipeline_, node_batches, num_workers=1, **kwargs):
            rounds.append(len(list(node_batches)))
            return []

        with patch(f'{PIPELINE}.run_pipeline', side_effect=capture):
            list(pipeline.extract(self._docs(8)))

        # batch_size 2 over 8 documents is four rounds, not the pipeline's one.
        assert len(rounds) == 4

    def test_a_run_id_with_nowhere_to_keep_its_plan_is_refused(self):
        pipeline = self._pipeline(cores=4, num_workers=4, run_id='run-1')

        with pytest.raises(ValueError, match='run_plan_store'):
            self._divide(pipeline, self._docs())

    def test_the_plan_is_resolved_before_any_work_is_submitted(self):
        store = self._store()
        pipeline = self._pipeline(cores=4, num_workers=4, run_id='run-1', run_plan_store=store)
        order = []

        store.resolve.side_effect = lambda plan: order.append('plan') or plan

        def capture(pipeline_, node_batches, num_workers=1, **kwargs):
            order.append('work')
            return []

        with patch(f'{PIPELINE}.run_pipeline', side_effect=capture):
            list(pipeline.extract(self._docs()))

        assert order[0] == 'plan'

    def test_the_plan_records_the_documents_in_the_order_they_are_extracted(self):
        store = self._store()
        pipeline = self._pipeline(cores=4, num_workers=4, run_id='run-1', run_plan_store=store)

        divided = self._divide(pipeline, self._docs())

        planned = store.resolve.call_args.args[0].document_ids
        extracted = [node_id for piece in divided['pieces'] for node_id in piece]
        assert planned == extracted

    def test_the_plan_records_the_settings_the_run_started_with(self):
        store = self._store()
        pipeline = self._pipeline(cores=4, num_workers=4, run_id='run-1', run_plan_store=store)

        self._divide(pipeline, self._docs())

        plan = store.resolve.call_args.args[0]
        assert (plan.run_id, plan.num_workers, plan.batch_size) == ('run-1', 4, 8)

    def test_the_recorded_worker_count_divides_a_restart_on_a_smaller_host(self):
        docs = self._docs()

        original = self._divide(self._pipeline(cores=8, num_workers=8), docs)

        recorded = RunPlan(
            run_id='run-1',
            document_ids=[],
            num_workers=8,
            batch_size=8,
        )
        restarted = self._divide(
            self._pipeline(cores=2, num_workers=2, run_id='run-1', run_plan_store=self._store(recorded)),
            docs,
        )

        assert restarted['pieces'] == original['pieces']
        assert restarted['processes'] == 2
        assert (original['processes'], restarted['processes']) == (8, 2)

    def test_documents_sharing_an_id_are_each_extracted_once(self):
        # The id comes from the content, so two copies of one text share it.
        first = SourceDocument(refNode=Document(text='same', id_='doc-a'))
        second = SourceDocument(refNode=Document(text='same', id_='doc-a'))
        other = SourceDocument(refNode=Document(text='other', id_='doc-b'))

        ordered = _in_plan_order([first, other, second], ['doc-a', 'doc-b', 'doc-a'])

        assert [id(d) for d in ordered] == [id(first), id(other), id(second)]


class TestWhatAPlanRefusesToRecord:
    """
    A plan that named a document badly would refuse the restart it exists to
    allow, so the naming fails now rather than the restart later.
    """

    def test_a_document_with_no_nodes_is_refused(self):
        with pytest.raises(ValueError, match='no nodes'):
            _document_id(SourceDocument(nodes=[]))

    def test_a_document_the_plan_names_but_the_collection_lacks_is_refused(self):
        present = SourceDocument(refNode=Document(text='a', id_='doc-a'))

        with pytest.raises(RunPlanMismatch, match='doc-b'):
            _in_plan_order([present], ['doc-a', 'doc-b'])

    def test_a_setting_json_cannot_carry_is_left_out_of_the_plan(self):
        from datetime import datetime

        carried = _json_carriable({'model': 'm', 'tags': {'a', 'b'}, 'since': datetime(2026, 1, 1), 'workers': 2})

        assert carried == {'model': 'm', 'workers': 2}
