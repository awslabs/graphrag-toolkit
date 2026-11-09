# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from unittest.mock import Mock, patch
import pytest

from llama_index.core.llms import LLM
from llama_index.core.llms.mock import MockLLM
from llama_index.llms.bedrock_converse import BedrockConverse

from graphrag_toolkit.lexical_graph import ExtractionConfig
from graphrag_toolkit.lexical_graph.lexical_graph_index import LexicalGraphIndex
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache

class TestExtractionConfig:

    def test_uninitialized_extraction_llm_returns_none(self):
        extraction_config = ExtractionConfig()
        assert extraction_config.extraction_llm is None 

    def test_extraction_lmm_configured_with_llm_returns_llm(self): 
        llm = MockLLM()

        extraction_config = ExtractionConfig(
            extraction_llm = llm
        )

        assert extraction_config.extraction_llm == llm

    def test_extraction_lmm_configured_with_model_name_returns_bedrock_converse_llm(self):
        with patch.dict(os.environ, {'AWS_REGION': 'us-west-2'}):
            extraction_config = ExtractionConfig(
                extraction_llm = 'anthropic.claude-v2'
            )

        assert isinstance(extraction_config.extraction_llm, BedrockConverse)

    def test_extraction_lmm_configured_with_llm_cache_returns_llm_cache(self):
        llm = MockLLM()
        llm_cache = LLMCache(llm=llm)

        extraction_config = ExtractionConfig(
            extraction_llm = llm_cache
        )

        assert extraction_config.extraction_llm == llm_cache

class TestLexicalGraphIndex:

    def test_init_invokes_graph_store_init_hook(self):
        graph_store = Mock()
        vector_store = Mock()

        with (
            patch(
                'graphrag_toolkit.lexical_graph.lexical_graph_index.GraphStoreFactory.for_graph_store',
                return_value=graph_store,
            ),
            patch(
                'graphrag_toolkit.lexical_graph.lexical_graph_index.MultiTenantGraphStore.wrap',
                return_value=graph_store,
            ),
            patch(
                'graphrag_toolkit.lexical_graph.lexical_graph_index.VectorStoreFactory.for_vector_store',
                return_value=vector_store,
            ),
            patch(
                'graphrag_toolkit.lexical_graph.lexical_graph_index.MultiTenantVectorStore.wrap',
                return_value=vector_store,
            ),
            patch.object(LexicalGraphIndex, '_configure_extraction_pipeline', return_value=([], [])),
        ):
            LexicalGraphIndex(graph_store='dummy://', vector_store='dummy://')

        graph_store.init.assert_called_once_with()

    def test_init_propagates_graph_store_init_failure(self):
        graph_store = Mock()
        graph_store.init.side_effect = RuntimeError("Graph store init failed")
        vector_store = Mock()

        with (
            patch(
                'graphrag_toolkit.lexical_graph.lexical_graph_index.GraphStoreFactory.for_graph_store',
                return_value=graph_store,
            ),
            patch(
                'graphrag_toolkit.lexical_graph.lexical_graph_index.MultiTenantGraphStore.wrap',
                return_value=graph_store,
            ),
            patch(
                'graphrag_toolkit.lexical_graph.lexical_graph_index.VectorStoreFactory.for_vector_store',
                return_value=vector_store,
            ),
            patch(
                'graphrag_toolkit.lexical_graph.lexical_graph_index.MultiTenantVectorStore.wrap',
                return_value=vector_store,
            ),
            patch.object(LexicalGraphIndex, '_configure_extraction_pipeline', return_value=([], [])),
        ):
            with pytest.raises(RuntimeError, match="Graph store init failed"):
                LexicalGraphIndex(graph_store='dummy://', vector_store='dummy://')

        graph_store.init.assert_called_once_with()


class TestRestartParameters:
    """run_id and run_plan_store are named parameters of extract() and
    extract_and_build(), and reach the extraction pipeline only. The build
    pipeline has no use for them."""

    MODULE = 'graphrag_toolkit.lexical_graph.lexical_graph_index'

    @staticmethod
    def _index():
        with (
            patch(f'{TestRestartParameters.MODULE}.GraphStoreFactory.for_graph_store', return_value=Mock()),
            patch(f'{TestRestartParameters.MODULE}.MultiTenantGraphStore.wrap', return_value=Mock()),
            patch(f'{TestRestartParameters.MODULE}.VectorStoreFactory.for_vector_store', return_value=Mock()),
            patch(f'{TestRestartParameters.MODULE}.MultiTenantVectorStore.wrap', return_value=Mock()),
            patch.object(LexicalGraphIndex, '_configure_extraction_pipeline', return_value=([], [])),
        ):
            return LexicalGraphIndex(graph_store='dummy://', vector_store='dummy://')

    def _extract(self, **kwargs):
        index = self._index()

        with (
            patch.object(LexicalGraphIndex, '_source_id_width', return_value=None),
            patch(f'{self.MODULE}.ExtractionPipeline.create') as extraction_create,
            patch(f'{self.MODULE}.BuildPipeline.create') as build_create,
        ):
            index.extract([], **kwargs)

        return extraction_create.call_args.kwargs, build_create.call_args.kwargs

    def test_extract_passes_the_restart_parameters_to_the_extraction_pipeline(self):
        run_plan_store = Mock()

        extraction_kwargs, build_kwargs = self._extract(run_id='a-run', run_plan_store=run_plan_store)

        assert extraction_kwargs['run_id'] == 'a-run'
        assert extraction_kwargs['run_plan_store'] is run_plan_store
        assert 'run_id' not in build_kwargs
        assert 'run_plan_store' not in build_kwargs

    def test_extract_without_restart_parameters_passes_none(self):
        extraction_kwargs, _ = self._extract()

        assert extraction_kwargs['run_id'] is None
        assert extraction_kwargs['run_plan_store'] is None

    def _extract_and_build(self, **kwargs):
        index = self._index()

        with (
            patch(f'{self.MODULE}.read_stored_source_id_width', return_value=None),
            patch.object(LexicalGraphIndex, '_source_id_width', return_value=None),
            patch(f'{self.MODULE}.GraphConstruction.for_graph_store', return_value=Mock()),
            patch(f'{self.MODULE}.VectorIndexing.for_vector_store', return_value=Mock()),
            patch(f'{self.MODULE}.ExtractionPipeline.create') as extraction_create,
            patch(f'{self.MODULE}.BuildPipeline.create') as build_create,
        ):
            index.extract_and_build([], enable_versioning=False, **kwargs)

        return extraction_create.call_args.kwargs, build_create.call_args.kwargs

    def test_extract_and_build_passes_the_restart_parameters_to_the_extraction_pipeline(self):
        run_plan_store = Mock()

        extraction_kwargs, build_kwargs = self._extract_and_build(run_id='a-run', run_plan_store=run_plan_store)

        assert extraction_kwargs['run_id'] == 'a-run'
        assert extraction_kwargs['run_plan_store'] is run_plan_store
        assert 'run_id' not in build_kwargs
        assert 'run_plan_store' not in build_kwargs

    def test_extract_and_build_without_restart_parameters_passes_none(self):
        extraction_kwargs, _ = self._extract_and_build()

        assert extraction_kwargs['run_id'] is None
        assert extraction_kwargs['run_plan_store'] is None
