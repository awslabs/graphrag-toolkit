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
