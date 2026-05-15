# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for defaulted parameter fallback to ByoKGConfig.

Verifies that components correctly resolve None parameters from the
centralized ByoKGConfig singleton.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock


class TestKGLinkerDefaults:
    """Tests for KGLinker defaulted parameter behavior."""

    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    def test_llm_generator_none_falls_back_to_config(self, mock_load_yaml):
        """KGLinker with llm_generator=None resolves from ByoKGConfig."""
        mock_load_yaml.return_value = {
            "kg-linker-prompt": {"system-prompt": "s", "user-prompt": "u"},
            "entity-extraction": "task",
            "path-extraction": "task",
            "draft-answer-generation": "task",
            "entity-extraction-iterative": "task",
        }
        mock_graph_store = Mock()
        mock_graph_store.get_linker_tasks.return_value = ["entity-extraction"]

        mock_generator = Mock()
        with patch('graphrag_toolkit.byokg_rag.config.ByoKGConfig.to_generator', return_value=mock_generator):
            from graphrag_toolkit.byokg_rag.graph_connectors.kg_linker import KGLinker
            linker = KGLinker(llm_generator=None, graph_store=mock_graph_store)

        assert linker.llm_generator is mock_generator

    def test_graph_store_none_raises_value_error(self):
        """KGLinker with graph_store=None raises ValueError."""
        from graphrag_toolkit.byokg_rag.graph_connectors.kg_linker import KGLinker
        with pytest.raises(ValueError, match="graph_store is required"):
            KGLinker(llm_generator=Mock(), graph_store=None)


class TestAgenticRetrieverDefaults:
    """Tests for AgenticRetriever defaulted parameter behavior."""

    def test_llm_generator_none_falls_back_to_config(self):
        """AgenticRetriever with llm_generator=None resolves from ByoKGConfig."""
        mock_generator = Mock()
        with patch('graphrag_toolkit.byokg_rag.config.ByoKGConfig.to_generator', return_value=mock_generator):
            from graphrag_toolkit.byokg_rag.graph_retrievers.graph_retrievers import AgenticRetriever
            retriever = AgenticRetriever(
                llm_generator=None,
                graph_traversal=Mock(),
                graph_verbalizer=Mock(),
            )

        assert retriever.llm_generator is mock_generator


class TestBedrockGeneratorDefaults:
    """Tests for BedrockGenerator defaulted parameter behavior."""

    def test_all_none_uses_config_defaults(self):
        """BedrockGenerator with all None params resolves from ByoKGConfig."""
        from graphrag_toolkit.byokg_rag.llm.bedrock_llms import BedrockGenerator
        gen = BedrockGenerator()

        from graphrag_toolkit.byokg_rag.config import ByoKGConfig
        assert gen.model_name == ByoKGConfig.llm_model
        assert gen.max_new_tokens == ByoKGConfig.max_tokens
        assert gen.max_retries == ByoKGConfig.max_retries

    def test_region_name_none_uses_config(self):
        """BedrockGenerator with region_name=None resolves from ByoKGConfig."""
        from graphrag_toolkit.byokg_rag.llm.bedrock_llms import BedrockGenerator
        from graphrag_toolkit.byokg_rag.config import _ByoKGConfig

        config = _ByoKGConfig()
        config.region_name = 'eu-west-1'

        with patch('graphrag_toolkit.byokg_rag.config.ByoKGConfig', config):
            gen = BedrockGenerator(region_name=None)

        assert gen.region_name == 'eu-west-1'


class TestGenerateLLMResponseDefaults:
    """Tests for generate_llm_response defaulted client behavior."""

    @patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.boto3.client')
    def test_client_none_creates_boto3_client(self, mock_boto3_client):
        """generate_llm_response with client=None creates a new boto3 client."""
        mock_client = Mock()
        mock_client.converse.return_value = {
            'output': {'message': {'content': [{'text': 'response'}]}}
        }
        mock_boto3_client.return_value = mock_client

        from graphrag_toolkit.byokg_rag.llm.bedrock_llms import generate_llm_response
        result = generate_llm_response(
            region_name='us-west-2',
            model_id='test-model',
            system_prompt='sys',
            query='test',
            max_tokens=100,
            max_retries=1,
            client=None,
        )

        assert result == 'response'
        mock_boto3_client.assert_called_once_with('bedrock-runtime', region_name='us-west-2')
