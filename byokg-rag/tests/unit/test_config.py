# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import pytest
from unittest.mock import patch, MagicMock
from botocore import exceptions as botocore_exceptions
from graphrag_toolkit.byokg_rag.config import _ByoKGConfig, ByoKGConfig, ResilientClient


class TestByoKGConfigSingleton:
    def test_singleton_instance_exists(self):
        assert ByoKGConfig is not None
        assert isinstance(ByoKGConfig, _ByoKGConfig)

    def test_singleton_is_same_instance(self):
        from graphrag_toolkit.byokg_rag.config import ByoKGConfig as config2
        assert ByoKGConfig is config2


class TestByoKGConfigDefaults:
    def setup_method(self):
        self.config = _ByoKGConfig()

    def test_default_llm_model(self):
        with patch.dict(os.environ, {}, clear=True):
            assert self.config.llm_model is not None and len(self.config.llm_model) > 0

    def test_default_region_is_none(self):
        with patch.dict(os.environ, {}, clear=True):
            assert self.config.region_name is None

    def test_default_embed_model(self):
        with patch.dict(os.environ, {}, clear=True):
            assert self.config.embed_model is not None and len(self.config.embed_model) > 0

    def test_default_embed_dimensions(self):
        with patch.dict(os.environ, {}, clear=True):
            assert self.config.embed_dimensions == 1024

    def test_default_reranking_model(self):
        with patch.dict(os.environ, {}, clear=True):
            assert self.config.reranking_model is not None and len(self.config.reranking_model) > 0

    def test_default_max_tokens(self):
        with patch.dict(os.environ, {}, clear=True):
            assert self.config.max_tokens == 4096

    def test_default_max_retries(self):
        with patch.dict(os.environ, {}, clear=True):
            assert self.config.max_retries == 10


class TestByoKGConfigEnvVars:
    def test_llm_model_from_env(self):
        config = _ByoKGConfig()
        with patch.dict(os.environ, {'BYOKG_LLM_MODEL': 'custom-model'}):
            assert config.llm_model == 'custom-model'

    def test_region_from_byokg_env(self):
        config = _ByoKGConfig()
        with patch.dict(os.environ, {'BYOKG_REGION': 'eu-west-1'}):
            assert config.region_name == 'eu-west-1'

    def test_embed_model_from_env(self):
        config = _ByoKGConfig()
        with patch.dict(os.environ, {'BYOKG_EMBED_MODEL': 'custom-embed'}):
            assert config.embed_model == 'custom-embed'

    def test_embed_dimensions_from_env(self):
        config = _ByoKGConfig()
        with patch.dict(os.environ, {'BYOKG_EMBED_DIMENSIONS': '512'}):
            assert config.embed_dimensions == 512

    def test_reranking_model_from_env(self):
        config = _ByoKGConfig()
        with patch.dict(os.environ, {'BYOKG_RERANKING_MODEL': 'custom-reranker'}):
            assert config.reranking_model == 'custom-reranker'

    def test_max_tokens_from_env(self):
        config = _ByoKGConfig()
        with patch.dict(os.environ, {'BYOKG_MAX_TOKENS': '8192'}):
            assert config.max_tokens == 8192

    def test_max_retries_from_env(self):
        config = _ByoKGConfig()
        with patch.dict(os.environ, {'BYOKG_MAX_RETRIES': '5'}):
            assert config.max_retries == 5


class TestByoKGConfigSetters:
    def test_set_llm_model(self):
        config = _ByoKGConfig()
        config.llm_model = 'new-model'
        assert config.llm_model == 'new-model'

    def test_set_region_clears_clients(self):
        config = _ByoKGConfig()
        config._aws_clients['test'] = 'dummy'
        config.region_name = 'us-west-2'
        assert config.region_name == 'us-west-2'
        assert len(config._aws_clients) == 0


class TestByoKGConfigFactoryMethods:
    def test_to_generator_returns_bedrock_generator(self):
        config = _ByoKGConfig()
        config.region_name = 'us-east-1'
        gen = config.to_generator()
        from graphrag_toolkit.byokg_rag.llm.bedrock_llms import BedrockGenerator
        assert isinstance(gen, BedrockGenerator)
        assert gen.model_name == config.llm_model
        assert gen.max_new_tokens == config.max_tokens
        assert gen.max_retries == config.max_retries

    def test_to_generator_with_overrides(self):
        config = _ByoKGConfig()
        config.region_name = 'us-east-1'
        gen = config.to_generator(model_name='custom-model', max_tokens=2048)
        assert gen.model_name == 'custom-model'
        assert gen.max_new_tokens == 2048

    def test_to_embedding_returns_embedding(self):
        config = _ByoKGConfig()
        with patch('langchain_aws.BedrockEmbeddings'):
            emb = config.to_embedding()
            from graphrag_toolkit.byokg_rag.indexing.embedding import BedrockEmbedding
            assert isinstance(emb, BedrockEmbedding)

    def test_to_reranker_returns_reranker(self):
        config = _ByoKGConfig()
        reranker = config.to_reranker(device='cpu')
        from graphrag_toolkit.byokg_rag.graph_retrievers.graph_reranker import LocalGReranker
        assert isinstance(reranker, LocalGReranker)
        assert reranker.model_name == config.reranking_model


class TestByoKGConfigSession:
    def test_session_creation(self):
        config = _ByoKGConfig()
        config.region_name = 'us-east-1'
        session = config.session
        assert session is not None

    def test_session_cached(self):
        config = _ByoKGConfig()
        config.region_name = 'us-east-1'
        s1 = config.session
        s2 = config.session
        assert s1 is s2


class TestResilientClient:
    def test_successful_call_passthrough(self):
        """ResilientClient passes calls through to underlying client."""
        config = _ByoKGConfig()
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        config._boto3_session = mock_session
        rc = ResilientClient(config, 'bedrock-runtime')
        mock_client.some_method.return_value = 'result'
        assert rc.some_method() == 'result'

    def test_credential_expiry_triggers_refresh(self):
        """ResilientClient refreshes client on ExpiredToken error."""
        config = _ByoKGConfig()
        mock_client_old = MagicMock()
        mock_client_new = MagicMock()

        error_response = {'Error': {'Code': 'ExpiredToken', 'Message': 'Token expired'}}
        mock_client_old.invoke_model.side_effect = botocore_exceptions.ClientError(error_response, 'InvokeModel')
        mock_client_new.invoke_model.return_value = 'refreshed_result'

        mock_session = MagicMock()
        mock_session.client.side_effect = [mock_client_old, mock_client_new]
        config._boto3_session = mock_session
        rc = ResilientClient(config, 'bedrock-runtime')
        result = rc.invoke_model()
        assert result == 'refreshed_result'

    def test_non_expired_error_reraised(self):
        """ResilientClient re-raises non-credential errors."""
        config = _ByoKGConfig()
        mock_client = MagicMock()

        error_response = {'Error': {'Code': 'ValidationException', 'Message': 'Bad input'}}
        mock_client.invoke_model.side_effect = botocore_exceptions.ClientError(error_response, 'InvokeModel')

        mock_session = MagicMock()
        mock_session.client.return_value = mock_client
        config._boto3_session = mock_session
        rc = ResilientClient(config, 'bedrock-runtime')
        with pytest.raises(botocore_exceptions.ClientError):
            rc.invoke_model()
