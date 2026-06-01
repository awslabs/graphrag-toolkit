# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache
from graphrag_toolkit.lexical_graph import ModelError
from llama_index.llms.bedrock_converse import BedrockConverse


class TestLLMCache:
    """Tests for LLMCache model property."""
    
    @patch('boto3.Session')
    def test_model_property_with_bedrock_converse(self, mock_session):
        """Test model property returns model from BedrockConverse LLM."""
        # Create a real BedrockConverse instance with minimal config
        llm = BedrockConverse(model="anthropic.claude-v2", region_name="us-east-1")
        
        cache = LLMCache(llm=llm, enable_cache=False)
        
        assert cache.model == "anthropic.claude-v2"
    
    def test_model_property_with_non_bedrock_llm_raises_error(self):
        """Test model property raises ModelError for non-BedrockConverse LLM."""
        # Create a mock LLM that's not BedrockConverse
        from llama_index.core.llms.mock import MockLLM
        llm = MockLLM()
        
        cache = LLMCache(llm=llm, enable_cache=False)
        
        with pytest.raises(ModelError) as exc_info:
            _ = cache.model
        
        assert "Invalid LLM type" in str(exc_info.value)
        assert "does not support model" in str(exc_info.value)


class TestLLMCacheInitialization:
    """Tests for LLMCache initialization."""
    
    def test_initialization_with_llm(self):
        """Test LLMCache initializes with LLM."""
        from llama_index.core.llms.mock import MockLLM
        llm = MockLLM()
        
        cache = LLMCache(llm=llm, enable_cache=False)
        
        assert cache.llm == llm
        assert cache.enable_cache == False
        assert cache.verbose_prompt == False
        assert cache.verbose_response == False
    
    def test_initialization_with_cache_enabled(self):
        """Test LLMCache initializes with cache enabled."""
        from llama_index.core.llms.mock import MockLLM
        llm = MockLLM()
        
        cache = LLMCache(llm=llm, enable_cache=True)
        
        assert cache.enable_cache == True
    
    def test_initialization_with_verbose_options(self):
        """Test LLMCache initializes with verbose options."""
        from llama_index.core.llms.mock import MockLLM
        llm = MockLLM()
        
        cache = LLMCache(llm=llm, enable_cache=False, verbose_prompt=True, verbose_response=True)
        
        assert cache.verbose_prompt == True
        assert cache.verbose_response == True
    
    def test_initialization_defaults(self):
        """Test LLMCache initialization with default values."""
        from llama_index.core.llms.mock import MockLLM
        llm = MockLLM()
        
        cache = LLMCache(llm=llm)
        
        assert cache.enable_cache == False
        assert cache.verbose_prompt == False
        assert cache.verbose_response == False


class TestLLMCacheConfiguration:
    """Tests for LLMCache configuration options."""
    
    def test_cache_disabled_by_default(self):
        """Test that cache is disabled by default."""
        from llama_index.core.llms.mock import MockLLM
        llm = MockLLM()
        
        cache = LLMCache(llm=llm)
        
        assert cache.enable_cache == False
    
    def test_verbose_options_disabled_by_default(self):
        """Test that verbose options are disabled by default."""
        from llama_index.core.llms.mock import MockLLM
        llm = MockLLM()
        
        cache = LLMCache(llm=llm)
        
        assert cache.verbose_prompt == False
        assert cache.verbose_response == False
    
    def test_can_enable_cache(self):
        """Test that cache can be enabled."""
        from llama_index.core.llms.mock import MockLLM
        llm = MockLLM()
        
        cache = LLMCache(llm=llm, enable_cache=True)
        
        assert cache.enable_cache == True
    
    def test_can_enable_verbose_prompt(self):
        """Test that verbose_prompt can be enabled."""
        from llama_index.core.llms.mock import MockLLM
        llm = MockLLM()
        
        cache = LLMCache(llm=llm, verbose_prompt=True)
        
        assert cache.verbose_prompt == True
    
    def test_can_enable_verbose_response(self):
        """Test that verbose_response can be enabled."""
        from llama_index.core.llms.mock import MockLLM
        llm = MockLLM()
        
        cache = LLMCache(llm=llm, verbose_response=True)
        
        assert cache.verbose_response == True
    
    def test_can_enable_all_options(self):
        """Test that all options can be enabled together."""
        from llama_index.core.llms.mock import MockLLM
        llm = MockLLM()
        
        cache = LLMCache(llm=llm, enable_cache=True, verbose_prompt=True, verbose_response=True)
        
        assert cache.enable_cache == True
        assert cache.verbose_prompt == True
        assert cache.verbose_response == True


def _llm_with_spy(response='hello'):
    from llama_index.core.llms.mock import MockLLM
    llm = MockLLM()
    predict_mock = Mock(return_value=response)
    stream_mock = Mock(return_value=iter([response]))
    object.__setattr__(llm, 'predict', predict_mock)
    object.__setattr__(llm, 'stream', stream_mock)
    return llm


class TestPredictNoCache:
    def test_calls_llm_predict_and_returns_response(self):
        from llama_index.core.prompts import PromptTemplate
        llm = _llm_with_spy('answer')
        cache = LLMCache(llm=llm, enable_cache=False)
        result = cache.predict(PromptTemplate('Q: {q}'), q='ping')
        assert result == 'answer'
        llm.predict.assert_called_once()

    def test_llm_exception_wrapped_in_model_error(self):
        from llama_index.core.prompts import PromptTemplate
        llm = _llm_with_spy()
        llm.predict.side_effect = RuntimeError('upstream gone')
        cache = LLMCache(llm=llm, enable_cache=False)
        with pytest.raises(ModelError, match='upstream gone'):
            cache.predict(PromptTemplate('q: {q}'), q='x')


class TestPredictWithCache:
    def test_cache_miss_writes_then_returns(self, tmp_path, monkeypatch):
        from llama_index.core.prompts import PromptTemplate
        monkeypatch.chdir(tmp_path)
        llm = _llm_with_spy('fresh')
        cache = LLMCache(llm=llm, enable_cache=True)

        result = cache.predict(PromptTemplate('Q: {q}'), q='ping')

        assert result == 'fresh'
        cache_dir = tmp_path / 'cache' / 'llm'
        assert cache_dir.exists()
        files = list(cache_dir.glob('*.txt'))
        assert len(files) == 1
        assert files[0].read_text() == 'fresh'

    def test_cache_hit_skips_llm(self, tmp_path, monkeypatch):
        from llama_index.core.prompts import PromptTemplate
        monkeypatch.chdir(tmp_path)
        llm = _llm_with_spy('fresh')
        cache = LLMCache(llm=llm, enable_cache=True)

        cache.predict(PromptTemplate('Q: {q}'), q='ping')
        llm.predict.reset_mock()

        result = cache.predict(PromptTemplate('Q: {q}'), q='ping')
        assert result == 'fresh'
        llm.predict.assert_not_called()

    def test_exclude_cache_keys_removed_from_hash(self, tmp_path, monkeypatch):
        from llama_index.core.prompts import PromptTemplate
        monkeypatch.chdir(tmp_path)
        llm = _llm_with_spy('r1')
        cache = LLMCache(llm=llm, enable_cache=True)

        cache.predict(
            PromptTemplate('Q: {q}'),
            q='ping',
            exclude_cache_keys=['session'],
            session='abc',
        )
        llm.predict.reset_mock()
        result = cache.predict(
            PromptTemplate('Q: {q}'),
            q='ping',
            exclude_cache_keys=['session'],
            session='xyz',
        )
        assert result == 'r1'
        llm.predict.assert_not_called()

    def test_cache_miss_llm_exception_wrapped_in_model_error(self, tmp_path, monkeypatch):
        from llama_index.core.prompts import PromptTemplate
        monkeypatch.chdir(tmp_path)
        llm = _llm_with_spy()
        llm.predict.side_effect = RuntimeError('boom')
        cache = LLMCache(llm=llm, enable_cache=True)
        with pytest.raises(ModelError, match='boom'):
            cache.predict(PromptTemplate('Q: {q}'), q='x')


class TestStream:
    def test_stream_calls_llm_stream_and_returns_response(self):
        from llama_index.core.prompts import PromptTemplate
        llm = _llm_with_spy()
        object.__setattr__(llm, 'stream', Mock(return_value=iter(['tok1', 'tok2'])))
        cache = LLMCache(llm=llm, enable_cache=False)
        result = cache.stream(PromptTemplate('Q: {q}'), q='x')
        assert list(result) == ['tok1', 'tok2']

    def test_stream_exception_wrapped_in_model_error(self):
        from llama_index.core.prompts import PromptTemplate
        llm = _llm_with_spy()
        object.__setattr__(llm, 'stream', Mock(side_effect=RuntimeError('stream failure')))
        cache = LLMCache(llm=llm, enable_cache=False)
        with pytest.raises(ModelError, match='stream failure'):
            cache.stream(PromptTemplate('Q: {q}'), q='x')
