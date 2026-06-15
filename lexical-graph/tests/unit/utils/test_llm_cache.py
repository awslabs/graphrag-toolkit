# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache
from graphrag_toolkit.lexical_graph import ModelError
from graphrag_toolkit.core.llm import LLMProvider, BedrockLLMProvider
from graphrag_toolkit.core.prompt import PromptTemplate


class _MockLLMProvider(LLMProvider):
    """Mock LLMProvider for testing."""
    def __init__(self, response='hello'):
        self._response = response
    def predict(self, prompt, **kwargs):
        return self._response
    def stream(self, prompt, **kwargs):
        yield self._response


class TestLLMCache:
    """Tests for LLMCache model property."""
    
    @patch('graphrag_toolkit.core.llm.boto3.client')
    def test_model_property_with_bedrock_provider(self, mock_boto3):
        """Test model property returns model_id from BedrockLLMProvider."""
        llm = BedrockLLMProvider(model_id="anthropic.claude-v2", region_name="us-east-1")
        cache = LLMCache(llm=llm, enable_cache=False)
        assert cache.model == "anthropic.claude-v2"
    
    def test_model_property_with_non_bedrock_llm_raises_error(self):
        """Test model property raises ModelError for non-BedrockLLMProvider."""
        llm = _MockLLMProvider()
        cache = LLMCache(llm=llm, enable_cache=False)
        with pytest.raises(ModelError) as exc_info:
            _ = cache.model
        assert "Invalid LLM type" in str(exc_info.value)
        assert "does not support model" in str(exc_info.value)


class TestLLMCacheInitialization:
    """Tests for LLMCache initialization."""
    
    def test_initialization_with_llm(self):
        """Test LLMCache initializes with LLMProvider."""
        llm = _MockLLMProvider()
        cache = LLMCache(llm=llm, enable_cache=False)
        assert cache.llm == llm
        assert cache.enable_cache == False
        assert cache.verbose_prompt == False
        assert cache.verbose_response == False
    
    def test_initialization_with_cache_enabled(self):
        llm = _MockLLMProvider()
        cache = LLMCache(llm=llm, enable_cache=True)
        assert cache.enable_cache == True
    
    def test_initialization_with_verbose_options(self):
        llm = _MockLLMProvider()
        cache = LLMCache(llm=llm, enable_cache=False, verbose_prompt=True, verbose_response=True)
        assert cache.verbose_prompt == True
        assert cache.verbose_response == True
    
    def test_initialization_defaults(self):
        llm = _MockLLMProvider()
        cache = LLMCache(llm=llm)
        assert cache.enable_cache == False
        assert cache.verbose_prompt == False
        assert cache.verbose_response == False


class TestLLMCacheConfiguration:
    """Tests for LLMCache configuration options."""
    
    def test_cache_disabled_by_default(self):
        cache = LLMCache(llm=_MockLLMProvider())
        assert cache.enable_cache == False
    
    def test_verbose_options_disabled_by_default(self):
        cache = LLMCache(llm=_MockLLMProvider())
        assert cache.verbose_prompt == False
        assert cache.verbose_response == False
    
    def test_can_enable_cache(self):
        cache = LLMCache(llm=_MockLLMProvider(), enable_cache=True)
        assert cache.enable_cache == True
    
    def test_can_enable_verbose_prompt(self):
        cache = LLMCache(llm=_MockLLMProvider(), verbose_prompt=True)
        assert cache.verbose_prompt == True
    
    def test_can_enable_verbose_response(self):
        cache = LLMCache(llm=_MockLLMProvider(), verbose_response=True)
        assert cache.verbose_response == True
    
    def test_can_enable_all_options(self):
        cache = LLMCache(llm=_MockLLMProvider(), enable_cache=True, verbose_prompt=True, verbose_response=True)
        assert cache.enable_cache == True
        assert cache.verbose_prompt == True
        assert cache.verbose_response == True


class TestPredictNoCache:
    def test_calls_llm_predict_and_returns_response(self):
        llm = _MockLLMProvider('answer')
        llm.predict = Mock(return_value='answer')
        cache = LLMCache(llm=llm, enable_cache=False)
        result = cache.predict(PromptTemplate('Q: {q}'), q='ping')
        assert result == 'answer'
        llm.predict.assert_called_once_with('Q: ping')

    def test_llm_exception_wrapped_in_model_error(self):
        llm = _MockLLMProvider()
        llm.predict = Mock(side_effect=RuntimeError('upstream gone'))
        cache = LLMCache(llm=llm, enable_cache=False)
        with pytest.raises(ModelError, match='upstream gone'):
            cache.predict(PromptTemplate('q: {q}'), q='x')


class TestPredictWithCache:
    def test_cache_miss_writes_then_returns(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        llm = _MockLLMProvider('fresh')
        llm.predict = Mock(return_value='fresh')
        cache = LLMCache(llm=llm, enable_cache=True)

        result = cache.predict(PromptTemplate('Q: {q}'), q='ping')

        assert result == 'fresh'
        cache_dir = tmp_path / 'cache' / 'llm'
        assert cache_dir.exists()
        files = list(cache_dir.glob('*.txt'))
        assert len(files) == 1
        assert files[0].read_text() == 'fresh'

    def test_cache_hit_skips_llm(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        llm = _MockLLMProvider('fresh')
        llm.predict = Mock(return_value='fresh')
        cache = LLMCache(llm=llm, enable_cache=True)

        cache.predict(PromptTemplate('Q: {q}'), q='ping')
        llm.predict.reset_mock()

        result = cache.predict(PromptTemplate('Q: {q}'), q='ping')
        assert result == 'fresh'
        llm.predict.assert_not_called()

    def test_exclude_cache_keys_removed_from_hash(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        llm = _MockLLMProvider('r1')
        llm.predict = Mock(return_value='r1')
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
        monkeypatch.chdir(tmp_path)
        llm = _MockLLMProvider()
        llm.predict = Mock(side_effect=RuntimeError('boom'))
        cache = LLMCache(llm=llm, enable_cache=True)
        with pytest.raises(ModelError, match='boom'):
            cache.predict(PromptTemplate('Q: {q}'), q='x')


class TestStream:
    def test_stream_calls_llm_stream_and_returns_response(self):
        llm = _MockLLMProvider()
        llm.stream = Mock(return_value=iter(['tok1', 'tok2']))
        cache = LLMCache(llm=llm, enable_cache=False)
        result = cache.stream(PromptTemplate('Q: {q}'), q='x')
        assert list(result) == ['tok1', 'tok2']

    def test_stream_exception_wrapped_in_model_error(self):
        llm = _MockLLMProvider()
        llm.stream = Mock(side_effect=RuntimeError('stream failure'))
        cache = LLMCache(llm=llm, enable_cache=False)
        with pytest.raises(ModelError, match='stream failure'):
            cache.stream(PromptTemplate('Q: {q}'), q='x')
