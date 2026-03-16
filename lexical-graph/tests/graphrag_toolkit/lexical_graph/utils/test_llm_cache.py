# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for LLMCache.predict and LLMCache.stream.

Covers:
  - predict without cache (success, exception -> ModelError)
  - predict with cache (cache hit, cache miss -> writes file)
  - stream (success, exception -> ModelError)
  - verbose_prompt / verbose_response logging paths
"""

import os
import pytest
from unittest.mock import MagicMock, patch, mock_open

from graphrag_toolkit.lexical_graph import ModelError
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache
from llama_index.core.prompts import PromptTemplate


def _make_cache(enable_cache=False, verbose_prompt=False, verbose_response=False):
    mock_llm = MagicMock()
    mock_llm.to_json.return_value = '{"model": "test"}'
    mock_llm.predict.return_value = "LLM response"
    mock_llm.stream.return_value = iter(["token1", "token2"])
    cache = LLMCache.model_construct(
        llm=mock_llm,
        enable_cache=enable_cache,
        verbose_prompt=verbose_prompt,
        verbose_response=verbose_response,
        model=None,
    )
    return cache, mock_llm


# ---------------------------------------------------------------------------
# predict – no cache
# ---------------------------------------------------------------------------

class TestLLMCachePredictNoCache:

    def test_predict_calls_llm(self):
        cache, mock_llm = _make_cache(enable_cache=False)
        prompt = PromptTemplate("Hello {name}")
        result = cache.predict(prompt, name="World")
        assert result == "LLM response"
        mock_llm.predict.assert_called_once()

    def test_predict_exception_raises_model_error(self):
        cache, mock_llm = _make_cache(enable_cache=False)
        mock_llm.predict.side_effect = Exception("boom")
        prompt = PromptTemplate("Hello {name}")
        with pytest.raises(ModelError, match="boom"):
            cache.predict(prompt, name="World")

    def test_predict_verbose_prompt_logs(self):
        cache, mock_llm = _make_cache(enable_cache=False, verbose_prompt=True)
        prompt = PromptTemplate("Hello {name}")
        with patch("graphrag_toolkit.lexical_graph.utils.llm_cache.logger") as mock_logger:
            cache.predict(prompt, name="World")
            mock_logger.info.assert_called()


# ---------------------------------------------------------------------------
# predict – with cache
# ---------------------------------------------------------------------------

class TestLLMCachePredictWithCache:

    def test_predict_cache_hit_returns_cached(self, tmp_path):
        cache, mock_llm = _make_cache(enable_cache=True)
        prompt = PromptTemplate("Hello {name}")

        cached_response = "Cached LLM response"
        cache_file = tmp_path / "cache" / "llm" / "somehash.txt"
        cache_file.parent.mkdir(parents=True)
        cache_file.write_text(cached_response)

        # Patch os.path.exists to return True and open to return cached content
        with patch("graphrag_toolkit.lexical_graph.utils.llm_cache.os.path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data=cached_response)):
                result = cache.predict(prompt, name="World")
                assert result == cached_response
                mock_llm.predict.assert_not_called()

    def test_predict_cache_miss_calls_llm_and_writes(self):
        cache, mock_llm = _make_cache(enable_cache=True)
        prompt = PromptTemplate("Hello {name}")

        with patch("graphrag_toolkit.lexical_graph.utils.llm_cache.os.path.exists", return_value=False):
            with patch("graphrag_toolkit.lexical_graph.utils.llm_cache.os.makedirs"):
                with patch("builtins.open", mock_open()) as m:
                    result = cache.predict(prompt, name="World")
                    assert result == "LLM response"
                    mock_llm.predict.assert_called_once()
                    # File should have been written
                    m().write.assert_called_once_with("LLM response")

    def test_predict_cache_miss_exception_raises_model_error(self):
        cache, mock_llm = _make_cache(enable_cache=True)
        mock_llm.predict.side_effect = Exception("llm error")
        prompt = PromptTemplate("Hello {name}")

        with patch("graphrag_toolkit.lexical_graph.utils.llm_cache.os.path.exists", return_value=False):
            with patch("graphrag_toolkit.lexical_graph.utils.llm_cache.os.makedirs"):
                with pytest.raises(ModelError, match="llm error"):
                    cache.predict(prompt, name="World")

    def test_predict_verbose_response_logs(self):
        cache, mock_llm = _make_cache(enable_cache=False, verbose_response=True)
        prompt = PromptTemplate("Hello {name}")
        with patch("graphrag_toolkit.lexical_graph.utils.llm_cache.logger") as mock_logger:
            cache.predict(prompt, name="World")
            mock_logger.info.assert_called()


# ---------------------------------------------------------------------------
# stream
# ---------------------------------------------------------------------------

class TestLLMCacheStream:

    def test_stream_returns_generator(self):
        cache, mock_llm = _make_cache()
        prompt = PromptTemplate("Hello {name}")
        result = cache.stream(prompt, name="World")
        assert result is not None
        mock_llm.stream.assert_called_once()

    def test_stream_exception_raises_model_error(self):
        cache, mock_llm = _make_cache()
        mock_llm.stream.side_effect = Exception("stream error")
        prompt = PromptTemplate("Hello {name}")
        with pytest.raises(ModelError, match="stream error"):
            cache.stream(prompt, name="World")

    def test_stream_verbose_prompt_logs(self):
        cache, mock_llm = _make_cache(verbose_prompt=True)
        prompt = PromptTemplate("Hello {name}")
        with patch("graphrag_toolkit.lexical_graph.utils.llm_cache.logger") as mock_logger:
            cache.stream(prompt, name="World")
            mock_logger.info.assert_called()
