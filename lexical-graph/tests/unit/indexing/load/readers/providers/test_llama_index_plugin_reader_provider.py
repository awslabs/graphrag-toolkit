# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Comprehensive unit tests for LlamaIndexPluginReaderProvider.

Tests cover:
    - Happy path (reader loads documents successfully)
    - Import errors (package not installed)
    - Class not found in module
    - Invalid init_args (constructor mismatch)
    - Auth failures (401/403 detection)
    - Timeout enforcement
    - Retry with exponential backoff on transient errors
    - Graceful degradation (fail_on_error=False)
    - Empty results (warning logged)
    - Partial failures (non-Document items filtered)
    - input_source flexibility (passed or omitted)
    - Metadata enrichment
    - Load method not found
    - Generator/iterator results
"""

import logging
import time
import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from dataclasses import dataclass

from llama_index.core.schema import Document


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_config(**overrides):
    """Create a LlamaIndexPluginReaderConfig with sensible test defaults."""
    from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import (
        LlamaIndexPluginReaderConfig,
    )
    defaults = {
        "package": "llama-index-readers-confluence",
        "module_path": "llama_index.readers.confluence",
        "reader_class": "ConfluenceReader",
        "init_args": {"base_url": "https://test.atlassian.net/wiki"},
        "load_args": {"space_key": "ENG"},
        "timeout_seconds": 5,
        "max_retries": 0,
        "fail_on_error": True,
    }
    defaults.update(overrides)
    return LlamaIndexPluginReaderConfig(**defaults)


def _mock_reader_module(reader_class_name="ConfluenceReader", load_return=None):
    """Create a mock module with a mock reader class."""
    mock_module = MagicMock()
    mock_reader_instance = Mock()
    mock_reader_instance.load_data = Mock(
        return_value=load_return if load_return is not None else [
            Document(text="Page 1 content", metadata={"title": "Page 1"}),
            Document(text="Page 2 content", metadata={"title": "Page 2"}),
        ]
    )
    mock_reader_class = Mock(return_value=mock_reader_instance)
    setattr(mock_module, reader_class_name, mock_reader_class)
    return mock_module, mock_reader_class, mock_reader_instance


# ---------------------------------------------------------------------------
# Happy Path
# ---------------------------------------------------------------------------

class TestHappyPath:
    """Tests for successful document loading."""

    def test_loads_documents_successfully(self):
        """Reader returns documents — all pass through."""
        mock_module, mock_cls, mock_reader = _mock_reader_module()
        config = _make_config()

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            docs = provider.read()

        assert len(docs) == 2
        assert docs[0].text == "Page 1 content"
        assert docs[1].metadata["title"] == "Page 2"
        mock_reader.load_data.assert_called_once_with(space_key="ENG")

    def test_passes_input_source(self):
        """input_source is passed through to load_args."""
        mock_module, _, mock_reader = _mock_reader_module()
        config = _make_config(load_args={})

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            provider.read("https://wiki.example.com")

        mock_reader.load_data.assert_called_once_with(
            input_source="https://wiki.example.com"
        )

    def test_input_source_fallback_when_not_accepted(self):
        """If reader doesn't accept input_source kwarg, retry without it."""
        mock_module, _, mock_reader = _mock_reader_module()
        # First call raises TypeError (unexpected kwarg), second succeeds
        mock_reader.load_data.side_effect = [
            TypeError("unexpected keyword argument 'input_source'"),
            [Document(text="worked", metadata={})],
        ]
        # Reset side_effect for the _call_load_fn retry
        call_count = [0]
        original_side_effect = mock_reader.load_data.side_effect

        def flexible_load(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1 and "input_source" in kwargs:
                raise TypeError("unexpected keyword argument 'input_source'")
            return [Document(text="worked", metadata={})]

        mock_reader.load_data.side_effect = flexible_load
        config = _make_config(load_args={"space_key": "TEST"})

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            docs = provider.read("some_input")

        assert len(docs) == 1
        assert docs[0].text == "worked"

    def test_metadata_enrichment(self):
        """metadata_fn enriches all returned documents."""
        mock_module, _, _ = _mock_reader_module()
        config = _make_config(
            metadata_fn=lambda source: {"source": "confluence", "team": "platform"}
        )

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            docs = provider.read()

        for doc in docs:
            assert doc.metadata["source"] == "confluence"
            assert doc.metadata["team"] == "platform"


# ---------------------------------------------------------------------------
# Import Errors
# ---------------------------------------------------------------------------

class TestImportErrors:
    """Tests for missing packages and classes."""

    def test_raises_on_missing_package(self):
        """ImportError with install instructions when package not found."""
        config = _make_config(
            package="llama-index-readers-confluence",
            module_path="llama_index.readers.confluence",
        )

        with patch("importlib.import_module", side_effect=ImportError("No module named 'llama_index.readers.confluence'")):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
                ReaderImportError,
            )
            with pytest.raises(ReaderImportError) as exc_info:
                LlamaIndexPluginReaderProvider(config)

        assert "pip install" in str(exc_info.value)
        assert "llama-index-readers-confluence" in str(exc_info.value)

    def test_raises_on_missing_class(self):
        """ImportError when class not found in module."""
        mock_module = MagicMock()
        # Make getattr raise AttributeError for ConfluenceReader
        del mock_module.ConfluenceReader
        config = _make_config(reader_class="ConfluenceReader")

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
                ReaderImportError,
            )
            with pytest.raises(ReaderImportError) as exc_info:
                LlamaIndexPluginReaderProvider(config)

        assert "not found" in str(exc_info.value)

    def test_raises_on_invalid_init_args(self):
        """ValueError when constructor args don't match reader signature."""
        mock_module = MagicMock()
        mock_cls = Mock(side_effect=TypeError("__init__() got unexpected keyword argument 'bogus'"))
        mock_module.ConfluenceReader = mock_cls
        config = _make_config(init_args={"bogus": "value"})

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            with pytest.raises(ValueError) as exc_info:
                LlamaIndexPluginReaderProvider(config)

        assert "init_args" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class TestValidation:
    """Tests for config validation."""

    def test_raises_on_missing_reader_class(self):
        """ValueError when reader_class not provided."""
        config = _make_config(reader_class="")

        from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
            LlamaIndexPluginReaderProvider,
        )
        with pytest.raises(ValueError, match="reader_class"):
            LlamaIndexPluginReaderProvider(config)

    def test_raises_on_missing_module_and_package(self):
        """ValueError when neither module_path nor package provided."""
        config = _make_config(module_path="", package="")

        from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
            LlamaIndexPluginReaderProvider,
        )
        with pytest.raises(ValueError, match="module_path"):
            LlamaIndexPluginReaderProvider(config)


# ---------------------------------------------------------------------------
# Auth Failures
# ---------------------------------------------------------------------------

class TestAuthFailures:
    """Tests for authentication error detection."""

    @pytest.mark.parametrize("error_msg", [
        "401 Unauthorized",
        "403 Forbidden",
        "Invalid token provided",
        "Token expired at 2026-01-01",
        "Authentication failed: bad credentials",
        "Access denied for user",
    ])
    def test_detects_auth_errors(self, error_msg):
        """Auth-like errors raise ReaderAuthError and are NOT retried."""
        mock_module, _, mock_reader = _mock_reader_module()
        mock_reader.load_data.side_effect = RuntimeError(error_msg)
        config = _make_config(max_retries=3)  # Should NOT retry

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
                ReaderAuthError,
            )
            provider = LlamaIndexPluginReaderProvider(config)

            with pytest.raises(ReaderAuthError):
                provider.read()

        # Only called once — no retries on auth errors
        assert mock_reader.load_data.call_count == 1

    def test_auth_error_during_init(self):
        """Auth error during reader construction is detected."""
        mock_module = MagicMock()
        mock_cls = Mock(side_effect=RuntimeError("401 Unauthorized: invalid API key"))
        mock_module.ConfluenceReader = mock_cls
        config = _make_config()

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
                ReaderAuthError,
            )
            with pytest.raises(ReaderAuthError):
                LlamaIndexPluginReaderProvider(config)


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------

class TestTimeout:
    """Tests for timeout enforcement."""

    def test_raises_on_timeout(self):
        """ReaderTimeoutError when reader exceeds timeout_seconds."""
        mock_module, _, mock_reader = _mock_reader_module()

        def slow_load(**kwargs):
            time.sleep(10)
            return []

        mock_reader.load_data.side_effect = slow_load
        config = _make_config(timeout_seconds=1, fail_on_error=True)

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
                ReaderTimeoutError,
            )
            provider = LlamaIndexPluginReaderProvider(config)

            with pytest.raises((ReaderTimeoutError, RuntimeError)):
                provider.read()

    def test_timeout_returns_empty_when_not_fail_on_error(self):
        """Timeout returns [] when fail_on_error=False."""
        mock_module, _, mock_reader = _mock_reader_module()

        def slow_load(**kwargs):
            time.sleep(10)
            return []

        mock_reader.load_data.side_effect = slow_load
        config = _make_config(timeout_seconds=1, fail_on_error=False)

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            docs = provider.read()

        assert docs == []


# ---------------------------------------------------------------------------
# Retry with Backoff
# ---------------------------------------------------------------------------

class TestRetry:
    """Tests for retry with exponential backoff on transient errors."""

    def test_retries_on_transient_error(self):
        """Transient errors (429, 503) trigger retry."""
        mock_module, _, mock_reader = _mock_reader_module()
        mock_reader.load_data.side_effect = [
            RuntimeError("429 Too Many Requests"),
            [Document(text="success", metadata={})],
        ]
        config = _make_config(max_retries=1, retry_backoff_seconds=0.01)

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            docs = provider.read()

        assert len(docs) == 1
        assert docs[0].text == "success"
        assert mock_reader.load_data.call_count == 2

    def test_exhausts_retries_then_fails(self):
        """After max_retries, raises if fail_on_error=True."""
        mock_module, _, mock_reader = _mock_reader_module()
        mock_reader.load_data.side_effect = RuntimeError("503 Service Unavailable")
        config = _make_config(max_retries=2, retry_backoff_seconds=0.01, fail_on_error=True)

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)

            with pytest.raises(RuntimeError, match="all 3 attempt"):
                provider.read()

        # 1 initial + 2 retries = 3 total
        assert mock_reader.load_data.call_count == 3

    def test_exhausts_retries_returns_empty_gracefully(self):
        """After max_retries with fail_on_error=False, returns []."""
        mock_module, _, mock_reader = _mock_reader_module()
        mock_reader.load_data.side_effect = RuntimeError("500 Internal Server Error")
        config = _make_config(max_retries=1, retry_backoff_seconds=0.01, fail_on_error=False)

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            docs = provider.read()

        assert docs == []

    def test_non_transient_error_not_retried(self):
        """Non-transient errors (ValueError, etc.) are NOT retried."""
        mock_module, _, mock_reader = _mock_reader_module()
        mock_reader.load_data.side_effect = ValueError("Invalid space_key format")
        config = _make_config(max_retries=3, retry_backoff_seconds=0.01, fail_on_error=True)

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)

            with pytest.raises(RuntimeError):
                provider.read()

        # Only 1 attempt — no retries for non-transient errors
        assert mock_reader.load_data.call_count == 1


# ---------------------------------------------------------------------------
# Empty Results
# ---------------------------------------------------------------------------

class TestEmptyResults:
    """Tests for empty result handling."""

    def test_empty_list_returns_with_warning(self, caplog):
        """Empty result logs warning but doesn't error."""
        mock_module, _, mock_reader = _mock_reader_module(load_return=[])
        config = _make_config()

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)

            with caplog.at_level(logging.WARNING):
                docs = provider.read()

        assert docs == []
        assert "0 documents" in caplog.text

    def test_none_return_handled_gracefully(self):
        """Reader returning None doesn't crash."""
        mock_module, _, mock_reader = _mock_reader_module()
        mock_reader.load_data.return_value = None
        config = _make_config(fail_on_error=False)

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            docs = provider.read()

        assert docs == []


# ---------------------------------------------------------------------------
# Partial Failures / Invalid Documents
# ---------------------------------------------------------------------------

class TestPartialFailures:
    """Tests for filtering invalid items from results."""

    def test_filters_non_document_items(self):
        """Non-Document items in the result list are filtered out."""
        mixed_results = [
            Document(text="valid 1", metadata={}),
            "I am not a document",
            42,
            Document(text="valid 2", metadata={}),
            None,
        ]
        mock_module, _, mock_reader = _mock_reader_module(load_return=mixed_results)
        config = _make_config()

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            docs = provider.read()

        assert len(docs) == 2
        assert docs[0].text == "valid 1"
        assert docs[1].text == "valid 2"

    def test_handles_generator_return(self):
        """Reader returning a generator is consumed into a list."""
        def doc_generator():
            yield Document(text="gen 1", metadata={})
            yield Document(text="gen 2", metadata={})
            yield Document(text="gen 3", metadata={})

        mock_module, _, mock_reader = _mock_reader_module()
        mock_reader.load_data.return_value = doc_generator()
        config = _make_config()

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            docs = provider.read()

        assert len(docs) == 3


# ---------------------------------------------------------------------------
# Module Path Resolution
# ---------------------------------------------------------------------------

class TestModuleResolution:
    """Tests for deriving module_path from package name."""

    def test_module_path_takes_priority(self):
        """Explicit module_path is used over package derivation."""
        mock_module, _, _ = _mock_reader_module()
        config = _make_config(
            module_path="llama_index.readers.custom",
            package="something-else",
        )

        with patch("importlib.import_module", return_value=mock_module) as mock_import:
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            LlamaIndexPluginReaderProvider(config)

        mock_import.assert_called_once_with("llama_index.readers.custom")

    def test_derives_module_from_package_name(self):
        """Package name llama-index-readers-X → llama_index.readers.X."""
        mock_module, _, _ = _mock_reader_module(reader_class_name="NotionReader")
        config = _make_config(
            module_path="",
            package="llama-index-readers-notion",
            reader_class="NotionReader",
        )

        with patch("importlib.import_module", return_value=mock_module) as mock_import:
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            LlamaIndexPluginReaderProvider(config)

        mock_import.assert_called_once_with("llama_index.readers.notion")


# ---------------------------------------------------------------------------
# Load Method
# ---------------------------------------------------------------------------

class TestLoadMethod:
    """Tests for custom load method configuration."""

    def test_uses_custom_load_method(self):
        """load_method config routes to the correct reader method."""
        mock_module, _, mock_reader = _mock_reader_module()
        mock_reader.lazy_load = Mock(return_value=[
            Document(text="lazy doc", metadata={})
        ])
        config = _make_config(load_method="lazy_load")

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            docs = provider.read()

        assert len(docs) == 1
        mock_reader.lazy_load.assert_called_once()

    def test_missing_load_method_fails_gracefully(self):
        """Disallowed load_method raises ValueError (security: only allowed methods)."""
        mock_module, _, mock_reader = _mock_reader_module()
        config = _make_config(load_method="custom_method", fail_on_error=True)

        with patch("importlib.import_module", return_value=mock_module):
            from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
                LlamaIndexPluginReaderProvider,
            )
            provider = LlamaIndexPluginReaderProvider(config)
            with pytest.raises(ValueError, match="not allowed"):
                provider.read()


# ─── Security Hardening Tests (Items #1-7 from review) ────────────────────────

from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.llama_index_plugin_reader_provider import (
    LlamaIndexPluginReaderProvider,
    ReaderImportError,
    ReaderAuthError,
)


class TestNamespaceAllowlist:
    """Item #1: Verify namespace restriction prevents arbitrary module loading."""

    def test_disallowed_module_raises(self):
        """Module not in ALLOWED_MODULE_PREFIXES is rejected."""
        config = _make_config(module_path="os", reader_class="system")
        with pytest.raises(ReaderImportError, match="not in the allowed namespace"):
            LlamaIndexPluginReaderProvider(config)

    def test_shutil_blocked(self):
        """Filesystem destruction via shutil.rmtree is prevented."""
        config = _make_config(module_path="shutil", reader_class="rmtree")
        with pytest.raises(ReaderImportError, match="not in the allowed namespace"):
            LlamaIndexPluginReaderProvider(config)

    def test_allowed_prefix_passes(self):
        """Module in allowed namespace proceeds to import."""
        mock_module, _, _ = _mock_reader_module()
        config = _make_config(module_path="llama_index.readers.confluence")
        with patch("importlib.import_module", return_value=mock_module):
            provider = LlamaIndexPluginReaderProvider(config)
            assert provider._reader is not None

    def test_custom_subclass_extends_allowlist(self):
        """Subclass can extend ALLOWED_MODULE_PREFIXES for custom namespaces."""
        class MyProvider(LlamaIndexPluginReaderProvider):
            ALLOWED_MODULE_PREFIXES = ("llama_index.readers.", "mycompany.readers.")

        mock_module, _, _ = _mock_reader_module()
        config = _make_config(module_path="mycompany.readers.internal")
        with patch("importlib.import_module", return_value=mock_module):
            provider = MyProvider(config)
            assert provider._reader is not None


class TestInterfaceValidation:
    """Item #2: Verify interface check before instantiation."""

    def test_non_callable_rejected(self):
        """Non-callable attribute is rejected before constructor runs."""
        mock_module = Mock()
        mock_module.NotAClass = "just a string"
        config = _make_config(module_path="llama_index.readers.test", reader_class="NotAClass")
        with patch("importlib.import_module", return_value=mock_module):
            with pytest.raises(ReaderImportError, match="is not callable"):
                LlamaIndexPluginReaderProvider(config)

    def test_missing_load_data_rejected(self):
        """Class without load_data or lazy_load is rejected before instantiation."""
        mock_module = Mock()
        mock_cls = Mock()
        mock_cls.load_data = None
        del mock_cls.load_data
        del mock_cls.lazy_load
        mock_module.BadReader = mock_cls
        config = _make_config(module_path="llama_index.readers.test", reader_class="BadReader")
        with patch("importlib.import_module", return_value=mock_module):
            with pytest.raises(ReaderImportError, match="does not implement"):
                LlamaIndexPluginReaderProvider(config)


class TestLoadMethodRestriction:
    """Item #3: Only allowed methods can be called."""

    def test_dunder_method_blocked(self):
        """Dunder methods like __delattr__ cannot be called."""
        mock_module, _, _ = _mock_reader_module()
        config = _make_config(load_method="__delattr__")
        with patch("importlib.import_module", return_value=mock_module):
            provider = LlamaIndexPluginReaderProvider(config)
            with pytest.raises(ValueError, match="not allowed"):
                provider.read()

    def test_arbitrary_method_blocked(self):
        """Arbitrary method names are blocked."""
        mock_module, _, _ = _mock_reader_module()
        config = _make_config(load_method="execute_shell")
        with patch("importlib.import_module", return_value=mock_module):
            provider = LlamaIndexPluginReaderProvider(config)
            with pytest.raises(ValueError, match="not allowed"):
                provider.read()


class TestEnvVarResolution:
    """Item #4: $VAR_NAME references resolved from environment."""

    def test_resolves_env_var(self, monkeypatch):
        """$VAR_NAME is replaced with environment value."""
        monkeypatch.setenv("MY_TOKEN", "secret123")
        mock_module, mock_cls, _ = _mock_reader_module()
        config = _make_config(init_args={"token": "$MY_TOKEN", "url": "https://example.com"})
        with patch("importlib.import_module", return_value=mock_module):
            LlamaIndexPluginReaderProvider(config)
        # Verify the constructor received the resolved value
        mock_cls.assert_called_once_with(token="secret123", url="https://example.com")

    def test_missing_env_var_raises(self):
        """Reference to unset env var raises ValueError."""
        config = _make_config(init_args={"token": "$NONEXISTENT_VAR_XYZ"})
        mock_module, _, _ = _mock_reader_module()
        with patch("importlib.import_module", return_value=mock_module):
            with pytest.raises(ValueError, match="not set in the environment"):
                LlamaIndexPluginReaderProvider(config)

    def test_non_env_string_unchanged(self):
        """Strings without $ prefix are passed through unchanged."""
        mock_module, mock_cls, _ = _mock_reader_module()
        config = _make_config(init_args={"url": "https://example.com", "count": "5"})
        with patch("importlib.import_module", return_value=mock_module):
            LlamaIndexPluginReaderProvider(config)
        mock_cls.assert_called_once_with(url="https://example.com", count="5")

    def test_lowercase_dollar_not_resolved(self):
        """$lowercase is not treated as env var (only $UPPER_CASE)."""
        mock_module, mock_cls, _ = _mock_reader_module()
        config = _make_config(init_args={"note": "$not_an_env_var"})
        with patch("importlib.import_module", return_value=mock_module):
            LlamaIndexPluginReaderProvider(config)
        mock_cls.assert_called_once_with(note="$not_an_env_var")


class TestCredentialLogging:
    """Item #5: Credential values must never appear in logs."""

    def test_credentials_not_logged(self, caplog):
        """Secret values in init_args never appear in log output."""
        import logging as stdlib_logging
        secret = "super_secret_token_value_12345"
        mock_module, _, _ = _mock_reader_module()
        config = _make_config(init_args={"github_token": secret})
        with caplog.at_level(stdlib_logging.DEBUG):
            with patch("importlib.import_module", return_value=mock_module):
                try:
                    LlamaIndexPluginReaderProvider(config)
                except Exception:
                    pass
        assert secret not in caplog.text, "Credential value leaked into logs"


class TestPackageNameValidation:
    """Item #7: Only suggest pip install for validated package names."""

    def test_valid_package_gets_install_hint(self):
        """Valid llama-index-readers-* package gets pip install suggestion."""
        config = _make_config(
            package="llama-index-readers-confluence",
            module_path="llama_index.readers.confluence",
        )
        with pytest.raises(ReaderImportError, match="pip install llama-index-readers-confluence"):
            LlamaIndexPluginReaderProvider(config)

    def test_invalid_package_no_install_hint(self):
        """Non-llama-index package does NOT get pip install suggestion."""
        config = _make_config(
            package="some-random-package",
            module_path="llama_index.readers.random",
        )
        with pytest.raises(ReaderImportError) as exc_info:
            LlamaIndexPluginReaderProvider(config)
        assert "pip install" not in str(exc_info.value)

    def test_arbitrary_package_no_install_hint(self):
        """Completely arbitrary package name does NOT get pip install suggestion."""
        config = _make_config(
            package="evil-package",
            module_path="llama_index.readers.evil",
        )
        with pytest.raises(ReaderImportError) as exc_info:
            LlamaIndexPluginReaderProvider(config)
        assert "pip install" not in str(exc_info.value)
