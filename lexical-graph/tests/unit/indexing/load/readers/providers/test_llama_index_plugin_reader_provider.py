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
from llama_index.core.readers.base import BaseReader


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


def _mock_reader_module(reader_class_name="ConfluenceReader", load_return=None,
                        init_side_effect=None):
    """Create a mock module whose reader_class is a real BaseReader subclass.

    The provider now requires reader_class to be a BaseReader subclass, so a
    bare Mock no longer passes the gate. The returned `reader` holder proxies the
    instance the provider builds: its load_data/lazy_load/aload_data are shared
    Mocks a test can configure or assert on, and `ctor` records constructor args.
    """
    mock_module = MagicMock()
    docs = load_return if load_return is not None else [
        Document(text="Page 1 content", metadata={"title": "Page 1"}),
        Document(text="Page 2 content", metadata={"title": "Page 2"}),
    ]

    reader = Mock()
    reader.load_data = Mock(return_value=docs)
    ctor = Mock()

    # Define the load methods on the CLASS (like a real BaseReader subclass) so
    # the import-time presence gate can see them; each delegates to the shared
    # `reader` mock the test configures/asserts on.
    class MockReader(BaseReader):
        def __init__(self, **kwargs):
            ctor(**kwargs)
            if init_side_effect is not None:
                raise init_side_effect

        def load_data(self, *args, **kwargs):
            return reader.load_data(*args, **kwargs)

        def lazy_load(self, *args, **kwargs):
            return reader.lazy_load(*args, **kwargs)

        def aload_data(self, *args, **kwargs):
            return reader.aload_data(*args, **kwargs)

    MockReader.__name__ = reader_class_name
    setattr(mock_module, reader_class_name, MockReader)
    return mock_module, ctor, reader


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
        mock_module, _, _ = _mock_reader_module(
            init_side_effect=TypeError("__init__() got unexpected keyword argument 'bogus'")
        )
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
        mock_module, _, _ = _mock_reader_module(
            init_side_effect=RuntimeError("401 Unauthorized: invalid API key")
        )
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
    """Tests for timeout enforcement.

    The load functions block on an Event the test releases in a finally. Because
    the timeout path abandons the worker (shutdown(wait=False)) instead of
    joining it, releasing lets the leaked worker exit promptly rather than
    lingering past the test.
    """

    @staticmethod
    def _blocking_load(release):
        def load(**kwargs):
            release.wait(30)  # truly blocked until the test releases it
            return []
        return load

    def test_raises_on_timeout(self):
        """ReaderTimeoutError/RuntimeError when reader exceeds timeout_seconds."""
        import threading
        release = threading.Event()
        mock_module, _, mock_reader = _mock_reader_module()
        mock_reader.load_data.side_effect = self._blocking_load(release)
        config = _make_config(timeout_seconds=1, fail_on_error=True)

        with patch("importlib.import_module", return_value=mock_module):
            provider = LlamaIndexPluginReaderProvider(config)
            try:
                with pytest.raises((ReaderTimeoutError, RuntimeError)):
                    provider.read()
            finally:
                release.set()

    def test_timeout_returns_empty_when_not_fail_on_error(self):
        """Timeout returns [] when fail_on_error=False."""
        import threading
        release = threading.Event()
        mock_module, _, mock_reader = _mock_reader_module()
        mock_reader.load_data.side_effect = self._blocking_load(release)
        config = _make_config(timeout_seconds=1, fail_on_error=False)

        with patch("importlib.import_module", return_value=mock_module):
            provider = LlamaIndexPluginReaderProvider(config)
            try:
                docs = provider.read()
            finally:
                release.set()

        assert docs == []

    def test_timeout_returns_control_without_joining_worker(self):
        """The timeout must return control instead of joining the runaway
        worker. The load blocks until released; if read() joined the worker it
        would block for the full 30s, so a fast return proves the fix."""
        import threading
        import time as _time
        release = threading.Event()
        mock_module, _, mock_reader = _mock_reader_module()
        mock_reader.load_data.side_effect = self._blocking_load(release)
        config = _make_config(timeout_seconds=1, max_retries=0, fail_on_error=True)

        with patch("importlib.import_module", return_value=mock_module):
            provider = LlamaIndexPluginReaderProvider(config)
            start = _time.monotonic()
            try:
                with pytest.raises((ReaderTimeoutError, RuntimeError)):
                    provider.read()
                elapsed = _time.monotonic() - start
            finally:
                release.set()

        # ~1s (the timeout), nowhere near the 30s the worker blocks for.
        assert elapsed < 10


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
    ReaderTimeoutError,
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
    """Item #2: reader_class must resolve to a BaseReader subclass, not any
    callable in an allowed module."""

    def test_non_class_rejected(self):
        """A non-class attribute (e.g. a string) is rejected before instantiation."""
        mock_module = Mock()
        mock_module.NotAClass = "just a string"
        config = _make_config(module_path="llama_index.readers.test", reader_class="NotAClass")
        with patch("importlib.import_module", return_value=mock_module):
            with pytest.raises(ReaderImportError, match="BaseReader"):
                LlamaIndexPluginReaderProvider(config)

    def test_non_basereader_class_rejected(self):
        """A callable/class that isn't a BaseReader subclass is rejected, even
        if it happens to have a load_data method."""
        class NotAReader:
            def load_data(self):
                return []

        mock_module = Mock()
        mock_module.BadReader = NotAReader
        config = _make_config(module_path="llama_index.readers.test", reader_class="BadReader")
        with patch("importlib.import_module", return_value=mock_module):
            with pytest.raises(ReaderImportError, match="BaseReader"):
                LlamaIndexPluginReaderProvider(config)

    def test_duck_typed_class_with_load_and_lazy_still_rejected(self):
        """A class implementing both load_data and lazy_load is still rejected
        if it doesn't inherit BaseReader - documents that the old duck-type
        check is fully removed, not just tightened."""
        class DuckReader:
            def load_data(self):
                return []

            def lazy_load(self):
                return iter(())

        mock_module = Mock()
        mock_module.DuckReader = DuckReader
        config = _make_config(module_path="llama_index.readers.test", reader_class="DuckReader")
        with patch("importlib.import_module", return_value=mock_module):
            with pytest.raises(ReaderImportError, match="BaseReader"):
                LlamaIndexPluginReaderProvider(config)

    def test_basereader_subclass_accepted(self):
        """A genuine BaseReader subclass passes the gate."""
        mock_module, _, _ = _mock_reader_module()
        config = _make_config()
        with patch("importlib.import_module", return_value=mock_module):
            provider = LlamaIndexPluginReaderProvider(config)
            assert provider._reader is not None

    def test_reader_missing_configured_load_method_rejected_at_import(self):
        """Both gates are kept: a BaseReader subclass that doesn't implement the
        configured load method fails loud at import (not [] at read time under
        fail_on_error=False). BaseReader has no lazy_load, so a reader without it
        configured for lazy_load trips the presence gate."""
        class NoLazyReader(BaseReader):
            def load_data(self):
                return []

        mock_module = Mock()
        mock_module.NoLazyReader = NoLazyReader
        config = _make_config(
            module_path="llama_index.readers.test", reader_class="NoLazyReader",
            load_method="lazy_load", fail_on_error=False,
        )
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


class _PermissiveProvider(LlamaIndexPluginReaderProvider):
    """Opts into resolving the specific env vars these tests reference."""
    ALLOWED_ENV_VARS = ("MY_TOKEN", "NONEXISTENT_VAR_XYZ")


class TestConfigAllowedEnvVars:
    """The allowlist can be set per-config (no subclassing needed, which the
    factory can't do), and still blocks anything not listed."""

    def test_config_allowed_env_vars_permits_resolution(self, monkeypatch):
        monkeypatch.setenv("MY_TOKEN", "secret123")
        mock_module, ctor, _ = _mock_reader_module()
        config = _make_config(init_args={"token": "$MY_TOKEN"},
                              allowed_env_vars=["MY_TOKEN"])
        with patch("importlib.import_module", return_value=mock_module):
            LlamaIndexPluginReaderProvider(config)
        ctor.assert_called_once_with(token="secret123")

    def test_config_allowed_env_vars_still_blocks_others(self, monkeypatch):
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "AKIAsecret")
        mock_module, _, _ = _mock_reader_module()
        config = _make_config(init_args={"token": "$AWS_SECRET_ACCESS_KEY"},
                              allowed_env_vars=["MY_TOKEN"])
        with patch("importlib.import_module", return_value=mock_module):
            with pytest.raises(ValueError, match="not in the permitted set"):
                LlamaIndexPluginReaderProvider(config)

    def test_default_empty_allowlist_blocks_when_unset(self, monkeypatch):
        # No allowed_env_vars on the config and no subclass -> nothing resolves.
        monkeypatch.setenv("MY_TOKEN", "secret123")
        mock_module, _, _ = _mock_reader_module()
        config = _make_config(init_args={"token": "$MY_TOKEN"})
        with patch("importlib.import_module", return_value=mock_module):
            with pytest.raises(ValueError, match="not in the permitted set"):
                LlamaIndexPluginReaderProvider(config)


class TestEnvVarResolution:
    """Item #4: $VAR_NAME references resolved from environment, but only for
    names in ALLOWED_ENV_VARS."""

    def test_resolves_permitted_env_var(self, monkeypatch):
        """A permitted $VAR_NAME is replaced with its environment value."""
        monkeypatch.setenv("MY_TOKEN", "secret123")
        mock_module, ctor, _ = _mock_reader_module()
        config = _make_config(init_args={"token": "$MY_TOKEN", "url": "https://example.com"})
        with patch("importlib.import_module", return_value=mock_module):
            _PermissiveProvider(config)
        # Verify the constructor received the resolved value
        ctor.assert_called_once_with(token="secret123", url="https://example.com")

    def test_disallowed_env_var_rejected_even_when_set(self, monkeypatch):
        """A config cannot pull a process credential that isn't allowlisted,
        even when it is present in the environment - this is the exfiltration
        path the allowlist closes."""
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "AKIAsecret")
        mock_module, _, _ = _mock_reader_module()
        config = _make_config(init_args={
            "token": "$AWS_SECRET_ACCESS_KEY",
            "base_url": "http://attacker.example",
        })
        with patch("importlib.import_module", return_value=mock_module):
            with pytest.raises(ValueError, match="not in the permitted set") as excinfo:
                LlamaIndexPluginReaderProvider(config)
        # The secret value must never be echoed in the exception message.
        assert "AKIAsecret" not in str(excinfo.value)

    def test_missing_env_var_raises(self):
        """A permitted-but-unset env var raises ValueError."""
        config = _make_config(init_args={"token": "$NONEXISTENT_VAR_XYZ"})
        mock_module, _, _ = _mock_reader_module()
        with patch("importlib.import_module", return_value=mock_module):
            with pytest.raises(ValueError, match="not set in the environment"):
                _PermissiveProvider(config)

    def test_non_env_string_unchanged(self):
        """Strings without $ prefix are passed through unchanged."""
        mock_module, ctor, _ = _mock_reader_module()
        config = _make_config(init_args={"url": "https://example.com", "count": "5"})
        with patch("importlib.import_module", return_value=mock_module):
            LlamaIndexPluginReaderProvider(config)
        ctor.assert_called_once_with(url="https://example.com", count="5")

    def test_lowercase_dollar_not_resolved(self):
        """$lowercase is not treated as env var (only $UPPER_CASE)."""
        mock_module, ctor, _ = _mock_reader_module()
        config = _make_config(init_args={"note": "$not_an_env_var"})
        with patch("importlib.import_module", return_value=mock_module):
            LlamaIndexPluginReaderProvider(config)
        ctor.assert_called_once_with(note="$not_an_env_var")

    # Bypass-boundary tests: forms the regex must NOT treat as an env-var ref.
    # Under the base provider a false match would raise "not in the permitted
    # set" instead of reaching the constructor verbatim.

    def test_braced_syntax_not_resolved(self, monkeypatch):
        """${VAR} brace syntax is passed through literally, not resolved."""
        monkeypatch.setenv("MY_TOKEN", "secret123")
        mock_module, ctor, _ = _mock_reader_module()
        config = _make_config(init_args={"token": "${MY_TOKEN}"})
        with patch("importlib.import_module", return_value=mock_module):
            LlamaIndexPluginReaderProvider(config)
        ctor.assert_called_once_with(token="${MY_TOKEN}")

    def test_nested_dict_not_resolved(self, monkeypatch):
        """Only top-level string values are resolved; a $VAR nested in a dict
        is left literal (and so never trips the allowlist)."""
        monkeypatch.setenv("SECRET", "leaked")
        mock_module, ctor, _ = _mock_reader_module()
        config = _make_config(init_args={"auth": {"token": "$SECRET"}})
        with patch("importlib.import_module", return_value=mock_module):
            LlamaIndexPluginReaderProvider(config)
        ctor.assert_called_once_with(auth={"token": "$SECRET"})

    def test_mid_string_not_resolved(self, monkeypatch):
        """A $VAR embedded mid-string is not an exact match, so it's literal."""
        monkeypatch.setenv("SECRET", "leaked")
        mock_module, ctor, _ = _mock_reader_module()
        config = _make_config(init_args={"url": "https://host/?k=$SECRET"})
        with patch("importlib.import_module", return_value=mock_module):
            LlamaIndexPluginReaderProvider(config)
        ctor.assert_called_once_with(url="https://host/?k=$SECRET")

    def test_load_args_env_var_also_gated(self, monkeypatch):
        """load_args goes through the same allowlist as init_args, so a
        disallowed $VAR there is rejected too (at read time)."""
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "AKIAsecret")
        mock_module, _, _ = _mock_reader_module()
        config = _make_config(load_args={"query": "$AWS_SECRET_ACCESS_KEY"})
        with patch("importlib.import_module", return_value=mock_module):
            provider = LlamaIndexPluginReaderProvider(config)
            with pytest.raises(ValueError, match="not in the permitted set"):
                provider.read()


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
