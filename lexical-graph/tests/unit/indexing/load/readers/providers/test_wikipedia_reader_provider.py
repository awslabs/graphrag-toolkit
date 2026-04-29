# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch, MagicMock


def test_raises_exception_if_dependencies_not_installed():
    with patch.dict('sys.modules', {'llama_index.readers.wikipedia': None}):
        from importlib import reload
        import graphrag_toolkit.lexical_graph.indexing.load.readers.providers.wikipedia_reader_provider as mod
        reload(mod)

        from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import WikipediaReaderConfig

        with pytest.raises(ImportError) as exc_info:
            mod.WikipediaReaderProvider(WikipediaReaderConfig())

        assert "llama-index-readers-wikipedia" in str(exc_info.value)


def test_init_reader_creates_reader_instance():
    """Test that _init_reader uses the stored class from __init__."""
    from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import WikipediaReaderConfig

    mock_reader_cls = Mock()
    mock_reader_instance = Mock()
    mock_reader_cls.return_value = mock_reader_instance

    mock_module = MagicMock()
    mock_module.WikipediaReader = mock_reader_cls

    with patch.dict('sys.modules', {'llama_index.readers.wikipedia': mock_module}):
        from importlib import reload
        import graphrag_toolkit.lexical_graph.indexing.load.readers.providers.wikipedia_reader_provider as mod
        reload(mod)

        provider = mod.WikipediaReaderProvider(WikipediaReaderConfig())
        assert provider._reader is None

        provider._init_reader()
        assert provider._reader is mock_reader_instance
        mock_reader_cls.assert_called_once()


def test_init_reader_only_creates_once():
    """Test that _init_reader is lazy and only creates the reader once."""
    from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import WikipediaReaderConfig

    mock_reader_cls = Mock()
    mock_module = MagicMock()
    mock_module.WikipediaReader = mock_reader_cls

    with patch.dict('sys.modules', {'llama_index.readers.wikipedia': mock_module}):
        from importlib import reload
        import graphrag_toolkit.lexical_graph.indexing.load.readers.providers.wikipedia_reader_provider as mod
        reload(mod)

        provider = mod.WikipediaReaderProvider(WikipediaReaderConfig())
        provider._init_reader()
        provider._init_reader()
        mock_reader_cls.assert_called_once()


def test_read_raises_on_empty_input():
    """Test that read raises ValueError on empty input."""
    from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import WikipediaReaderConfig

    mock_module = MagicMock()
    with patch.dict('sys.modules', {'llama_index.readers.wikipedia': mock_module}):
        from importlib import reload
        import graphrag_toolkit.lexical_graph.indexing.load.readers.providers.wikipedia_reader_provider as mod
        reload(mod)

        provider = mod.WikipediaReaderProvider(WikipediaReaderConfig())

        with pytest.raises(ValueError, match="cannot be None or empty"):
            provider.read("")
