# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import logging
from unittest.mock import MagicMock, patch


def test_raises_exception_if_dependencies_not_installed():
    from graphrag_toolkit.lexical_graph.indexing.load.readers.providers import GitHubReaderProvider
    from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import GitHubReaderConfig 
       
    with pytest.raises(ImportError) as exc_info:  
         reader = GitHubReaderProvider(GitHubReaderConfig())
 
    assert exc_info.value.args[0] == "PyGithub package not found, install with 'pip install PyGithub'"


def test_read_returns_empty_list_when_token_missing(caplog):
    """When github_token is empty, read() returns [] and logs a warning."""
    from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import GitHubReaderConfig

    mock_github_module = MagicMock()
    with patch.dict('sys.modules', {'llama_index.readers.github': mock_github_module}):
        from importlib import reload
        import graphrag_toolkit.lexical_graph.indexing.load.readers.providers.github_reader_provider as mod
        reload(mod)

        config = GitHubReaderConfig(github_token="")
        provider = mod.GitHubReaderProvider(config)

        with caplog.at_level(logging.WARNING):
            result = provider.read("awslabs/graphrag-toolkit")

        assert result == []
        assert "No GitHub token configured" in caplog.text


def test_read_returns_empty_list_when_token_none(caplog):
    """When github_token is None, read() returns [] and logs a warning."""
    from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import GitHubReaderConfig

    mock_github_module = MagicMock()
    with patch.dict('sys.modules', {'llama_index.readers.github': mock_github_module}):
        from importlib import reload
        import graphrag_toolkit.lexical_graph.indexing.load.readers.providers.github_reader_provider as mod
        reload(mod)

        config = GitHubReaderConfig(github_token=None)
        provider = mod.GitHubReaderProvider(config)

        with caplog.at_level(logging.WARNING):
            result = provider.read("awslabs/graphrag-toolkit")

        assert result == []
        assert "No GitHub token configured" in caplog.text