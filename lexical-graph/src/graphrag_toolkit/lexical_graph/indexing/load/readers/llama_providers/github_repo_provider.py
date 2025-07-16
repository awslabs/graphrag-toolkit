"""
GitHub repository reader provider using LlamaIndex.

This provider reads the contents of a public or private GitHub repository using
LlamaIndex's GithubRepositoryReader. Requires a GitHub token for authenticated access.
"""

import os
from typing import List, Any
from urllib.parse import urlparse
from llama_index.core.schema import Document

# Lazy import for GitHub readers
try:
    from llama_index.readers.github import GithubRepositoryReader, GithubClient
except ImportError as e:
    raise ImportError(
        "GitHubReaderProvider requires the 'llama-index[readers-github]' optional dependency. "
        "Install it via: pip install llama-index[readers-github]"
    ) from e

from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_base import ReaderProvider
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)


class GitHubReaderProvider(ReaderProvider):
    """
    Reader provider for GitHub repositories using LlamaIndex's GithubRepositoryReader.
    """

    def __init__(self, github_token: str = None):
        """
        Initialize GitHub client for use in load operations.

        Args:
            github_token: GitHub personal access token. If not provided, falls back to GITHUB_TOKEN env var.
        """
        github_token = github_token or os.environ.get("GITHUB_TOKEN")
        if not github_token:
            raise ValueError("GitHub token is required. Set GITHUB_TOKEN or pass it explicitly.")

        logger.debug("Using authenticated GitHub client")
        self.github_client = GithubClient(github_token=github_token)

    def read(self, input_source: Any) -> List[Document]:
        """
        Read contents from a GitHub repository.

        Args:
            input_source: GitHub repository URL (e.g., https://github.com/user/repo)

        Returns:
            A list of LlamaIndex Document objects
        """
        logger.debug(f"Reading from GitHub repo URL: {input_source}")

        parsed = urlparse(input_source)
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 2:
            raise ValueError(f"Invalid GitHub repo URL: {input_source}")

        owner, repo = parts[0], parts[1]
        branch = "main"  # This can be made configurable later

        logger.debug(f"Owner: {owner}, Repo: {repo}, Branch: {branch}")
        reader = GithubRepositoryReader(
            github_client=self.github_client,
            owner=owner,
            repo=repo
        )
        return reader.load_data(branch=branch)

    def self_test(self) -> bool:
        """
        Sanity check: Attempts to read a small known repo (public).

        Returns:
            True if test passes and documents are loaded.
        """
        docs = self.read("https://github.com/octocat/Hello-World")
        assert isinstance(docs, list)
        return len(docs) > 0

