# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import GitHubReaderConfig
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document

logger = logging.getLogger(__name__)

class GitHubReaderProvider:
    """Reader provider for GitHub repositories using LlamaIndex's GithubRepositoryReader."""

    def __init__(self, config: GitHubReaderConfig):
        """Initialize with GitHubReaderConfig."""

        try:
            from llama_index.readers.github import GithubRepositoryReader, GithubClient
        except ImportError as e:
            logger.error("Failed to import GithubRepositoryReader: missing PyGithub")
            raise ImportError(
                "PyGithub package not found, install with 'pip install PyGithub'"
            ) from e
        

        self.github_config = config
        self.metadata_fn = config.metadata_fn
        logger.debug(f"Initialized GitHubReaderProvider with verbose={config.verbose}")

    def read(self, input_source) -> List[Document]:
        """Read GitHub repository documents with metadata handling."""
        

        if not input_source:
            logger.error("No input source provided to GitHubReaderProvider")
            raise ValueError("input_source cannot be None or empty")

        if isinstance(input_source, tuple):
            repo_id, branch = input_source
        else:
            repo_id = input_source
            branch = "main"

        if "/" not in repo_id:
            logger.error(f"Invalid repository format: {repo_id}")
            raise ValueError(f"Expected input like 'owner/repo', got: {repo_id}")
        
        owner, repo = repo_id.split("/", 1)
        logger.info(f"Reading GitHub repository: {owner}/{repo} (branch: {branch})")

        try:
            github_client = GithubClient(
                github_token=self.github_config.github_token,
                verbose=self.github_config.verbose
            )

            reader = GithubRepositoryReader(
                owner=owner,
                repo=repo,
                github_client=github_client,
                verbose=self.github_config.verbose
            )

            documents = reader.load_data(branch=branch)
            logger.info(f"Successfully read {len(documents)} document(s) from GitHub repository")

            if self.metadata_fn:
                for doc in documents:
                    doc.metadata.update(self.metadata_fn(repo_id))

            return documents
        except Exception as e:
            logger.error(f"Failed to read GitHub repository {repo_id}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read GitHub repository: {e}") from e

