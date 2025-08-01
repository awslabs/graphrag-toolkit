from typing import List
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import GitHubReaderConfig
from llama_index.core.schema import Document


class GitHubReaderProvider:
    """Reader provider for GitHub repositories using LlamaIndex's GithubRepositoryReader."""

    def __init__(self, config: GitHubReaderConfig):
        """Initialize with GitHubReaderConfig."""
        self.github_config = config
        self.metadata_fn = config.metadata_fn

    def read(self, input_source) -> List[Document]:
        """Read GitHub repository documents with metadata handling."""
        try:
            from llama_index.readers.github import GithubRepositoryReader, GithubClient
        except ImportError as e:
            raise ImportError(
                "GithubRepositoryReader requires 'PyGithub'. Install with: pip install PyGithub"
            ) from e

        # Support both string input and (repo_id, branch) tuple
        if isinstance(input_source, tuple):
            repo_id, branch = input_source
        else:
            repo_id = input_source
            branch = "main"  # fallback to default

        # Parse "owner/repo"
        if "/" not in repo_id:
            raise ValueError(f"Expected input like 'owner/repo', got: {repo_id}")
        owner, repo = repo_id.split("/", 1)

        # Init GitHub client
        github_client = GithubClient(
            github_token=self.github_config.github_token,
            verbose=self.github_config.verbose
        )

        # Init reader
        reader = GithubRepositoryReader(
            owner=owner,
            repo=repo,
            github_client=github_client,
            verbose=self.github_config.verbose
        )

        # Load documents from specified branch
        documents = reader.load_data(branch=branch)

        # Optional: attach metadata
        if self.metadata_fn:
            for doc in documents:
                doc.metadata.update(self.metadata_fn(repo_id))

        return documents

