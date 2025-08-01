from typing import List
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import GitHubReaderConfig
from llama_index.core.schema import Document

class GitHubReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for GitHub repositories using LlamaIndex's GithubRepositoryReader."""

    def __init__(self, config: GitHubReaderConfig):
        """Initialize with GitHubReaderConfig."""
        # Lazy import
        try:
            from llama_index.readers.github import GithubRepositoryReader, GithubClient
        except ImportError as e:
            raise ImportError(
                "GithubRepositoryReader requires 'PyGithub'. Install with: pip install PyGithub"
            ) from e

        # Initialize GitHub client
        github_client = GithubClient(
            github_token=config.github_token,
            verbose=config.verbose
        )
        
        reader_kwargs = {
            "github_client": github_client,
            "verbose": config.verbose
        }
        
        super().__init__(config=config, reader_cls=GithubRepositoryReader, **reader_kwargs)
        self.github_config = config
        self.metadata_fn = config.metadata_fn

    def read(self, input_source) -> List[Document]:
        """Read GitHub repository documents with metadata handling."""
        documents = self._reader.load_data(repo=input_source)
        
        # Apply metadata function if provided
        if self.metadata_fn:
            for doc in documents:
                additional_metadata = self.metadata_fn(input_source)
                doc.metadata.update(additional_metadata)
        
        return documents