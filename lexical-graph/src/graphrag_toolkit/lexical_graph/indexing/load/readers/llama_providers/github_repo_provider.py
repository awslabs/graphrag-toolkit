from typing import Optional, List
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_base import ReaderProvider

class GitHubReaderProvider(ReaderProvider):
    """
    Reader provider for GitHub repositories using LlamaIndex's GithubRepositoryReader.
    """

    def __init__(
        self,
        github_token: str = None,
        default_branch: str = "main",
        filter_directories: Optional[List[str]] = None,
        filter_file_extensions: Optional[List[str]] = None,
        verbose: bool = False
    ):
        """
        Initialize GitHub client and reader options.

        Args:
            github_token: GitHub personal access token. If not provided, falls back to GITHUB_TOKEN env var.
            default_branch: Branch name to use when reading the repo.
            filter_directories: Limit crawling to these directories.
            filter_file_extensions: Limit to files with these extensions.
            verbose: Enable verbose logging from the underlying reader.
        """
        github_token = github_token or os.environ.get("GITHUB_TOKEN")
        if not github_token:
            raise ValueError("GitHub token is required. Set GITHUB_TOKEN or pass it explicitly.")

        self.default_branch = default_branch
        self.filter_directories = filter_directories
        self.filter_file_extensions = filter_file_extensions
        self.verbose = verbose

        logger.debug(f"Using GitHub token. Branch={self.default_branch}")
        self.github_client = GithubClient(github_token=github_token)

    def read(self, input_source: Any) -> List[Document]:
        """
        Read contents from a GitHub repository.

        Args:
            input_source: GitHub repository URL (e.g., https://github.com/user/repo)

        Returns:
            A list of LlamaIndex Document objects
        """
        parsed = urlparse(input_source)
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 2:
            raise ValueError(f"Invalid GitHub repo URL: {input_source}")

        owner, repo = parts[0], parts[1]

        reader = GithubRepositoryReader(
            github_client=self.github_client,
            owner=owner,
            repo=repo,
            filter_directories=self.filter_directories,
            filter_file_extensions=self.filter_file_extensions,
            verbose=self.verbose
        )

        return reader.load_data(branch=self.default_branch)

    def self_test(self) -> bool:
        docs = self.read("https://github.com/octocat/Hello-World")
        assert isinstance(docs, list)
        return len(docs) > 0
