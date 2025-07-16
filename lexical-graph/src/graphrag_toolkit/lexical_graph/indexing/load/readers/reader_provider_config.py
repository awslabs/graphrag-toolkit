from pydantic import BaseModel
from typing import Optional, Literal, List


class ReaderProviderConfig(BaseModel):
    """
    Base configuration model for reader providers.
    """
    type: Literal[
        "static", "s3_directory", "directory", "pdf", "web", "youtube", "docx", "github", "pptx"
    ]
    id: Optional[str] = None


class DirectoryReaderConfig(ReaderProviderConfig):
    type: Literal["directory"]
    input_dir: str


class S3DirectoryReaderConfig(ReaderProviderConfig):
    type: Literal["s3_directory"] = "s3_directory"
    bucket: str
    prefix: str = ""

    # ✅ Do NOT default these — fallback should happen in provider using GraphRAGConfig
    region: Optional[str] = None
    profile: Optional[str] = None


class PPTXReaderConfig(ReaderProviderConfig):
    type: Literal["pptx"]
    file_list: List[str]


class GitHubReaderConfig(ReaderProviderConfig):
    """
    Configuration for reading content from GitHub repositories.
    A GitHub token is **required** for authenticated API access.
    """
    type: Literal["github"]
    github_token: Optional[str] = None  # ✅ make optional to allow fallback via env or GraphRAGConfig
    repo_url: str
    branch: Optional[str] = "main"
