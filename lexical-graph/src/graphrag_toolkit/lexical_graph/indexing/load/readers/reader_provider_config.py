from pydantic import BaseModel, Field
from typing import Optional, Union, Dict, Any, Callable, Literal, List


class ReaderProviderConfig(BaseModel):
    """
    Base configuration model for reader providers.
    """
    type: Literal[
        "static", "s3_directory", "directory", "pdf", "web",
        "youtube", "docx", "github", "pptx"
    ]
    id: Optional[str] = Field(default=None, description="Optional identifier for this reader config")


class DirectoryReaderConfig(ReaderProviderConfig):
    type: Literal["directory"]
    input_dir: str = Field(..., description="Local directory path containing input files")


class S3DirectoryReaderConfig(ReaderProviderConfig):
    type: Literal["s3_directory"] = "s3_directory"
    bucket: str = Field(..., description="S3 bucket name where documents are stored")
    prefix: str = Field(default="", description="S3 prefix (folder path) to read from")

    region: Optional[str] = Field(default=None, description="AWS region of the S3 bucket")
    profile: Optional[str] = Field(default=None, description="AWS profile to use for S3 access")

    recursive: bool = Field(default=False, description="Whether to recursively read subdirectories")
    required_exts: Optional[List[str]] = Field(default=None, description="List of required file extensions")
    file_metadata: Optional[Union[Dict[str, Any], Callable[[str], Dict[str, Any]]]] = Field(
        default=None,
        description="Static metadata dictionary or callable that returns metadata per file"
    )
    filename_as_id: bool = Field(default=False, description="Whether to use the filename as the document ID")
    num_files_limit: Optional[int] = Field(default=None, description="Optional max number of files to read")
    exclude_hidden: bool = Field(default=True, description="Exclude hidden files from reading")
    exclude_empty: bool = Field(default=False, description="Exclude empty files from reading")
    encoding: Optional[str] = Field(default="utf-8", description="File encoding to use when reading")
    errors: Optional[str] = Field(default="ignore", description="Error handling strategy for decoding")


class PPTXReaderConfig(ReaderProviderConfig):
    type: Literal["pptx"]
    file_list: List[str] = Field(..., description="List of PowerPoint file paths (.pptx)")


class GitHubReaderConfig(ReaderProviderConfig):
    """
    Configuration for reading content from GitHub repositories.
    """
    type: Literal["github"]
    repo_url: str = Field(..., description="GitHub repository URL (e.g., https://github.com/user/repo)")
    branch: Optional[str] = Field(default="main", description="Branch name to read from")
    github_token: Optional[str] = Field(default=None, description="GitHub access token (or set GITHUB_TOKEN env)")
    filter_directories: Optional[List[str]] = Field(default=None, description="Directories to include")
    filter_file_extensions: Optional[List[str]] = Field(default=None, description="File extensions to include")
    verbose: bool = Field(default=False, description="Enable verbose output during reading")


class PDFReaderConfig(ReaderProviderConfig):
    """
    Configuration for reading PDF documents with optional metadata and table extraction.
    """
    type: Literal["pdf"]
    file_list: List[str] = Field(..., description="List of PDF file paths to read")
    metadata_filename: Optional[str] = Field(default=None, description="Optional metadata file path")
    extract_images: bool = Field(default=False, description="Whether to extract images from PDFs")
    extract_metadata: bool = Field(default=True, description="Whether to extract metadata from PDFs")
    infer_table_struct: bool = Field(default=False, description="Enable table structure inference in PDFs")

class WebReaderConfig(ReaderProviderConfig):
    """
    Configuration for reading web pages using LlamaIndex's WebPageReader.
    """
    type: Literal["web"]
    html_to_text: bool = True
    metadata_fn: Optional[Callable[[str], dict]] = Field(
        default_factory=lambda: (lambda url: {"url": url}),
        description="Function to extract metadata from a given URL"
    )
    max_num_tokens: Optional[int] = None
    continue_on_failure: bool = True