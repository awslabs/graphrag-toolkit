"""
S3-based directory reader provider using LlamaIndex.

This provider downloads documents from an S3 bucket prefix into a temporary
directory and reads them using LlamaIndex's SimpleDirectoryReader.
"""

import os
import tempfile
import boto3
from typing import Any, List, Dict, Optional, Union, Callable
from pathlib import Path

from llama_index.core.schema import Document
from llama_index.core import SimpleDirectoryReader

from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_base import ReaderProvider
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import S3DirectoryReaderConfig
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)


class S3DirectoryReaderProvider(ReaderProvider):
    """
    Reader provider for S3-hosted directory trees with mixed document formats.
    Downloads files to a temporary directory and uses SimpleDirectoryReader.
    """

    def __init__(self, config: S3DirectoryReaderConfig):
        """
        Initialize the S3 directory reader provider.

        Args:
            config: Configuration object containing S3 bucket, prefix, region, profile, and reader parameters
        """
        self.bucket = config.bucket
        self.prefix = config.prefix
        self.region = config.region
        self.profile = config.profile

        # SimpleDirectoryReader parameters
        self.recursive = config.recursive
        self.required_exts = config.required_exts
        self.file_metadata = config.file_metadata
        self.filename_as_id = config.filename_as_id
        self.num_files_limit = config.num_files_limit
        self.exclude_hidden = config.exclude_hidden
        self.exclude_empty = config.exclude_empty
        self.encoding = config.encoding
        self.errors = config.errors

        self.file_extractor = self._build_extractor_map()

        session_kwargs = {"region_name": self.region}
        if self.profile:
            session_kwargs["profile_name"] = self.profile

        session = boto3.Session(**session_kwargs)
        self.s3_client = session.client("s3")

    def _build_extractor_map(self) -> Dict[str, Any]:
        """
        Build a mapping of file extensions to LlamaIndex reader instances.
        """
        try:
            from llama_index.readers.file import (
                DocxReader, HWPReader, PDFReader, EpubReader, FlatReader,
                HTMLTagReader, ImageReader, IPYNBReader, MarkdownReader,
                MboxReader, PptxReader, PandasCSVReader, PyMuPDFReader,
                XMLReader, PagedCSVReader, CSVReader
            )
        except ImportError as e:
            raise ImportError(
                "Missing optional LlamaIndex file readers. Install with:\n"
                "  pip install llama-index[readers]"
            ) from e

        return {
            ".docx": DocxReader(),
            ".hwp": HWPReader(),
            ".pdf": PyMuPDFReader(),
            ".epub": EpubReader(),
            ".txt": FlatReader(),
            ".html": HTMLTagReader(),
            ".jpg": ImageReader(),
            ".jpeg": ImageReader(),
            ".png": ImageReader(),
            ".ipynb": IPYNBReader(),
            ".md": MarkdownReader(),
            ".mbox": MboxReader(),
            ".pptx": PptxReader(),
            ".csv": PandasCSVReader(),
            ".xml": XMLReader(),
        }

    def _download_s3_prefix(self, local_dir: str):
        """
        Download supported files from the S3 bucket prefix to a local directory.
        """
        supported_exts = set(self.file_extractor.keys())
        skipped_files = []
        downloaded_files = 0

        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue

                _, ext = os.path.splitext(key.lower())
                rel_path = os.path.relpath(key, self.prefix)
                local_path = os.path.join(local_dir, rel_path)

                if ext in supported_exts:
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    self.s3_client.download_file(self.bucket, key, local_path)
                    downloaded_files += 1
                else:
                    skipped_files.append(key)

        if skipped_files:
            logger.warning("Skipped unsupported files in S3:")
            for f in skipped_files:
                logger.warning(f" - s3://{self.bucket}/{f}")

        if downloaded_files == 0:
            logger.warning("No supported files were downloaded from S3.")

    def read(self, input_source: Any = None) -> List[Document]:
        """
        Read documents from an S3 bucket directory using SimpleDirectoryReader.

        Args:
            input_source: Ignored. Bucket/prefix is set via config.

        Returns:
            A list of LlamaIndex Document objects
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._download_s3_prefix(tmp_dir)
            reader = SimpleDirectoryReader(
                input_dir=tmp_dir,
                file_extractor=self.file_extractor,
                recursive=self.recursive,
                required_exts=self.required_exts,
                file_metadata=self.file_metadata,
                filename_as_id=self.filename_as_id,
                num_files_limit=self.num_files_limit,
                exclude_hidden=self.exclude_hidden,
                exclude_empty=self.exclude_empty,
                encoding=self.encoding,
                errors=self.errors,
            )
            return reader.load_data()

    def self_test(self) -> bool:
        """
        Optional: Attempt to list objects from the configured bucket/prefix.

        Returns:
            True if S3 connection succeeds
        """
        try:
            result = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prefix,
                MaxKeys=1
            )
            return "Contents" in result and len(result["Contents"]) > 0
        except Exception as e:
            logger.error(f"S3 self_test failed: {e}")
            return False
