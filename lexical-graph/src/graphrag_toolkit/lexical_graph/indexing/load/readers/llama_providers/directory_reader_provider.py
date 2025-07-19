"""
Directory-based document reader provider using LlamaIndex.

This module provides a provider that reads documents from a local directory,
supporting multiple file formats through LlamaIndex's SimpleDirectoryReader.
"""

import os
from typing import Any, List, Dict, Optional, Union, Callable
from pathlib import Path

try:
    from llama_index.core import SimpleDirectoryReader
    from llama_index.core.schema import Document
except ImportError as e:
    raise ImportError("This provider requires 'llama-index'. Install it via: pip install llama-index") from e

from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_base import ReaderProvider
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)


class DirectoryReaderProvider(ReaderProvider):
    """
    Reader provider for directories containing mixed document formats.
    Uses LlamaIndex's SimpleDirectoryReader and format-specific file readers.
    """

    def __init__(self, data_dir: str = "./data"):
        """
        Initialize the directory reader provider.

        Args:
            data_dir: Default directory path to read documents from if not specified in `read()`
        """
        self.data_dir = data_dir
        self.file_extractor = self._build_extractor_map()

    def _build_extractor_map(self) -> Dict[str, Any]:
        """
        Map file extensions to appropriate LlamaIndex reader instances.
        Returns a dictionary mapping file extensions to reader objects.
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
                "DirectoryReaderProvider requires optional file readers. "
                "Try installing: pip install llama-index[readers-docs]"
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
            ".csv": PagedCSVReader(),
            ".xml": XMLReader(),
        }

    def read(
        self,
        input_source: Optional[Union[str, Path]] = None,
        recursive: bool = False,
        required_exts: Optional[List[str]] = None,
        file_metadata: Optional[Union[Dict[str, Any], Callable[[str], Dict[str, Any]]]] = None,
        filename_as_id: bool = False,
        input_files: Optional[List[Union[str, Path]]] = None,
    ) -> List[Document]:
        """
        Read documents from a directory using LlamaIndex's SimpleDirectoryReader.
        Logs any unsupported/skipped files.

        Args:
            input_source: Directory path to read from. If None, uses the default `data_dir`.
            recursive: Whether to recurse into subdirectories.
            required_exts: List of file extensions to include (e.g., [".pdf", ".txt"]).
            file_metadata: Dict or callable to attach metadata to each document.
            filename_as_id: Whether to use filename as document ID.
            input_files: Explicit list of files to read instead of scanning a directory.

        Returns:
            A list of LlamaIndex Document objects
        """
        directory = Path(input_source or self.data_dir)
        supported_extensions = set(self.file_extractor.keys())

        if input_files is None:
            all_files = [f for f in directory.rglob("*") if f.is_file()]
            skipped_files = [
                f for f in all_files
                if not any(str(f).lower().endswith(ext) for ext in supported_extensions)
            ]

            if skipped_files:
                logger.warning("The following files were skipped (unsupported formats):")
                for f in skipped_files:
                    logger.warning(f" - {f}")

        reader = SimpleDirectoryReader(
            input_dir=str(directory),
            recursive=recursive,
            required_exts=required_exts,
            file_metadata=file_metadata,
            filename_as_id=filename_as_id,
            file_extractor=self.file_extractor,
            input_files=input_files
        )

        return reader.load_data()

    def self_test(self) -> bool:
        """
        Sanity check: Attempts to read from the default data_dir.

        Returns:
            True if at least one document was loaded
        """
        docs = self.read()
        assert isinstance(docs, list)
        return len(docs) > 0
