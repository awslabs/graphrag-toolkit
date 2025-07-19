"""
Microsoft Word document reader provider using LlamaIndex.

This module provides a provider that reads text and metadata from Microsoft Word (.docx)
documents using LlamaIndex's DocxReader.
"""

from pathlib import Path
from typing import List, Union
from llama_index.core.schema import Document

# Lazy import for optional dependency
try:
    from llama_index.readers.file.docs import DocxReader
except ImportError as e:
    raise ImportError(
        "DocxReader requires the 'python-docx' package. "
        "Install with: pip install llama-index[readers-docs]"
    ) from e

from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import (
    LlamaIndexReaderProviderBase,
)
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)


class DocxReaderProvider(LlamaIndexReaderProviderBase):
    """
    Reader provider for Word .docx documents using LlamaIndex's DocxReader.
    """

    def __init__(self):
        """
        No parameters required for initialization. Accepts one or more .docx file paths at read() time.
        """
        super().__init__(reader_cls=DocxReader)

    def self_test(self) -> bool:
        """
        Sanity check: Attempts to read a sample .docx file.

        Returns:
            True if at least one document is loaded.
        """
        test_file = Path("tests/fixtures/sample.docx")
        if not test_file.exists():
            raise FileNotFoundError(f"Test file not found: {test_file}")
        docs: List[Document] = self.read(str(test_file))
        assert isinstance(docs, list)
        return len(docs) > 0
