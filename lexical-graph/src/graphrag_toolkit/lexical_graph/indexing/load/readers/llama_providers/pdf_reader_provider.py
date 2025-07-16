"""
PDF document reader provider using LlamaIndex.

This provider reads and extracts text and metadata from PDF documents
using LlamaIndex's PyMuPDFReader.
"""

from pathlib import Path
from typing import List
from llama_index.core.schema import Document

# Lazy import for optional dependency
try:
    from llama_index.readers.file.pymu_pdf import PyMuPDFReader
except ImportError as e:
    raise ImportError(
        "PyMuPDFReader requires the optional dependency 'pymupdf'.\n"
        "Install it using: pip install llama-index[readers-pymupdf]"
    ) from e

from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import (
    LlamaIndexReaderProviderBase,
)
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class PDFReaderProvider(LlamaIndexReaderProviderBase):
    """
    Reader provider for PDF files using LlamaIndex's PyMuPDFReader.
    """

    def __init__(self):
        """
        No parameters required for initialization. Accepts PDF file paths at read() time.
        """
        super().__init__(reader_cls=PyMuPDFReader)

    def self_test(self) -> bool:
        """
        Sanity check: Attempts to read a sample PDF file.

        Returns:
            True if at least one document is read successfully.
        """
        test_file = Path("tests/fixtures/sample.pdf")
        if not test_file.exists():
            raise FileNotFoundError(f"Test file not found: {test_file}")
        docs: List[Document] = self.read(str(test_file))
        assert isinstance(docs, list)
        return len(docs) > 0

