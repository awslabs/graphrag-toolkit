"""
PowerPoint document reader provider using LlamaIndex.

This module provides a provider that reads text and metadata from PowerPoint (.pptx)
presentations using LlamaIndex's PptxReader.
"""

from pathlib import Path
from typing import List
from llama_index.core.schema import Document

# Lazy import for optional dependency
try:
    from llama_index.readers.file.slides import PptxReader
except ImportError as e:
    raise ImportError(
        "PptxReader requires the optional dependency 'python-pptx'.\n"
        "Install it with: pip install llama-index[readers-pptx]"
    ) from e

from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import (
    LlamaIndexReaderProviderBase,
)
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class PPTXReaderProvider(LlamaIndexReaderProviderBase):
    """
    Reader provider for PowerPoint .pptx files using LlamaIndex's PptxReader.
    """

    def __init__(self):
        """
        No parameters required for initialization. Accepts one or more .pptx file paths at read() time.
        """
        super().__init__(reader_cls=PptxReader)

    def self_test(self) -> bool:
        """
        Sanity check: Attempts to read a sample .pptx file.

        Returns:
            True if documents were successfully read.
        """
        test_file = Path("tests/fixtures/sample.pptx")
        if not test_file.exists():
            raise FileNotFoundError(f"Test file not found: {test_file}")
        docs: List[Document] = self.read(str(test_file))
        assert isinstance(docs, list)
        return len(docs) > 0

