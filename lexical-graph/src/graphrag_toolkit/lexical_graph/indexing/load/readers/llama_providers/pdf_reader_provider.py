from typing import Optional, List
from pathlib import Path
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import (
    LlamaIndexReaderProviderBase,
)

class PDFReaderProvider(LlamaIndexReaderProviderBase):
    """
    Reader provider for PDF files using LlamaIndex's PyMuPDFReader.
    """

    def __init__(
        self,
        metadata_filename: Optional[str] = None,
        extract_images: bool = False,
        extract_metadata: bool = True,
        infer_table_struct: bool = False
    ):
        """
        Initialize the PyMuPDFReader with optional parameters.

        Args:
            metadata_filename: Used to populate Document.metadata['filename']
            extract_images: Whether to extract inline images
            extract_metadata: Whether to extract PDF metadata
            infer_table_struct: Whether to attempt table structure inference
        """
        try:
            from llama_index.readers.file.pymu_pdf import PyMuPDFReader
        except ImportError as e:
            raise ImportError(
                "PyMuPDFReader requires the optional dependency 'pymupdf'. "
                "Install it with: pip install llama-index[readers-pymupdf]"
            ) from e

        reader = PyMuPDFReader(
            metadata_filename=metadata_filename,
            extract_images=extract_images,
            extract_metadata=extract_metadata,
            infer_table_struct=infer_table_struct,
        )
        super().__init__(reader_instance=reader)

    def self_test(self) -> bool:
        test_file = Path("tests/fixtures/sample.pdf")
        if not test_file.exists():
            raise FileNotFoundError(f"Test file not found: {test_file}")
        docs: List[Document] = self.read(str(test_file))
        assert isinstance(docs, list)
        return len(docs) > 0
