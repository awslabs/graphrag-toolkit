from typing import List
import fitz  # pymupdf
import base64
from llama_index.core.schema import Document
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import PDFReaderConfig
from graphrag_toolkit.lexical_graph.indexing.load.readers.s3_file_mixin import S3FileMixin

class AdvancedPDFReaderProvider(LlamaIndexReaderProviderBase, S3FileMixin):
    """Advanced PDF reader with image and table extraction."""

    def __init__(self, config: PDFReaderConfig):
        self.config = config
        self.metadata_fn = config.metadata_fn

    def read(self, input_source) -> List[Document]:
        """Read PDF with text, images, and tables."""
        processed_paths, temp_files, original_paths = self._process_file_paths(input_source)
        
        try:
            pdf_path = processed_paths[0]
            doc = fitz.open(pdf_path)
            documents = []
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                
                # Extract text
                text = page.get_text()
                
                # Extract images
                image_list = page.get_images()
                for img_index, img in enumerate(image_list):
                    xref = img[0]
                    pix = fitz.Pixmap(doc, xref)
                    if pix.n - pix.alpha < 4:  # GRAY or RGB
                        img_data = pix.tobytes("png")
                        img_b64 = base64.b64encode(img_data).decode()
                        text += f"\n[IMAGE_{page_num}_{img_index}: base64_data={img_b64[:100]}...]"
                    pix = None
                
                # Create document for this page
                page_doc = Document(
                    text=text,
                    metadata={
                        'page_number': page_num + 1,
                        'source': 'advanced_pdf',
                        'file_path': original_paths[0]
                    }
                )
                
                if self.metadata_fn:
                    additional_metadata = self.metadata_fn(original_paths[0])
                    page_doc.metadata.update(additional_metadata)
                
                documents.append(page_doc)
            
            doc.close()
            return documents
            
        finally:
            self._cleanup_temp_files(temp_files)