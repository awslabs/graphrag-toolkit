# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List
import base64
from llama_index.core.schema import Document
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import PDFReaderConfig
from graphrag_toolkit.lexical_graph.indexing.load.readers.s3_file_mixin import S3FileMixin
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class AdvancedPDFReaderProvider(LlamaIndexReaderProviderBase, S3FileMixin):
    """Advanced PDF reader with image and table extraction."""

    def __init__(self, config: PDFReaderConfig):

        try:
            import pymupdf 
        except ImportError as e:
            raise ImportError(
                "pymupdf package not found, install with 'pip install pymupdf'"
            ) from e


        self.config = config
        self.metadata_fn = config.metadata_fn
        logger.debug("Initialized AdvancedPDFReaderProvider")

    def read(self, input_source) -> List[Document]:
        """Read PDF with text, images, and tables."""
        if not input_source:
            logger.error("No input source provided to AdvancedPDFReaderProvider")
            raise ValueError("input_source cannot be None or empty")
        
        logger.info(f"Reading advanced PDF from: {input_source}")
        processed_paths, temp_files, original_paths = self._process_file_paths(input_source)
        
        try:
            pdf_path = processed_paths[0]
            logger.debug(f"Opening PDF file: {pdf_path}")
            doc = pymupdf.open(pdf_path)
            documents = []
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                text = page.get_text()
                
                image_list = page.get_images()
                for img_index, img in enumerate(image_list):
                    try:
                        xref = img[0]
                        pix = pymupdf.Pixmap(doc, xref)
                        if pix.n - pix.alpha < 4:
                            img_data = pix.tobytes("png")
                            img_b64 = base64.b64encode(img_data).decode()
                            text += f"\n[IMAGE_{page_num}_{img_index}: base64_data={img_b64[:100]}...]"
                        pix = None
                    except Exception as e:
                        logger.warning(f"Failed to extract image {img_index} from page {page_num}: {e}")
                
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
            logger.info(f"Successfully read {len(documents)} page(s) from advanced PDF")
            return documents
            
        except Exception as e:
            logger.error(f"Failed to read advanced PDF from {input_source}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read advanced PDF: {e}") from e
        finally:
            self._cleanup_temp_files(temp_files)