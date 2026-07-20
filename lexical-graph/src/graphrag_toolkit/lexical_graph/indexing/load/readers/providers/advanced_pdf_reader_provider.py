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

        # Hold the module on the instance: the import above is local to __init__,
        # so read() cannot reach the bare `pymupdf` name without this.
        self._pymupdf = pymupdf
        self.config = config
        self.extract_tables = config.extract_tables
        self.metadata_fn = config.metadata_fn
        logger.debug(f"Initialized AdvancedPDFReaderProvider with extract_tables={config.extract_tables}")

    def _find_tables(self, page, page_number):
        """Return the tables detected on a page, or an empty list on failure."""
        try:
            return list(page.find_tables().tables)
        except Exception as e:
            logger.warning(f"Failed to detect tables on page {page_number}: {e}")
            return []

    @staticmethod
    def _block_in_a_table(x0, y0, x1, y1, table_bboxes, threshold=0.5):
        """True if more than `threshold` of a block's area lies inside any table
        bbox (default: more than half), i.e. the block belongs to the table."""
        block_area = max(0.0, x1 - x0) * max(0.0, y1 - y0)
        if block_area == 0:
            return False
        for bx0, by0, bx1, by1 in table_bboxes:
            overlap_w = max(0.0, min(x1, bx1) - max(x0, bx0))
            overlap_h = max(0.0, min(y1, by1) - max(y0, by0))
            if overlap_w * overlap_h > threshold * block_area:
                return True
        return False

    def _page_text(self, page, tables):
        """Page text with table-region text removed.

        get_text() returns each table's cell text inline, and the table is also
        appended as markdown, so without this the cell content would land in the
        document twice and inflate downstream extraction (which dedups only on
        exact-string output). Drop any text block sitting mostly inside a table's
        bounding box, leaving the markdown as the table's single representation.
        """
        if not tables:
            return page.get_text()
        # Skip tables with a missing/empty bbox: a None would raise in tuple()
        # and a partial bbox would unpack short in _block_in_a_table.
        table_bboxes = [tuple(table.bbox) for table in tables if table.bbox]
        kept = []
        # "blocks" tuple layout: (x0, y0, x1, y1, text, block_no, block_type).
        # block_type 1 is an image block (placeholder text); images are handled
        # separately below, so keep only text blocks (type 0).
        for block in page.get_text("blocks"):
            x0, y0, x1, y1, block_text = block[:5]
            block_type = block[6] if len(block) > 6 else 0
            if block_type != 0:
                continue
            if not self._block_in_a_table(x0, y0, x1, y1, table_bboxes):
                kept.append(block_text)
        return "".join(kept)

    def _append_tables(self, page_number, tables, text):
        """Append each table to the text as a markdown block.

        Returns the augmented text and the number of tables rendered. Row/column
        relationships are preserved via the pipe-delimited markdown that pymupdf's
        TableFinder produces. The count reflects tables actually written, not just
        detected, so table_count never overstates what a consumer can find.
        """
        rendered = 0
        for tbl_index, table in enumerate(tables):
            try:
                text += f"\n[TABLE_{page_number}_{tbl_index}]\n{table.to_markdown()}"
                rendered += 1
            except Exception as e:
                logger.warning(f"Failed to render table {tbl_index} on page {page_number}: {e}")
        return text, rendered

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
            doc = self._pymupdf.open(pdf_path)
            documents = []

            for page_num in range(len(doc)):
                page = doc[page_num]
                # 1-indexed page number used consistently across markers, log
                # messages, and metadata (page_num itself stays 0-based for the
                # doc[page_num] lookup).
                page_number = page_num + 1

                tables = self._find_tables(page, page_number) if self.extract_tables else []
                text = self._page_text(page, tables)

                image_list = page.get_images()
                for img_index, img in enumerate(image_list):
                    try:
                        xref = img[0]
                        pix = self._pymupdf.Pixmap(doc, xref)
                        if pix.n - pix.alpha < 4:
                            img_data = pix.tobytes("png")
                            img_b64 = base64.b64encode(img_data).decode()
                            text += f"\n[IMAGE_{page_number}_{img_index}: base64_data={img_b64[:100]}...]"
                        pix = None
                    except Exception as e:
                        logger.warning(f"Failed to extract image {img_index} from page {page_number}: {e}")

                text, table_count = self._append_tables(page_number, tables, text)

                page_doc = Document(
                    text=text,
                    metadata={
                        'page_number': page_number,
                        'source': 'advanced_pdf',
                        'file_path': original_paths[0],
                        'table_count': table_count
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
