# Copyright (c) Evan Erwee. All rights reserved.

"""Text Chunker module for splitting document content into manageable chunks.

This module provides functionality to divide long text content into smaller,
overlapping chunks for more efficient processing and analysis. This is particularly
useful for large documents that need to be processed in smaller segments, such as
for embedding generation, semantic search, or other NLP operations that have
input size limitations.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider


class TextChunkerTransformer(TransformerProvider):
    """Splits long text content into smaller, potentially overlapping chunks.
    
    This transformer divides text content from records into smaller chunks
    of a specified size, with optional overlap between consecutive chunks.
    Each chunk becomes a separate record that maintains the original record's
    metadata while adding chunk-specific information.
    
    Configuration:
        chunk_size (int): Maximum size of each text chunk in characters.
                         Defaults to 512.
        overlap (int): Number of characters to overlap between consecutive chunks.
                      Defaults to 0 (no overlap).
        text_field (str): The field name containing the text to chunk.
                         Defaults to 'content'.
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by splitting text content into smaller chunks.
        
        This method processes each record by:
        1. Extracting the text from the specified field
        2. Dividing it into chunks of the configured size
        3. Creating new records for each chunk with additional metadata
        
        Records without text content or with non-string content in the specified
        field are passed through unchanged.
        
        Each chunk record includes the following additional fields:
        - chunk_index: Sequential index of the chunk (0-based)
        - chunk_start: Character position where the chunk starts in the original text
        - chunk_end: Character position where the chunk ends in the original text
        - original_id: The ID of the original record
        
        If the original record has an 'id' field, each chunk's ID will be
        formatted as "{original_id}-chunk-{chunk_index}".
        
        Args:
            records: A list of record dictionaries to transform
            
        Returns:
            A list of transformed records, where text content has been
            divided into chunks with each chunk as a separate record.
        """
        chunk_size = int(self.args.get("chunk_size", 512))
        overlap = int(self.args.get("overlap", 0))
        text_field = self.args.get("text_field", "content")
        
        output_records = []

        for record in records:
            text = record.get(text_field, "")
            if not text or not isinstance(text, str):
                output_records.append(record)
                continue

            start = 0
            chunk_index = 0

            while start < len(text):
                end = start + chunk_size
                chunk = text[start:end]
                
                chunk_record = record.copy()
                chunk_record[text_field] = chunk
                chunk_record['chunk_index'] = chunk_index
                chunk_record['chunk_start'] = start
                chunk_record['chunk_end'] = min(end, len(text))
                chunk_record['original_id'] = record.get('id', '')
                
                if 'id' in chunk_record:
                    chunk_record['id'] = f"{chunk_record['id']}-chunk-{chunk_index}"
                
                output_records.append(chunk_record)

                chunk_index += 1
                start = end - overlap

        return output_records