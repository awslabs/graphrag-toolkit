# Copyright (c) Evan Erwee. All rights reserved.

"""Language Enricher Provider for detecting document languages.

This module provides functionality to automatically detect the language of text
content in documents. It uses the 'langdetect' library to identify the language
of text fields and adds this information as a new field in the document record.
This enrichment is useful for multilingual document collections, enabling filtering,
routing, or specialized processing based on document language.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from langdetect import detect
from document_graph.transform.transformer_provider_base import TransformerProvider


class LanguageDetectionEnricher(TransformerProvider):
    """Enriches records with language detection for text content.
    
    This transformer analyzes text content in records and determines the language
    of the text using the 'langdetect' library. It adds a new field to each record
    containing the detected language code (e.g., 'en' for English, 'es' for Spanish).
    
    Configuration:
        text_field (str): The field name containing the text to analyze.
                         Defaults to 'content'.
        output_field (str): The field name where the detected language will be stored.
                           Defaults to 'language'.
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by detecting and adding language information.
        
        This method processes each record by:
        1. Extracting the text from the specified field
        2. Using langdetect to identify the language of the text
        3. Adding the detected language code to a new field in the record
        
        If text detection fails or the text field is empty/non-string,
        the language is set to "unknown".
        
        Args:
            records: A list of record dictionaries to process
            
        Returns:
            A list of enriched records with language information added.
            Each record maintains its original structure with an additional
            language field.
        """
        text_field = self.args.get("text_field", "content")
        output_field = self.args.get("output_field", "language")
        
        enriched_records = []
        for record in records:
            enriched_record = record.copy()
            text = record.get(text_field, "")
            
            if text and isinstance(text, str):
                try:
                    language = detect(text)
                except Exception:
                    language = "unknown"
                enriched_record[output_field] = language
            else:
                enriched_record[output_field] = "unknown"
                
            enriched_records.append(enriched_record)

        return enriched_records