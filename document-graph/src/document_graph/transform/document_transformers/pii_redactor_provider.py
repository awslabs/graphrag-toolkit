# Copyright (c) Evan Erwee. All rights reserved.

"""PII Redactor Provider for removing sensitive information from documents.

This module provides functionality to identify and redact Personally Identifiable
Information (PII) from document content. It uses the 'sanitary' library to detect
and replace sensitive information such as IP addresses, login names, credentials,
and other PII with redaction markers.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import Any, Dict, List

try:
    from sanitary import Sanitizer
except ImportError:
    Sanitizer = None

from document_graph.transform.transformer_provider_base import TransformerProvider
from document_graph.transform.transformer_provider_config import TransformerProviderConfig


class PIIRedactorProvider(TransformerProvider):
    """Redacts PII such as IP addresses, login names, credentials from text fields.
    
    This transformer identifies and redacts Personally Identifiable Information (PII)
    from specified fields in document records. It uses the `sanitary` library to
    detect and replace sensitive information with a redaction marker.
    
    Configuration:
        fields (List[str]): List of field names to scan for PII and redact.
                           If empty, no fields will be redacted.
    """

    def __init__(self, config: TransformerProviderConfig):
        """Initialize the PII redactor with configuration.
        
        Args:
            config: Configuration object containing transformer settings.
                   Must include an 'args' dictionary with a 'fields' list
                   specifying which fields to redact.
        """
        if Sanitizer is None:
            raise ImportError("PIIRedactorProvider requires 'sanitary' package: pip install sanitary")
        super().__init__(config)
        self.fields_to_redact = config.args.get("fields", [])
        self.sanitizer = Sanitizer(
            keys=self.fields_to_redact,
            replacement="***REDACTED***"
        )

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by redacting PII from specified fields.
        
        This method processes each record by applying the sanitizer to
        redact PII in the configured fields. The original record structure
        is preserved, with only the sensitive information replaced.
        
        Args:
            records: A list of record dictionaries to process
            
        Returns:
            A list of records with PII redacted from the specified fields.
            Each record maintains its original structure.
        """
        redacted_records = []
        for record in records:
            redacted = self.sanitizer.sanitize(record)
            redacted_records.append(redacted)
        return redacted_records
