# Copyright (c) Evan Erwee. All rights reserved.

"""Token Truncator module for document graph operations.

This module provides a transformer that limits the number of tokens in string fields
of a record to prevent oversized text fields and ensure consistent processing.
It uses a pre-trained tokenizer to tokenize text and truncate to a maximum token count.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from transformers import AutoTokenizer
from document_graph.transform.transformer_provider_base import TransformerProvider

class TokenTruncator(TransformerProvider):
    """Transformer that limits the number of tokens in string fields.
    
    This transformer ensures string fields don't exceed a maximum token count
    by tokenizing the text using a pre-trained tokenizer and truncating to the
    specified maximum number of tokens. This helps prevent oversized text fields
    and ensures consistent processing downstream, especially for models with
    token limits.
    
    Attributes:
        config: Configuration object containing transformer settings
        tokenizer: Pre-trained tokenizer from the Hugging Face transformers library
        
    Examples:
        >>> from document_graph.transform.transformer_provider_config import TransformerProviderConfig
        >>> config = TransformerProviderConfig(
        ...     name="token_truncator",
        ...     args={
        ...         "max_tokens": 5,
        ...         "fields": ["text"],
        ...         "model_name": "bert-base-uncased"
        ...     }
        ... )
        >>> truncator = TokenTruncator(config)
        >>> result = truncator.transform({"text": "This is a long text that will be truncated to just five tokens"})
        >>> result["text"]
        'this is a long text'
    """
    
    def __init__(self, config):
        """Initialize the token truncator with configuration.
        
        Args:
            config: Transformer configuration with name, type, and args
            
        Note:
            Loads a pre-trained tokenizer based on the model_name in config.args.
            Defaults to "bert-base-uncased" if not specified.
        """
        super().__init__(config)
        model_name = self.config.args.get("model_name", "bert-base-uncased")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)

    def transform(self, record: dict) -> dict:
        """Truncate specified string fields to a maximum token count.
        
        Args:
            record: A dictionary containing record data
            
        Returns:
            A dictionary with string fields truncated to max_tokens tokens
            
        Note:
            This implementation differs from the base class by accepting a single
            record dictionary rather than a list of records.
            
            Only processes fields specified in the "fields" configuration parameter.
            Fields that don't exist in the record or aren't strings are ignored.
            
            The tokenization and detokenization process may slightly alter the text
            beyond just truncation, as the tokenizer's conversion back to string
            may normalize whitespace or other characters.
        """
        max_tokens = self.config.args.get("max_tokens", 128)
        fields = self.config.args.get("fields", [])

        for field in fields:
            if field in record and isinstance(record[field], str):
                tokens = self.tokenizer.tokenize(record[field])
                truncated = self.tokenizer.convert_tokens_to_string(tokens[:max_tokens])
                record[field] = truncated
        return record
