# Copyright (c) Evan Erwee. All rights reserved.

"""Transformer provider configuration models.

This module defines the configuration models used for transformer providers
in the document graph processing system. These configurations are used to
instantiate and parameterize transformer instances that perform various
data transformation operations during document processing.
"""

import logging
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available

class TransformerProviderConfig(BaseModel):
    """Configuration for transformers providers.
    
    Used by plugins and transformation steps to configure
    data transformation operations.
    
    Attributes:
        name: Transformer name for identification
        type: Transformer type (optional, for factory resolution)
        args: Transformer-specific arguments and parameters
        parameters: Alias for args (user-friendly)
        
    Examples:
        >>> config = TransformerProviderConfig(
        ...     name="text_normalizer",
        ...     type="normalizer",
        ...     args={"lowercase": True, "remove_punctuation": True}
        ... )
        >>> config.name
        'text_normalizer'
        >>> config.args["lowercase"]
        True
    """
    name: str = Field(..., description="Transformer name")
    type: Optional[str] = Field(default=None, description="Transformer type")
    args: Dict[str, Any] = Field(default_factory=dict, description="Transformer arguments")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Transformer parameters (alias for args)")
    
    def __init__(self, **data):
        """Initialize transformer provider configuration.
        
        Initializes the configuration and synchronizes the args and parameters
        fields to ensure they contain the same data, regardless of which one
        was provided in the input.
        
        Args:
            **data: Keyword arguments for configuration fields including name,
                   type, args, and parameters
                   
        Examples:
            >>> config = TransformerProviderConfig(
            ...     name="entity_enricher",
            ...     parameters={"entities": ["person", "organization"]}
            ... )
            >>> config.args == config.parameters
            True
        """
        super().__init__(**data)
        # Sync parameters and args
        if self.parameters and not self.args:
            self.args = self.parameters
        elif self.args and not self.parameters:
            self.parameters = self.args

# Alias for backward compatibility
BaseTransformerConfig = TransformerProviderConfig
