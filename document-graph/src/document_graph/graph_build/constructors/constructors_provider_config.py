# Copyright (c) Evan Erwee. All rights reserved.
"""Constructors provider config — configuration dataclass for constructor providers."""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List

@dataclass
class ConstructorProviderConfig:
    """
    Represents the configuration for a constructor provider.

    This class defines the parameters required to initialize and validate a constructor provider. It ensures
    that mandatory fields are present and allows for optional customization of additional configurations.

    Attributes:
        name: A string representing the name of the constructor.
        type: A string representing the type of the constructor.
        args: A dictionary containing additional arguments for the constructor configuration. Defaults to
            an empty dictionary.
        description: An optional string providing a description for the constructor. Defaults to None.
        required_columns: An optional list of strings specifying the required columns for the constructor.
            Defaults to None.
        batch_size: An optional integer specifying the batch size. Defaults to None.
    """
    name: str
    type: str
    args: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None
    required_columns: Optional[List[str]] = None
    batch_size: Optional[int] = None
    
    def __post_init__(self):
        """
        Validates and initializes post-construction logic for the object.

        Ensures that the `name` and `type` attributes are not empty upon initialization.
        If the `description` attribute is not provided, it sets a default description
        using the `type` and `name` attributes.

        Raises:
            ValueError: If `name` is empty.
            ValueError: If `type` is empty.
        """
        if not self.name:
            raise ValueError("Constructor name cannot be empty")
        if not self.type:
            raise ValueError("Constructor type cannot be empty")
        
        # Set default description if not provided
        if not self.description:
            self.description = f"{self.type} constructor: {self.name}"