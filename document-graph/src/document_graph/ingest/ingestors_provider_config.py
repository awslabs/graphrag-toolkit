"""Ingestors provider config — configuration dataclass for ingestor providers."""
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List

@dataclass
class IngestorProviderConfig:
    """
    Represents the configuration for an ingestor provider.

    This class encapsulates the necessary attributes and validation rules for
    defining an ingestor provider's configuration. It is mainly used to store
    the metadata and validation requirements for the ingestor.

    Attributes:
        name: A unique name identifying the ingestor.
        type: The type or category of the ingestor.
        args: A dictionary containing additional parameters for the ingestor.
        description: An optional description for the ingestor; defaults to a
                     combination of type and name if not provided.
        required_columns: An optional list of column names required by the
                          ingestor.

    Raises:
        ValueError: Raised if `name` or `type` is empty during initialization.

    """
    name: str
    type: str
    args: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None
    required_columns: Optional[List[str]] = None
    
    def __post_init__(self):
        """
        Performs post-initialization validation and sets default values for some
        attributes of the ingestor instance after the object is instantiated.

        Raises
        ------
        ValueError
            If the 'name' attribute is empty or None.
        ValueError
            If the 'type' attribute is empty or None.
        """
        if not self.name:
            raise ValueError("Ingestor name cannot be empty")
        if not self.type:
            raise ValueError("Ingestor type cannot be empty")
        
        # Set default description if not provided
        if not self.description:
            self.description = f"{self.type} ingestor: {self.name}"