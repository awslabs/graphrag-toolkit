from typing import Any, List
from pydantic import BaseModel, validator
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document
from llama_index.core.readers.base import BasePydanticReader
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_base import ReaderProvider
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config_base import ReaderProviderConfig

logger = logging.getLogger(__name__)

class PydanticReaderProviderBase(ReaderProvider):
    """
    Base class for Pydantic-enabled reader providers.
    Provides automatic validation and serialization capabilities.
    """

    def __init__(self, config: ReaderProviderConfig, reader_cls, **reader_kwargs):
        self.config = config
        logger.debug(f"Instantiating Pydantic reader: {reader_cls.__name__}")
        
        # Ensure reader_cls is a BasePydanticReader
        if not issubclass(reader_cls, BasePydanticReader):
            raise ValueError(f"{reader_cls.__name__} must inherit from BasePydanticReader")
        
        self._reader = reader_cls(**reader_kwargs)

    def read(self, input_source: Any) -> List[Document]:
        """Read documents with Pydantic validation."""
        logger.debug("Starting Pydantic read()")
        logger.debug(f"Reader class: {self._reader.__class__.__name__}")
        
        try:
            return self._reader.load_data(input_source)
        except Exception as e:
            logger.exception("Error during Pydantic read()")
            raise RuntimeError(
                f"Failed to read using {self._reader.__class__.__name__}: {e}"
            ) from e

    def to_dict(self) -> dict:
        """Serialize reader configuration to dictionary."""
        return self._reader.dict()

    def validate_config(self) -> bool:
        """Validate reader configuration."""
        try:
            self._reader.dict()
            return True
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False