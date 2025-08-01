from typing import Any, List
from pydantic import BaseModel, validator
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import \
    LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config_base import ReaderProviderConfig

logger = logging.getLogger(__name__)


class ValidatedReaderProviderBase(LlamaIndexReaderProviderBase):
    """
    Base class that adds Pydantic-style validation to any LlamaIndex reader.
    Provides input validation, output validation, and configuration serialization.
    """

    def __init__(self, config: ReaderProviderConfig, reader_cls, **reader_kwargs):
        super().__init__(config, reader_cls, **reader_kwargs)
        self._validate_config()

    def _validate_config(self):
        """Validate configuration using Pydantic if config is a BaseModel."""
        if isinstance(self.config, BaseModel):
            try:
                self.config.model_dump()  # Triggers validation
                logger.debug("Configuration validation passed")
            except Exception as e:
                logger.error(f"Configuration validation failed: {e}")
                raise ValueError(f"Invalid configuration: {e}") from e

    def read(self, input_source: Any) -> List[Document]:
        """Read with input/output validation."""
        # Input validation
        self._validate_input(input_source)

        # Read documents
        documents = super().read(input_source)

        # Output validation
        self._validate_output(documents)

        return documents

    @staticmethod
    def _validate_input(input_source: Any):
        """Validate input source."""
        if input_source is None:
            raise ValueError("Input source cannot be None")

        # Add reader-specific input validation in subclasses
        logger.debug(f"Input validation passed for: {type(input_source)}")

    @staticmethod
    def _validate_output(documents: List[Document]):
        """Validate output documents."""
        if not isinstance(documents, list):
            raise ValueError("Reader must return a list of Documents")

        for i, doc in enumerate(documents):
            if not isinstance(doc, Document):
                raise ValueError(f"Item {i} is not a Document: {type(doc)}")

            if not doc.text or not doc.text.strip():
                logger.warning(f"Document {i} has empty text content")

        logger.debug(f"Output validation passed for {len(documents)} documents")

    def to_dict(self) -> dict:
        """Serialize configuration to dictionary."""
        if isinstance(self.config, BaseModel):
            return self.config.model_dump()
        else:
            return {"config_type": type(self.config).__name__}

    def validate(self) -> bool:
        """Validate the entire reader configuration and state."""
        try:
            self._validate_config()
            return True
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False
