from typing import Any, List
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_base import ReaderProvider
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config_base import ReaderProviderConfig


logger = logging.getLogger(__name__)

class LlamaIndexReaderProviderBase(ReaderProvider):
    """
    Base class for LlamaIndex reader providers.
    Provides a simple wrapper around LlamaIndex readers.
    """

    def __init__(self, config: ReaderProviderConfig, reader_cls, **reader_kwargs):
        self.config = config
        logger.debug(f"Instantiating reader: {reader_cls.__name__} with args: {reader_kwargs}")
        self._reader = reader_cls(**reader_kwargs)

    def read(self, input_source: Any) -> List[Document]:
        """
        Read documents using the underlying LlamaIndex reader.
        Subclasses should override this method to handle specific reader requirements.
        """
        logger.debug("Starting read()")
        logger.debug(f"Reader class: {self._reader.__class__.__name__}")
        logger.debug(f"Input source: {input_source} (type={type(input_source)})")

        try:
            # Default implementation - subclasses should override for specific readers
            return self._reader.load_data(input_source)
        except Exception as e:
            logger.exception("Error during read()")
            raise RuntimeError(
                f"Failed to read using {self._reader.__class__.__name__}: {e}"
            ) from e