from typing import Any, List
import asyncio
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document
from llama_index.core.readers.base import BaseReader
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_base import ReaderProvider
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config_base import ReaderProviderConfig

logger = logging.getLogger(__name__)

class BaseReaderProvider(ReaderProvider, BaseReader):
    """
    Provider that implements both GraphRAG ReaderProvider and LlamaIndex BaseReader interfaces.
    Provides full LlamaIndex compatibility while maintaining GraphRAG patterns.
    """

    def __init__(self, config: ReaderProviderConfig):
        self.config = config
        logger.debug(f"Initializing BaseReader provider: {self.__class__.__name__}")

    # GraphRAG interface
    def read(self, input_source: Any) -> List[Document]:
        """GraphRAG interface - delegates to load_data."""
        return self.load_data(input_source)

    # LlamaIndex interface
    def load_data(self, *args: Any, **kwargs: Any) -> List[Document]:
        """LlamaIndex BaseReader interface - must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement load_data method")

    # Optional BaseReader methods
    def lazy_load_data(self, *args: Any, **kwargs: Any):
        """Lazy loading interface - yields documents one at a time."""
        yield from self.load_data(*args, **kwargs)

    def aload_data(self, *args: Any, **kwargs: Any) -> List[Document]:
        """Async loading interface - can be overridden for async readers."""
        return asyncio.run(self._async_load_data(*args, **kwargs))

    async def _async_load_data(self, *args: Any, **kwargs: Any) -> List[Document]:
        """Default async implementation - runs sync load_data in a thread pool."""
        loop = asyncio.get_event_loop()
        
        def blocking_call() -> List[Document]:
            return self.load_data(*args, **kwargs)
        
        return await loop.run_in_executor(None, blocking_call)