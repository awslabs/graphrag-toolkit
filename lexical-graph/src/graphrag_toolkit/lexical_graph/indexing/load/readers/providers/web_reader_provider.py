from typing import List
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import WebReaderConfig
from llama_index.core.schema import Document

class WebReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for web pages using LlamaIndex's SimpleWebPageReader."""

    def __init__(self, config: WebReaderConfig):
        """Initialize with WebReaderConfig."""
        # Lazy import
        try:
            from llama_index.readers.web import SimpleWebPageReader
        except ImportError as e:
            raise ImportError(
                "SimpleWebPageReader requires 'requests' and 'beautifulsoup4'. "
                "Install with: pip install requests beautifulsoup4"
            ) from e

        reader_kwargs = {
            "html_to_text": config.html_to_text
        }
        
        super().__init__(config=config, reader_cls=SimpleWebPageReader, **reader_kwargs)

    def read(self, input_source) -> List[Document]:
        """Read web page documents with proper parameter handling."""
        # SimpleWebPageReader expects urls parameter as a list
        urls = [input_source] if isinstance(input_source, str) else input_source
        return self._reader.load_data(urls=urls)