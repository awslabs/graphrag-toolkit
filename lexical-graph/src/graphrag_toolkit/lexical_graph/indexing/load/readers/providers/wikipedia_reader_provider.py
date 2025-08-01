from typing import List, Union
from llama_index.core.schema import Document
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import WikipediaReaderConfig

class WikipediaReaderProvider:
    """Reader provider for Wikipedia articles using LlamaIndex's WikipediaReader."""

    def __init__(self, config: WikipediaReaderConfig):
        self.config = config
        self.lang = config.lang
        self.metadata_fn = config.metadata_fn
        self._reader = None  # Lazy-loaded reader

    def _init_reader(self):
        """Lazily initialize WikipediaReader if not already created."""
        if self._reader is None:
            try:
                from llama_index.readers.wikipedia import WikipediaReader
            except ImportError as e:
                raise ImportError(
                    "WikipediaReader requires the 'wikipedia' package. Install with: pip install wikipedia"
                ) from e

            self._reader = WikipediaReader()

    def read(self, input_source: Union[str, List[str]]) -> List[Document]:
        """Read Wikipedia documents with metadata handling and title correction."""
        self._init_reader()

        try:
            import wikipedia
        except ImportError as e:
            raise ImportError(
                "The 'wikipedia' package is required for WikipediaReaderProvider. Install it with: pip install wikipedia"
            ) from e

        pages = [input_source] if isinstance(input_source, str) else input_source

        validated_pages = []
        for page in pages:
            try:
                wikipedia.set_lang(self.lang)
                wikipedia.page(page)
                validated_pages.append(page)
            except wikipedia.exceptions.PageError:
                try:
                    if search_results := wikipedia.search(page, results=1):
                        wikipedia.page(search_results[0])
                        validated_pages.append(search_results[0])
                        print(f"Corrected page title: '{page}' -> '{search_results[0]}'")
                    else:
                        print(f"Warning: No Wikipedia page found for '{page}'")
                except (wikipedia.exceptions.PageError, wikipedia.exceptions.DisambiguationError) as e:
                    print(f"Warning: Could not resolve Wikipedia page for '{page}': {e}")

        if not validated_pages:
            raise ValueError(f"No valid Wikipedia pages found for: {pages}")

        documents = self._reader.load_data(pages=validated_pages)

        if self.metadata_fn:
            for doc in documents:
                page_context = validated_pages[0] if validated_pages else str(input_source)
                additional_metadata = self.metadata_fn(page_context)
                doc.metadata.update(additional_metadata)

        return documents
