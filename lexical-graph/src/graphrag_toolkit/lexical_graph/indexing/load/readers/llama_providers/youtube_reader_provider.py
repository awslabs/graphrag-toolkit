"""
YouTube transcript reader provider using LlamaIndex.

This module provides a provider that reads transcripts from YouTube videos
using LlamaIndex's YoutubeTranscriptReader.
"""

from typing import List
from llama_index.core.schema import Document

from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import (
    LlamaIndexReaderProviderBase,
)
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)


class YouTubeReaderProvider(LlamaIndexReaderProviderBase):
    """
    Reader provider for YouTube videos using LlamaIndex's YoutubeTranscriptReader.
    """

    def __init__(self):
        """
        No parameters required for initialization. Accepts YouTube video URL or ID at read() time.
        """
        try:
            from llama_index.readers.youtube_transcript import YoutubeTranscriptReader
        except ImportError as e:
            raise ImportError(
                "The 'llama-index[youtube]' optional dependency is required for YouTubeReaderProvider.\n"
                "Install it with: pip install llama-index[youtube]"
            ) from e

        super().__init__(reader_cls=YoutubeTranscriptReader)

    def self_test(self) -> bool:
        """
        Sanity check: Attempts to read transcript from a known YouTube video.

        Returns:
            True if transcript was successfully read.
        """
        test_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"  # Public video with captions
        try:
            docs: List[Document] = self.read(test_url)
            assert isinstance(docs, list)
            return len(docs) > 0
        except Exception as e:
            logger.error(f"YouTubeReaderProvider self_test failed: {e}")
            return False

