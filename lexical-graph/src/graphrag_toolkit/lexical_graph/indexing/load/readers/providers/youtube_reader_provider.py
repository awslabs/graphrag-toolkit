from typing import List, Union
import re
from llama_index.core.schema import Document
from ..reader_provider_config import YouTubeReaderConfig

class YouTubeReaderProvider:
    """Direct YouTube transcript reader using youtube-transcript-api."""

    def __init__(self, config: YouTubeReaderConfig):
        self.language = config.language
        self.metadata_fn = config.metadata_fn

    def _extract_video_id(self, url: str) -> str:
        """Extract video ID from YouTube URL."""
        patterns = [
            r'(?:v=|/)([0-9A-Za-z_-]{11}).*',
            r'(?:embed/)([0-9A-Za-z_-]{11})',
            r'(?:watch\?v=)([0-9A-Za-z_-]{11})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        raise ValueError(f"Could not extract video ID from URL: {url}")

    def read(self, input_source: Union[str, List[str]]) -> List[Document]:
        """Read YouTube transcript documents."""
        try:
            from youtube_transcript_api import YouTubeTranscriptApi
        except ImportError as e:
            raise ImportError(
                "YouTubeTranscriptApi requires 'youtube-transcript-api'. "
                "Install with: pip install youtube-transcript-api"
            ) from e

        if isinstance(input_source, str):
            urls = [input_source]
        else:
            urls = input_source

        documents = []
        
        for url in urls:
            try:
                video_id = self._extract_video_id(url)
                
                # Create an instance of the API
                api = YouTubeTranscriptApi()
                
                # Use the instance method fetch
                transcript_list = api.fetch(video_id, languages=[self.language])
                
                # transcript_list should be a list of transcript segments
                if isinstance(transcript_list, list):
                    # Combine transcript segments
                    full_text = " ".join([segment.get('text', '') for segment in transcript_list])
                else:
                    # If it's a single transcript object, get its text
                    full_text = str(transcript_list)
                
                # Create metadata
                metadata = {
                    'video_id': video_id,
                    'url': url,
                    'language': self.language,
                    'source': 'youtube'
                }
                
                # Apply custom metadata function if provided
                if self.metadata_fn:
                    custom_metadata = self.metadata_fn(url)
                    metadata.update(custom_metadata)
                
                # Create document
                doc = Document(
                    text=full_text,
                    metadata=metadata
                )
                
                documents.append(doc)
                
            except Exception as e:
                print(f"Error processing YouTube URL {url}: {e}")
                # Try without language specification as fallback
                try:
                    api = YouTubeTranscriptApi()
                    transcript_list = api.fetch(video_id)
                    
                    if isinstance(transcript_list, list):
                        full_text = " ".join([segment.get('text', '') for segment in transcript_list])
                    else:
                        full_text = str(transcript_list)
                    
                    metadata = {
                        'video_id': video_id,
                        'url': url,
                        'language': 'auto',
                        'source': 'youtube'
                    }
                    
                    if self.metadata_fn:
                        custom_metadata = self.metadata_fn(url)
                        metadata.update(custom_metadata)
                    
                    doc = Document(text=full_text, metadata=metadata)
                    documents.append(doc)
                    
                except Exception as e2:
                    print(f"Fallback also failed for {url}: {e2}")
                    continue
        
        return documents