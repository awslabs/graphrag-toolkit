from abc import ABC, abstractmethod
from typing import Any, List
from llama_index.core.schema import Document

class ReaderProvider(ABC):
    """
    Abstract base class for all document reader providers.
    All concrete reader classes should inherit from this and implement the `read` method.
    """

    @abstractmethod
    def read(self, input_source: Any) -> List[Document]:
        """
        Extract and return a list of Documents from the input source.

        Args:
            input_source: The source from which to read documents

        Returns:
            List[Document]: The list of extracted Document objects.
        """
        pass