from abc import ABC, abstractmethod
from typing import Any, List
from llama_index.core.schema import Document

class ReaderProvider(ABC):
    """
    Base class for extraction providers. All extract providers must inherit from this class
    and implement the `extract` method.
    """

    @abstractmethod
    def read(self, input_source: Any) -> List[Document]:
        """
        Extract structured documents or data from the given input source.
        """
        pass
