from abc import ABC, abstractmethod
from typing import List

class Embedding(ABC):
    """
    Abstract base class for computing embeddings for BYOKG on inputs.
    """

    @abstractmethod
    def embed(self, text_input):
        """
        Converts a piece of text to vector embeddings.

        Parameters:
        - text_input (str): The text to embed.

        Returns:
        - list: A list of floats representing the document embeddings.
        """
        pass

    @abstractmethod
    def batch_embed(self, text_inputs):
        """
        Converts a list of text to vector embeddings.

        Parameters:
        - documents List[(str)]: A list containing the text documents to embed.

        Returns:
        - list: A list of floats representing the document embeddings.
        """
        pass
class LangChainEmbedding(Embedding):
    """
    Embedding class that accepts any lang_chain embedding but is compatabile with BYOKG retrievers
    """

    def __init__(self, langchain_embedding):
        self.embedder = langchain_embedding

    def embed(self, text_input):
        return self.embedder.embed_documents([text_input])[0]

    def batch_embed(self, text_inputs):
        return self.embedder.embed_documents(text_inputs)

class HuggingFaceEmbedding(LangChainEmbedding):
    """
    Class for using a huggingface embedding model via LangChain
    """
    def __init__(self, **kwargs):
        from langchain_huggingface import HuggingFaceEmbeddings

        self.embedder = HuggingFaceEmbeddings(**kwargs)

class BedrockEmbedding(LangChainEmbedding):
    """
    Class for using a Bedrock embedding model via LangChain
    """
    def __init__(self, **kwargs):
        from langchain_aws import BedrockEmbeddings

        self.embedder = BedrockEmbeddings(**kwargs)
class LLamaIndexEmbedding(Embedding):
    """
    Class for using a LLamaIndex embedding model with BYOKG retrievers
    """

    def __init__(self, llama_index_embedding):
        self.embedder = llama_index_embedding

    def embed(self, text_input):
        return self.embedder.get_text_embedding(text_input)

    def batch_embed(self, text_inputs):
        return self.embedder.get_text_embedding_batch(text_inputs)

class LLamaIndexBedrockEmbedding(LLamaIndexEmbedding):
    """
    Class for using a Bedrock embedding model via LlamaIndex
    """

    def __init__(self, **kwargs):
        from llama_index.embeddings.bedrock import BedrockEmbedding

        self.embedder = BedrockEmbedding(**kwargs)