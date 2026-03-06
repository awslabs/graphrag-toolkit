"""Tests for embedding.py module.

This module tests the Embedding abstract base class and its concrete implementations
including LangChainEmbedding, BedrockEmbedding, HuggingFaceEmbedding, and LlamaIndex variants.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from graphrag_toolkit.byokg_rag.indexing.embedding import (
    Embedding,
    LangChainEmbedding,
    BedrockEmbedding,
    HuggingFaceEmbedding,
    LLamaIndexEmbedding,
    LLamaIndexBedrockEmbedding
)


class TestEmbeddingAbstract:
    """Tests for abstract Embedding base class."""
    
    def test_embedding_is_abstract(self):
        """Verify Embedding is an abstract class that cannot be instantiated."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            Embedding()
    
    def test_embedding_subclass_must_implement_embed(self):
        """Verify Embedding subclass must implement embed method."""
        class IncompleteEmbedding(Embedding):
            def batch_embed(self, text_inputs):
                pass
        
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteEmbedding()
    
    def test_embedding_subclass_must_implement_batch_embed(self):
        """Verify Embedding subclass must implement batch_embed method."""
        class IncompleteEmbedding(Embedding):
            def embed(self, text_input):
                pass
        
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteEmbedding()


class TestLangChainEmbedding:
    """Tests for LangChainEmbedding wrapper class."""
    
    def test_initialization(self):
        """Verify LangChainEmbedding initializes with a LangChain embedder."""
        mock_embedder = Mock()
        embedding = LangChainEmbedding(langchain_embedding=mock_embedder)
        
        assert embedding.embedder == mock_embedder
    
    def test_embed_single_text(self):
        """Verify embed() method converts single text to embedding."""
        mock_embedder = Mock()
        mock_embedder.embed_documents.return_value = [[0.1, 0.2, 0.3]]
        
        embedding = LangChainEmbedding(langchain_embedding=mock_embedder)
        result = embedding.embed("test text")
        
        assert result == [0.1, 0.2, 0.3]
        mock_embedder.embed_documents.assert_called_once_with(["test text"])
    
    def test_batch_embed_multiple_texts(self):
        """Verify batch_embed() method converts multiple texts to embeddings."""
        mock_embedder = Mock()
        mock_embedder.embed_documents.return_value = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6]
        ]
        
        embedding = LangChainEmbedding(langchain_embedding=mock_embedder)
        result = embedding.batch_embed(["text1", "text2"])
        
        assert len(result) == 2
        assert result[0] == [0.1, 0.2, 0.3]
        assert result[1] == [0.4, 0.5, 0.6]
        mock_embedder.embed_documents.assert_called_once_with(["text1", "text2"])
    
    def test_embed_empty_string(self):
        """Verify embed() handles empty string input."""
        mock_embedder = Mock()
        mock_embedder.embed_documents.return_value = [[0.0, 0.0, 0.0]]
        
        embedding = LangChainEmbedding(langchain_embedding=mock_embedder)
        result = embedding.embed("")
        
        assert result == [0.0, 0.0, 0.0]
        mock_embedder.embed_documents.assert_called_once_with([""])
    
    def test_batch_embed_empty_list(self):
        """Verify batch_embed() handles empty list input."""
        mock_embedder = Mock()
        mock_embedder.embed_documents.return_value = []
        
        embedding = LangChainEmbedding(langchain_embedding=mock_embedder)
        result = embedding.batch_embed([])
        
        assert result == []
        mock_embedder.embed_documents.assert_called_once_with([])


class TestBedrockEmbedding:
    """Tests for BedrockEmbedding class."""
    
    @patch('langchain_aws.BedrockEmbeddings')
    def test_initialization_defaults(self, mock_bedrock_embeddings):
        """Verify BedrockEmbedding initializes with default parameters."""
        mock_embedder = Mock()
        mock_bedrock_embeddings.return_value = mock_embedder
        
        embedding = BedrockEmbedding()
        
        assert embedding.embedder == mock_embedder
        mock_bedrock_embeddings.assert_called_once_with()
    
    @patch('langchain_aws.BedrockEmbeddings')
    def test_initialization_with_kwargs(self, mock_bedrock_embeddings):
        """Verify BedrockEmbedding accepts custom parameters."""
        mock_embedder = Mock()
        mock_bedrock_embeddings.return_value = mock_embedder
        
        embedding = BedrockEmbedding(
            model_id="amazon.titan-embed-text-v1",
            region_name="us-west-2"
        )
        
        assert embedding.embedder == mock_embedder
        mock_bedrock_embeddings.assert_called_once_with(
            model_id="amazon.titan-embed-text-v1",
            region_name="us-west-2"
        )
    
    @patch('langchain_aws.BedrockEmbeddings')
    def test_embed_inherits_from_langchain(self, mock_bedrock_embeddings):
        """Verify BedrockEmbedding inherits embed() from LangChainEmbedding."""
        mock_embedder = Mock()
        mock_embedder.embed_documents.return_value = [[0.1, 0.2, 0.3]]
        mock_bedrock_embeddings.return_value = mock_embedder
        
        embedding = BedrockEmbedding()
        result = embedding.embed("test text")
        
        assert result == [0.1, 0.2, 0.3]
    
    @patch('langchain_aws.BedrockEmbeddings')
    def test_batch_embed_inherits_from_langchain(self, mock_bedrock_embeddings):
        """Verify BedrockEmbedding inherits batch_embed() from LangChainEmbedding."""
        mock_embedder = Mock()
        mock_embedder.embed_documents.return_value = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6]
        ]
        mock_bedrock_embeddings.return_value = mock_embedder
        
        embedding = BedrockEmbedding()
        result = embedding.batch_embed(["text1", "text2"])
        
        assert len(result) == 2


class TestHuggingFaceEmbedding:
    """Tests for HuggingFaceEmbedding class."""
    
    @patch('langchain_huggingface.HuggingFaceEmbeddings')
    def test_initialization_defaults(self, mock_hf_embeddings):
        """Verify HuggingFaceEmbedding initializes with default parameters."""
        mock_embedder = Mock()
        mock_hf_embeddings.return_value = mock_embedder
        
        embedding = HuggingFaceEmbedding()
        
        assert embedding.embedder == mock_embedder
        mock_hf_embeddings.assert_called_once_with()
    
    @patch('langchain_huggingface.HuggingFaceEmbeddings')
    def test_initialization_with_model_name(self, mock_hf_embeddings):
        """Verify HuggingFaceEmbedding accepts custom model name."""
        mock_embedder = Mock()
        mock_hf_embeddings.return_value = mock_embedder
        
        embedding = HuggingFaceEmbedding(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
        
        assert embedding.embedder == mock_embedder
        mock_hf_embeddings.assert_called_once_with(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
    
    @patch('langchain_huggingface.HuggingFaceEmbeddings')
    def test_embed_inherits_from_langchain(self, mock_hf_embeddings):
        """Verify HuggingFaceEmbedding inherits embed() from LangChainEmbedding."""
        mock_embedder = Mock()
        mock_embedder.embed_documents.return_value = [[0.1, 0.2, 0.3]]
        mock_hf_embeddings.return_value = mock_embedder
        
        embedding = HuggingFaceEmbedding()
        result = embedding.embed("test text")
        
        assert result == [0.1, 0.2, 0.3]


class TestLLamaIndexEmbedding:
    """Tests for LLamaIndexEmbedding wrapper class."""
    
    def test_initialization(self):
        """Verify LLamaIndexEmbedding initializes with a LlamaIndex embedder."""
        mock_embedder = Mock()
        embedding = LLamaIndexEmbedding(llama_index_embedding=mock_embedder)
        
        assert embedding.embedder == mock_embedder
    
    def test_embed_single_text(self):
        """Verify embed() method uses get_text_embedding."""
        mock_embedder = Mock()
        mock_embedder.get_text_embedding.return_value = [0.1, 0.2, 0.3]
        
        embedding = LLamaIndexEmbedding(llama_index_embedding=mock_embedder)
        result = embedding.embed("test text")
        
        assert result == [0.1, 0.2, 0.3]
        mock_embedder.get_text_embedding.assert_called_once_with("test text")
    
    def test_batch_embed_multiple_texts(self):
        """Verify batch_embed() method uses get_text_embedding_batch."""
        mock_embedder = Mock()
        mock_embedder.get_text_embedding_batch.return_value = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6]
        ]
        
        embedding = LLamaIndexEmbedding(llama_index_embedding=mock_embedder)
        result = embedding.batch_embed(["text1", "text2"])
        
        assert len(result) == 2
        assert result[0] == [0.1, 0.2, 0.3]
        assert result[1] == [0.4, 0.5, 0.6]
        mock_embedder.get_text_embedding_batch.assert_called_once_with(["text1", "text2"])
    
    def test_embed_empty_string(self):
        """Verify embed() handles empty string input."""
        mock_embedder = Mock()
        mock_embedder.get_text_embedding.return_value = [0.0, 0.0, 0.0]
        
        embedding = LLamaIndexEmbedding(llama_index_embedding=mock_embedder)
        result = embedding.embed("")
        
        assert result == [0.0, 0.0, 0.0]
        mock_embedder.get_text_embedding.assert_called_once_with("")


class TestLLamaIndexBedrockEmbedding:
    """Tests for LLamaIndexBedrockEmbedding class."""
    
    @patch('llama_index.embeddings.bedrock.BedrockEmbedding')
    def test_initialization_defaults(self, mock_bedrock_embedding):
        """Verify LLamaIndexBedrockEmbedding initializes with default parameters."""
        mock_embedder = Mock()
        mock_bedrock_embedding.return_value = mock_embedder
        
        embedding = LLamaIndexBedrockEmbedding()
        
        assert embedding.embedder == mock_embedder
        mock_bedrock_embedding.assert_called_once_with()
    
    @patch('llama_index.embeddings.bedrock.BedrockEmbedding')
    def test_initialization_with_kwargs(self, mock_bedrock_embedding):
        """Verify LLamaIndexBedrockEmbedding accepts custom parameters."""
        mock_embedder = Mock()
        mock_bedrock_embedding.return_value = mock_embedder
        
        embedding = LLamaIndexBedrockEmbedding(
            model_name="amazon.titan-embed-text-v1",
            region_name="us-west-2"
        )
        
        assert embedding.embedder == mock_embedder
        mock_bedrock_embedding.assert_called_once_with(
            model_name="amazon.titan-embed-text-v1",
            region_name="us-west-2"
        )
    
    @patch('llama_index.embeddings.bedrock.BedrockEmbedding')
    def test_embed_inherits_from_llamaindex(self, mock_bedrock_embedding):
        """Verify LLamaIndexBedrockEmbedding inherits embed() from LLamaIndexEmbedding."""
        mock_embedder = Mock()
        mock_embedder.get_text_embedding.return_value = [0.1, 0.2, 0.3]
        mock_bedrock_embedding.return_value = mock_embedder
        
        embedding = LLamaIndexBedrockEmbedding()
        result = embedding.embed("test text")
        
        assert result == [0.1, 0.2, 0.3]
    
    @patch('llama_index.embeddings.bedrock.BedrockEmbedding')
    def test_batch_embed_inherits_from_llamaindex(self, mock_bedrock_embedding):
        """Verify LLamaIndexBedrockEmbedding inherits batch_embed() from LLamaIndexEmbedding."""
        mock_embedder = Mock()
        mock_embedder.get_text_embedding_batch.return_value = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6]
        ]
        mock_bedrock_embedding.return_value = mock_embedder
        
        embedding = LLamaIndexBedrockEmbedding()
        result = embedding.batch_embed(["text1", "text2"])
        
        assert len(result) == 2


class TestEmbeddingErrorHandling:
    """Tests for error handling in embedding classes."""
    
    def test_langchain_embed_api_failure(self):
        """Verify LangChainEmbedding handles API failures gracefully."""
        mock_embedder = Mock()
        mock_embedder.embed_documents.side_effect = Exception("API Error")
        
        embedding = LangChainEmbedding(langchain_embedding=mock_embedder)
        
        with pytest.raises(Exception, match="API Error"):
            embedding.embed("test text")
    
    def test_langchain_batch_embed_api_failure(self):
        """Verify LangChainEmbedding batch_embed handles API failures."""
        mock_embedder = Mock()
        mock_embedder.embed_documents.side_effect = Exception("Batch API Error")
        
        embedding = LangChainEmbedding(langchain_embedding=mock_embedder)
        
        with pytest.raises(Exception, match="Batch API Error"):
            embedding.batch_embed(["text1", "text2"])
    
    def test_llamaindex_embed_api_failure(self):
        """Verify LLamaIndexEmbedding handles API failures gracefully."""
        mock_embedder = Mock()
        mock_embedder.get_text_embedding.side_effect = Exception("LlamaIndex API Error")
        
        embedding = LLamaIndexEmbedding(llama_index_embedding=mock_embedder)
        
        with pytest.raises(Exception, match="LlamaIndex API Error"):
            embedding.embed("test text")
    
    def test_llamaindex_batch_embed_api_failure(self):
        """Verify LLamaIndexEmbedding batch_embed handles API failures."""
        mock_embedder = Mock()
        mock_embedder.get_text_embedding_batch.side_effect = Exception("Batch Error")
        
        embedding = LLamaIndexEmbedding(llama_index_embedding=mock_embedder)
        
        with pytest.raises(Exception, match="Batch Error"):
            embedding.batch_embed(["text1", "text2"])
