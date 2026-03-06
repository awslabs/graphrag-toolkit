"""Tests for graph_reranker.py module.

This module tests the GReranker and LocalGReranker classes including
initialization and abstract class behavior.

NOTE: Full integration tests for LocalGReranker require transformers and torch,
which are complex to mock. These tests focus on the abstract interface and
basic initialization patterns.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from graphrag_toolkit.byokg_rag.graph_retrievers.graph_reranker import (
    GReranker,
    LocalGReranker
)


class TestGRerankerAbstract:
    """Tests for abstract GReranker base class."""
    
    def test_greranker_is_abstract(self):
        """Verify GReranker is an abstract class that cannot be instantiated."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            GReranker()
    
    def test_greranker_subclass_must_implement_rerank(self):
        """Verify GReranker subclass must implement rerank_input_with_query."""
        class IncompleteReranker(GReranker):
            pass
        
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteReranker()


class TestLocalGRerankerInitialization:
    """Tests for LocalGReranker initialization."""
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_initialization_defaults(self, mock_model_class, mock_tokenizer_class):
        """Verify LocalGReranker initializes with default parameters."""
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        
        reranker = LocalGReranker()
        
        assert reranker.model_name == "BAAI/bge-reranker-base"
        assert reranker.topk == 10
        assert reranker.tokenizer == mock_tokenizer
        assert reranker.reranker == mock_model
        mock_model.eval.assert_called_once()
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_initialization_custom_parameters(self, mock_model_class, mock_tokenizer_class):
        """Verify LocalGReranker accepts custom parameters."""
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        
        reranker = LocalGReranker(
            model_name="BAAI/bge-reranker-large",
            topk=20,
            device="cpu"
        )
        
        assert reranker.model_name == "BAAI/bge-reranker-large"
        assert reranker.topk == 20
        mock_model.to.assert_called_with("cpu")
    
    def test_initialization_invalid_model_name(self):
        """Verify AssertionError raised for unsupported model name."""
        with pytest.raises(AssertionError, match="Model name not supported"):
            LocalGReranker(model_name="unsupported-model")


class TestLocalGRerankerInterface:
    """Tests for LocalGReranker interface methods."""
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_has_calculate_score_method(self, mock_model_class, mock_tokenizer_class):
        """Verify LocalGReranker has calculate_score method."""
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        
        reranker = LocalGReranker()
        
        assert hasattr(reranker, 'calculate_score')
        assert callable(reranker.calculate_score)
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_has_filter_topk_method(self, mock_model_class, mock_tokenizer_class):
        """Verify LocalGReranker has filter_topk method."""
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        
        reranker = LocalGReranker()
        
        assert hasattr(reranker, 'filter_topk')
        assert callable(reranker.filter_topk)
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_has_rerank_input_with_query_method(self, mock_model_class, mock_tokenizer_class):
        """Verify LocalGReranker implements rerank_input_with_query."""
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        
        reranker = LocalGReranker()
        
        assert hasattr(reranker, 'rerank_input_with_query')
        assert callable(reranker.rerank_input_with_query)


class TestLocalGRerankerSupportedModels:
    """Tests for supported model validation."""
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_supports_bge_reranker_base(self, mock_model_class, mock_tokenizer_class):
        """Verify BAAI/bge-reranker-base is supported."""
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        
        reranker = LocalGReranker(model_name="BAAI/bge-reranker-base")
        
        assert reranker.model_name == "BAAI/bge-reranker-base"
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_supports_bge_reranker_large(self, mock_model_class, mock_tokenizer_class):
        """Verify BAAI/bge-reranker-large is supported."""
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        
        reranker = LocalGReranker(model_name="BAAI/bge-reranker-large")
        
        assert reranker.model_name == "BAAI/bge-reranker-large"
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_supports_bge_reranker_v2_m3(self, mock_model_class, mock_tokenizer_class):
        """Verify BAAI/bge-reranker-v2-m3 is supported."""
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        
        reranker = LocalGReranker(model_name="BAAI/bge-reranker-v2-m3")
        
        assert reranker.model_name == "BAAI/bge-reranker-v2-m3"


class TestLocalGRerankerCalculateScore:
    """Tests for calculate_score method."""
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    @patch('torch.no_grad')
    def test_calculate_score_single_pair(self, mock_no_grad, mock_model_class, mock_tokenizer_class):
        """Verify calculate_score computes scores for query-text pairs."""
        import torch
        
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        mock_model.device = 'cpu'
        
        # Create a mock that supports .to() method and can be unpacked with **
        mock_inputs = MagicMock()
        mock_inputs.to.return_value = mock_inputs
        # Make it behave like a dict when unpacked
        mock_inputs.keys.return_value = ['input_ids', 'attention_mask']
        mock_inputs.__getitem__.side_effect = lambda key: torch.tensor([[1, 2, 3]])
        mock_tokenizer.return_value = mock_inputs
        
        mock_logits = Mock()
        mock_logits.view.return_value.float.return_value = torch.tensor([0.85])
        mock_model.return_value.logits = mock_logits
        
        reranker = LocalGReranker()
        result = reranker.calculate_score([["query", "text"]])
        
        assert isinstance(result, torch.Tensor)
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_calculate_score_unsupported_model(self, mock_model_class, mock_tokenizer_class):
        """Verify calculate_score raises NotImplementedError for unsupported models."""
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        
        reranker = LocalGReranker()
        reranker.model_name = "unsupported-model"
        
        with pytest.raises(NotImplementedError):
            reranker.calculate_score([["query", "text"]])


class TestLocalGRerankerFilterTopK:
    """Tests for filter_topk method."""
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_filter_topk_returns_top_results(self, mock_model_class, mock_tokenizer_class):
        """Verify filter_topk returns top-k results based on scores."""
        import torch
        import numpy as np
        
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        mock_model.device = 'cpu'
        
        mock_inputs = {'input_ids': torch.tensor([[1, 2, 3]])}
        mock_tokenizer.return_value = mock_inputs
        
        mock_logits = Mock()
        mock_logits.view.return_value.float.return_value = torch.tensor([0.9, 0.5, 0.7])
        mock_model.return_value.logits = mock_logits
        
        reranker = LocalGReranker()
        
        with patch.object(reranker, 'calculate_score', return_value=torch.tensor([0.9, 0.5, 0.7])):
            result, indices = reranker.filter_topk("query", ["text1", "text2", "text3"], topk=2)
            
            assert len(result) == 2
            assert len(indices) == 2
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_filter_topk_with_scores(self, mock_model_class, mock_tokenizer_class):
        """Verify filter_topk returns scores when return_scores=True."""
        import torch
        
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        mock_model.device = 'cpu'
        
        reranker = LocalGReranker()
        
        with patch.object(reranker, 'calculate_score', return_value=torch.tensor([0.9, 0.5, 0.7])):
            result, scores, indices = reranker.filter_topk(
                "query", 
                ["text1", "text2", "text3"], 
                topk=2, 
                return_scores=True
            )
            
            assert len(result) == 2
            assert len(scores) == 2
            assert len(indices) == 2
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_filter_topk_with_list_queries(self, mock_model_class, mock_tokenizer_class):
        """Verify filter_topk handles list of queries."""
        import torch
        
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        mock_model.device = 'cpu'
        
        reranker = LocalGReranker()
        
        with patch.object(reranker, 'calculate_score', return_value=torch.tensor([0.9, 0.5])):
            result, indices = reranker.filter_topk(
                ["query1", "query2"], 
                ["text1", "text2"], 
                topk=2
            )
            
            assert len(result) == 2


class TestLocalGRerankerRerankInputWithQuery:
    """Tests for rerank_input_with_query method."""
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_rerank_uses_default_topk(self, mock_model_class, mock_tokenizer_class):
        """Verify rerank_input_with_query uses default topk when not specified."""
        import torch
        
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        mock_model.device = 'cpu'
        
        reranker = LocalGReranker(topk=5)
        
        with patch.object(reranker, 'filter_topk', return_value=(["text1"], [0])) as mock_filter:
            reranker.rerank_input_with_query("query", ["text1", "text2", "text3"])
            
            mock_filter.assert_called_once()
            call_args = mock_filter.call_args[1]
            assert call_args['topk'] == 5
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_rerank_uses_custom_topk(self, mock_model_class, mock_tokenizer_class):
        """Verify rerank_input_with_query uses custom topk when specified."""
        import torch
        
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        mock_model.device = 'cpu'
        
        reranker = LocalGReranker(topk=10)
        
        with patch.object(reranker, 'filter_topk', return_value=(["text1"], [0])) as mock_filter:
            reranker.rerank_input_with_query("query", ["text1", "text2"], topk=3)
            
            mock_filter.assert_called_once()
            call_args = mock_filter.call_args[1]
            assert call_args['topk'] == 3
    
    @patch('transformers.AutoTokenizer')
    @patch('transformers.AutoModelForSequenceClassification')
    def test_rerank_with_return_scores(self, mock_model_class, mock_tokenizer_class):
        """Verify rerank_input_with_query passes return_scores parameter."""
        import torch
        
        mock_tokenizer = Mock()
        mock_model = Mock()
        mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer
        mock_model_class.from_pretrained.return_value = mock_model
        mock_model.to.return_value = mock_model
        mock_model.device = 'cpu'
        
        reranker = LocalGReranker()
        
        with patch.object(reranker, 'filter_topk', return_value=(["text1"], [0.9], [0])) as mock_filter:
            result = reranker.rerank_input_with_query(
                "query", 
                ["text1", "text2"], 
                return_scores=True
            )
            
            mock_filter.assert_called_once()
            call_args = mock_filter.call_args[1]
            assert call_args['return_scores'] is True
            assert len(result) == 3

