"""Tests for ByoKGQueryEngine.

This module tests the query engine orchestration including initialization,
query processing, context deduplication, and response generation.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from graphrag_toolkit.byokg_rag.byokg_query_engine import ByoKGQueryEngine


@pytest.fixture
def mock_graph_store_with_schema():
    """Fixture providing a mock graph store with schema and execute_query."""
    mock_store = Mock()
    mock_store.get_schema.return_value = {
        'node_types': ['Person', 'Organization', 'Location'],
        'edge_types': ['WORKS_FOR', 'FOUNDED', 'LOCATED_IN']
    }
    mock_store.nodes.return_value = ['Organization', 'John Doe', 'Portland']
    mock_store.execute_query = Mock(return_value=[])
    return mock_store


@pytest.fixture
def mock_llm_generator():
    """Fixture providing a mock LLM generator."""
    mock_gen = Mock()
    mock_gen.generate.return_value = "<entity-extraction>Organization</entity-extraction><task-completion>FINISH</task-completion>"
    return mock_gen


@pytest.fixture
def mock_entity_linker():
    """Fixture providing a mock entity linker."""
    mock_linker = Mock()
    mock_linker.link.return_value = ['Organization', 'Portland']
    return mock_linker


@pytest.fixture
def mock_kg_linker():
    """Fixture providing a mock KG linker."""
    mock_linker = Mock()
    mock_linker.task_prompts = "Mock task prompts"
    mock_linker.task_prompts_iterative = "Mock iterative task prompts"
    mock_linker.generate_response.return_value = (
        "<entity-extraction>Organization</entity-extraction>"
        "<task-completion>FINISH</task-completion>"
    )
    mock_linker.parse_response.return_value = {
        'entity-extraction': ['Organization'],
        'draft-answer-generation': []
    }
    return mock_linker


class TestQueryEngineInitialization:
    """Tests for query engine initialization."""
    
    def test_initialization_with_defaults(self, mock_graph_store_with_schema):
        """Verify query engine initializes with default components."""
        with patch('graphrag_toolkit.byokg_rag.llm.bedrock_llms.BedrockGenerator') as mock_bedrock, \
             patch('graphrag_toolkit.byokg_rag.indexing.FuzzyStringIndex') as mock_fuzzy, \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.EntityLinker') as mock_entity, \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.AgenticRetriever') as mock_agentic, \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.PathRetriever') as mock_path, \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.GraphQueryRetriever') as mock_graph_query, \
             patch('graphrag_toolkit.byokg_rag.graph_connectors.KGLinker') as mock_kg, \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.GTraversal'), \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.TripletGVerbalizer'), \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.PathVerbalizer'):
            
            # Setup mock returns
            mock_bedrock_instance = Mock()
            mock_bedrock.return_value = mock_bedrock_instance
            
            mock_fuzzy_instance = Mock()
            mock_fuzzy.return_value = mock_fuzzy_instance
            mock_fuzzy_instance.add.return_value = None
            mock_fuzzy_instance.as_entity_matcher.return_value = Mock()
            
            mock_entity_instance = Mock()
            mock_entity.return_value = mock_entity_instance
            
            mock_kg_instance = Mock()
            mock_kg_instance.task_prompts = "test prompts"
            mock_kg_instance.task_prompts_iterative = "test iterative prompts"
            mock_kg.return_value = mock_kg_instance
            
            engine = ByoKGQueryEngine(graph_store=mock_graph_store_with_schema)
            
            assert engine.graph_store == mock_graph_store_with_schema
            assert engine.schema is not None
            assert engine.llm_generator is not None
            assert engine.entity_linker is not None
            assert engine.kg_linker is not None
    
    def test_initialization_with_custom_components(
        self, mock_graph_store_with_schema, mock_llm_generator, 
        mock_entity_linker, mock_kg_linker
    ):
        """Verify query engine accepts custom components."""
        mock_triplet_retriever = Mock()
        mock_path_retriever = Mock()
        
        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            entity_linker=mock_entity_linker,
            triplet_retriever=mock_triplet_retriever,
            path_retriever=mock_path_retriever,
            kg_linker=mock_kg_linker
        )
        
        assert engine.llm_generator == mock_llm_generator
        assert engine.entity_linker == mock_entity_linker
        assert engine.triplet_retriever == mock_triplet_retriever
        assert engine.path_retriever == mock_path_retriever
        assert engine.kg_linker == mock_kg_linker


class TestQueryEngineQuery:
    """Tests for query processing."""
    
    def test_query_single_iteration(
        self, mock_graph_store_with_schema, mock_llm_generator, 
        mock_entity_linker, mock_kg_linker
    ):
        """Verify single iteration query processing."""
        mock_triplet_retriever = Mock()
        mock_triplet_retriever.retrieve.return_value = ['John Doe founded Organization']
        
        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            entity_linker=mock_entity_linker,
            triplet_retriever=mock_triplet_retriever,
            kg_linker=mock_kg_linker
        )
        
        result = engine.query("Who founded Organization?", iterations=1)
        
        assert isinstance(result, list)
        mock_kg_linker.generate_response.assert_called_once()
        mock_kg_linker.parse_response.assert_called_once()
    
    def test_query_context_deduplication(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify context deduplication in _add_to_context."""
        with patch('graphrag_toolkit.byokg_rag.graph_connectors.KGLinker') as mock_kg:
            mock_kg_instance = Mock()
            mock_kg_instance.task_prompts = "test"
            mock_kg_instance.task_prompts_iterative = "test"
            mock_kg.return_value = mock_kg_instance
            
            engine = ByoKGQueryEngine(
                graph_store=mock_graph_store_with_schema,
                llm_generator=mock_llm_generator
            )
            
            context = ['item1', 'item2']
            engine._add_to_context(context, ['item2', 'item3', 'item1'])
            
            assert context == ['item1', 'item2', 'item3']
            assert context.count('item1') == 1
            assert context.count('item2') == 1


class TestQueryEngineGenerateResponse:
    """Tests for response generation."""
    
    def test_generate_response_default_prompt(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify response generation with default prompt."""
        mock_llm_generator.generate.return_value = (
            "<answers>Organization was founded by John Doe</answers>"
        )
        
        with patch('graphrag_toolkit.byokg_rag.byokg_query_engine.load_yaml') as mock_load_yaml, \
             patch('graphrag_toolkit.byokg_rag.graph_connectors.KGLinker') as mock_kg:
            
            mock_load_yaml.return_value = {
                "generate-response-qa": "Question: {question}\nContext: {graph_context}\nUser Input: {user_input}\nAnswer:"
            }
            
            mock_kg_instance = Mock()
            mock_kg_instance.task_prompts = "test"
            mock_kg_instance.task_prompts_iterative = "test"
            mock_kg.return_value = mock_kg_instance
            
            engine = ByoKGQueryEngine(
                graph_store=mock_graph_store_with_schema,
                llm_generator=mock_llm_generator
            )
            
            answers, response = engine.generate_response(
                query="Who founded Organization?",
                graph_context="John Doe founded Organization"
            )
            
            assert isinstance(answers, list)
            assert isinstance(response, str)
            assert "Organization was founded by John Doe" in response
            mock_llm_generator.generate.assert_called_once()



class TestQueryEngineWithCypherLinker:
    """Tests for query engine with cypher linker."""
    
    def test_initialization_with_cypher_linker(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify initialization with cypher linker."""
        mock_cypher_linker = Mock()
        mock_cypher_linker.is_cypher_linker = True
        mock_cypher_linker.task_prompts = "cypher prompts"
        mock_cypher_linker.task_prompts_iterative = "cypher iterative prompts"
        
        mock_graph_query_executor = Mock()
        
        with patch('graphrag_toolkit.byokg_rag.graph_connectors.KGLinker') as mock_kg:
            mock_kg_instance = Mock()
            mock_kg_instance.task_prompts = "test"
            mock_kg_instance.task_prompts_iterative = "test"
            mock_kg.return_value = mock_kg_instance
            
            engine = ByoKGQueryEngine(
                graph_store=mock_graph_store_with_schema,
                llm_generator=mock_llm_generator,
                cypher_kg_linker=mock_cypher_linker,
                graph_query_executor=mock_graph_query_executor
            )
            
            assert engine.cypher_kg_linker == mock_cypher_linker
            assert engine.graph_query_executor == mock_graph_query_executor
    
    def test_initialization_cypher_linker_without_attribute(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify error when cypher linker lacks is_cypher_linker attribute."""
        mock_cypher_linker = Mock(spec=[])
        
        with pytest.raises(AssertionError, match="must be an instance of CypherKGLinker"):
            ByoKGQueryEngine(
                graph_store=mock_graph_store_with_schema,
                llm_generator=mock_llm_generator,
                cypher_kg_linker=mock_cypher_linker
            )
    
    @patch('graphrag_toolkit.byokg_rag.byokg_query_engine.parse_response')
    def test_query_with_cypher_linker_finish(
        self, mock_parse_response, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify query with cypher linker that finishes early."""
        mock_cypher_linker = Mock()
        mock_cypher_linker.is_cypher_linker = True
        mock_cypher_linker.task_prompts = "cypher prompts"
        mock_cypher_linker.task_prompts_iterative = "cypher iterative prompts"
        mock_cypher_linker.generate_response.return_value = (
            "<opencypher>MATCH (n) RETURN n</opencypher>"
            "<task-completion>FINISH</task-completion>"
        )
        mock_cypher_linker.parse_response.return_value = {
            'opencypher': ['MATCH (n) RETURN n']
        }
        
        mock_graph_query_executor = Mock()
        mock_graph_query_executor.retrieve.return_value = (
            ['Query result'], [{'name': 'Organization'}]
        )
        
        # Mock parse_response to return FINISH
        mock_parse_response.return_value = ['FINISH']
        
        with patch('graphrag_toolkit.byokg_rag.graph_connectors.KGLinker') as mock_kg:
            mock_kg_instance = Mock()
            mock_kg_instance.task_prompts = "test"
            mock_kg_instance.task_prompts_iterative = "test"
            mock_kg.return_value = mock_kg_instance
            
            engine = ByoKGQueryEngine(
                graph_store=mock_graph_store_with_schema,
                llm_generator=mock_llm_generator,
                cypher_kg_linker=mock_cypher_linker,
                graph_query_executor=mock_graph_query_executor
            )
            
            result = engine.query("test query", cypher_iterations=2)
            
            assert isinstance(result, list)
            # Should finish early, so only called once
            assert mock_cypher_linker.generate_response.call_count == 1
    
    @patch('graphrag_toolkit.byokg_rag.byokg_query_engine.parse_response')
    def test_query_with_cypher_linker_no_kg_linker(
        self, mock_parse_response, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify query with only cypher linker (no kg_linker)."""
        mock_cypher_linker = Mock()
        mock_cypher_linker.is_cypher_linker = True
        mock_cypher_linker.task_prompts = "cypher prompts"
        mock_cypher_linker.task_prompts_iterative = "cypher iterative prompts"
        mock_cypher_linker.generate_response.return_value = (
            "<opencypher>MATCH (n) RETURN n</opencypher>"
        )
        mock_cypher_linker.parse_response.return_value = {
            'opencypher': ['MATCH (n) RETURN n']
        }
        
        mock_graph_query_executor = Mock()
        mock_graph_query_executor.retrieve.return_value = (
            ['Query result'], [{'name': 'Amazon'}]
        )
        
        mock_parse_response.return_value = []
        
        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            cypher_kg_linker=mock_cypher_linker,
            graph_query_executor=mock_graph_query_executor,
            kg_linker=None
        )
        
        result = engine.query("test query", cypher_iterations=1)
        
        assert isinstance(result, list)
        # Should return cypher context directly
        assert len(result) > 0
