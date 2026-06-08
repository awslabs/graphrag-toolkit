# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for graph_retrievers.py.

This module tests the various retriever classes including AgenticRetriever,
GraphScoringRetriever, PathRetriever, and GraphQueryRetriever.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from hypothesis import given, settings, assume
from hypothesis import strategies as st
from graphrag_toolkit.byokg_rag.graph_retrievers.graph_retrievers import (
    GRetriever,
    AgenticRetriever,
    GraphScoringRetriever,
    PathRetriever,
    GraphQueryRetriever
)


@pytest.fixture
def mock_llm_generator():
    """Fixture providing a mock LLM generator."""
    mock_gen = Mock()
    mock_gen.generate.return_value = "<selected>relation1\nrelation2</selected>"
    return mock_gen


@pytest.fixture
def mock_graph_traversal():
    """Fixture providing a mock graph traversal component."""
    mock_traversal = Mock()
    mock_traversal.one_hop_triplets.return_value = [
        ('Organization', 'FOUNDED_BY', 'John Doe'),
        ('Organization', 'LOCATED_IN', 'Portland')
    ]
    mock_traversal.multi_hop_triplets.return_value = [
        ('Organization', 'FOUNDED_BY', 'John Doe'),
        ('Organization', 'LOCATED_IN', 'Portland'),
        ('Portland', 'IN_STATE', 'Oregon')
    ]
    mock_traversal.follow_paths.return_value = [
        ['Organization', 'FOUNDED_BY', 'John Doe', 'BORN_IN', 'Chicago']
    ]
    mock_traversal.shortest_paths.return_value = [
        ['Organization', 'LOCATED_IN', 'Portland']
    ]
    return mock_traversal


@pytest.fixture
def mock_graph_verbalizer():
    """Fixture providing a mock graph verbalizer."""
    mock_verbalizer = Mock()
    mock_verbalizer.verbalize_relations.return_value = ['FOUNDED_BY', 'LOCATED_IN']
    mock_verbalizer.verbalize_merge_triplets.return_value = [
        'Organization FOUNDED_BY John Doe',
        'Organization LOCATED_IN Portland'
    ]
    return mock_verbalizer


@pytest.fixture
def mock_path_verbalizer():
    """Fixture providing a mock path verbalizer."""
    mock_verbalizer = Mock()
    mock_verbalizer.verbalize.return_value = [
        'Organization -> FOUNDED_BY -> John Doe -> BORN_IN -> Chicago'
    ]
    return mock_verbalizer


@pytest.fixture
def mock_graph_reranker():
    """Fixture providing a mock graph reranker."""
    mock_reranker = Mock()
    mock_reranker.rerank_input_with_query.return_value = (
        ['Organization FOUNDED_BY John Doe', 'Organization LOCATED_IN Portland'],
        [0.9, 0.8]
    )
    return mock_reranker


@pytest.fixture
def mock_pruning_reranker():
    """Fixture providing a mock pruning reranker."""
    mock_reranker = Mock()
    # Return tuple of (items, ids) for most cases
    mock_reranker.rerank_input_with_query.return_value = (
        ['FOUNDED_BY', 'LOCATED_IN'],
        [0, 1]
    )
    return mock_reranker


@pytest.fixture
def mock_graph_store():
    """Fixture providing a mock graph store."""
    mock_store = Mock()
    mock_store.execute_query.return_value = [
        {'name': 'Organization', 'founded': 2010}
    ]
    return mock_store


class TestGRetrieverAbstract:
    """Tests for GRetriever abstract base class."""
    
    def test_gretriever_is_abstract(self):
        """Verify GRetriever can be instantiated as base class."""
        retriever = GRetriever()
        assert retriever is not None
    
    def test_gretriever_retrieve_method_exists(self):
        """Verify GRetriever has retrieve method."""
        retriever = GRetriever()
        assert hasattr(retriever, 'retrieve')


class TestAgenticRetrieverInitialization:
    """Tests for AgenticRetriever initialization."""
    
    def test_initialization_with_defaults(self, mock_llm_generator, mock_graph_traversal, 
                                         mock_graph_verbalizer):
        """Verify AgenticRetriever initializes with default parameters."""
        retriever = AgenticRetriever(
            llm_generator=mock_llm_generator,
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer
        )
        
        assert retriever.llm_generator == mock_llm_generator
        assert retriever.graph_traversal == mock_graph_traversal
        assert retriever.graph_verbalizer == mock_graph_verbalizer
        assert retriever.max_num_relations == 5
        assert retriever.max_num_entities == 3
        assert retriever.max_num_iterations == 3
        assert retriever.max_num_triplets == 50
    
    def test_initialization_with_custom_parameters(self, mock_llm_generator, 
                                                   mock_graph_traversal, mock_graph_verbalizer):
        """Verify AgenticRetriever accepts custom parameters."""
        retriever = AgenticRetriever(
            llm_generator=mock_llm_generator,
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer,
            max_num_relations=10,
            max_num_entities=5,
            max_num_iterations=5,
            max_num_triplets=100
        )
        
        assert retriever.max_num_relations == 10
        assert retriever.max_num_entities == 5
        assert retriever.max_num_iterations == 5
        assert retriever.max_num_triplets == 100
    
    def test_initialization_with_pruning_reranker(self, mock_llm_generator, 
                                                  mock_graph_traversal, mock_graph_verbalizer,
                                                  mock_pruning_reranker):
        """Verify AgenticRetriever accepts pruning reranker."""
        retriever = AgenticRetriever(
            llm_generator=mock_llm_generator,
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer,
            pruning_reranker=mock_pruning_reranker
        )
        
        assert retriever.pruning_reranker == mock_pruning_reranker


class TestAgenticRetrieverRelationSearchPrune:
    """Tests for AgenticRetriever relation_search_prune method."""
    
    def test_relation_search_prune_basic(self, mock_llm_generator, mock_graph_traversal,
                                        mock_graph_verbalizer):
        """Verify relation search and pruning works."""
        retriever = AgenticRetriever(
            llm_generator=mock_llm_generator,
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer
        )
        
        relations = retriever.relation_search_prune("test query", ['Organization'], max_num_relations=10)
        
        assert isinstance(relations, (list, set))
        mock_graph_traversal.one_hop_triplets.assert_called_once_with(['Organization'])
    
    def test_relation_search_prune_empty_triplets(self, mock_llm_generator, mock_graph_traversal,
                                                  mock_graph_verbalizer):
        """Verify handling of empty triplets."""
        mock_graph_traversal.one_hop_triplets.return_value = []
        
        retriever = AgenticRetriever(
            llm_generator=mock_llm_generator,
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer
        )
        
        relations = retriever.relation_search_prune("test query", ['Organization'])
        
        assert relations == []
    
    def test_relation_search_prune_with_reranker(self, mock_llm_generator, mock_graph_traversal,
                                                mock_graph_verbalizer, mock_pruning_reranker):
        """Verify relation pruning with reranker."""
        # Mock reranker to return 3 values when return_scores=True
        mock_pruning_reranker.rerank_input_with_query.return_value = (
            ['FOUNDED_BY', 'LOCATED_IN'],
            [0.9, 0.8],
            [0, 1]
        )
        
        retriever = AgenticRetriever(
            llm_generator=mock_llm_generator,
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer,
            pruning_reranker=mock_pruning_reranker
        )
        
        relations = retriever.relation_search_prune("test query", ['Organization'], max_num_relations=5)
        
        mock_pruning_reranker.rerank_input_with_query.assert_called_once()
        assert isinstance(relations, (list, tuple))


class TestAgenticRetrieverRetrieve:
    """Tests for AgenticRetriever retrieve method."""
    
    @patch('graphrag_toolkit.byokg_rag.graph_retrievers.graph_retrievers.load_yaml')
    def test_retrieve_basic(self, mock_load_yaml, mock_llm_generator, mock_graph_traversal,
                           mock_graph_verbalizer):
        """Verify basic retrieval works."""
        mock_load_yaml.return_value = {
            'relation_selection_prompt': 'Select relations for {question} from {entity}: {relations}',
            'entity_selection_prompt': 'Select next entities for {question} given {graph_context}'
        }
        
        mock_llm_generator.generate.side_effect = [
            '<selected>FOUNDED_BY\nLOCATED_IN</selected>',
            '<next-entities>FINISH</next-entities>'
        ]
        
        retriever = AgenticRetriever(
            llm_generator=mock_llm_generator,
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer
        )
        
        result = retriever.retrieve("Who founded Organization?", ['Organization'])
        
        assert isinstance(result, list)
        mock_graph_traversal.one_hop_triplets.assert_called()
    
    @patch('graphrag_toolkit.byokg_rag.graph_retrievers.graph_retrievers.load_yaml')
    def test_retrieve_with_history_context(self, mock_load_yaml, mock_llm_generator,
                                          mock_graph_traversal, mock_graph_verbalizer):
        """Verify retrieval with existing history context."""
        mock_load_yaml.return_value = {
            'relation_selection_prompt': 'Select relations for {question} from {entity}: {relations}',
            'entity_selection_prompt': 'Select next entities for {question} given {graph_context}'
        }
        
        mock_llm_generator.generate.side_effect = [
            '<selected>FOUNDED_BY</selected>',
            '<next-entities>FINISH</next-entities>'
        ]
        
        retriever = AgenticRetriever(
            llm_generator=mock_llm_generator,
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer
        )
        
        history = ['Previous context']
        result = retriever.retrieve("test query", ['Organization'], history_context=history)
        
        assert isinstance(result, list)


class TestGraphScoringRetrieverInitialization:
    """Tests for GraphScoringRetriever initialization."""
    
    def test_initialization_basic(self, mock_graph_traversal, mock_graph_verbalizer,
                                  mock_graph_reranker):
        """Verify GraphScoringRetriever initializes correctly."""
        retriever = GraphScoringRetriever(
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer,
            graph_reranker=mock_graph_reranker
        )
        
        assert retriever.graph_traversal == mock_graph_traversal
        assert retriever.graph_verbalizer == mock_graph_verbalizer
        assert retriever.graph_reranker == mock_graph_reranker
        assert retriever.pruning_reranker is None
    
    def test_initialization_with_pruning_reranker(self, mock_graph_traversal, 
                                                  mock_graph_verbalizer, mock_graph_reranker,
                                                  mock_pruning_reranker):
        """Verify GraphScoringRetriever accepts pruning reranker."""
        retriever = GraphScoringRetriever(
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer,
            graph_reranker=mock_graph_reranker,
            pruning_reranker=mock_pruning_reranker
        )
        
        assert retriever.pruning_reranker == mock_pruning_reranker


class TestGraphScoringRetrieverRetrieve:
    """Tests for GraphScoringRetriever retrieve method."""
    
    def test_retrieve_basic(self, mock_graph_traversal, mock_graph_verbalizer,
                           mock_graph_reranker):
        """Verify basic retrieval works."""
        retriever = GraphScoringRetriever(
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer,
            graph_reranker=mock_graph_reranker
        )
        
        result = retriever.retrieve("test query", ['Organization'], hops=2)
        
        assert isinstance(result, list)
        mock_graph_traversal.multi_hop_triplets.assert_called_once_with(['Organization'], hop=2)
        mock_graph_reranker.rerank_input_with_query.assert_called_once()
    
    def test_retrieve_empty_source_nodes(self, mock_graph_traversal, mock_graph_verbalizer,
                                        mock_graph_reranker):
        """Verify handling of empty source nodes."""
        retriever = GraphScoringRetriever(
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer,
            graph_reranker=mock_graph_reranker
        )
        
        result = retriever.retrieve("test query", [])
        
        assert result == []
    
    def test_retrieve_with_pruning(self, mock_graph_traversal, mock_graph_verbalizer,
                                  mock_graph_reranker, mock_pruning_reranker):
        """Verify retrieval with pruning reranker."""
        # Set up mock to return many relations to trigger pruning
        mock_graph_verbalizer.verbalize_relations.return_value = [
            f'RELATION_{i}' for i in range(25)
        ]
        
        retriever = GraphScoringRetriever(
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer,
            graph_reranker=mock_graph_reranker,
            pruning_reranker=mock_pruning_reranker
        )
        
        result = retriever.retrieve("test query", ['Organization'], hops=2, max_num_relations=5)
        
        assert isinstance(result, list)
        # Pruning should be called because we have more than max_num_relations
        assert mock_pruning_reranker.rerank_input_with_query.call_count >= 1
    
    def test_retrieve_with_topk(self, mock_graph_traversal, mock_graph_verbalizer,
                               mock_graph_reranker):
        """Verify retrieval with topk parameter."""
        retriever = GraphScoringRetriever(
            graph_traversal=mock_graph_traversal,
            graph_verbalizer=mock_graph_verbalizer,
            graph_reranker=mock_graph_reranker
        )
        
        result = retriever.retrieve("test query", ['Organization'], topk=5)
        
        assert isinstance(result, list)
        call_args = mock_graph_reranker.rerank_input_with_query.call_args
        assert call_args[1]['topk'] == 5


class TestPathRetrieverInitialization:
    """Tests for PathRetriever initialization."""
    
    def test_initialization_success(self, mock_graph_traversal, mock_path_verbalizer):
        """Verify PathRetriever initializes correctly."""
        retriever = PathRetriever(
            graph_traversal=mock_graph_traversal,
            path_verbalizer=mock_path_verbalizer
        )
        
        assert retriever.graph_traversal == mock_graph_traversal
        assert retriever.path_verbalizer == mock_path_verbalizer
    
    def test_initialization_missing_follow_paths(self, mock_path_verbalizer):
        """Verify error when graph_traversal lacks follow_paths method."""
        mock_traversal = Mock(spec=[])
        
        with pytest.raises(AttributeError, match="must implement 'follow_paths' method"):
            PathRetriever(
                graph_traversal=mock_traversal,
                path_verbalizer=mock_path_verbalizer
            )
    
    def test_initialization_missing_shortest_paths(self, mock_path_verbalizer):
        """Verify error when graph_traversal lacks shortest_paths method."""
        mock_traversal = Mock()
        mock_traversal.follow_paths = Mock()
        delattr(mock_traversal, 'shortest_paths')
        
        with pytest.raises(AttributeError, match="must implement 'shortest_paths' method"):
            PathRetriever(
                graph_traversal=mock_traversal,
                path_verbalizer=mock_path_verbalizer
            )


class TestPathRetrieverFollowPaths:
    """Tests for PathRetriever follow_paths method."""
    
    def test_follow_paths_basic(self, mock_graph_traversal, mock_path_verbalizer):
        """Verify follow_paths works correctly."""
        retriever = PathRetriever(
            graph_traversal=mock_graph_traversal,
            path_verbalizer=mock_path_verbalizer
        )
        
        metapaths = [['FOUNDED_BY', 'BORN_IN']]
        result = retriever.follow_paths(['Organization'], metapaths)
        
        assert isinstance(result, list)
        mock_graph_traversal.follow_paths.assert_called_once_with(['Organization'], metapaths)
        mock_path_verbalizer.verbalize.assert_called_once()
    
    def test_follow_paths_empty_result(self, mock_graph_traversal, mock_path_verbalizer):
        """Verify handling of empty path results."""
        mock_graph_traversal.follow_paths.return_value = []
        
        retriever = PathRetriever(
            graph_traversal=mock_graph_traversal,
            path_verbalizer=mock_path_verbalizer
        )
        
        result = retriever.follow_paths(['Organization'], [['FOUNDED_BY']])
        
        assert result == []



class TestPathRetrieverShortestPaths:
    """Tests for PathRetriever shortest_paths method."""
    
    def test_shortest_paths_basic(self, mock_graph_traversal, mock_path_verbalizer):
        """Verify shortest_paths works correctly."""
        retriever = PathRetriever(
            graph_traversal=mock_graph_traversal,
            path_verbalizer=mock_path_verbalizer
        )
        
        result = retriever.shortest_paths(['Organization'], ['Portland'])
        
        assert isinstance(result, list)
        mock_graph_traversal.shortest_paths.assert_called_once_with(['Organization'], ['Portland'])
        mock_path_verbalizer.verbalize.assert_called_once()
    
    def test_shortest_paths_empty_result(self, mock_graph_traversal, mock_path_verbalizer):
        """Verify handling of empty shortest path results."""
        mock_graph_traversal.shortest_paths.return_value = []
        
        retriever = PathRetriever(
            graph_traversal=mock_graph_traversal,
            path_verbalizer=mock_path_verbalizer
        )
        
        result = retriever.shortest_paths(['Organization'], ['Portland'])
        
        assert result == []


class TestPathRetrieverRetrieve:
    """Tests for PathRetriever retrieve method."""
    
    def test_retrieve_with_metapaths(self, mock_graph_traversal, mock_path_verbalizer):
        """Verify retrieve with metapaths."""
        retriever = PathRetriever(
            graph_traversal=mock_graph_traversal,
            path_verbalizer=mock_path_verbalizer
        )
        
        metapaths = [['FOUNDED_BY', 'BORN_IN']]
        result = retriever.retrieve(['Organization'], metapaths=metapaths)
        
        assert isinstance(result, list)
        mock_graph_traversal.follow_paths.assert_called_once()
    
    def test_retrieve_with_target_nodes(self, mock_graph_traversal, mock_path_verbalizer):
        """Verify retrieve with target nodes."""
        retriever = PathRetriever(
            graph_traversal=mock_graph_traversal,
            path_verbalizer=mock_path_verbalizer
        )
        
        result = retriever.retrieve(['Organization'], target_nodes=['Portland'])
        
        assert isinstance(result, list)
        mock_graph_traversal.shortest_paths.assert_called_once()
    
    def test_retrieve_with_both_metapaths_and_targets(self, mock_graph_traversal, 
                                                      mock_path_verbalizer):
        """Verify retrieve with both metapaths and target nodes."""
        retriever = PathRetriever(
            graph_traversal=mock_graph_traversal,
            path_verbalizer=mock_path_verbalizer
        )
        
        metapaths = [['FOUNDED_BY']]
        result = retriever.retrieve(['Organization'], metapaths=metapaths, target_nodes=['Portland'])
        
        assert isinstance(result, list)
        mock_graph_traversal.follow_paths.assert_called_once()
        mock_graph_traversal.shortest_paths.assert_called_once()
    
    def test_retrieve_empty_metapaths_and_targets(self, mock_graph_traversal, 
                                                  mock_path_verbalizer):
        """Verify retrieve with empty metapaths and targets returns empty list."""
        retriever = PathRetriever(
            graph_traversal=mock_graph_traversal,
            path_verbalizer=mock_path_verbalizer
        )
        
        result = retriever.retrieve(['Organization'], metapaths=[], target_nodes=[])
        
        assert result == []


class TestGraphQueryRetrieverInitialization:
    """Tests for GraphQueryRetriever initialization."""
    
    def test_initialization_success(self, mock_graph_store):
        """Verify GraphQueryRetriever initializes correctly."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        assert retriever.graph_store == mock_graph_store
        assert retriever.block_graph_modification is True
    
    def test_initialization_with_block_modification_false(self, mock_graph_store):
        """Verify initialization with block_graph_modification=False."""
        retriever = GraphQueryRetriever(
            graph_store=mock_graph_store,
            block_graph_modification=False
        )
        
        assert retriever.block_graph_modification is False
    
    def test_initialization_missing_execute_query(self):
        """Verify error when graph_store lacks execute_query method."""
        mock_store = Mock(spec=[])
        
        with pytest.raises(AttributeError, match="must implement 'execute_query' method"):
            GraphQueryRetriever(graph_store=mock_store)



class TestGraphQueryRetrieverIsQuerySafe:
    """Tests for GraphQueryRetriever is_query_safe method."""
    
    def test_is_query_safe_select_query(self, mock_graph_store):
        """Verify SELECT queries are considered safe."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        assert retriever.is_query_safe("MATCH (n) RETURN n") is True
        assert retriever.is_query_safe("SELECT * FROM nodes") is True
    
    def test_is_query_safe_create_query(self, mock_graph_store):
        """Verify CREATE queries are blocked."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        assert retriever.is_query_safe("CREATE (n:Person {name: 'John'})") is False
    
    def test_is_query_safe_merge_query(self, mock_graph_store):
        """Verify MERGE queries are blocked."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        assert retriever.is_query_safe("MERGE (n:Person {name: 'John'})") is False
    
    def test_is_query_safe_delete_query(self, mock_graph_store):
        """Verify DELETE queries are blocked."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        assert retriever.is_query_safe("MATCH (n) DELETE n") is False
        assert retriever.is_query_safe("MATCH (n) DETACH DELETE n") is False
    
    def test_is_query_safe_set_query(self, mock_graph_store):
        """Verify SET queries are blocked."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        assert retriever.is_query_safe("MATCH (n) SET n.name = 'John'") is False
    
    def test_is_query_safe_remove_query(self, mock_graph_store):
        """Verify REMOVE queries are blocked."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        assert retriever.is_query_safe("MATCH (n) REMOVE n.name") is False
    
    def test_is_query_safe_drop_query(self, mock_graph_store):
        """Verify DROP queries are blocked."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        assert retriever.is_query_safe("DROP INDEX my_index") is False
    
    def test_is_query_safe_case_insensitive(self, mock_graph_store):
        """Verify query safety check is case insensitive."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        assert retriever.is_query_safe("create (n:Person)") is False
        assert retriever.is_query_safe("CrEaTe (n:Person)") is False
    
    def test_is_query_safe_multiline_query(self, mock_graph_store):
        """Verify multiline queries are checked correctly."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        query = """MATCH (n:Person)
        WHERE n.name = 'John'
        RETURN n"""
        assert retriever.is_query_safe(query) is True
        
        query_with_create = """MATCH (n:Person)
        CREATE (m:Person {name: 'Jane'})
        RETURN n"""
        assert retriever.is_query_safe(query_with_create) is False


class TestGraphQueryRetrieverRetrieve:
    """Tests for GraphQueryRetriever retrieve method."""
    
    def test_retrieve_safe_query(self, mock_graph_store):
        """Verify retrieval with safe query."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        result = retriever.retrieve("MATCH (n) RETURN n")
        
        assert isinstance(result, list)
        assert len(result) == 1
        mock_graph_store.execute_query.assert_called_once_with("MATCH (n) RETURN n", read_only=True)
    
    def test_retrieve_unsafe_query(self, mock_graph_store):
        """Verify retrieval blocks unsafe query."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        result = retriever.retrieve("CREATE (n:Person)")
        
        assert isinstance(result, list)
        assert "Cannot execute query that modifies the graph" in result[0]
        mock_graph_store.execute_query.assert_not_called()
    
    def test_retrieve_with_return_answers_true(self, mock_graph_store):
        """Verify retrieval with return_answers=True."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        context, answers = retriever.retrieve("MATCH (n) RETURN n", return_answers=True)
        
        assert isinstance(context, list)
        assert isinstance(answers, list)
        assert len(answers) == 1
        assert answers[0]['name'] == 'Organization'
    
    def test_retrieve_with_return_answers_false(self, mock_graph_store):
        """Verify retrieval with return_answers=False."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        result = retriever.retrieve("MATCH (n) RETURN n", return_answers=False)
        
        assert isinstance(result, list)
        assert not isinstance(result, tuple)
    
    def test_retrieve_query_execution_error(self, mock_graph_store):
        """Verify error handling during query execution."""
        mock_graph_store.execute_query.side_effect = Exception("Query failed")
        
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        result = retriever.retrieve("MATCH (n) RETURN n")
        
        assert isinstance(result, list)
        assert "Error executing query" in result[0]
        assert "Query failed" in result[0]
    
    def test_retrieve_query_execution_error_with_return_answers(self, mock_graph_store):
        """Verify error handling with return_answers=True."""
        mock_graph_store.execute_query.side_effect = Exception("Query failed")
        
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        context, answers = retriever.retrieve("MATCH (n) RETURN n", return_answers=True)
        
        assert isinstance(context, list)
        assert isinstance(answers, list)
        assert "Error executing query" in context[0]
        assert len(answers) == 0
    
    def test_retrieve_unsafe_query_with_return_answers(self, mock_graph_store):
        """Verify unsafe query handling with return_answers=True."""
        retriever = GraphQueryRetriever(graph_store=mock_graph_store)
        
        context, answers = retriever.retrieve("CREATE (n:Person)", return_answers=True)
        
        assert isinstance(context, list)
        assert isinstance(answers, list)
        assert "Cannot execute query that modifies the graph" in context[0]
        assert len(answers) == 0



# =============================================================================
# Bug Condition Exploration Property-Based Tests
# =============================================================================

# Strategies for generating obfuscated Cypher queries

# Modification keywords that should be blocked
MODIFICATION_KEYWORDS = ['CREATE', 'DELETE', 'MERGE', 'SET', 'REMOVE', 'DROP', 'DETACH']

# Strategy: Comment bypass - insert /**/ within modification keywords
@st.composite
def comment_bypass_queries(draw):
    """Generate queries with modification keywords split by inline comments.
    
    E.g., CRE/**/ATE (n:Person), DE/**/LETE n, ME/**/RGE (n:Node)
    """
    keyword = draw(st.sampled_from(MODIFICATION_KEYWORDS))
    # Split keyword at a random position and insert /**/
    split_pos = draw(st.integers(min_value=1, max_value=len(keyword) - 1))
    obfuscated = keyword[:split_pos] + '/**/' + keyword[split_pos:]
    suffix = draw(st.sampled_from([
        ' (n:Person {name: "Evil"})',
        ' (n:Malicious)',
        ' n',
        ' (n)-[:REL]->(m)',
    ]))
    return obfuscated + suffix


# Strategy: APOC procedure bypass
@st.composite
def apoc_bypass_queries(draw):
    """Generate queries using APOC procedures that perform write operations."""
    apoc_call = draw(st.sampled_from([
        "CALL apoc.create.node(['Person'], {name: 'Evil'})",
        "CALL apoc.create.nodes(['Person'], [{name: 'Evil'}])",
        "CALL apoc.create.relationship(n, 'KNOWS', {}, m)",
        "CALL apoc.refactor.mergeNodes([n, m])",
        "CALL apoc.refactor.mergeRelationships([r1, r2])",
        "CALL apoc.create.vNode(['Person'], {name: 'Virtual'})",
    ]))
    return apoc_call


# Strategy: CALL subquery bypass
@st.composite
def call_subquery_bypass_queries(draw):
    """Generate queries using CALL subqueries that wrap write operations."""
    inner_op = draw(st.sampled_from([
        'CREATE (n:Malicious) RETURN n',
        'CREATE (n:Person {name: "Evil"}) RETURN n',
        'MERGE (n:Person {name: "Evil"}) RETURN n',
        'MATCH (n) DELETE n RETURN count(n) as c',
        'MATCH (n) SET n.hacked = true RETURN n',
    ]))
    return f'CALL {{ {inner_op} }}'


# Strategy: Unicode bypass - fullwidth Latin characters
@st.composite
def unicode_bypass_queries(draw):
    """Generate queries using fullwidth Unicode characters to form keywords.
    
    Fullwidth Latin characters (U+FF21-U+FF3A for uppercase) look similar
    to ASCII but bypass word-boundary regex matching.
    """
    keyword = draw(st.sampled_from(['CREATE', 'DELETE', 'MERGE', 'DROP']))
    # Convert to fullwidth Unicode (U+FF21 is fullwidth 'A', offset from 'A' is 0xFF21 - 0x41 = 0xFEE0)
    fullwidth = ''.join(chr(ord(c) + 0xFEE0) if c.isalpha() else c for c in keyword)
    suffix = draw(st.sampled_from([
        ' (n:Person)',
        ' (n:Malicious {name: "Evil"})',
        ' n',
    ]))
    return fullwidth + suffix


class TestBugConditionExploration:
    """Property-based tests for Cypher query safety bypass vectors.

    These tests use Hypothesis to generate obfuscated Cypher queries that
    attempt to bypass the is_query_safe() blocklist through various techniques:
    - Embedding comments within keywords (CRE/**/ATE)
    - Using APOC write procedures (CALL apoc.create.node(...))
    - Wrapping writes in CALL subqueries (CALL { CREATE ... })
    - Using fullwidth Unicode characters that resemble ASCII keywords
    - Verifying the block_graph_modification flag disables checks when False
    """

    @given(query=comment_bypass_queries())
    @settings(max_examples=50)
    def test_comment_bypass_detected(self, query):
        """Queries with comment-split keywords (e.g. CRE/**/ATE) are detected as unsafe."""
        mock_store = Mock()
        mock_store.execute_query = Mock()
        retriever = GraphQueryRetriever(graph_store=mock_store)
        
        result = retriever.is_query_safe(query)
        assert result is False, (
            f"BYPASS FOUND: is_query_safe({query!r}) returned True "
            f"- comment-split keyword was not detected"
        )

    @given(query=apoc_bypass_queries())
    @settings(max_examples=50)
    def test_apoc_procedure_bypass_detected(self, query):
        """APOC procedure calls (e.g. CALL apoc.create.node) are detected as unsafe."""
        mock_store = Mock()
        mock_store.execute_query = Mock()
        retriever = GraphQueryRetriever(graph_store=mock_store)
        
        result = retriever.is_query_safe(query)
        assert result is False, (
            f"BYPASS FOUND: is_query_safe({query!r}) returned True "
            f"- APOC procedure was not detected"
        )

    @given(query=call_subquery_bypass_queries())
    @settings(max_examples=50)
    def test_call_subquery_bypass_detected(self, query):
        """CALL subqueries wrapping writes (e.g. CALL { CREATE ... }) are detected as unsafe."""
        mock_store = Mock()
        mock_store.execute_query = Mock()
        retriever = GraphQueryRetriever(graph_store=mock_store)
        
        result = retriever.is_query_safe(query)
        assert result is False, (
            f"BYPASS FOUND: is_query_safe({query!r}) returned True "
            f"- CALL subquery was not detected"
        )

    @given(query=unicode_bypass_queries())
    @settings(max_examples=50)
    def test_unicode_bypass_detected(self, query):
        """Fullwidth Unicode lookalike keywords (e.g. ＣＲＥＡＴＥ) are detected as unsafe."""
        mock_store = Mock()
        mock_store.execute_query = Mock()
        retriever = GraphQueryRetriever(graph_store=mock_store)
        
        result = retriever.is_query_safe(query)
        assert result is False, (
            f"BYPASS FOUND: is_query_safe({query!r}) returned True "
            f"- Unicode lookalike keyword was not detected"
        )

    def test_block_graph_modification_flag_bypass(self):
        """When block_graph_modification=False, is_query_safe() allows all queries."""
        mock_store = Mock()
        mock_store.execute_query = Mock()
        retriever = GraphQueryRetriever(
            graph_store=mock_store,
            block_graph_modification=False
        )
        
        # These modification queries should be ALLOWED when flag is False
        modification_queries = [
            "CREATE (n:Person {name: 'Test'})",
            "MATCH (n) DELETE n",
            "MERGE (n:Person {name: 'Test'})",
            "MATCH (n) SET n.name = 'Evil'",
            "MATCH (n) REMOVE n.name",
            "DROP INDEX my_index",
        ]
        
        for query in modification_queries:
            result = retriever.is_query_safe(query)
            assert result is True, (
                f"FLAG BYPASS CONFIRMED: is_query_safe({query!r}) returned False "
                f"even though block_graph_modification=False - flag is non-functional"
            )


# =============================================================================
# Preservation Property-Based Tests
# =============================================================================

# Strategies for generating legitimate read-only Cypher queries

# Read-only Cypher clauses that should always be allowed
READ_ONLY_CLAUSES = ['MATCH', 'RETURN', 'WHERE', 'WITH', 'OPTIONAL MATCH', 'UNWIND', 'ORDER BY', 'LIMIT']

# Blocked keywords that may appear as substrings in property names or labels
BLOCKED_KEYWORDS_FOR_SUBSTRING = ['CREATE', 'DELETE', 'MERGE', 'SET', 'REMOVE', 'DROP', 'DETACH']


@st.composite
def read_only_cypher_queries(draw):
    """Generate valid read-only Cypher queries using only safe clauses.

    Constructs queries from MATCH, RETURN, WHERE, WITH, OPTIONAL MATCH,
    UNWIND, ORDER BY, and LIMIT without any modification keywords.
    """
    # Generate a node label (simple alphanumeric, no blocked keywords)
    label = draw(st.sampled_from([
        'Person', 'Organization', 'Location', 'Event', 'Document',
        'Product', 'Category', 'Tag', 'User', 'Account'
    ]))

    # Generate a property name (safe, no blocked keywords as standalone words)
    prop = draw(st.sampled_from([
        'name', 'age', 'title', 'description', 'value',
        'count', 'status', 'type', 'date', 'score'
    ]))

    # Generate a variable name
    var = draw(st.sampled_from(['n', 'm', 'p', 'x', 'node', 'result']))

    # Build query pattern
    pattern = draw(st.sampled_from([
        f'MATCH ({var}:{label}) RETURN {var}',
        f'MATCH ({var}:{label}) WHERE {var}.{prop} IS NOT NULL RETURN {var}',
        f'MATCH ({var}:{label}) RETURN {var}.{prop}',
        f'MATCH ({var}:{label}) WITH {var} RETURN {var}',
        f'MATCH ({var}:{label}) WITH {var} ORDER BY {var}.{prop} RETURN {var}',
        f'MATCH ({var}:{label}) WITH {var} ORDER BY {var}.{prop} LIMIT 10 RETURN {var}',
        f'MATCH ({var}:{label}) WHERE {var}.{prop} > 0 RETURN {var}',
        f'OPTIONAL MATCH ({var}:{label}) RETURN {var}',
        f'MATCH ({var}:{label}) UNWIND [{var}] AS item RETURN item',
        f'MATCH ({var})-[r]->({var}2:{label}) RETURN {var}, r, {var}2',
        f'MATCH p=({var}:{label})-[*1..3]->() RETURN p',
        f'MATCH ({var}:{label}) WHERE {var}.{prop} = "test" RETURN {var} LIMIT 5',
    ]))

    return pattern


@st.composite
def substring_keyword_queries(draw):
    """Generate queries where blocked keywords appear as substrings in identifiers.

    E.g., CREATED_AT contains CREATE, SETTINGS contains SET,
    REMOVED_DATE contains REMOVE, DROPPED_ITEMS contains DROP.
    """
    # Property names or labels that contain blocked keywords as substrings
    identifier = draw(st.sampled_from([
        'CREATED_AT', 'CREATED_BY', 'CREATION_DATE',
        'SETTINGS', 'OFFSET', 'RESET_TOKEN', 'CHARSET',
        'REMOVED_DATE', 'REMOVED_BY', 'REMOVAL_REASON',
        'DELETED_AT', 'UNDELETED', 'PREDELETE_STATE',
        'MERGED_FROM', 'EMERGED', 'SUBMERGED',
        'DROPPED_ITEMS', 'BACKDROP', 'RAINDROP',
        'DETACHED_FROM', 'DETACHMENT_DATE',
    ]))

    # Build a read-only query using the identifier as a property or label
    usage = draw(st.sampled_from(['property', 'label']))

    if usage == 'property':
        query = f"MATCH (n:Entity) WHERE n.{identifier} IS NOT NULL RETURN n.{identifier}"
    else:
        query = f"MATCH (n:{identifier}) RETURN n"

    return query


class TestPreservationReadOnlyQueries:
    """Property-based tests ensuring read-only queries are not false-positive blocked.

    These tests verify that legitimate read-only Cypher queries and queries
    containing blocked keywords as substrings (e.g. CREATED_AT, SETTINGS)
    continue to pass is_query_safe() without being incorrectly rejected.
    """

    @given(query=read_only_cypher_queries())
    @settings(max_examples=100)
    def test_read_only_queries_are_safe(self, query):
        """Read-only queries (MATCH/RETURN/WHERE/WITH) are allowed by is_query_safe()."""
        mock_store = Mock()
        mock_store.execute_query = Mock()
        retriever = GraphQueryRetriever(graph_store=mock_store)

        result = retriever.is_query_safe(query)
        assert result is True, (
            f"FALSE POSITIVE: is_query_safe({query!r}) returned False "
            f"for a legitimate read-only query"
        )

    @given(query=substring_keyword_queries())
    @settings(max_examples=100)
    def test_substring_keywords_are_safe(self, query):
        """Blocked keywords as substrings (CREATED_AT, SETTINGS) don't trigger false positives."""
        mock_store = Mock()
        mock_store.execute_query = Mock()
        retriever = GraphQueryRetriever(graph_store=mock_store)

        result = retriever.is_query_safe(query)
        assert result is True, (
            f"FALSE POSITIVE: is_query_safe({query!r}) returned False "
            f"for a query with a keyword substring in an identifier"
        )


class TestPreservationRetrieveFormat:
    """Property-based tests for retrieve() return format consistency.

    Verifies that retrieve() returns:
    - A list when return_answers=False
    - A tuple of (context_list, answers_list) when return_answers=True
    - Error messages containing the query and exception details on failure
    """

    @given(query=read_only_cypher_queries(), return_answers=st.booleans())
    @settings(max_examples=100)
    def test_retrieve_returns_correct_format(self, query, return_answers):
        """retrieve() returns list or (list, list) tuple based on return_answers flag."""
        mock_store = Mock()
        mock_store.execute_query = Mock(return_value=[{'id': 1, 'name': 'Test'}])
        retriever = GraphQueryRetriever(graph_store=mock_store)

        result = retriever.retrieve(query, return_answers=return_answers)

        if return_answers:
            assert isinstance(result, tuple), (
                f"retrieve({query!r}, return_answers=True) did not return a tuple"
            )
            context, answers = result
            assert isinstance(context, list), (
                f"First element of tuple is not a list for query {query!r}"
            )
            assert isinstance(answers, list), (
                f"Second element of tuple is not a list for query {query!r}"
            )
        else:
            assert isinstance(result, list), (
                f"retrieve({query!r}, return_answers=False) did not return a list"
            )
            assert not isinstance(result, tuple), (
                f"retrieve({query!r}, return_answers=False) returned a tuple instead of list"
            )

    @given(query=read_only_cypher_queries())
    @settings(max_examples=50)
    def test_retrieve_error_format(self, query):
        """Query execution errors return a message containing the query and exception details."""
        mock_store = Mock()
        mock_store.execute_query = Mock(side_effect=RuntimeError("Connection timeout"))
        retriever = GraphQueryRetriever(graph_store=mock_store)

        result = retriever.retrieve(query)

        assert isinstance(result, list)
        assert len(result) == 1
        error_msg = result[0]
        assert query in error_msg, (
            f"Error message does not contain the query: {error_msg!r}"
        )
        assert "RuntimeError" in error_msg, (
            f"Error message does not contain exception type: {error_msg!r}"
        )
        assert "Connection timeout" in error_msg, (
            f"Error message does not contain exception details: {error_msg!r}"
        )
