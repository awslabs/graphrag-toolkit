"""Tests for graph_retrievers.py.

This module tests the various retriever classes including AgenticRetriever,
GraphScoringRetriever, PathRetriever, and GraphQueryRetriever.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
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
        mock_graph_store.execute_query.assert_called_once_with("MATCH (n) RETURN n")
    
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
