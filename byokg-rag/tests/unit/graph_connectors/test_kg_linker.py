"""Tests for KGLinker and CypherKGLinker.

This module tests KG linker functionality including initialization,
response generation, response parsing, and task management.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from graphrag_toolkit.byokg_rag.graph_connectors.kg_linker import (
    KGLinker,
    CypherKGLinker
)


def get_mock_load_yaml():
    """Helper function to create a mock load_yaml with proper side effects."""
    def load_yaml_side_effect(path):
        if "kg_linker_prompt" in path:
            return {"kg-linker-prompt": {
                "system-prompt": "System prompt",
                "user-prompt": "User prompt {{task_prompts}}"
            }}
        else:  # task_prompts.yaml
            return {
                "entity-extraction": "Entity extraction task",
                "path-extraction": "Path extraction task",
                "draft-answer-generation": "Answer generation task",
                "entity-extraction-iterative": "Entity extraction iterative task",
                "opencypher-linking": "Cypher linking task",
                "opencypher": "Cypher task",
                "opencypher-linking-iterative": "Cypher linking iterative task"
            }
    return load_yaml_side_effect


@pytest.fixture
def mock_llm_generator():
    """Fixture providing a mock LLM generator."""
    mock_gen = Mock()
    mock_gen.generate.return_value = (
        "<entities>Amazon, Seattle</entities>"
        "<paths>Organization -> LOCATED_IN -> Location</paths>"
        "<answers>Amazon is headquartered in Seattle</answers>"
    )
    return mock_gen


@pytest.fixture
def mock_graph_store():
    """Fixture providing a mock graph store."""
    mock_store = Mock()
    mock_store.get_linker_tasks.return_value = [
        "entity-extraction",
        "path-extraction",
        "draft-answer-generation"
    ]
    return mock_store


@pytest.fixture
def mock_graph_store_with_cypher():
    """Fixture providing a mock graph store that supports cypher."""
    mock_store = Mock()
    mock_store.get_linker_tasks.return_value = [
        "entity-extraction",
        "path-extraction",
        "opencypher",
        "draft-answer-generation"
    ]
    return mock_store


class TestKGLinkerInitialization:
    """Tests for KGLinker initialization."""
    
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    def test_initialization_with_defaults(
        self, mock_load_yaml, mock_llm_generator, mock_graph_store
    ):
        """Verify KGLinker initializes with default parameters."""
        # Mock load_yaml to return appropriate values for each call
        def load_yaml_side_effect(path):
            if "kg_linker_prompt" in path:
                return {"kg-linker-prompt": {
                    "system-prompt": "System prompt",
                    "user-prompt": "User prompt {{task_prompts}}"
                }}
            else:  # task_prompts.yaml
                return {
                    "entity-extraction": "Entity extraction task",
                    "path-extraction": "Path extraction task",
                    "draft-answer-generation": "Answer generation task",
                    "entity-extraction-iterative": "Entity extraction iterative task"
                }
        
        mock_load_yaml.side_effect = load_yaml_side_effect
        
        linker = KGLinker(
            llm_generator=mock_llm_generator,
            graph_store=mock_graph_store
        )
        
        assert linker.llm_generator == mock_llm_generator
        assert linker.max_input_tokens == 32000
        assert "entity-extraction" in linker.AVAILABLE_TASKS
        assert "path-extraction" in linker.AVAILABLE_TASKS
        assert "draft-answer-generation" in linker.AVAILABLE_TASKS
    
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    def test_initialization_custom_max_tokens(
        self, mock_load_yaml, mock_llm_generator, mock_graph_store
    ):
        """Verify KGLinker accepts custom max_input_tokens."""
        def load_yaml_side_effect(path):
            if "kg_linker_prompt" in path:
                return {"kg-linker-prompt": {
                    "system-prompt": "System prompt",
                    "user-prompt": "User prompt {{task_prompts}}"
                }}
            else:
                return {
                    "entity-extraction": "Entity extraction task",
                    "path-extraction": "Path extraction task",
                    "draft-answer-generation": "Answer generation task",
                    "entity-extraction-iterative": "Entity extraction iterative task"
                }
        
        mock_load_yaml.side_effect = load_yaml_side_effect
        
        linker = KGLinker(
            llm_generator=mock_llm_generator,
            graph_store=mock_graph_store,
            max_input_tokens=16000
        )
        
        assert linker.max_input_tokens == 16000


class TestKGLinkerGenerateResponse:
    """Tests for response generation."""
    
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.validate_input_length')
    def test_generate_response_success(
        self, mock_validate, mock_load_yaml, mock_llm_generator, mock_graph_store
    ):
        """Verify response generation with valid inputs."""
        def load_yaml_side_effect(path):
            if "kg_linker_prompt" in path:
                return {"kg-linker-prompt": {
                    "system-prompt": "System prompt",
                    "user-prompt": "Question: {question}\nSchema: {schema}\nContext: {graph_context}\nUser Input: {user_input}\n{{task_prompts}}"
                }}
            else:
                return {
                    "entity-extraction": "Entity extraction task",
                    "path-extraction": "Path extraction task",
                    "draft-answer-generation": "Answer generation task",
                    "entity-extraction-iterative": "Entity extraction iterative task"
                }
        
        mock_load_yaml.side_effect = load_yaml_side_effect
        
        linker = KGLinker(
            llm_generator=mock_llm_generator,
            graph_store=mock_graph_store
        )
        
        response = linker.generate_response(
            question="Where is Amazon located?",
            schema="Node types: Organization, Location",
            graph_context="Amazon is a tech company",
            user_input=""
        )
        
        assert isinstance(response, str)
        mock_llm_generator.generate.assert_called_once()
        
        # Verify validate_input_length was called for user_input and question
        assert mock_validate.call_count == 2
    
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.validate_input_length')
    def test_generate_response_with_custom_task_prompts(
        self, mock_validate, mock_load_yaml, mock_llm_generator, mock_graph_store
    ):
        """Verify response generation with custom task prompts."""
        mock_load_yaml.side_effect = get_mock_load_yaml()
        
        linker = KGLinker(
            llm_generator=mock_llm_generator,
            graph_store=mock_graph_store
        )
        
        custom_prompts = "Custom task instructions"
        response = linker.generate_response(
            question="Test question",
            schema="Test schema",
            task_prompts=custom_prompts
        )
        
        assert isinstance(response, str)
        mock_llm_generator.generate.assert_called_once()


class TestKGLinkerParseResponse:
    """Tests for response parsing."""
    
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.parse_response')
    def test_parse_response_extracts_artifacts(
        self, mock_parse, mock_load_yaml, mock_llm_generator, mock_graph_store
    ):
        """Verify parse_response extracts task artifacts correctly."""
        mock_load_yaml.side_effect = get_mock_load_yaml()
        
        # Mock parse_response to return different results for different patterns
        def parse_side_effect(response, pattern):
            if "entities" in pattern:
                return ["Amazon", "Seattle"]
            elif "paths" in pattern:
                return ["Organization -> LOCATED_IN -> Location"]
            elif "answers" in pattern:
                return ["Amazon is headquartered in Seattle"]
            return []
        
        mock_parse.side_effect = parse_side_effect
        
        linker = KGLinker(
            llm_generator=mock_llm_generator,
            graph_store=mock_graph_store
        )
        
        llm_response = (
            "<entities>Amazon, Seattle</entities>"
            "<paths>Organization -> LOCATED_IN -> Location</paths>"
            "<answers>Amazon is headquartered in Seattle</answers>"
        )
        
        artifacts = linker.parse_response(llm_response)
        
        assert isinstance(artifacts, dict)
        assert "entity-extraction" in artifacts
        assert "path-extraction" in artifacts
        assert "draft-answer-generation" in artifacts
        assert artifacts["entity-extraction"] == ["Amazon", "Seattle"]


class TestKGLinkerGetTasks:
    """Tests for task retrieval."""
    
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    def test_get_tasks_from_graph_store(
        self, mock_load_yaml, mock_llm_generator, mock_graph_store
    ):
        """Verify get_tasks retrieves tasks from graph store."""
        mock_load_yaml.side_effect = get_mock_load_yaml()
        
        linker = KGLinker(
            llm_generator=mock_llm_generator,
            graph_store=mock_graph_store
        )
        
        tasks = linker.get_tasks(mock_graph_store)
        
        assert isinstance(tasks, list)
        assert "entity-extraction" in tasks
        assert "path-extraction" in tasks
        assert "draft-answer-generation" in tasks


class TestCypherKGLinkerInitialization:
    """Tests for CypherKGLinker initialization."""
    
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    def test_cypher_linker_initialization(
        self, mock_load_yaml, mock_llm_generator, mock_graph_store_with_cypher
    ):
        """Verify CypherKGLinker initializes with cypher support."""
        mock_load_yaml.side_effect = get_mock_load_yaml()
        
        linker = CypherKGLinker(
            llm_generator=mock_llm_generator,
            graph_store=mock_graph_store_with_cypher
        )
        
        assert "opencypher" in linker.AVAILABLE_TASKS
        assert "opencypher-linking" in linker.AVAILABLE_TASKS
        assert linker.is_cypher_linker() is True
    
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    def test_cypher_linker_requires_opencypher_support(
        self, mock_load_yaml, mock_llm_generator, mock_graph_store
    ):
        """Verify CypherKGLinker requires graph store with opencypher support."""
        mock_load_yaml.side_effect = get_mock_load_yaml()
        
        # Graph store without opencypher support should raise assertion error
        with pytest.raises(AssertionError, match="Graphstore needs to support openCypher"):
            linker = CypherKGLinker(
                llm_generator=mock_llm_generator,
                graph_store=mock_graph_store
            )
    
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    def test_cypher_linker_get_tasks(
        self, mock_load_yaml, mock_llm_generator, mock_graph_store_with_cypher
    ):
        """Verify CypherKGLinker returns correct task list."""
        mock_load_yaml.side_effect = get_mock_load_yaml()
        
        linker = CypherKGLinker(
            llm_generator=mock_llm_generator,
            graph_store=mock_graph_store_with_cypher
        )
        
        tasks = linker.get_tasks(mock_graph_store_with_cypher)
        
        assert isinstance(tasks, list)
        assert "opencypher-linking" in tasks
        assert "opencypher" in tasks
        assert "draft-answer-generation" in tasks


class TestKGLinkerPromptFinalization:
    """Tests for prompt finalization methods."""
    
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    def test_finalize_prompt_combines_tasks(
        self, mock_load_yaml, mock_llm_generator, mock_graph_store
    ):
        """Verify _finalize_prompt combines task prompts correctly."""
        mock_load_yaml.side_effect = get_mock_load_yaml()
        
        linker = KGLinker(
            llm_generator=mock_llm_generator,
            graph_store=mock_graph_store
        )
        
        assert isinstance(linker.task_prompts, str)
        assert len(linker.task_prompts) > 0
    
    @patch('graphrag_toolkit.byokg_rag.graph_connectors.kg_linker.load_yaml')
    def test_finalize_prompt_iterative(
        self, mock_load_yaml, mock_llm_generator, mock_graph_store
    ):
        """Verify _finalize_prompt_iterative_prompt uses iterative versions."""
        mock_load_yaml.side_effect = get_mock_load_yaml()
        
        linker = KGLinker(
            llm_generator=mock_llm_generator,
            graph_store=mock_graph_store
        )
        
        assert isinstance(linker.task_prompts_iterative, str)
        assert len(linker.task_prompts_iterative) > 0
