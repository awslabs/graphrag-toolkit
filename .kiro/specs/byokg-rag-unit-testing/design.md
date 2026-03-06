# Design Document: BYOKG-RAG Unit Testing Infrastructure

## Overview

This design document specifies the architecture, implementation approach, and technical details for adding comprehensive unit testing infrastructure to the byokg-rag module of the GraphRAG Toolkit.

The design covers test directory structure, test dependencies, core module tests, reusable fixtures, coverage reporting configuration, CI/CD integration, and comprehensive documentation.

### Design Goals

- Establish consistent testing patterns across both GraphRAG Toolkit packages
- Achieve meaningful code coverage (50-70% depending on module complexity)
- Enable fast, reliable test execution without external service dependencies
- Provide reusable fixtures and mocking patterns for AWS services
- Integrate seamlessly with existing CI/CD workflows
- Support developer productivity with clear documentation and examples

### Target Audiences

This design serves three primary audiences:

- Package maintainers: Need to understand the overall testing architecture and coverage strategy
- Contributors: Need clear patterns for writing new tests and using fixtures
- CI/CD engineers: Need to understand workflow configuration and test execution requirements

## Architecture

### Testing Architecture Overview

The testing infrastructure follows a layered architecture that mirrors the source code structure:

```
byokg-rag/
├── src/graphrag_toolkit/byokg_rag/     # Source code
│   ├── utils.py
│   ├── byokg_query_engine.py
│   ├── indexing/
│   ├── graph_retrievers/
│   ├── graph_connectors/
│   ├── graphstore/
│   └── llm/
└── tests/                               # Test infrastructure
    ├── conftest.py                      # Shared fixtures
    ├── README.md                        # Test documentation
    └── unit/                            # Unit tests
        ├── __init__.py
        ├── test_utils.py
        ├── test_byokg_query_engine.py
        ├── indexing/
        │   ├── __init__.py
        │   ├── test_dense_index.py
        │   ├── test_fuzzy_string.py
        │   └── test_graph_store_index.py
        ├── graph_retrievers/
        │   ├── __init__.py
        │   ├── test_entity_linker.py
        │   ├── test_graph_traversal.py
        │   ├── test_graph_reranker.py
        │   └── test_graph_verbalizer.py
        ├── graph_connectors/
        │   ├── __init__.py
        │   └── test_kg_linker.py
        ├── graphstore/
        │   ├── __init__.py
        │   └── test_neptune.py
        └── llm/
            ├── __init__.py
            └── test_bedrock_llms.py
```

### Test Isolation Strategy

The testing architecture ensures complete isolation from external dependencies:

1. AWS Service Mocking: All AWS Bedrock and Neptune calls use mocked responses
2. Graph Store Abstraction: Mock graph store implementations provide test data
3. LLM Response Mocking: Deterministic LLM responses for predictable test behavior
4. No Network Dependencies: All tests run without network access or credentials

### Fixture Architecture

Fixtures are organized in three tiers:

1. Base Fixtures (conftest.py): Core mocks for LLM clients, graph stores, and common data structures
2. Module Fixtures: Specialized fixtures defined in test modules for specific use cases
3. Parametrized Fixtures: Fixtures that generate multiple test scenarios from single definitions


## Components and Interfaces

### Test Directory Structure

The test directory mirrors the source code structure to maintain clear correspondence between tests and implementation:

- `tests/conftest.py`: Shared pytest fixtures available to all tests
- `tests/unit/`: Contains all unit tests organized by module
- `tests/README.md`: Comprehensive testing documentation

Each source module has a corresponding test module with the `test_` prefix. Subdirectories in the source are replicated in the test structure.

### Test Dependencies

The testing infrastructure requires the following dependencies:

```python
# Test framework
pytest>=7.0.0

# Coverage reporting
pytest-cov>=4.0.0

# Mocking capabilities
pytest-mock>=3.10.0

# AWS mocking (optional, for integration-style tests)
moto>=4.0.0  # For mocking boto3 calls
```

These dependencies are configured in the pyproject.toml file and installed separately from production dependencies.

### Core Test Fixtures

#### Mock LLM Client Fixture

```python
@pytest.fixture
def mock_bedrock_generator():
    """
    Fixture providing a mock BedrockGenerator with deterministic responses.
    
    Returns a mock that simulates LLM generation without AWS API calls.
    """
    mock_gen = Mock(spec=BedrockGenerator)
    mock_gen.generate.return_value = "Mock LLM response"
    mock_gen.model_name = "mock-model"
    mock_gen.region_name = "us-west-2"
    return mock_gen
```

#### Mock Graph Store Fixture

```python
@pytest.fixture
def mock_graph_store():
    """
    Fixture providing a mock graph store with sample data.
    
    Returns a mock graph store that provides schema and node data
    without requiring a real graph database connection.
    """
    mock_store = Mock()
    mock_store.get_schema.return_value = {
        'node_types': ['Person', 'Organization', 'Location'],
        'edge_types': ['WORKS_FOR', 'LOCATED_IN']
    }
    mock_store.nodes.return_value = ['Organization', 'Portland', 'John Doe']
    return mock_store
```

#### Sample Query Data Fixture

```python
@pytest.fixture
def sample_queries():
    """
    Fixture providing sample query strings for testing.
    
    Returns a list of representative queries covering different patterns.
    """
    return [
        "Who founded Organization?",
        "Where is Organization headquartered?",
        "What products does Organization sell?"
    ]
```

#### Sample Graph Data Fixture

```python
@pytest.fixture
def sample_graph_data():
    """
    Fixture providing sample graph structures for testing.
    
    Returns dictionaries representing nodes, edges, and paths.
    """
    return {
        'nodes': [
            {'id': 'n1', 'label': 'Person', 'name': 'John Doe'},
            {'id': 'n2', 'label': 'Organization', 'name': 'Organization'},
            {'id': 'n3', 'label': 'Location', 'name': 'Portland'}
        ],
        'edges': [
            {'source': 'n1', 'target': 'n2', 'type': 'FOUNDED'},
            {'source': 'n2', 'target': 'n3', 'type': 'LOCATED_IN'}
        ],
        'paths': [
            ['n1', 'FOUNDED', 'n2', 'LOCATED_IN', 'n3']
        ]
    }
```

### Test Module Organization

#### Utils Module Tests (test_utils.py)

Tests for utility functions in utils.py:

- `test_load_yaml_valid_file`: Verify YAML loading with valid file
- `test_load_yaml_relative_path`: Verify relative path resolution
- `test_parse_response_valid_pattern`: Verify regex pattern matching
- `test_parse_response_no_match`: Verify behavior when pattern doesn't match
- `test_count_tokens_empty_string`: Verify token counting for empty input
- `test_count_tokens_normal_text`: Verify token counting for normal text
- `test_validate_input_length_within_limit`: Verify validation passes for valid input
- `test_validate_input_length_exceeds_limit`: Verify ValueError raised for oversized input

#### Indexing Module Tests

Tests for indexing/fuzzy_string.py:

- `test_fuzzy_string_index_initialization`: Verify index starts empty
- `test_fuzzy_string_index_add_vocab`: Verify vocabulary addition
- `test_fuzzy_string_index_query_exact_match`: Verify exact string matching
- `test_fuzzy_string_index_query_fuzzy_match`: Verify fuzzy matching with typos
- `test_fuzzy_string_index_query_topk`: Verify topk result limiting
- `test_fuzzy_string_index_match_multiple_inputs`: Verify batch matching
- `test_fuzzy_string_index_match_length_filtering`: Verify max_len_difference filtering

Tests for indexing/dense_index.py:

- `test_dense_index_creation`: Verify index initialization
- `test_dense_index_add_embeddings`: Verify embedding addition
- `test_dense_index_query_similarity`: Verify similarity search
- `test_dense_index_query_with_mock_llm`: Verify embedding generation with mocked LLM

Tests for indexing/graph_store_index.py:

- `test_graph_store_index_initialization`: Verify index setup with mock graph store
- `test_graph_store_index_query`: Verify graph-based querying

#### Graph Retriever Module Tests

Tests for graph_retrievers/entity_linker.py:

- `test_entity_linker_initialization`: Verify linker setup with retriever
- `test_entity_linker_link_return_dict`: Verify dictionary return format
- `test_entity_linker_link_return_list`: Verify list return format
- `test_entity_linker_link_with_topk`: Verify topk parameter handling
- `test_entity_linker_link_no_retriever_error`: Verify error when retriever missing

Tests for graph_retrievers/graph_traversal.py:

- `test_graph_traversal_initialization`: Verify traversal setup with mock graph store
- `test_graph_traversal_single_hop`: Verify single-hop traversal
- `test_graph_traversal_multi_hop`: Verify multi-hop path traversal
- `test_graph_traversal_with_metapath`: Verify metapath-guided traversal

Tests for graph_retrievers/graph_verbalizer.py:

- `test_triplet_verbalizer_format`: Verify triplet formatting
- `test_path_verbalizer_format`: Verify path formatting
- `test_verbalizer_empty_input`: Verify handling of empty inputs

#### Query Engine Module Tests

Tests for byokg_query_engine.py:

- `test_query_engine_initialization_defaults`: Verify default component initialization
- `test_query_engine_initialization_custom_components`: Verify custom component injection
- `test_query_engine_query_single_iteration`: Verify single iteration query processing
- `test_query_engine_query_multiple_iterations`: Verify iterative retrieval
- `test_query_engine_query_with_mocked_llm`: Verify LLM interaction mocking
- `test_query_engine_generate_response`: Verify response generation
- `test_query_engine_add_to_context_deduplication`: Verify context deduplication

#### LLM Module Tests

Tests for llm/bedrock_llms.py:

- `test_bedrock_generator_initialization`: Verify generator setup
- `test_bedrock_generator_generate_with_mock`: Verify mocked generation
- `test_bedrock_generator_retry_logic`: Verify retry behavior on failures
- `test_bedrock_generator_error_handling`: Verify error message handling

#### Graph Store Module Tests

Tests for graphstore/neptune.py:

- `test_neptune_store_initialization`: Verify Neptune store setup with mocked boto3
- `test_neptune_store_get_schema`: Verify schema retrieval
- `test_neptune_store_execute_query`: Verify query execution with mocked responses

### Coverage Reporting Configuration

Coverage reporting is configured via pytest.ini or pyproject.toml:

```ini
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = [
    "-v",
    "--tb=short",
    "--cov=src/graphrag_toolkit/byokg_rag",
    "--cov-report=term-missing",
    "--cov-report=html:htmlcov",
    "--cov-config=.coveragerc"
]

[tool.coverage.run]
source = ["src/graphrag_toolkit/byokg_rag"]
omit = [
    "*/tests/*",
    "*/test_*.py",
    "*/__pycache__/*",
    "*/prompts/*"
]

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "raise AssertionError",
    "raise NotImplementedError",
    "if __name__ == .__main__.:",
    "if TYPE_CHECKING:",
    "@abstractmethod"
]
```

### CI/CD Workflow Configuration

The GitHub Actions workflow file (.github/workflows/byokg-rag-tests.yml):

```yaml
name: BYOKG-RAG Unit Tests

on:
  push:
    branches: [main]
    paths:
      - "byokg-rag/**"
      - ".github/workflows/byokg-rag-tests.yml"
  pull_request:
    branches: [main]
    paths:
      - "byokg-rag/**"
      - ".github/workflows/byokg-rag-tests.yml"

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11", "3.12"]

    defaults:
      run:
        working-directory: byokg-rag

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Create virtual environment
        run: uv venv .venv

      - name: Install dependencies
        run: |
          uv pip install --python .venv/bin/python \
            pytest \
            pytest-cov \
            pytest-mock \
            -r src/graphrag_toolkit/byokg_rag/requirements.txt

      - name: Run unit tests with coverage
        run: |
          PYTHONPATH=src .venv/bin/python -m pytest tests/ \
            -v \
            --tb=short \
            --cov=src/graphrag_toolkit/byokg_rag \
            --cov-report=term-missing \
            --cov-report=html:htmlcov

      - name: Upload coverage report
        if: matrix.python-version == '3.12'
        uses: actions/upload-artifact@v4
        with:
          name: coverage-report
          path: byokg-rag/htmlcov/
```


## Data Models

### Test Result Data Model

Test execution produces structured results:

```python
TestResult = {
    'test_name': str,           # Full test name including module path
    'status': str,              # 'passed', 'failed', 'skipped', 'error'
    'duration': float,          # Execution time in seconds
    'failure_message': str,     # Error message if failed
    'coverage_delta': float     # Coverage change from this test
}
```

### Coverage Report Data Model

Coverage reports contain module-level metrics:

```python
CoverageReport = {
    'overall_coverage': float,  # Overall percentage
    'modules': {
        'module_name': {
            'coverage': float,      # Module coverage percentage
            'lines_total': int,     # Total lines
            'lines_covered': int,   # Covered lines
            'lines_missing': List[int],  # Uncovered line numbers
            'branches_total': int,  # Total branches
            'branches_covered': int  # Covered branches
        }
    },
    'timestamp': str,           # ISO 8601 timestamp
    'python_version': str       # Python version used
}
```

### Mock Response Data Model

Mock AWS service responses follow consistent structure:

```python
MockBedrockResponse = {
    'output': {
        'message': {
            'content': [
                {'text': str}  # Generated text response
            ]
        }
    },
    'stopReason': str,  # 'end_turn', 'max_tokens', etc.
    'usage': {
        'inputTokens': int,
        'outputTokens': int
    }
}

MockNeptuneResponse = {
    'results': List[Dict],  # Query results
    'status': str,          # '200 OK'
    'requestId': str        # Mock request ID
}
```

### Test Fixture Data Model

Fixtures provide structured test data:

```python
GraphTestData = {
    'nodes': List[{
        'id': str,
        'label': str,
        'properties': Dict[str, Any]
    }],
    'edges': List[{
        'source': str,
        'target': str,
        'type': str,
        'properties': Dict[str, Any]
    }],
    'schema': {
        'node_types': List[str],
        'edge_types': List[str],
        'properties': Dict[str, str]  # property_name -> type
    }
}
```

## Correctness Properties

A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.

NOTE: This feature is about creating testing infrastructure, not testing the byokg-rag system itself. The requirements specify what the testing infrastructure should provide (directory structure, fixtures, CI/CD configuration, documentation), not functional behaviors of the byokg-rag system that can be tested with property-based tests.

All acceptance criteria in the requirements document describe:
- File system structure to create (directories, files)
- Configuration to add (dependencies, CI/CD workflows)
- Documentation to write (README, test patterns)
- Test coverage targets to achieve (percentage goals)
- Test quality standards to follow (naming conventions, docstrings)

None of these are testable properties of a system's behavior. They are deliverables and standards for the testing infrastructure itself. Therefore, there are no correctness properties to specify for this feature.

The testing infrastructure will enable property-based testing of the byokg-rag system in the future, but the infrastructure creation itself is not subject to property-based testing.

## Error Handling

### Test Execution Errors

The testing infrastructure handles several error scenarios:

#### Missing Dependencies

When required test dependencies are not installed:

```python
try:
    import pytest
except ImportError:
    print("ERROR: pytest is not installed. Install test dependencies:")
    print("  uv pip install pytest pytest-cov pytest-mock")
    sys.exit(1)
```

#### AWS Credential Errors

Tests must not require AWS credentials. If a test accidentally makes a real AWS call:

```python
@pytest.fixture(autouse=True)
def block_aws_calls(monkeypatch):
    """
    Fixture that blocks all real AWS API calls during tests.
    
    Raises an error if any test attempts to make a real AWS call,
    ensuring tests remain isolated and fast.
    """
    def mock_boto3_client(*args, **kwargs):
        raise RuntimeError(
            "Tests must not make real AWS API calls. "
            "Use mocked clients from conftest.py fixtures."
        )
    
    monkeypatch.setattr('boto3.client', mock_boto3_client)
```

#### Fixture Initialization Errors

When fixtures fail to initialize properly:

```python
@pytest.fixture
def mock_graph_store():
    """Fixture providing a mock graph store."""
    try:
        mock_store = Mock(spec=GraphStore)
        mock_store.get_schema.return_value = {...}
        return mock_store
    except Exception as e:
        pytest.fail(f"Failed to initialize mock_graph_store fixture: {e}")
```

#### Test Data Loading Errors

When test data files are missing or malformed:

```python
def load_test_data(filename):
    """Load test data from fixtures directory."""
    try:
        path = Path(__file__).parent / 'fixtures' / filename
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        pytest.skip(f"Test data file not found: {filename}")
    except json.JSONDecodeError as e:
        pytest.fail(f"Invalid JSON in test data file {filename}: {e}")
```

### Coverage Reporting Errors

Coverage tool errors are handled gracefully:

```python
# In pytest configuration
[tool.pytest.ini_options]
addopts = [
    "--cov-fail-under=0",  # Don't fail on low coverage initially
]
```

Coverage failures generate warnings but don't block test execution:

```bash
# CI workflow includes coverage check but doesn't fail build
- name: Check coverage thresholds
  run: |
    coverage report --fail-under=50 || echo "WARNING: Coverage below target"
```

### CI/CD Error Handling

The CI workflow handles common failure scenarios:

```yaml
- name: Run unit tests with coverage
  id: test
  continue-on-error: false  # Fail fast on test failures
  run: |
    PYTHONPATH=src .venv/bin/python -m pytest tests/ -v

- name: Report test failure
  if: failure() && steps.test.outcome == 'failure'
  run: |
    echo "::error::Unit tests failed. Check test output above."
    exit 1
```

### Mock Validation Errors

Mocks validate their usage to catch test errors:

```python
@pytest.fixture
def mock_bedrock_generator():
    """Mock LLM generator with usage validation."""
    mock_gen = Mock(spec=BedrockGenerator)
    mock_gen.generate.return_value = "Mock response"
    
    # Validate that generate() is called with required parameters
    def validate_generate_call(*args, **kwargs):
        if 'prompt' not in kwargs and len(args) < 1:
            raise ValueError("generate() requires 'prompt' parameter")
        return "Mock response"
    
    mock_gen.generate.side_effect = validate_generate_call
    return mock_gen
```

## Testing Strategy

### Dual Testing Approach

The byokg-rag testing infrastructure uses a dual approach:

1. Unit Tests: Verify specific examples, edge cases, and error conditions
2. Property Tests: Not applicable for this infrastructure feature (see Correctness Properties section)

Unit tests focus on:
- Specific examples demonstrating correct behavior
- Integration points between components
- Edge cases (empty inputs, boundary conditions)
- Error conditions (missing parameters, invalid inputs)

### Test Organization Strategy

Tests are organized by module with clear naming conventions:

```
test_<module_name>.py
  ├── test_<function_name>_<scenario>
  ├── test_<function_name>_<edge_case>
  └── test_<class_name>_<method>_<scenario>
```

Example:

```python
# tests/unit/test_utils.py

def test_count_tokens_empty_string():
    """Verify token counting returns 0 for empty string."""
    assert count_tokens("") == 0

def test_count_tokens_normal_text():
    """Verify token counting for normal text (~4 chars per token)."""
    text = "This is a test"  # 14 chars
    assert count_tokens(text) == 3  # 14 // 4 = 3

def test_validate_input_length_within_limit():
    """Verify validation passes when input is within limit."""
    validate_input_length("short text", max_tokens=100)  # Should not raise

def test_validate_input_length_exceeds_limit():
    """Verify ValueError raised when input exceeds limit."""
    long_text = "x" * 1000  # ~250 tokens
    with pytest.raises(ValueError, match="exceeds maximum token limit"):
        validate_input_length(long_text, max_tokens=100)
```

### Mocking Strategy

The testing infrastructure uses three levels of mocking:

#### Level 1: External Service Mocking

Mock all AWS service calls (Bedrock, Neptune):

```python
@pytest.fixture
def mock_bedrock_client(monkeypatch):
    """Mock boto3 Bedrock client."""
    mock_client = Mock()
    mock_client.converse.return_value = {
        'output': {
            'message': {
                'content': [{'text': 'Mock LLM response'}]
            }
        }
    }
    
    def mock_boto3_client(service_name, **kwargs):
        if service_name == 'bedrock-runtime':
            return mock_client
        raise ValueError(f"Unexpected service: {service_name}")
    
    monkeypatch.setattr('boto3.client', mock_boto3_client)
    return mock_client
```

#### Level 2: Component Mocking

Mock byokg-rag components for integration tests:

```python
@pytest.fixture
def mock_entity_linker():
    """Mock EntityLinker for query engine tests."""
    mock_linker = Mock(spec=EntityLinker)
    mock_linker.link.return_value = ['Organization', 'Portland']
    return mock_linker
```

#### Level 3: Data Mocking

Provide realistic test data:

```python
@pytest.fixture
def sample_graph_schema():
    """Sample graph schema for testing."""
    return {
        'node_types': ['Person', 'Organization', 'Location'],
        'edge_types': ['WORKS_FOR', 'FOUNDED', 'LOCATED_IN'],
        'properties': {
            'Person': ['name', 'age'],
            'Organization': ['name', 'industry'],
            'Location': ['name', 'country']
        }
    }
```

### Coverage Strategy

Coverage targets vary by module complexity:

| Module Type | Target Coverage | Rationale |
|-------------|----------------|-----------|
| Utility modules (utils.py) | 70% | Simple, deterministic functions |
| Indexing modules | 60% | Mix of algorithms and I/O |
| Graph retrievers | 60% | Complex logic with external dependencies |
| LLM integration | 50% | Heavy AWS service interaction |
| Graph stores | 50% | Database-specific implementations |

Coverage is measured with pytest-cov:

```bash
# Run tests with coverage
pytest tests/ --cov=src/graphrag_toolkit/byokg_rag --cov-report=term-missing

# Generate HTML report
pytest tests/ --cov=src/graphrag_toolkit/byokg_rag --cov-report=html

# Check coverage thresholds
pytest tests/ --cov=src/graphrag_toolkit/byokg_rag --cov-fail-under=50
```

### Test Execution Strategy

Tests are designed for fast, parallel execution:

```bash
# Run all tests
pytest tests/

# Run specific module tests
pytest tests/unit/test_utils.py

# Run specific test function
pytest tests/unit/test_utils.py::test_count_tokens_empty_string

# Run with verbose output
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src/graphrag_toolkit/byokg_rag

# Run in parallel (requires pytest-xdist)
pytest tests/ -n auto
```

### Test Documentation Strategy

Each test includes a docstring explaining what it verifies:

```python
def test_fuzzy_string_index_query_exact_match():
    """
    Verify exact string matching returns 100% match score.
    
    When the query exactly matches a vocabulary item, the fuzzy
    string index should return that item with a match score of 100.
    """
    index = FuzzyStringIndex()
    index.add(['Organization', 'DataCorp', 'CloudCorp'])
    
    result = index.query('Organization', topk=1)
    
    assert len(result['hits']) == 1
    assert result['hits'][0]['document'] == 'Organization'
    assert result['hits'][0]['match_score'] == 100
```

### Continuous Integration Strategy

Tests run automatically on:

1. Push to main branch (when byokg-rag files change)
2. Pull requests to main branch (when byokg-rag files change)
3. Manual workflow dispatch (for testing infrastructure changes)

The CI workflow:
- Tests against Python 3.10, 3.11, and 3.12
- Runs all unit tests with coverage reporting
- Fails if any test fails
- Uploads coverage reports as artifacts
- Completes in under 5 minutes

### Test Maintenance Strategy

Tests are maintained through:

1. Regression Tests: Add test for every bug fix
2. Feature Tests: Add tests for every new feature
3. Refactoring Tests: Update tests when implementation changes
4. Deprecation Tests: Mark tests as deprecated when features are deprecated
5. Flaky Test Handling: Investigate and fix flaky tests immediately

Documentation in tests/README.md covers:
- How to run tests locally
- How to write new tests
- How to use fixtures
- How to mock AWS services
- How to debug test failures
- How to update tests when code changes


## Implementation Examples

### Example 1: Utils Module Test Implementation

Complete test file for utils.py:

```python
"""Tests for utils.py functions.

This module tests utility functions including YAML loading, response parsing,
token counting, and input validation.
"""

import pytest
import tempfile
import os
from pathlib import Path
from graphrag_toolkit.byokg_rag.utils import (
    load_yaml,
    parse_response,
    count_tokens,
    validate_input_length
)


class TestLoadYaml:
    """Tests for load_yaml function."""
    
    def test_load_yaml_valid_file(self):
        """Verify YAML loading with valid file content."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write('key: value\nlist:\n  - item1\n  - item2')
            temp_path = f.name
        
        try:
            result = load_yaml(temp_path)
            assert result == {'key': 'value', 'list': ['item1', 'item2']}
        finally:
            os.unlink(temp_path)
    
    def test_load_yaml_relative_path(self, monkeypatch):
        """Verify relative path resolution from module directory."""
        # This test would verify the path resolution logic
        # Implementation depends on actual module structure
        pass


class TestParseResponse:
    """Tests for parse_response function."""
    
    def test_parse_response_valid_pattern(self):
        """Verify regex pattern matching extracts content correctly."""
        response = "Some text <tag>line1\nline2\nline3</tag> more text"
        pattern = r"<tag>(.*?)</tag>"
        
        result = parse_response(response, pattern)
        
        assert result == ['line1', 'line2', 'line3']
    
    def test_parse_response_no_match(self):
        """Verify empty list returned when pattern doesn't match."""
        response = "No tags here"
        pattern = r"<tag>(.*?)</tag>"
        
        result = parse_response(response, pattern)
        
        assert result == []
    
    def test_parse_response_non_string_input(self):
        """Verify empty list returned for non-string input."""
        result = parse_response(None, r"<tag>(.*?)</tag>")
        assert result == []
        
        result = parse_response(123, r"<tag>(.*?)</tag>")
        assert result == []


class TestCountTokens:
    """Tests for count_tokens function."""
    
    def test_count_tokens_empty_string(self):
        """Verify token counting returns 0 for empty string."""
        assert count_tokens("") == 0
    
    def test_count_tokens_none_input(self):
        """Verify token counting returns 0 for None input."""
        assert count_tokens(None) == 0
    
    def test_count_tokens_normal_text(self):
        """Verify token counting for normal text (~4 chars per token)."""
        text = "This is a test"  # 14 chars
        assert count_tokens(text) == 3  # 14 // 4 = 3
    
    def test_count_tokens_long_text(self):
        """Verify token counting for longer text."""
        text = "x" * 1000  # 1000 chars
        assert count_tokens(text) == 250  # 1000 // 4 = 250


class TestValidateInputLength:
    """Tests for validate_input_length function."""
    
    def test_validate_input_length_within_limit(self):
        """Verify validation passes when input is within limit."""
        validate_input_length("short text", max_tokens=100)
        # Should not raise any exception
    
    def test_validate_input_length_at_limit(self):
        """Verify validation passes when input is exactly at limit."""
        text = "x" * 400  # Exactly 100 tokens
        validate_input_length(text, max_tokens=100)
        # Should not raise any exception
    
    def test_validate_input_length_exceeds_limit(self):
        """Verify ValueError raised when input exceeds limit."""
        long_text = "x" * 1000  # ~250 tokens
        
        with pytest.raises(ValueError) as exc_info:
            validate_input_length(long_text, max_tokens=100, input_name="test_input")
        
        assert "test_input exceeds maximum token limit" in str(exc_info.value)
        assert "~250 tokens" in str(exc_info.value)
        assert "Maximum: 100 tokens" in str(exc_info.value)
    
    def test_validate_input_length_empty_string(self):
        """Verify validation passes for empty string."""
        validate_input_length("", max_tokens=100)
        # Should not raise any exception
    
    def test_validate_input_length_none_input(self):
        """Verify validation passes for None input."""
        validate_input_length(None, max_tokens=100)
        # Should not raise any exception
```

### Example 2: Fuzzy String Index Test Implementation

Complete test file for indexing/fuzzy_string.py:

```python
"""Tests for FuzzyStringIndex.

This module tests fuzzy string matching functionality including
vocabulary management, exact matching, fuzzy matching, and topk retrieval.
"""

import pytest
from graphrag_toolkit.byokg_rag.indexing.fuzzy_string import FuzzyStringIndex


class TestFuzzyStringIndexInitialization:
    """Tests for FuzzyStringIndex initialization."""
    
    def test_initialization_empty_vocab(self):
        """Verify index initializes with empty vocabulary."""
        index = FuzzyStringIndex()
        assert index.vocab == []
    
    def test_reset_clears_vocab(self):
        """Verify reset() clears the vocabulary."""
        index = FuzzyStringIndex()
        index.add(['item1', 'item2'])
        
        index.reset()
        
        assert index.vocab == []


class TestFuzzyStringIndexAdd:
    """Tests for adding vocabulary to the index."""
    
    def test_add_single_item(self):
        """Verify adding a single vocabulary item."""
        index = FuzzyStringIndex()
        index.add(['Organization'])
        
        assert 'Organization' in index.vocab
        assert len(index.vocab) == 1
    
    def test_add_multiple_items(self):
        """Verify adding multiple vocabulary items."""
        index = FuzzyStringIndex()
        index.add(['Organization', 'DataCorp', 'CloudCorp'])
        
        assert len(index.vocab) == 3
        assert all(item in index.vocab for item in ['Organization', 'DataCorp', 'CloudCorp'])
    
    def test_add_duplicate_items(self):
        """Verify duplicate items are deduplicated."""
        index = FuzzyStringIndex()
        index.add(['Organization', 'Organization', 'DataCorp'])
        
        assert len(index.vocab) == 2
        assert index.vocab.count('Organization') == 1
    
    def test_add_with_ids_not_implemented(self):
        """Verify add_with_ids raises NotImplementedError."""
        index = FuzzyStringIndex()
        
        with pytest.raises(NotImplementedError):
            index.add_with_ids(['id1'], ['Organization'])


class TestFuzzyStringIndexQuery:
    """Tests for querying the index."""
    
    def test_query_exact_match(self):
        """Verify exact string matching returns 100% match score."""
        index = FuzzyStringIndex()
        index.add(['Organization', 'DataCorp', 'CloudCorp'])
        
        result = index.query('Organization', topk=1)
        
        assert len(result['hits']) == 1
        assert result['hits'][0]['document'] == 'Organization'
        assert result['hits'][0]['match_score'] == 100
    
    def test_query_fuzzy_match(self):
        """Verify fuzzy matching handles typos."""
        index = FuzzyStringIndex()
        index.add(['Organization', 'DataCorp', 'CloudCorp'])
        
        result = index.query('Organizaton', topk=1)  # Missing 'i'
        
        assert len(result['hits']) == 1
        assert result['hits'][0]['document'] == 'Organization'
        assert result['hits'][0]['match_score'] > 80  # High but not perfect
    
    def test_query_topk_limiting(self):
        """Verify topk parameter limits results."""
        index = FuzzyStringIndex()
        index.add(['Organization', 'DataCorp', 'CloudCorp', 'WebCorp', 'AppCorp'])
        
        result = index.query('Org', topk=3)
        
        assert len(result['hits']) == 3
    
    def test_query_empty_vocab(self):
        """Verify querying empty index returns empty results."""
        index = FuzzyStringIndex()
        
        result = index.query('Organization', topk=1)
        
        assert len(result['hits']) == 0
    
    def test_query_with_id_selector_not_implemented(self):
        """Verify id_selector parameter raises NotImplementedError."""
        index = FuzzyStringIndex()
        index.add(['Organization'])
        
        with pytest.raises(NotImplementedError):
            index.query('Organization', topk=1, id_selector=['id1'])


class TestFuzzyStringIndexMatch:
    """Tests for batch matching functionality."""
    
    def test_match_multiple_inputs(self):
        """Verify batch matching of multiple queries."""
        index = FuzzyStringIndex()
        index.add(['Organization', 'DataCorp', 'CloudCorp'])
        
        result = index.match(['Organization', 'CloudCorp'], topk=1)
        
        assert len(result['hits']) == 2
        documents = [hit['document'] for hit in result['hits']]
        assert 'Organization' in documents
        assert 'CloudCorp' in documents
    
    def test_match_length_filtering(self):
        """Verify max_len_difference filters short matches."""
        index = FuzzyStringIndex()
        index.add(['Organization Solutions', 'OS', 'Organization'])
        
        # Query for long string, should filter out 'OS' (too short)
        result = index.match(['Organization Solutions'], topk=3, max_len_difference=4)
        
        documents = [hit['document'] for hit in result['hits']]
        assert 'TC' not in documents  # Too short compared to query
    
    def test_match_sorted_by_score(self):
        """Verify results are sorted by match score descending."""
        index = FuzzyStringIndex()
        index.add(['Organization', 'Organizations', 'Organize'])
        
        result = index.match(['Organization'], topk=3)
        
        scores = [hit['match_score'] for hit in result['hits']]
        assert scores == sorted(scores, reverse=True)
    
    def test_match_with_id_selector_not_implemented(self):
        """Verify id_selector parameter raises NotImplementedError."""
        index = FuzzyStringIndex()
        index.add(['Organization'])
        
        with pytest.raises(NotImplementedError):
            index.match(['Organization'], topk=1, id_selector=['id1'])
```

### Example 3: Entity Linker Test Implementation

Complete test file for graph_retrievers/entity_linker.py:

```python
"""Tests for EntityLinker.

This module tests entity linking functionality including initialization,
linking with different return formats, and error handling.
"""

import pytest
from unittest.mock import Mock
from graphrag_toolkit.byokg_rag.graph_retrievers.entity_linker import (
    EntityLinker,
    Linker
)


@pytest.fixture
def mock_retriever():
    """Fixture providing a mock entity retriever."""
    mock = Mock()
    mock.retrieve.return_value = {
        'hits': [
            {
                'document_id': 'Organization',
                'document': 'Organization',
                'match_score': 95.0
            },
            {
                'document_id': 'Microsoft',
                'document': 'Microsoft',
                'match_score': 90.0
            }
        ]
    }
    return mock


class TestEntityLinkerInitialization:
    """Tests for EntityLinker initialization."""
    
    def test_initialization_with_retriever(self, mock_retriever):
        """Verify linker initializes with retriever."""
        linker = EntityLinker(retriever=mock_retriever, topk=5)
        
        assert linker.retriever == mock_retriever
        assert linker.topk == 5
    
    def test_initialization_defaults(self):
        """Verify default topk value."""
        linker = EntityLinker()
        
        assert linker.topk == 3
        assert linker.retriever is None


class TestEntityLinkerLink:
    """Tests for entity linking functionality."""
    
    def test_link_return_dict(self, mock_retriever):
        """Verify linking returns dictionary format."""
        linker = EntityLinker(retriever=mock_retriever)
        
        result = linker.link(['tech companies'], return_dict=True)
        
        assert isinstance(result, dict)
        assert 'hits' in result
        assert len(result['hits']) == 2
    
    def test_link_return_list(self, mock_retriever):
        """Verify linking returns list of entity IDs."""
        linker = EntityLinker(retriever=mock_retriever)
        
        result = linker.link(['tech companies'], return_dict=False)
        
        assert isinstance(result, list)
        assert 'Amazon' in result
        assert 'Microsoft' in result
    
    def test_link_with_custom_topk(self, mock_retriever):
        """Verify custom topk parameter is passed to retriever."""
        linker = EntityLinker(retriever=mock_retriever, topk=3)
        
        linker.link(['query'], topk=5)
        
        mock_retriever.retrieve.assert_called_once()
        call_kwargs = mock_retriever.retrieve.call_args[1]
        assert call_kwargs['topk'] == 5
    
    def test_link_with_custom_retriever(self, mock_retriever):
        """Verify custom retriever parameter overrides instance retriever."""
        linker = EntityLinker(retriever=Mock())
        
        linker.link(['query'], retriever=mock_retriever)
        
        mock_retriever.retrieve.assert_called_once()
    
    def test_link_no_retriever_error(self):
        """Verify error when no retriever is available."""
        linker = EntityLinker()
        
        with pytest.raises(ValueError, match="Either 'retriever' or 'self.retriever' must be provided"):
            linker.link(['query'])
    
    def test_link_multiple_queries(self, mock_retriever):
        """Verify linking handles multiple query entities."""
        linker = EntityLinker(retriever=mock_retriever)
        
        result = linker.link(['Amazon', 'Microsoft', 'Google'])
        
        mock_retriever.retrieve.assert_called_once_with(
            queries=['Amazon', 'Microsoft', 'Google'],
            topk=3
        )


class TestLinkerBaseClass:
    """Tests for Linker abstract base class."""
    
    def test_linker_is_abstract(self):
        """Verify Linker is an abstract base class."""
        # Linker.link is marked as abstractmethod
        assert hasattr(Linker.link, '__isabstractmethod__')
    
    def test_linker_default_implementation(self):
        """Verify default link implementation returns empty results."""
        # Create a concrete subclass for testing
        class ConcreteLinker(Linker):
            def link(self, queries, return_dict=True, **kwargs):
                return super().link(queries, return_dict, **kwargs)
        
        linker = ConcreteLinker()
        
        result_dict = linker.link(['query'], return_dict=True)
        assert result_dict == [{'hits': [{'document_id': [], 'document': [], 'match_score': []}]}]
        
        result_list = linker.link(['query'], return_dict=False)
        assert result_list == [[]]
```

### Example 4: Query Engine Test Implementation

Partial test file for byokg_query_engine.py showing key patterns:

```python
"""Tests for ByoKGQueryEngine.

This module tests the query engine orchestration including initialization,
query processing, and response generation.
"""

import pytest
from unittest.mock import Mock, MagicMock
from graphrag_toolkit.byokg_rag.byokg_query_engine import ByoKGQueryEngine


@pytest.fixture
def mock_graph_store():
    """Fixture providing a mock graph store."""
    mock_store = Mock()
    mock_store.get_schema.return_value = {
        'node_types': ['Person', 'Organization'],
        'edge_types': ['WORKS_FOR']
    }
    mock_store.nodes.return_value = ['Organization', 'John Doe']
    return mock_store


@pytest.fixture
def mock_llm_generator():
    """Fixture providing a mock LLM generator."""
    mock_gen = Mock()
    mock_gen.generate.return_value = "<entity-extraction>Organization</entity-extraction>"
    return mock_gen


@pytest.fixture
def mock_entity_linker():
    """Fixture providing a mock entity linker."""
    mock_linker = Mock()
    mock_linker.link.return_value = ['Amazon', 'Seattle']
    return mock_linker


class TestQueryEngineInitialization:
    """Tests for query engine initialization."""
    
    def test_initialization_with_defaults(self, mock_graph_store, monkeypatch):
        """Verify query engine initializes with default components."""
        # Mock the default component creation
        monkeypatch.setattr(
            'graphrag_toolkit.byokg_rag.byokg_query_engine.BedrockGenerator',
            Mock
        )
        
        engine = ByoKGQueryEngine(graph_store=mock_graph_store)
        
        assert engine.graph_store == mock_graph_store
        assert engine.schema is not None
        assert engine.llm_generator is not None
    
    def test_initialization_with_custom_components(
        self, mock_graph_store, mock_llm_generator, mock_entity_linker
    ):
        """Verify query engine accepts custom components."""
        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store,
            llm_generator=mock_llm_generator,
            entity_linker=mock_entity_linker
        )
        
        assert engine.llm_generator == mock_llm_generator
        assert engine.entity_linker == mock_entity_linker


class TestQueryEngineQuery:
    """Tests for query processing."""
    
    def test_query_single_iteration(
        self, mock_graph_store, mock_llm_generator, mock_entity_linker
    ):
        """Verify single iteration query processing."""
        # Setup mocks
        mock_kg_linker = Mock()
        mock_kg_linker.generate_response.return_value = (
            "<entity-extraction>Amazon</entity-extraction>"
            "<task-completion>FINISH</task-completion>"
        )
        mock_kg_linker.parse_response.return_value = {
            'entity-extraction': ['Amazon']
        }
        mock_kg_linker.task_prompts = {}
        
        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store,
            llm_generator=mock_llm_generator,
            entity_linker=mock_entity_linker,
            kg_linker=mock_kg_linker
        )
        
        result = engine.query("Who founded Amazon?", iterations=1)
        
        assert isinstance(result, list)
        mock_kg_linker.generate_response.assert_called_once()
    
    def test_query_context_deduplication(
        self, mock_graph_store, mock_llm_generator
    ):
        """Verify context deduplication in _add_to_context."""
        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store,
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
        self, mock_graph_store, mock_llm_generator
    ):
        """Verify response generation with default prompt."""
        mock_llm_generator.generate.return_value = (
            "<answers>Organization was founded by John Doe</answers>"
        )
        
        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store,
            llm_generator=mock_llm_generator
        )
        
        answers, response = engine.generate_response(
            query="Who founded Organization?",
            graph_context="John Doe founded Organization"
        )
        
        assert isinstance(answers, list)
        assert isinstance(response, str)
        mock_llm_generator.generate.assert_called_once()
```

### Example 5: Bedrock LLM Test Implementation

Test file showing AWS service mocking patterns:

```python
"""Tests for BedrockGenerator.

This module tests LLM generation functionality with mocked AWS Bedrock calls.
"""

import pytest
from unittest.mock import Mock, patch
from graphrag_toolkit.byokg_rag.llm.bedrock_llms import (
    BedrockGenerator,
    generate_llm_response
)


@pytest.fixture
def mock_bedrock_client():
    """Fixture providing a mock Bedrock client."""
    mock_client = Mock()
    mock_client.converse.return_value = {
        'output': {
            'message': {
                'content': [
                    {'text': 'Mock LLM response'}
                ]
            }
        }
    }
    return mock_client


class TestBedrockGeneratorInitialization:
    """Tests for BedrockGenerator initialization."""
    
    def test_initialization_defaults(self):
        """Verify generator initializes with default parameters."""
        gen = BedrockGenerator()
        
        assert gen.model_name == "anthropic.claude-3-7-sonnet-20250219-v1:0"
        assert gen.region_name == "us-west-2"
        assert gen.max_new_tokens == 4096
        assert gen.max_retries == 10
    
    def test_initialization_custom_parameters(self):
        """Verify generator accepts custom parameters."""
        gen = BedrockGenerator(
            model_name="custom-model",
            region_name="us-east-1",
            max_tokens=2048,
            max_retries=5
        )
        
        assert gen.model_name == "custom-model"
        assert gen.region_name == "us-east-1"
        assert gen.max_new_tokens == 2048
        assert gen.max_retries == 5


class TestBedrockGeneratorGenerate:
    """Tests for text generation."""
    
    @patch('boto3.client')
    def test_generate_success(self, mock_boto3_client, mock_bedrock_client):
        """Verify successful text generation."""
        mock_boto3_client.return_value = mock_bedrock_client
        
        gen = BedrockGenerator()
        result = gen.generate(prompt="Test prompt")
        
        assert result == "Mock LLM response"
        mock_bedrock_client.converse.assert_called_once()
    
    @patch('boto3.client')
    def test_generate_with_custom_system_prompt(
        self, mock_boto3_client, mock_bedrock_client
    ):
        """Verify custom system prompt is used."""
        mock_boto3_client.return_value = mock_bedrock_client
        
        gen = BedrockGenerator()
        gen.generate(
            prompt="Test prompt",
            system_prompt="Custom system prompt"
        )
        
        call_args = mock_bedrock_client.converse.call_args[1]
        assert call_args['system'][0]['text'] == "Custom system prompt"
    
    @patch('boto3.client')
    def test_generate_retry_on_throttling(
        self, mock_boto3_client, mock_bedrock_client
    ):
        """Verify retry logic on throttling errors."""
        # First call raises throttling error, second succeeds
        mock_bedrock_client.converse.side_effect = [
            Exception("Too many requests"),
            {
                'output': {
                    'message': {
                        'content': [{'text': 'Success after retry'}]
                    }
                }
            }
        ]
        mock_boto3_client.return_value = mock_bedrock_client
        
        gen = BedrockGenerator(max_retries=2)
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = gen.generate(prompt="Test prompt")
        
        assert result == "Success after retry"
        assert mock_bedrock_client.converse.call_count == 2
    
    @patch('boto3.client')
    def test_generate_failure_after_max_retries(
        self, mock_boto3_client, mock_bedrock_client
    ):
        """Verify exception raised after max retries."""
        mock_bedrock_client.converse.side_effect = Exception("Persistent error")
        mock_boto3_client.return_value = mock_bedrock_client
        
        gen = BedrockGenerator(max_retries=2)
        
        with patch('time.sleep'):
            with pytest.raises(Exception, match="Failed due to other reasons"):
                gen.generate(prompt="Test prompt")
```


## Test Documentation Structure

The tests/README.md file provides comprehensive documentation for developers:

### README.md Content Outline

```markdown
# BYOKG-RAG Testing Guide

## Overview

This directory contains the unit test suite for the byokg-rag package. The tests verify core functionality including indexing, entity linking, graph traversal, query processing, and LLM integration.

## Prerequisites

- Python >= 3.10
- pytest >= 7.0.0
- pytest-cov >= 4.0.0
- pytest-mock >= 3.10.0

## Installation

Install test dependencies using uv:

```bash
cd byokg-rag
uv pip install pytest pytest-cov pytest-mock
```

## Running Tests

### Run all tests

```bash
pytest tests/
```

### Run specific test module

```bash
pytest tests/unit/test_utils.py
```

### Run specific test function

```bash
pytest tests/unit/test_utils.py::test_count_tokens_empty_string
```

### Run with verbose output

```bash
pytest tests/ -v
```

### Run with coverage report

```bash
pytest tests/ --cov=src/graphrag_toolkit/byokg_rag --cov-report=term-missing
```

### Generate HTML coverage report

```bash
pytest tests/ --cov=src/graphrag_toolkit/byokg_rag --cov-report=html
open htmlcov/index.html
```

## Test Structure

Tests mirror the source code structure:

```
tests/
├── conftest.py              # Shared fixtures
├── README.md                # This file
└── unit/
    ├── test_utils.py
    ├── test_byokg_query_engine.py
    ├── indexing/
    ├── graph_retrievers/
    ├── graph_connectors/
    ├── graphstore/
    └── llm/
```

## Fixture Architecture

### Core Fixtures (conftest.py)

- `mock_bedrock_generator`: Mock LLM client for testing without AWS calls
- `mock_graph_store`: Mock graph store with sample schema and data
- `sample_queries`: Representative query strings for testing
- `sample_graph_data`: Sample graph structures (nodes, edges, paths)

### Using Fixtures

```python
def test_example(mock_bedrock_generator, sample_queries):
    """Example test using fixtures."""
    result = mock_bedrock_generator.generate(prompt=sample_queries[0])
    assert isinstance(result, str)
```

## Mocking AWS Services

### Mocking Bedrock LLM Calls

```python
from unittest.mock import Mock, patch

@patch('boto3.client')
def test_with_mocked_bedrock(mock_boto3_client):
    """Test with mocked Bedrock client."""
    mock_client = Mock()
    mock_client.converse.return_value = {
        'output': {
            'message': {
                'content': [{'text': 'Mock response'}]
            }
        }
    }
    mock_boto3_client.return_value = mock_client
    
    # Your test code here
```

### Mocking Neptune Graph Queries

```python
@patch('boto3.client')
def test_with_mocked_neptune(mock_boto3_client):
    """Test with mocked Neptune client."""
    mock_client = Mock()
    mock_client.execute_query.return_value = {
        'results': [{'id': 'n1', 'label': 'Person'}]
    }
    mock_boto3_client.return_value = mock_client
    
    # Your test code here
```

## Writing New Tests

### Test Naming Convention

Follow the pattern: `test_<function_name>_<scenario>`

```python
def test_count_tokens_empty_string():
    """Verify token counting returns 0 for empty string."""
    pass

def test_count_tokens_normal_text():
    """Verify token counting for normal text."""
    pass
```

### Test Structure

Each test should:

1. Have a descriptive docstring
2. Test one logical behavior
3. Use clear assertions
4. Avoid external dependencies

```python
def test_example_function():
    """
    Verify example_function returns expected result.
    
    This test verifies that when given valid input, the function
    processes it correctly and returns the expected output format.
    """
    # Arrange
    input_data = "test input"
    
    # Act
    result = example_function(input_data)
    
    # Assert
    assert result == "expected output"
```

### Testing Error Conditions

```python
def test_function_raises_error_on_invalid_input():
    """Verify ValueError raised for invalid input."""
    with pytest.raises(ValueError, match="expected error message"):
        function_with_validation("invalid input")
```

## Coverage Targets

| Module Type | Target Coverage |
|-------------|----------------|
| Utility modules | 70% |
| Indexing modules | 60% |
| Graph retrievers | 60% |
| LLM integration | 50% |
| Graph stores | 50% |

## Debugging Test Failures

### Run with detailed output

```bash
pytest tests/ -vv --tb=long
```

### Run specific failing test

```bash
pytest tests/unit/test_utils.py::test_failing_test -vv
```

### Use pytest debugger

```bash
pytest tests/ --pdb
```

### Print debugging

```python
def test_with_debugging():
    """Test with print statements."""
    result = function_under_test()
    print(f"Result: {result}")  # Will show in pytest output with -s flag
    assert result == expected
```

Run with: `pytest tests/ -s`

## Continuous Integration

Tests run automatically on:

- Push to main branch (when byokg-rag files change)
- Pull requests to main branch
- Python versions: 3.10, 3.11, 3.12

See `.github/workflows/byokg-rag-tests.yml` for CI configuration.

## Test Maintenance

### When to Update Tests

- **Bug fixes**: Add regression test for the bug
- **New features**: Add tests for new functionality
- **Refactoring**: Update tests if interfaces change
- **API changes**: Update mocks to match new AWS API responses

### Handling Flaky Tests

If a test fails intermittently:

1. Investigate the root cause (timing, randomness, external dependencies)
2. Add appropriate mocking or fixtures
3. Increase test isolation
4. Document the issue if it can't be immediately fixed

### Adding Tests for New Modules

1. Create test file: `tests/unit/test_<module_name>.py`
2. Import the module under test
3. Create test class: `class Test<ClassName>`
4. Write test functions: `def test_<function>_<scenario>()`
5. Add fixtures to conftest.py if needed
6. Run tests and verify coverage

## Common Issues

### Import Errors

Ensure PYTHONPATH includes src directory:

```bash
PYTHONPATH=src pytest tests/
```

### AWS Credential Errors

Tests should never require real AWS credentials. If you see credential errors:

1. Check that boto3.client is properly mocked
2. Verify the test uses fixtures from conftest.py
3. Add `@patch('boto3.client')` decorator if needed

### Fixture Not Found

If pytest can't find a fixture:

1. Check fixture is defined in conftest.py or test file
2. Verify fixture name matches parameter name
3. Ensure conftest.py is in the correct directory

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
- [unittest.mock documentation](https://docs.python.org/3/library/unittest.mock.html)
- [GraphRAG Toolkit documentation](../../docs/byokg-rag/)
```

## Implementation Notes

### Phase 1: Directory Structure and Configuration

1. Create test directory structure
2. Add test dependencies to pyproject.toml
3. Create conftest.py with base fixtures
4. Create pytest.ini or add pytest configuration to pyproject.toml
5. Create tests/README.md

### Phase 2: Core Module Tests

1. Implement tests for utils.py (highest priority, simplest module)
2. Implement tests for indexing modules (fuzzy_string, dense_index, graph_store_index)
3. Implement tests for graph_retrievers (entity_linker, graph_traversal, graph_verbalizer)
4. Implement tests for byokg_query_engine.py

### Phase 3: Integration and AWS Service Tests

1. Implement tests for llm/bedrock_llms.py with mocked boto3
2. Implement tests for graphstore/neptune.py with mocked boto3
3. Implement tests for graph_connectors/kg_linker.py

### Phase 4: CI/CD Integration

1. Create .github/workflows/byokg-rag-tests.yml
2. Test workflow on feature branch
3. Verify coverage reporting works
4. Verify multi-Python version testing works

### Phase 5: Documentation and Refinement

1. Complete tests/README.md with all sections
2. Add inline documentation to complex test fixtures
3. Review coverage reports and add tests for uncovered critical paths
4. Document any known limitations or edge cases

### Implementation Priorities

High Priority (Must Have):
- Test directory structure
- Core fixtures (mock_bedrock_generator, mock_graph_store)
- Tests for utils.py
- Tests for fuzzy_string.py
- Tests for entity_linker.py
- CI/CD workflow
- Basic README.md

Medium Priority (Should Have):
- Tests for dense_index.py
- Tests for graph_traversal.py
- Tests for byokg_query_engine.py
- Tests for bedrock_llms.py
- Comprehensive README.md
- Coverage configuration

Lower Priority (Nice to Have):
- Tests for graph_verbalizer.py
- Tests for graph_reranker.py
- Tests for neptune.py
- Tests for kg_linker.py
- Advanced fixtures for complex scenarios

### Testing Best Practices

1. **Isolation**: Each test should be independent and not rely on other tests
2. **Clarity**: Test names and docstrings should clearly describe what is being tested
3. **Simplicity**: Tests should be simple and focused on one behavior
4. **Speed**: Tests should run quickly (< 1 second per test typically)
5. **Reliability**: Tests should not be flaky or dependent on external factors
6. **Maintainability**: Tests should be easy to update when code changes

### Common Patterns

#### Pattern 1: Testing Functions with External Dependencies

```python
@patch('module.external_dependency')
def test_function_with_dependency(mock_dependency):
    """Test function that calls external dependency."""
    mock_dependency.return_value = "mocked result"
    
    result = function_under_test()
    
    assert result == "expected result"
    mock_dependency.assert_called_once()
```

#### Pattern 2: Testing Error Handling

```python
def test_function_handles_error():
    """Verify function handles errors gracefully."""
    with pytest.raises(SpecificException, match="error message pattern"):
        function_that_should_raise()
```

#### Pattern 3: Parametrized Tests

```python
@pytest.mark.parametrize("input,expected", [
    ("input1", "output1"),
    ("input2", "output2"),
    ("input3", "output3"),
])
def test_function_with_multiple_inputs(input, expected):
    """Test function with various inputs."""
    assert function_under_test(input) == expected
```

#### Pattern 4: Testing Async Functions

```python
@pytest.mark.asyncio
async def test_async_function():
    """Test asynchronous function."""
    result = await async_function()
    assert result == expected
```

### Coverage Analysis Strategy

After implementing tests, analyze coverage to identify:

1. **Critical uncovered paths**: Functions that handle important logic but lack tests
2. **Error handling gaps**: Exception handling code that isn't exercised
3. **Edge cases**: Boundary conditions that aren't tested
4. **Dead code**: Code that is never executed (candidate for removal)

Use coverage reports to guide additional test development:

```bash
# Generate detailed coverage report
pytest tests/ --cov=src/graphrag_toolkit/byokg_rag --cov-report=html

# Open report and identify gaps
open htmlcov/index.html
```

### Performance Considerations

The test suite should complete quickly to support rapid development:

- Target: < 60 seconds for full test suite
- Individual tests: < 1 second each
- Use mocks to avoid slow I/O operations
- Avoid unnecessary setup/teardown
- Consider pytest-xdist for parallel execution if needed

### Future Enhancements

Potential future improvements to the testing infrastructure:

1. **Integration tests**: Tests that verify component interactions with real (local) services
2. **Performance tests**: Tests that measure execution time and resource usage
3. **Property-based tests**: Tests using hypothesis library for generative testing
4. **Mutation testing**: Tests using mutmut to verify test quality
5. **Contract tests**: Tests that verify API contracts between components
6. **Snapshot tests**: Tests that compare output against saved snapshots

These enhancements are out of scope for the initial implementation but may be valuable as the codebase matures.

