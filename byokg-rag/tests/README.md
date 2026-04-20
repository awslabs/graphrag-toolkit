# BYOKG-RAG Testing Guide

## Overview

This directory contains the unit test suite for the byokg-rag package. The tests verify core functionality including indexing, entity linking, graph traversal, query processing, and LLM integration. All tests use mocked AWS services to ensure fast, isolated execution without requiring credentials or network access.

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

### Exclude specific tests

To exclude tests that may cause issues (e.g., FAISS-related tests on certain platforms):

```bash
pytest tests/ -k "not faiss"
```

NOTE: Some FAISS-based tests may cause segmentation faults on certain platforms or Python versions. If you encounter segfaults when running the dense index tests, use the command above to exclude them.

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
    │   ├── test_dense_index.py
    │   ├── test_fuzzy_string.py
    │   └── test_graph_store_index.py
    ├── graph_retrievers/
    │   ├── test_entity_linker.py
    │   ├── test_graph_traversal.py
    │   ├── test_graph_reranker.py
    │   └── test_graph_verbalizer.py
    ├── graph_connectors/
    │   └── test_kg_linker.py
    ├── graphstore/
    │   └── test_neptune.py
    └── llm/
        └── test_bedrock_llms.py
```

## Fixture Architecture

### Core Fixtures (conftest.py)

The following fixtures are available to all tests:

- `mock_bedrock_generator`: Mock LLM client for testing without AWS calls
- `mock_graph_store`: Mock graph store with sample schema and data
- `sample_queries`: Representative query strings for testing
- `sample_graph_data`: Sample graph structures (nodes, edges, paths)
- `block_aws_calls`: Autouse fixture that prevents real AWS API calls

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

Current overall coverage: 94%

| Module | Coverage |
|--------|----------|
| utils.py | 100% |
| fuzzy_string.py | 100% |
| graph_store_index.py | 100% |
| index.py | 100% |
| graphstore.py | 100% |
| kg_linker.py | 100% |
| entity_linker.py | 100% |
| embedding.py | 100% |
| graph_reranker.py | 100% |
| graph_verbalizer.py | 99% |
| graph_traversal.py | 95% |
| graph_retrievers.py | 93% |
| bedrock_llms.py | 92% |
| dense_index.py | 91% |
| neptune.py | 91% |
| byokg_query_engine.py | 87% |

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

## Using AI Agents for Test Development

This test suite was developed using AI-assisted spec-driven development. You can use the same approach to maintain and extend tests.

### Spec-Driven Test Development

The test infrastructure was created following a structured spec workflow documented in `.kiro/specs/byokg-rag-unit-testing/`:

- `requirements.md` - Test requirements and acceptance criteria
- `design.md` - Test architecture and implementation approach
- `tasks.md` - Implementation tasks and progress tracking

### Creating New Tests with AI Agents

To add tests for new modules or features:

1. Create a new spec or update the existing one:
   ```bash
   # Ask your AI agent to create a spec for new test requirements
   "Create a spec for adding tests to the new <module_name> module"
   ```

2. The agent will guide you through:
   - Defining test requirements
   - Designing test structure and fixtures
   - Creating implementation tasks
   - Executing the tasks

3. Review and iterate on the generated tests

### Updating Existing Tests

To update tests when code changes:

1. Reference the existing spec:
   ```bash
   # Ask your AI agent to update tests
   "Update tests in .kiro/specs/byokg-rag-unit-testing to cover the new <feature>"
   ```

2. The agent will:
   - Analyze the existing test structure
   - Identify gaps in coverage
   - Generate new test cases
   - Update fixtures if needed

### Benefits of Spec-Driven Testing

- Systematic test coverage planning
- Clear documentation of test requirements
- Traceable implementation progress
- Consistent test structure and patterns
- Easy onboarding for new contributors

### Example Workflow

```bash
# 1. Create spec for new feature tests
"I need to add tests for the new graph_optimizer module"

# 2. Agent creates spec with requirements and design

# 3. Review and approve the plan

# 4. Execute implementation
"Run all tasks in the spec"

# 5. Verify coverage
pytest tests/ --cov=src/graphrag_toolkit/byokg_rag/graph_optimizer
```

TIP: Keep specs updated as tests evolve. They serve as living documentation for your test strategy.

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
4. Document the issue if it cannot be immediately fixed

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

If pytest cannot find a fixture:

1. Check fixture is defined in conftest.py or test file
2. Verify fixture name matches parameter name
3. Ensure conftest.py is in the correct directory

### FAISS Segmentation Faults

NOTE: FAISS-based tests in `tests/unit/indexing/test_dense_index.py` may cause segmentation faults on certain platforms or Python versions. This is a known issue with the FAISS library.

If you encounter segfaults:

1. Exclude FAISS tests: `pytest tests/ -k "not faiss"`
2. Run other tests normally
3. Report the issue with your platform and Python version details

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
- [unittest.mock documentation](https://docs.python.org/3/library/unittest.mock.html)
- [GraphRAG Toolkit documentation](../../docs/byokg-rag/)
