# Implementation Plan: BYOKG-RAG Unit Testing Infrastructure

## Overview

This implementation plan creates comprehensive unit testing infrastructure for the byokg-rag module, replicating the proven testing patterns from lexical-graph. The plan follows a five-phase approach: directory structure setup, core module tests, integration tests, CI/CD configuration, and documentation.

## Tasks

- [x] 1. Set up test directory structure and configuration
  - Create `byokg-rag/tests/` directory with subdirectories mirroring source structure
  - Create `byokg-rag/tests/conftest.py` for shared fixtures
  - Create `byokg-rag/tests/unit/` directory with `__init__.py`
  - Create subdirectories: `tests/unit/indexing/`, `tests/unit/graph_retrievers/`, `tests/unit/graph_connectors/`, `tests/unit/graphstore/`, `tests/unit/llm/`
  - Add `__init__.py` files to all test subdirectories
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [x] 2. Configure test dependencies and pytest settings
  - Add pytest, pytest-cov, and pytest-mock to test dependencies
  - Configure pytest settings in pyproject.toml (test paths, coverage options, addopts)
  - Configure coverage settings (source paths, omit patterns, exclude_lines)
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 5.1, 5.2, 5.3, 5.4, 5.5_

- [x] 3. Create core test fixtures in conftest.py
  - [x] 3.1 Implement mock_bedrock_generator fixture
    - Create fixture that returns Mock BedrockGenerator with deterministic responses
    - Configure mock to return "Mock LLM response" for generate() calls
    - Set model_name and region_name attributes
    - _Requirements: 4.1, 12.1_
  
  - [x] 3.2 Implement mock_graph_store fixture
    - Create fixture that returns Mock graph store with sample schema
    - Configure get_schema() to return node_types and edge_types
    - Configure nodes() to return sample node list
    - _Requirements: 4.2, 12.2_
  
  - [x] 3.3 Implement sample_queries fixture
    - Create fixture returning list of representative query strings
    - Include queries covering different patterns (who, where, what)
    - _Requirements: 4.3_
  
  - [x] 3.4 Implement sample_graph_data fixture
    - Create fixture returning dictionary with nodes, edges, and paths
    - Include sample Person, Organization, and Location nodes
    - Include sample FOUNDED and LOCATED_IN edges
    - _Requirements: 4.4_
  
  - [x] 3.5 Implement block_aws_calls autouse fixture
    - Create autouse fixture that blocks real AWS API calls
    - Monkeypatch boto3.client to raise RuntimeError
    - Ensure tests remain isolated and fast
    - _Requirements: 3.7, 12.4, 13.2_

- [x] 4. Implement utils module tests
  - [x] 4.1 Create tests/unit/test_utils.py
    - Write test_load_yaml_valid_file
    - Write test_load_yaml_relative_path
    - Write test_parse_response_valid_pattern
    - Write test_parse_response_no_match
    - Write test_parse_response_non_string_input
    - Write test_count_tokens_empty_string
    - Write test_count_tokens_none_input
    - Write test_count_tokens_normal_text
    - Write test_count_tokens_long_text
    - Write test_validate_input_length_within_limit
    - Write test_validate_input_length_at_limit
    - Write test_validate_input_length_exceeds_limit
    - Write test_validate_input_length_empty_string
    - Write test_validate_input_length_none_input
    - _Requirements: 3.1, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 11.1_

- [x] 5. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement indexing module tests
  - [x] 6.1 Create tests/unit/indexing/test_fuzzy_string.py
    - Write test_initialization_empty_vocab
    - Write test_reset_clears_vocab
    - Write test_add_single_item
    - Write test_add_multiple_items
    - Write test_add_duplicate_items
    - Write test_add_with_ids_not_implemented
    - Write test_query_exact_match
    - Write test_query_fuzzy_match
    - Write test_query_topk_limiting
    - Write test_query_empty_vocab
    - Write test_query_with_id_selector_not_implemented
    - Write test_match_multiple_inputs
    - Write test_match_length_filtering
    - Write test_match_sorted_by_score
    - Write test_match_with_id_selector_not_implemented
    - _Requirements: 3.2, 9.2, 11.2_
  
  - [x] 6.2 Create tests/unit/indexing/test_dense_index.py
    - Write test_dense_index_creation
    - Write test_dense_index_add_embeddings
    - Write test_dense_index_query_similarity
    - Write test_dense_index_query_with_mock_llm
    - _Requirements: 3.2, 9.4, 11.2_
  
  - [x] 6.3 Create tests/unit/indexing/test_graph_store_index.py
    - Write test_graph_store_index_initialization
    - Write test_graph_store_index_query
    - _Requirements: 3.2, 9.3, 11.2_

- [x] 7. Implement graph retriever module tests
  - [x] 7.1 Create tests/unit/graph_retrievers/test_entity_linker.py
    - Create mock_retriever fixture
    - Write test_initialization_with_retriever
    - Write test_initialization_defaults
    - Write test_link_return_dict
    - Write test_link_return_list
    - Write test_link_with_custom_topk
    - Write test_link_with_custom_retriever
    - Write test_link_no_retriever_error
    - Write test_link_multiple_queries
    - Write test_linker_is_abstract
    - Write test_linker_default_implementation
    - _Requirements: 3.3, 10.1, 11.3_
  
  - [x] 7.2 Create tests/unit/graph_retrievers/test_graph_traversal.py
    - Write test_graph_traversal_initialization
    - Write test_graph_traversal_single_hop
    - Write test_graph_traversal_multi_hop
    - Write test_graph_traversal_with_metapath
    - _Requirements: 3.3, 10.2, 11.3_
  
  - [x] 7.3 Create tests/unit/graph_retrievers/test_graph_verbalizer.py
    - Write test_triplet_verbalizer_format
    - Write test_path_verbalizer_format
    - Write test_verbalizer_empty_input
    - _Requirements: 3.3, 10.4, 11.3_
  
  - [x] 7.4 Create tests/unit/graph_retrievers/test_graph_reranker.py
    - Write tests for graph reranking logic with sample results
    - _Requirements: 3.3, 10.3, 11.3_

- [x] 8. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Implement query engine tests
  - [x] 9.1 Create tests/unit/test_byokg_query_engine.py
    - Create mock_graph_store, mock_llm_generator, mock_entity_linker fixtures
    - Write test_initialization_with_defaults
    - Write test_initialization_with_custom_components
    - Write test_query_single_iteration
    - Write test_query_context_deduplication
    - Write test_generate_response_default_prompt
    - _Requirements: 3.4, 11.3_

- [x] 10. Implement LLM integration tests
  - [x] 10.1 Create tests/unit/llm/test_bedrock_llms.py
    - Create mock_bedrock_client fixture
    - Write test_initialization_defaults
    - Write test_initialization_custom_parameters
    - Write test_generate_success with @patch('boto3.client')
    - Write test_generate_with_custom_system_prompt
    - Write test_generate_retry_on_throttling
    - Write test_generate_failure_after_max_retries
    - _Requirements: 3.5, 3.6, 12.1, 12.4, 11.4_

- [x] 11. Implement graph store tests
  - [x] 11.1 Create tests/unit/graphstore/test_neptune.py
    - Write test_neptune_store_initialization with mocked boto3
    - Write test_neptune_store_get_schema
    - Write test_neptune_store_execute_query with mocked responses
    - _Requirements: 3.6, 12.2, 12.4, 11.4_

- [x] 12. Implement graph connector tests
  - [x] 12.1 Create tests/unit/graph_connectors/test_kg_linker.py
    - Write tests for KG linker functionality
    - _Requirements: 3.6, 11.4_

- [x] 13. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 14. Create CI/CD workflow configuration
  - [x] 14.1 Create .github/workflows/byokg-rag-tests.yml
    - Configure workflow to trigger on push to main (byokg-rag paths)
    - Configure workflow to trigger on pull requests to main (byokg-rag paths)
    - Set up matrix strategy for Python 3.10, 3.11, 3.12
    - Set working-directory to byokg-rag
    - Add checkout step
    - Add Python setup step with matrix version
    - Add uv installation step
    - Add virtual environment creation step
    - Add dependencies installation step (pytest, pytest-cov, pytest-mock, requirements.txt)
    - Add test execution step with coverage (PYTHONPATH=src)
    - Add coverage report upload step (Python 3.12 only)
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 13.1_

- [x] 15. Create comprehensive test documentation
  - [x] 15.1 Create tests/README.md
    - Write Overview section describing test suite purpose
    - Write Prerequisites section listing Python and package requirements
    - Write Installation section with uv pip install commands
    - Write Running Tests section with examples (all tests, specific module, specific function, verbose, coverage, HTML report)
    - Write Test Structure section showing directory layout
    - Write Fixture Architecture section documenting core fixtures and usage
    - Write Mocking AWS Services section with Bedrock and Neptune examples
    - Write Writing New Tests section with naming conventions, structure, and error testing patterns
    - Write Coverage Targets table
    - Write Debugging Test Failures section with commands
    - Write Continuous Integration section referencing workflow file
    - Write Test Maintenance section (when to update, handling flaky tests, adding tests for new modules)
    - Write Common Issues section (import errors, AWS credential errors, fixture not found)
    - Write Resources section with links to pytest, pytest-cov, unittest.mock, and GraphRAG Toolkit docs
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 12.5, 14.1, 14.2, 14.3, 14.4_

- [x] 16. Final checkpoint - Verify complete test infrastructure
  - Run full test suite and verify all tests pass
  - Generate coverage report and verify coverage targets are met
  - Verify CI/CD workflow configuration is valid
  - Review documentation for completeness
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- This workflow creates testing infrastructure artifacts only; implementation of the byokg-rag system itself is not part of this workflow
- Tests use mocked AWS services (Bedrock, Neptune) to avoid requiring credentials or network access
- Coverage targets vary by module complexity: 70% for utils, 60% for indexing/retrievers, 50% for integration modules
- All tests should complete in under 60 seconds to support rapid development
- Test naming follows the pattern: `test_<function_name>_<scenario>`
- Each test includes a docstring explaining what it verifies
- Fixtures are organized in three tiers: base fixtures (conftest.py), module fixtures, and parametrized fixtures
