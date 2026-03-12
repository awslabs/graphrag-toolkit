"""
Property-based tests for StreamingJSONLReaderProvider.

Uses hypothesis library to verify correctness properties across randomly generated inputs.
Each property test runs a minimum of 100 iterations.
"""

import json
import math
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from unittest.mock import MagicMock

import pytest
from hypothesis import given, settings, strategies as st

# Mock the problematic imports before importing the modules
# This prevents the import chain from triggering missing optional dependencies
sys.modules['fitz'] = MagicMock()

# Now we can import normally
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import (
    StreamingJSONLReaderConfig,
)
from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.streaming_jsonl_reader_provider import (
    StreamingJSONLReaderProvider,
)


# =============================================================================
# Property 1: Batch Size Consistency
# Feature: streaming-jsonl-reader, Property 1: Batch Size Consistency
# Validates: Requirements 2.2, 2.3, 2.4, 4.3
# =============================================================================

@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=500),
    batch_size=st.integers(min_value=1, max_value=100)
)
def test_batch_size_consistency(num_lines: int, batch_size: int):
    """
    Property 1: Batch Size Consistency
    
    For any valid JSONL file with N valid lines and any batch_size B > 0,
    lazy_load_data() shall yield exactly ceil(N/B) batches, where each batch
    except possibly the last contains exactly B documents, and the last batch
    contains N mod B documents (or B if N is divisible by B).
    
    Validates: Requirements 2.2, 2.3, 2.4, 4.3
    """
    # Generate JSONL file using tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        lines = [json.dumps({"text": f"line {i}"}) for i in range(num_lines)]
        f.write("\n".join(lines))
        jsonl_file = f.name
    
    try:
        # Configure reader
        config = StreamingJSONLReaderConfig(batch_size=batch_size)
        reader = StreamingJSONLReaderProvider(config)
        
        # Collect batches
        batches = list(reader.lazy_load_data(jsonl_file))
        
        # Verify batch count
        expected_batches = math.ceil(num_lines / batch_size)
        assert len(batches) == expected_batches, (
            f"Expected {expected_batches} batches, got {len(batches)} "
            f"(num_lines={num_lines}, batch_size={batch_size})"
        )
        
        # Verify all batches except last have exactly batch_size documents
        for i, batch in enumerate(batches[:-1]):
            assert len(batch) == batch_size, (
                f"Batch {i} has {len(batch)} documents, expected {batch_size}"
            )
        
        # Verify final batch size
        expected_final = num_lines % batch_size
        if expected_final == 0:
            expected_final = batch_size
        assert len(batches[-1]) == expected_final, (
            f"Final batch has {len(batches[-1])} documents, expected {expected_final}"
        )
    finally:
        os.unlink(jsonl_file)


# =============================================================================
# Property 2: Read Returns All Valid Documents
# Feature: streaming-jsonl-reader, Property 2: Read Returns All Valid Documents
# Validates: Requirements 4.2, 9.3
# =============================================================================

@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=500),
    batch_size=st.integers(min_value=1, max_value=100)
)
def test_read_returns_all_valid_documents(num_lines: int, batch_size: int):
    """
    Property 2: Read Returns All Valid Documents
    
    For any valid JSONL file with N valid lines, read() shall return a
    List[Document] containing exactly N documents, one for each valid
    JSON line in the file.
    
    Validates: Requirements 4.2, 9.3
    """
    # Generate JSONL file using tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        lines = [json.dumps({"text": f"line {i}"}) for i in range(num_lines)]
        f.write("\n".join(lines))
        jsonl_file = f.name
    
    try:
        # Configure reader
        config = StreamingJSONLReaderConfig(batch_size=batch_size)
        reader = StreamingJSONLReaderProvider(config)
        
        # Read all documents
        documents = reader.read(jsonl_file)
        
        # Verify document count
        assert len(documents) == num_lines, (
            f"Expected {num_lines} documents, got {len(documents)}"
        )
        
        # Verify each document has correct text
        for i, doc in enumerate(documents):
            expected_text = f"line {i}"
            assert doc.text == expected_text, (
                f"Document {i} has text '{doc.text}', expected '{expected_text}'"
            )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=200),
    batch_size=st.integers(min_value=1, max_value=50)
)
def test_lazy_load_and_read_return_same_documents(num_lines: int, batch_size: int):
    """
    Verify that lazy_load_data and read return the same documents.
    
    This ensures consistency between the streaming and batch interfaces.
    """
    # Generate JSONL file using tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        lines = [json.dumps({"text": f"line {i}"}) for i in range(num_lines)]
        f.write("\n".join(lines))
        jsonl_file = f.name
    
    try:
        # Configure reader
        config = StreamingJSONLReaderConfig(batch_size=batch_size)
        reader = StreamingJSONLReaderProvider(config)
        
        # Get documents from both methods
        lazy_docs = []
        for batch in reader.lazy_load_data(jsonl_file):
            lazy_docs.extend(batch)
        
        read_docs = reader.read(jsonl_file)
        
        # Verify same count
        assert len(lazy_docs) == len(read_docs), (
            f"lazy_load_data returned {len(lazy_docs)} docs, "
            f"read returned {len(read_docs)} docs"
        )
        
        # Verify same content
        for i, (lazy_doc, read_doc) in enumerate(zip(lazy_docs, read_docs)):
            assert lazy_doc.text == read_doc.text, (
                f"Document {i} text mismatch: lazy='{lazy_doc.text}', read='{read_doc.text}'"
            )
    finally:
        os.unlink(jsonl_file)


# =============================================================================
# Property 3: Metadata Function Integration
# Feature: streaming-jsonl-reader, Property 3: Metadata Function Integration
# Validates: Requirements 5.2, 5.3
# =============================================================================

@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=100),
    custom_key=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('L', 'N'))),
    custom_value=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('L', 'N', 'P', 'S')))
)
def test_metadata_function_integration(num_lines: int, custom_key: str, custom_value: str):
    """
    Property 3: Metadata Function Integration
    
    For any JSONL file and any metadata_fn that returns a dictionary,
    all documents returned by read() shall contain all key-value pairs
    from metadata_fn(source_path) in their metadata.
    
    Validates: Requirements 5.2, 5.3
    """
    # Skip if custom_key would conflict with built-in metadata keys
    reserved_keys = {"file_path", "source", "line_number", "document_type"}
    if custom_key in reserved_keys:
        return  # Skip this test case
    
    # Track metadata_fn calls
    metadata_fn_calls = []
    
    def custom_metadata_fn(source_path: str) -> Dict[str, Any]:
        metadata_fn_calls.append(source_path)
        return {custom_key: custom_value, "extra_field": "extra_value"}
    
    # Generate JSONL file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        lines = [json.dumps({"text": f"line {i}"}) for i in range(num_lines)]
        f.write("\n".join(lines))
        jsonl_file = f.name
    
    try:
        # Configure reader with metadata_fn
        config = StreamingJSONLReaderConfig(metadata_fn=custom_metadata_fn)
        reader = StreamingJSONLReaderProvider(config)
        
        # Read all documents
        documents = reader.read(jsonl_file)
        
        # Verify metadata_fn was called for each document
        assert len(metadata_fn_calls) == num_lines, (
            f"metadata_fn called {len(metadata_fn_calls)} times, expected {num_lines}"
        )
        
        # Verify all documents contain the custom metadata
        for i, doc in enumerate(documents):
            assert custom_key in doc.metadata, (
                f"Document {i} missing custom key '{custom_key}' in metadata"
            )
            assert doc.metadata[custom_key] == custom_value, (
                f"Document {i} has wrong value for '{custom_key}': "
                f"got '{doc.metadata[custom_key]}', expected '{custom_value}'"
            )
            assert "extra_field" in doc.metadata, (
                f"Document {i} missing 'extra_field' in metadata"
            )
            assert doc.metadata["extra_field"] == "extra_value", (
                f"Document {i} has wrong value for 'extra_field'"
            )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=50)
)
def test_metadata_fn_receives_correct_source_path(num_lines: int):
    """
    Verify that metadata_fn receives the correct source path.
    
    The source path passed to metadata_fn should be the original input path,
    not a temporary file path (for S3 files).
    """
    received_paths = []
    
    def tracking_metadata_fn(source_path: str) -> Dict[str, Any]:
        received_paths.append(source_path)
        return {"tracked": True}
    
    # Generate JSONL file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        lines = [json.dumps({"text": f"line {i}"}) for i in range(num_lines)]
        f.write("\n".join(lines))
        jsonl_file = f.name
    
    try:
        config = StreamingJSONLReaderConfig(metadata_fn=tracking_metadata_fn)
        reader = StreamingJSONLReaderProvider(config)
        
        documents = reader.read(jsonl_file)
        
        # All calls should receive the same source path
        assert len(received_paths) == num_lines
        for path in received_paths:
            assert path == jsonl_file, (
                f"metadata_fn received path '{path}', expected '{jsonl_file}'"
            )
    finally:
        os.unlink(jsonl_file)


# =============================================================================
# Property 4: Required Metadata Fields Present
# Feature: streaming-jsonl-reader, Property 4: Required Metadata Fields Present
# Validates: Requirements 5.4, 5.5
# =============================================================================

@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=100),
    batch_size=st.integers(min_value=1, max_value=50)
)
def test_required_metadata_fields_present(num_lines: int, batch_size: int):
    """
    Property 4: Required Metadata Fields Present
    
    For any document returned by the reader, the document's metadata shall contain:
    - source: either "s3" or "local_file" depending on input path type
    - line_number: a positive integer representing the 1-based line index
    - file_path: the original source path
    
    Validates: Requirements 5.4, 5.5
    """
    # Generate JSONL file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        lines = [json.dumps({"text": f"line {i}"}) for i in range(num_lines)]
        f.write("\n".join(lines))
        jsonl_file = f.name
    
    try:
        config = StreamingJSONLReaderConfig(batch_size=batch_size)
        reader = StreamingJSONLReaderProvider(config)
        
        documents = reader.read(jsonl_file)
        
        for i, doc in enumerate(documents):
            # Verify 'source' field
            assert "source" in doc.metadata, (
                f"Document {i} missing 'source' in metadata"
            )
            assert doc.metadata["source"] in ("s3", "local_file"), (
                f"Document {i} has invalid source: '{doc.metadata['source']}'"
            )
            # For local files, source should be 'local_file'
            assert doc.metadata["source"] == "local_file", (
                f"Document {i} should have source='local_file' for local file"
            )
            
            # Verify 'line_number' field
            assert "line_number" in doc.metadata, (
                f"Document {i} missing 'line_number' in metadata"
            )
            assert isinstance(doc.metadata["line_number"], int), (
                f"Document {i} line_number is not an int: {type(doc.metadata['line_number'])}"
            )
            assert doc.metadata["line_number"] > 0, (
                f"Document {i} has non-positive line_number: {doc.metadata['line_number']}"
            )
            # Line numbers should be 1-based and sequential
            expected_line = i + 1
            assert doc.metadata["line_number"] == expected_line, (
                f"Document {i} has line_number {doc.metadata['line_number']}, expected {expected_line}"
            )
            
            # Verify 'file_path' field
            assert "file_path" in doc.metadata, (
                f"Document {i} missing 'file_path' in metadata"
            )
            assert doc.metadata["file_path"] == jsonl_file, (
                f"Document {i} has wrong file_path: '{doc.metadata['file_path']}'"
            )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=50)
)
def test_metadata_fields_present_in_lazy_load(num_lines: int):
    """
    Verify required metadata fields are present when using lazy_load_data.
    
    This ensures the streaming interface also provides complete metadata.
    """
    # Generate JSONL file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        lines = [json.dumps({"text": f"line {i}"}) for i in range(num_lines)]
        f.write("\n".join(lines))
        jsonl_file = f.name
    
    try:
        config = StreamingJSONLReaderConfig(batch_size=10)
        reader = StreamingJSONLReaderProvider(config)
        
        doc_index = 0
        for batch in reader.lazy_load_data(jsonl_file):
            for doc in batch:
                # Verify all required fields
                assert "source" in doc.metadata
                assert "line_number" in doc.metadata
                assert "file_path" in doc.metadata
                assert "document_type" in doc.metadata
                
                # Verify values
                assert doc.metadata["source"] == "local_file"
                assert doc.metadata["line_number"] == doc_index + 1
                assert doc.metadata["file_path"] == jsonl_file
                assert doc.metadata["document_type"] == "jsonl"
                
                doc_index += 1
        
        assert doc_index == num_lines
    finally:
        os.unlink(jsonl_file)



# =============================================================================
# Property 5: Invalid Line Skipping
# Feature: streaming-jsonl-reader, Property 5: Invalid Line Skipping
# Validates: Requirements 6.2, 8.3
# =============================================================================

@settings(max_examples=100)
@given(
    valid_line_count=st.integers(min_value=1, max_value=100),
    invalid_json_count=st.integers(min_value=0, max_value=20),
    missing_field_count=st.integers(min_value=0, max_value=20),
    batch_size=st.integers(min_value=1, max_value=50)
)
def test_invalid_line_skipping(
    valid_line_count: int,
    invalid_json_count: int,
    missing_field_count: int,
    batch_size: int
):
    """
    Property 5: Invalid Line Skipping

    For any JSONL file containing a mix of valid JSON lines and invalid lines
    (malformed JSON or missing text_field), the number of documents returned
    shall equal the number of valid lines, and processing shall complete
    without raising an exception (when strict_mode=False).

    Validates: Requirements 6.2, 8.3
    """
    # Ensure at least one invalid line for meaningful test
    if invalid_json_count == 0 and missing_field_count == 0:
        invalid_json_count = 1

    # Build lines: valid lines, malformed JSON lines, and lines missing text_field
    lines = []

    # Add valid lines with "text" field
    for i in range(valid_line_count):
        lines.append(json.dumps({"text": f"valid line {i}", "id": i}))

    # Add malformed JSON lines (invalid JSON syntax)
    for i in range(invalid_json_count):
        lines.append(f"{{not valid json {i}")  # Missing closing brace and quotes

    # Add lines missing the text_field
    for i in range(missing_field_count):
        lines.append(json.dumps({"other_field": f"no text field {i}", "id": i}))

    # Shuffle to intersperse invalid lines among valid ones
    import random
    random.shuffle(lines)

    # Write to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        # Configure reader with strict_mode=False (default)
        config = StreamingJSONLReaderConfig(
            batch_size=batch_size,
            text_field="text",
            strict_mode=False
        )
        reader = StreamingJSONLReaderProvider(config)

        # Read should complete without exception
        documents = reader.read(jsonl_file)

        # Verify document count equals valid line count
        assert len(documents) == valid_line_count, (
            f"Expected {valid_line_count} documents, got {len(documents)}. "
            f"Invalid JSON lines: {invalid_json_count}, missing field lines: {missing_field_count}"
        )

        # Verify all returned documents have valid text
        for i, doc in enumerate(documents):
            assert doc.text.startswith("valid line"), (
                f"Document {i} has unexpected text: '{doc.text}'"
            )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    valid_line_count=st.integers(min_value=1, max_value=50),
    invalid_json_count=st.integers(min_value=1, max_value=10)
)
def test_malformed_json_lines_skipped(valid_line_count: int, invalid_json_count: int):
    """
    Verify that malformed JSON lines are skipped and processing continues.

    This specifically tests Requirement 6.2: IF a malformed line is encountered,
    THEN THE Streaming_Reader SHALL skip the line and continue processing subsequent lines.
    """
    lines = []

    # Add valid lines
    for i in range(valid_line_count):
        lines.append(json.dumps({"text": f"line {i}"}))

    # Add various types of malformed JSON
    malformed_examples = [
        "{not json",
        "just plain text",
        '{"unclosed": "string',
        "{'single': 'quotes'}",  # Python dict syntax, not JSON
        "",  # Empty line (should be skipped silently)
        "   ",  # Whitespace only (should be skipped silently)
        '{"text": undefined}',  # JavaScript undefined, not valid JSON
        "[1, 2, 3]",  # Array, not object - valid JSON but missing text field
    ]

    # Add malformed lines up to the count
    for i in range(invalid_json_count):
        lines.append(malformed_examples[i % len(malformed_examples)])

    import random
    random.shuffle(lines)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field="text", strict_mode=False)
        reader = StreamingJSONLReaderProvider(config)

        # Should not raise exception
        documents = reader.read(jsonl_file)

        # Should return only valid documents
        assert len(documents) == valid_line_count, (
            f"Expected {valid_line_count} documents, got {len(documents)}"
        )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    valid_line_count=st.integers(min_value=1, max_value=50),
    missing_field_count=st.integers(min_value=1, max_value=10),
    text_field=st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=('L',)))
)
def test_missing_text_field_lines_skipped(
    valid_line_count: int,
    missing_field_count: int,
    text_field: str
):
    """
    Verify that lines missing the specified text_field are skipped.

    This specifically tests Requirement 8.3: IF the specified text_field is missing
    from a JSON line, THEN THE Streaming_Reader SHALL log a warning and skip the line.
    """
    lines = []

    # Add valid lines with the specified text_field
    for i in range(valid_line_count):
        lines.append(json.dumps({text_field: f"content {i}", "id": i}))

    # Add lines missing the text_field (but valid JSON)
    for i in range(missing_field_count):
        lines.append(json.dumps({"other_field": f"no text {i}", "another": i}))

    import random
    random.shuffle(lines)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field=text_field, strict_mode=False)
        reader = StreamingJSONLReaderProvider(config)

        # Should not raise exception
        documents = reader.read(jsonl_file)

        # Should return only documents with the text_field
        assert len(documents) == valid_line_count, (
            f"Expected {valid_line_count} documents, got {len(documents)}. "
            f"text_field='{text_field}', missing_field_count={missing_field_count}"
        )

        # Verify all documents have correct text
        for doc in documents:
            assert doc.text.startswith("content "), (
                f"Document has unexpected text: '{doc.text}'"
            )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    valid_line_count=st.integers(min_value=1, max_value=50),
    invalid_count=st.integers(min_value=1, max_value=20)
)
def test_lazy_load_skips_invalid_lines(valid_line_count: int, invalid_count: int):
    """
    Verify that lazy_load_data also skips invalid lines correctly.

    This ensures the streaming interface has the same skipping behavior as read().
    """
    lines = []

    # Add valid lines
    for i in range(valid_line_count):
        lines.append(json.dumps({"text": f"valid {i}"}))

    # Add invalid lines (mix of malformed JSON and missing field)
    for i in range(invalid_count):
        if i % 2 == 0:
            lines.append("{malformed json")
        else:
            lines.append(json.dumps({"no_text_field": i}))

    import random
    random.shuffle(lines)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field="text", strict_mode=False, batch_size=10)
        reader = StreamingJSONLReaderProvider(config)

        # Collect all documents from lazy_load_data
        all_docs = []
        for batch in reader.lazy_load_data(jsonl_file):
            all_docs.extend(batch)

        # Should return only valid documents
        assert len(all_docs) == valid_line_count, (
            f"Expected {valid_line_count} documents from lazy_load_data, got {len(all_docs)}"
        )
    finally:
        os.unlink(jsonl_file)


# =============================================================================
# Property 6: Strict Mode Exception Behavior
# Feature: streaming-jsonl-reader, Property 6: Strict Mode Exception Behavior
# Validates: Requirements 6.4
# =============================================================================

@settings(max_examples=100)
@given(
    valid_lines_before=st.integers(min_value=0, max_value=20),
    valid_lines_after=st.integers(min_value=0, max_value=20)
)
def test_strict_mode_raises_on_malformed_json(valid_lines_before: int, valid_lines_after: int):
    """
    Property 6: Strict Mode Exception Behavior (Malformed JSON)

    For any JSONL file containing at least one invalid line and strict_mode=True,
    calling read() shall raise a JSONDecodeError exception, and no documents
    shall be returned.

    Validates: Requirements 6.4
    """
    lines = []

    # Add valid lines before the malformed line
    for i in range(valid_lines_before):
        lines.append(json.dumps({"text": f"valid before {i}"}))

    # Add a malformed JSON line
    lines.append("{this is not valid json")

    # Add valid lines after the malformed line
    for i in range(valid_lines_after):
        lines.append(json.dumps({"text": f"valid after {i}"}))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field="text", strict_mode=True)
        reader = StreamingJSONLReaderProvider(config)

        # Should raise JSONDecodeError
        with pytest.raises(json.JSONDecodeError):
            reader.read(jsonl_file)
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    valid_lines_before=st.integers(min_value=0, max_value=20),
    valid_lines_after=st.integers(min_value=0, max_value=20),
    text_field=st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=('L',)))
)
def test_strict_mode_raises_on_missing_text_field(
    valid_lines_before: int,
    valid_lines_after: int,
    text_field: str
):
    """
    Property 6: Strict Mode Exception Behavior (Missing Text Field)

    For any JSONL file containing at least one line missing the specified text_field
    and strict_mode=True, calling read() shall raise a ValueError exception.

    Validates: Requirements 6.4
    """
    lines = []

    # Add valid lines before the line with missing field
    for i in range(valid_lines_before):
        lines.append(json.dumps({text_field: f"valid before {i}"}))

    # Add a line missing the text_field (but valid JSON)
    lines.append(json.dumps({"other_field": "no text field here", "id": 999}))

    # Add valid lines after
    for i in range(valid_lines_after):
        lines.append(json.dumps({text_field: f"valid after {i}"}))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field=text_field, strict_mode=True)
        reader = StreamingJSONLReaderProvider(config)

        # Should raise ValueError for missing text_field
        with pytest.raises(ValueError) as exc_info:
            reader.read(jsonl_file)

        # Verify error message mentions the missing field
        assert text_field in str(exc_info.value), (
            f"Error message should mention the missing field '{text_field}'"
        )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    valid_lines_before=st.integers(min_value=0, max_value=20),
    valid_lines_after=st.integers(min_value=0, max_value=20)
)
def test_strict_mode_raises_on_lazy_load_malformed_json(
    valid_lines_before: int,
    valid_lines_after: int
):
    """
    Verify that lazy_load_data also raises exceptions in strict_mode.

    This ensures the streaming interface has the same strict behavior as read().
    """
    lines = []

    # Add valid lines before
    for i in range(valid_lines_before):
        lines.append(json.dumps({"text": f"valid before {i}"}))

    # Add malformed JSON
    lines.append("{malformed json line")

    # Add valid lines after
    for i in range(valid_lines_after):
        lines.append(json.dumps({"text": f"valid after {i}"}))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field="text", strict_mode=True, batch_size=5)
        reader = StreamingJSONLReaderProvider(config)

        # Should raise JSONDecodeError when iterating
        with pytest.raises(json.JSONDecodeError):
            # Consume the generator to trigger the exception
            list(reader.lazy_load_data(jsonl_file))
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    valid_lines_before=st.integers(min_value=0, max_value=20),
    valid_lines_after=st.integers(min_value=0, max_value=20)
)
def test_strict_mode_raises_on_lazy_load_missing_field(
    valid_lines_before: int,
    valid_lines_after: int
):
    """
    Verify that lazy_load_data raises ValueError for missing text_field in strict_mode.
    """
    lines = []

    # Add valid lines before
    for i in range(valid_lines_before):
        lines.append(json.dumps({"text": f"valid before {i}"}))

    # Add line missing text_field
    lines.append(json.dumps({"other_field": "missing text"}))

    # Add valid lines after
    for i in range(valid_lines_after):
        lines.append(json.dumps({"text": f"valid after {i}"}))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field="text", strict_mode=True, batch_size=5)
        reader = StreamingJSONLReaderProvider(config)

        # Should raise ValueError when iterating
        with pytest.raises(ValueError) as exc_info:
            list(reader.lazy_load_data(jsonl_file))

        assert "text" in str(exc_info.value)
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=100),
    batch_size=st.integers(min_value=1, max_value=50)
)
def test_strict_mode_succeeds_with_all_valid_lines(num_lines: int, batch_size: int):
    """
    Verify that strict_mode=True succeeds when all lines are valid.

    This ensures strict_mode doesn't break normal operation with valid files.
    """
    lines = []

    # Add only valid lines
    for i in range(num_lines):
        lines.append(json.dumps({"text": f"valid line {i}", "id": i}))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(
            text_field="text",
            strict_mode=True,
            batch_size=batch_size
        )
        reader = StreamingJSONLReaderProvider(config)

        # Should succeed without exception
        documents = reader.read(jsonl_file)

        # Verify all documents returned
        assert len(documents) == num_lines, (
            f"Expected {num_lines} documents, got {len(documents)}"
        )

        # Verify document content
        for i, doc in enumerate(documents):
            assert doc.text == f"valid line {i}", (
                f"Document {i} has unexpected text: '{doc.text}'"
            )
    finally:
        os.unlink(jsonl_file)



# =============================================================================
# Property 7: Text Field Extraction
# Feature: streaming-jsonl-reader, Property 7: Text Field Extraction
# Validates: Requirements 8.2
# =============================================================================

@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=100),
    text_field=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('L', 'N'))),
    batch_size=st.integers(min_value=1, max_value=50)
)
def test_text_field_extraction(num_lines: int, text_field: str, batch_size: int):
    """
    Property 7: Text Field Extraction

    For any JSONL file where each line contains a JSON object with key K,
    and text_field=K, each document's text shall equal str(json_obj[K])
    for the corresponding line.

    Validates: Requirements 8.2
    """
    # Generate JSONL file with the specified text_field
    expected_texts = []
    lines = []
    for i in range(num_lines):
        text_value = f"content for line {i} with special chars: éàü"
        expected_texts.append(text_value)
        lines.append(json.dumps({text_field: text_value, "other_field": f"other {i}"}))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False, encoding='utf-8') as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field=text_field, batch_size=batch_size)
        reader = StreamingJSONLReaderProvider(config)

        documents = reader.read(jsonl_file)

        # Verify document count
        assert len(documents) == num_lines, (
            f"Expected {num_lines} documents, got {len(documents)}"
        )

        # Verify each document's text equals str(json_obj[text_field])
        for i, doc in enumerate(documents):
            assert doc.text == expected_texts[i], (
                f"Document {i} text mismatch: got '{doc.text}', expected '{expected_texts[i]}'"
            )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=50),
    text_field=st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=('L',)))
)
def test_text_field_extraction_with_various_types(num_lines: int, text_field: str):
    """
    Verify text field extraction converts various JSON types to strings.

    The text field value should be converted to string regardless of its JSON type.
    """
    lines = []
    expected_texts = []

    # Generate lines with various value types for the text field
    value_types = [
        ("string value", "string value"),
        (12345, "12345"),
        (3.14159, "3.14159"),
        (True, "True"),
        (False, "False"),
        (None, "None"),
        (["list", "items"], "['list', 'items']"),
        ({"nested": "dict"}, "{'nested': 'dict'}"),
    ]

    for i in range(num_lines):
        value, expected = value_types[i % len(value_types)]
        lines.append(json.dumps({text_field: value, "id": i}))
        expected_texts.append(expected)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field=text_field)
        reader = StreamingJSONLReaderProvider(config)

        documents = reader.read(jsonl_file)

        assert len(documents) == num_lines

        for i, doc in enumerate(documents):
            assert doc.text == expected_texts[i], (
                f"Document {i} text mismatch: got '{doc.text}', expected '{expected_texts[i]}'"
            )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=50)
)
def test_text_field_extraction_lazy_load(num_lines: int):
    """
    Verify text field extraction works correctly with lazy_load_data.

    This ensures the streaming interface extracts text fields the same way as read().
    """
    text_field = "content"
    expected_texts = []
    lines = []

    for i in range(num_lines):
        text_value = f"lazy load content {i}"
        expected_texts.append(text_value)
        lines.append(json.dumps({text_field: text_value, "metadata": {"index": i}}))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field=text_field, batch_size=10)
        reader = StreamingJSONLReaderProvider(config)

        all_docs = []
        for batch in reader.lazy_load_data(jsonl_file):
            all_docs.extend(batch)

        assert len(all_docs) == num_lines

        for i, doc in enumerate(all_docs):
            assert doc.text == expected_texts[i], (
                f"Document {i} text mismatch in lazy_load: got '{doc.text}', expected '{expected_texts[i]}'"
            )
    finally:
        os.unlink(jsonl_file)


# =============================================================================
# Property 8: Null Text Field Uses Full JSON
# Feature: streaming-jsonl-reader, Property 8: Null Text Field Uses Full JSON
# Validates: Requirements 8.4
# =============================================================================

@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=100),
    batch_size=st.integers(min_value=1, max_value=50)
)
def test_null_text_field_uses_full_json(num_lines: int, batch_size: int):
    """
    Property 8: Null Text Field Uses Full JSON

    For any JSONL file and text_field=None, each document's text shall be
    the JSON string representation of the entire parsed JSON object from that line.

    Validates: Requirements 8.4
    """
    # Generate JSONL file with various JSON objects
    json_objects = []
    lines = []

    for i in range(num_lines):
        obj = {"id": i, "name": f"item_{i}", "value": i * 10}
        json_objects.append(obj)
        lines.append(json.dumps(obj))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        # Configure reader with text_field=None
        config = StreamingJSONLReaderConfig(text_field=None, batch_size=batch_size)
        reader = StreamingJSONLReaderProvider(config)

        documents = reader.read(jsonl_file)

        # Verify document count
        assert len(documents) == num_lines, (
            f"Expected {num_lines} documents, got {len(documents)}"
        )

        # Verify each document's text is the JSON string of the entire object
        for i, doc in enumerate(documents):
            expected_text = json.dumps(json_objects[i])
            assert doc.text == expected_text, (
                f"Document {i} text mismatch: got '{doc.text}', expected '{expected_text}'"
            )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=50)
)
def test_null_text_field_preserves_json_structure(num_lines: int):
    """
    Verify that text_field=None preserves the complete JSON structure.

    The document text should be parseable back to the original JSON object.
    """
    json_objects = []
    lines = []

    for i in range(num_lines):
        # Create complex nested objects
        obj = {
            "id": i,
            "nested": {"level1": {"level2": f"deep value {i}"}},
            "array": [1, 2, 3, f"item {i}"],
            "boolean": i % 2 == 0,
            "null_value": None,
        }
        json_objects.append(obj)
        lines.append(json.dumps(obj))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field=None)
        reader = StreamingJSONLReaderProvider(config)

        documents = reader.read(jsonl_file)

        assert len(documents) == num_lines

        for i, doc in enumerate(documents):
            # Parse the document text back to JSON
            parsed = json.loads(doc.text)

            # Verify it matches the original object
            assert parsed == json_objects[i], (
                f"Document {i} JSON mismatch: parsed={parsed}, original={json_objects[i]}"
            )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=50)
)
def test_null_text_field_lazy_load(num_lines: int):
    """
    Verify text_field=None works correctly with lazy_load_data.

    This ensures the streaming interface handles null text_field the same way as read().
    """
    json_objects = []
    lines = []

    for i in range(num_lines):
        obj = {"index": i, "data": f"value_{i}"}
        json_objects.append(obj)
        lines.append(json.dumps(obj))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field=None, batch_size=10)
        reader = StreamingJSONLReaderProvider(config)

        all_docs = []
        for batch in reader.lazy_load_data(jsonl_file):
            all_docs.extend(batch)

        assert len(all_docs) == num_lines

        for i, doc in enumerate(all_docs):
            expected_text = json.dumps(json_objects[i])
            assert doc.text == expected_text, (
                f"Document {i} text mismatch in lazy_load: got '{doc.text}', expected '{expected_text}'"
            )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=50),
    batch_size=st.integers(min_value=1, max_value=20)
)
def test_null_text_field_read_and_lazy_load_consistency(num_lines: int, batch_size: int):
    """
    Verify that read() and lazy_load_data() produce identical results with text_field=None.

    This ensures consistency between the two interfaces.
    """
    lines = []
    for i in range(num_lines):
        obj = {"seq": i, "payload": f"data_{i}"}
        lines.append(json.dumps(obj))

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(text_field=None, batch_size=batch_size)
        reader = StreamingJSONLReaderProvider(config)

        # Get documents from read()
        read_docs = reader.read(jsonl_file)

        # Get documents from lazy_load_data()
        lazy_docs = []
        for batch in reader.lazy_load_data(jsonl_file):
            lazy_docs.extend(batch)

        # Verify same count
        assert len(read_docs) == len(lazy_docs), (
            f"read() returned {len(read_docs)} docs, lazy_load_data() returned {len(lazy_docs)}"
        )

        # Verify same content
        for i, (read_doc, lazy_doc) in enumerate(zip(read_docs, lazy_docs)):
            assert read_doc.text == lazy_doc.text, (
                f"Document {i} text mismatch: read='{read_doc.text}', lazy='{lazy_doc.text}'"
            )
    finally:
        os.unlink(jsonl_file)


# =============================================================================
# Property 9: Document Count Equals Valid Line Count
# Feature: streaming-jsonl-reader, Property 9: Document Count Equals Valid Line Count
# Validates: Requirements 4.2, 4.3
# =============================================================================

@settings(max_examples=100)
@given(
    num_lines=st.integers(min_value=1, max_value=500),
    batch_size=st.integers(min_value=1, max_value=100)
)
def test_document_count_equals_valid_line_count(num_lines: int, batch_size: int):
    """
    Property 9: Document Count Equals Valid Line Count

    For any JSONL file, the total number of documents yielded across all batches
    from lazy_load_data() shall equal the number of documents returned by read()
    for the same input.

    Validates: Requirements 4.2, 4.3
    """
    # Generate JSONL file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        lines = [json.dumps({"text": f"line {i}"}) for i in range(num_lines)]
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(batch_size=batch_size)
        reader = StreamingJSONLReaderProvider(config)

        # Count documents from lazy_load_data
        lazy_load_count = 0
        for batch in reader.lazy_load_data(jsonl_file):
            lazy_load_count += len(batch)

        # Count documents from read
        read_docs = reader.read(jsonl_file)
        read_count = len(read_docs)

        # Verify counts match
        assert lazy_load_count == read_count, (
            f"lazy_load_data yielded {lazy_load_count} documents, "
            f"read() returned {read_count} documents"
        )

        # Both should equal the number of valid lines
        assert lazy_load_count == num_lines, (
            f"Expected {num_lines} documents, got {lazy_load_count}"
        )
    finally:
        os.unlink(jsonl_file)


@settings(max_examples=100)
@given(
    valid_line_count=st.integers(min_value=1, max_value=200),
    invalid_line_count=st.integers(min_value=0, max_value=50),
    batch_size=st.integers(min_value=1, max_value=50)
)
def test_document_count_with_invalid_lines(
    valid_line_count: int,
    invalid_line_count: int,
    batch_size: int
):
    """
    Verify document count consistency when file contains invalid lines.

    Both lazy_load_data and read should return the same count of valid documents,
    skipping invalid lines consistently.
    """
    lines = []

    # Add valid lines
    for i in range(valid_line_count):
        lines.append(json.dumps({"text": f"valid {i}"}))

    # Add invalid lines (malformed JSON)
    for i in range(invalid_line_count):
        lines.append(f"{{invalid json {i}")

    # Shuffle to intersperse
    import random
    random.shuffle(lines)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        f.write("\n".join(lines))
        jsonl_file = f.name

    try:
        config = StreamingJSONLReaderConfig(
            batch_size=batch_size,
            text_field="text",
            strict_mode=False
        )
        reader = StreamingJSONLReaderProvider(config)

        # Count from lazy_load_data
        lazy_count = sum(len(batch) for batch in reader.lazy_load_data(jsonl_file))

        # Count from read
        read_count = len(reader.read(jsonl_file))

        # Both should match and equal valid line count
        assert lazy_count == read_count, (
            f"lazy_load_data: {lazy_count}, read: {read_count}"
        )
        assert lazy_count == valid_line_count, (
            f"Expected {valid_line_count} valid documents, got {lazy_count}"
        )
    finally:
        os.unlink(jsonl_file)
