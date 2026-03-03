"""Tests for batch inference utilities (batch_inference_utils.py).

split_nodes           — partitions a list of nodes into batches for Bedrock batch
                        inference, enforcing Bedrock's minimum (100) and maximum
                        (50,000) batch-size constraints. The final batch absorbs any
                        remainder that would be smaller than the minimum.

get_parse_output_text_fn — returns a model-specific function that extracts the text
                           response from a Bedrock batch-output JSON record. Raises
                           ValueError for unrecognised model IDs.

Splitting algorithm
-------------------
At each iteration the algorithm checks whether the remaining nodes after taking
the next full batch would be fewer than BEDROCK_MIN_BATCH_SIZE (100). If so it
appends all remaining nodes as one final batch (avoiding a too-small tail batch).
The boundary condition uses strict less-than (<), so a remainder of exactly 100
is NOT merged — it becomes a separate batch.
"""

import pytest

from graphrag_toolkit.lexical_graph import BatchJobError
from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import (
    split_nodes,
    get_parse_output_text_fn,
)


# ---------------------------------------------------------------------------
# split_nodes — happy-path splitting
# ---------------------------------------------------------------------------


def test_split_nodes_even_split():
    """1 000 nodes with batch_size=500 -> 2 batches of exactly 500."""
    nodes = list(range(1000))
    result = split_nodes(nodes, 500)
    assert len(result) == 2
    assert all(len(batch) == 500 for batch in result)


def test_split_nodes_remainder_smaller_than_minimum_merged():
    """250 nodes, batch_size=200: remainder (50) < 100 -> merged into single batch of 250."""
    nodes = list(range(250))
    result = split_nodes(nodes, 200)
    assert len(result) == 1
    assert len(result[0]) == 250


def test_split_nodes_exact_minimum_size():
    """100 nodes, batch_size=100: remainder (0) < 100 -> single batch of 100."""
    nodes = list(range(100))
    result = split_nodes(nodes, 100)
    assert len(result) == 1
    assert len(result[0]) == 100


def test_split_nodes_remainder_exactly_minimum_not_merged():
    """300 nodes, batch_size=200: remainder is exactly 100 (== min, not <).
    The strict-less-than check means the remainder is NOT merged, producing
    two batches [200, 100].
    """
    nodes = list(range(300))
    result = split_nodes(nodes, 200)
    assert len(result) == 2
    assert len(result[0]) == 200
    assert len(result[1]) == 100


def test_split_nodes_preserves_all_items():
    """All input items appear in the output in the original order, with no duplicates."""
    nodes = list(range(500))
    result = split_nodes(nodes, 200)
    flattened = [item for batch in result for item in batch]
    assert flattened == nodes


def test_split_nodes_large_batch():
    """A batch_size equal to the maximum (50 000) is accepted."""
    nodes = list(range(50000))
    result = split_nodes(nodes, 50000)
    assert len(result) == 1
    assert len(result[0]) == 50000


# ---------------------------------------------------------------------------
# split_nodes — validation errors
# ---------------------------------------------------------------------------


def test_split_nodes_batch_size_too_small_raises():
    """batch_size < 100 -> BatchJobError."""
    with pytest.raises(BatchJobError):
        split_nodes(list(range(100)), 50)


def test_split_nodes_batch_size_too_large_raises():
    """batch_size > 50 000 -> BatchJobError."""
    with pytest.raises(BatchJobError):
        split_nodes(list(range(100)), 60000)


def test_split_nodes_empty_list_raises():
    """Empty node list -> BatchJobError."""
    with pytest.raises(BatchJobError):
        split_nodes([], 100)


def test_split_nodes_fewer_than_minimum_raises():
    """Fewer nodes than the minimum (50 < 100) -> BatchJobError."""
    with pytest.raises(BatchJobError):
        split_nodes(list(range(50)), 100)


def test_split_nodes_exactly_minimum_batch_size_valid():
    """batch_size == 100 (the minimum) is accepted."""
    nodes = list(range(100))
    result = split_nodes(nodes, 100)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# get_parse_output_text_fn — Nova
# ---------------------------------------------------------------------------


def test_get_parse_output_text_fn_nova_basic():
    """Nova response: concatenates all text content blocks."""
    fn = get_parse_output_text_fn("amazon.nova-lite")
    json_data = {
        "modelOutput": {
            "output": {
                "message": {
                    "content": [{"text": "Hello "}, {"text": "World"}]
                }
            }
        }
    }
    assert fn(json_data) == "Hello World"


def test_get_parse_output_text_fn_nova_single_block():
    """Nova response with a single content block."""
    fn = get_parse_output_text_fn("amazon.nova-pro")
    json_data = {
        "modelOutput": {
            "output": {
                "message": {
                    "content": [{"text": "Answer"}]
                }
            }
        }
    }
    assert fn(json_data) == "Answer"


def test_get_parse_output_text_fn_nova_empty_content():
    """Nova response with missing nested keys returns empty string (safe .get chain)."""
    fn = get_parse_output_text_fn("amazon.nova-lite")
    json_data = {"modelOutput": {}}
    assert fn(json_data) == ""


# ---------------------------------------------------------------------------
# get_parse_output_text_fn — Claude
# ---------------------------------------------------------------------------


def test_get_parse_output_text_fn_claude_basic():
    """Claude response: concatenates all text content blocks."""
    fn = get_parse_output_text_fn("anthropic.claude-v3")
    json_data = {
        "modelOutput": {
            "content": [{"text": "Hello "}, {"text": "World"}]
        }
    }
    assert fn(json_data) == "Hello World"


def test_get_parse_output_text_fn_claude_empty_content():
    """Claude response with empty content list returns empty string."""
    fn = get_parse_output_text_fn("anthropic.claude-v3")
    json_data = {"modelOutput": {"content": []}}
    assert fn(json_data) == ""


# ---------------------------------------------------------------------------
# get_parse_output_text_fn — Llama
# ---------------------------------------------------------------------------


def test_get_parse_output_text_fn_llama_basic():
    """Llama response: reads the top-level 'generation' key."""
    fn = get_parse_output_text_fn("meta.llama-3")
    json_data = {"generation": "Hello World"}
    assert fn(json_data) == "Hello World"


# ---------------------------------------------------------------------------
# get_parse_output_text_fn — unknown model
# ---------------------------------------------------------------------------


def test_get_parse_output_text_fn_unknown_raises():
    """An unrecognised model ID raises ValueError with a helpful message."""
    with pytest.raises(ValueError, match="Unrecognized model_id"):
        get_parse_output_text_fn("unknown.model")


def test_get_parse_output_text_fn_returns_callable():
    """Each recognised model returns a callable (not None)."""
    for model_id in ["amazon.nova-lite", "anthropic.claude-v3", "meta.llama-3"]:
        fn = get_parse_output_text_fn(model_id)
        assert callable(fn)
