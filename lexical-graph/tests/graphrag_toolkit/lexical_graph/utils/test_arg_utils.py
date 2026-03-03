import pytest

from graphrag_toolkit.lexical_graph.utils.arg_utils import coalesce

def test_coalesce():
    assert coalesce(None, None, 3) == 3
    assert coalesce(None, 2, 3) == 2
    assert coalesce(1, 2, 3) == 1
    assert coalesce(None, False, True) == False
    assert coalesce(None, True, False) == True
    assert coalesce(None, None, None) is None