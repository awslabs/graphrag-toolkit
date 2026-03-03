import pytest

from graphrag_toolkit.lexical_graph.utils.arg_utils import first_non_none

def test_first_non_none():
    assert first_non_none([None, None, 3]) == 3
    assert first_non_none([None, 2, 3]) == 2
    assert first_non_none([1, 2, 3]) == 1
    assert first_non_none([None, False, True]) == False
    assert first_non_none([None, True, False]) == True
    assert first_non_none([None, None, None]) is None