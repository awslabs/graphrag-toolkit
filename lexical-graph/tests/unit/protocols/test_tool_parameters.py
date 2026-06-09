# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import patch, MagicMock

# Mock fastmcp before importing ToolParameters
mock_not_set = object()

mock_fastmcp_modules = {
    'fastmcp': MagicMock(),
    'fastmcp.tools': MagicMock(),
    'fastmcp.tools.tool_transform': MagicMock(ArgTransform=MagicMock),
    'fastmcp.utilities': MagicMock(),
    'fastmcp.utilities.types': MagicMock(NotSet=mock_not_set),
}


def make_arg_transform(name):
    """Create a mock ArgTransform with a name."""
    arg = MagicMock()
    arg.name = name
    return arg


@pytest.fixture(autouse=True)
def mock_fastmcp():
    with patch.dict('sys.modules', mock_fastmcp_modules):
        yield


def _import_tool_parameters():
    """Import ToolParameters with mocked dependencies."""
    import importlib
    import graphrag_toolkit.lexical_graph.protocols.mcp_server as mod
    importlib.reload(mod)
    return mod.ToolParameters


class TestToolParametersValidation:
    """Tests for ToolParameters parameter count validation."""

    def test_zero_parameters_accepted(self):
        """ToolParameters should accept an empty parameter list."""
        ToolParameters = _import_tool_parameters()
        tp = ToolParameters(parameters=[])
        assert tp.parameters == []

    def test_one_parameter_accepted(self):
        """ToolParameters should accept 1 ArgTransform."""
        ToolParameters = _import_tool_parameters()
        params = [make_arg_transform('param1')]
        tp = ToolParameters(parameters=params)
        assert len(tp.parameters) == 1

    def test_two_parameters_accepted(self):
        """ToolParameters should accept 2 ArgTransforms."""
        ToolParameters = _import_tool_parameters()
        params = [make_arg_transform('param1'), make_arg_transform('param2')]
        tp = ToolParameters(parameters=params)
        assert len(tp.parameters) == 2

    def test_three_parameters_accepted(self):
        """ToolParameters should accept 3 ArgTransforms (maximum allowed)."""
        ToolParameters = _import_tool_parameters()
        params = [make_arg_transform('param1'), make_arg_transform('param2'), make_arg_transform('param3')]
        tp = ToolParameters(parameters=params)
        assert len(tp.parameters) == 3

    def test_four_parameters_rejected(self):
        """ToolParameters should reject more than 3 ArgTransforms."""
        ToolParameters = _import_tool_parameters()
        params = [make_arg_transform(f'param{i}') for i in range(4)]
        with pytest.raises(ValueError, match='Maximum number of tool parameters exceeded'):
            ToolParameters(parameters=params)

    def test_five_parameters_rejected(self):
        """ToolParameters should reject 5 ArgTransforms."""
        ToolParameters = _import_tool_parameters()
        params = [make_arg_transform(f'param{i}') for i in range(5)]
        with pytest.raises(ValueError, match='Maximum number of tool parameters exceeded'):
            ToolParameters(parameters=params)

    def test_parameter_without_name_rejected(self):
        """ToolParameters should reject ArgTransform with missing name."""
        ToolParameters = _import_tool_parameters()
        arg = MagicMock()
        arg.name = mock_not_set
        with pytest.raises(ValueError, match='Tool parameter name missing'):
            ToolParameters(parameters=[arg])

    def test_parameters_marked_required(self):
        """All parameters should be marked as required."""
        ToolParameters = _import_tool_parameters()
        params = [make_arg_transform('param1'), make_arg_transform('param2')]
        tp = ToolParameters(parameters=params)
        for p in tp.parameters:
            assert p.required is True

    def test_parameters_marked_not_hidden(self):
        """All parameters should be marked as not hidden."""
        ToolParameters = _import_tool_parameters()
        params = [make_arg_transform('param1'), make_arg_transform('param2')]
        tp = ToolParameters(parameters=params)
        for p in tp.parameters:
            assert p.hide is False
