# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import sys

import pytest

from benchmarks.utils.mcp_stdio import McpError, McpStdioClient

SERVER = os.path.join(os.path.dirname(__file__), 'fake_mcp_server.py')


def _client(mode='normal', tmp_path=None, **kw):
    return McpStdioClient([sys.executable, SERVER, mode],
                          stderr_path=str(tmp_path / 'server.log') if tmp_path else None, **kw)


class TestSession:
    def test_handshake_records_the_server(self, tmp_path):
        with _client(tmp_path=tmp_path) as client:
            assert client.server_info == {'name': 'fake-neptune', 'version': '0.0.1'}
            assert client.protocol_version == '2025-06-18'

    def test_tools_are_listed_across_pages(self):
        with _client() as client:
            assert [t['name'] for t in client.list_tools()] == ['run_opencypher_query', 'get_graph_status']

    def test_call_returns_the_result_through_interleaved_server_messages(self, tmp_path):
        with _client(tmp_path=tmp_path) as client:
            result = client.call_tool('run_opencypher_query', {'query': "MATCH (n {name: 'Buffyverse'}) RETURN n"})
            assert result == {'content': [{'type': 'text', 'text': json.dumps({'name': 'Australia'}, indent=2)}],
                              'isError': False}
            # A second call after the ping still pairs request and response.
            assert client.call_tool('run_opencypher_query', {'query': 'FAIL'})['isError'] is True
        log = (tmp_path / 'server.log').read_text()
        assert 'starting' in log and "'result': {}" in log, 'stderr kept, and the ping was answered'

    def test_an_empty_result_has_no_content(self):
        with _client() as client:
            assert client.call_tool('run_opencypher_query', {'query': 'RETURN 1'})['content'] == []


class TestFailsLoud:
    def test_json_rpc_error(self):
        with _client('error') as client, pytest.raises(McpError, match='bad arguments'):
            client.call_tool('run_opencypher_query', {'query': 'x'})

    def test_server_exit(self):
        with _client('crash') as client, pytest.raises(McpError, match=r'exited \(code 3\)'):
            client.call_tool('run_opencypher_query', {'query': 'x'})

    def test_non_json_stdout(self):
        with _client('garbage') as client, pytest.raises(McpError, match='non-JSON line'):
            client.call_tool('run_opencypher_query', {'query': 'x'})

    def test_timeout(self):
        from benchmarks.utils.mcp_stdio import McpTimeout
        with _client('silent', timeout=0.5) as client, pytest.raises(McpTimeout, match='no response within'):
            client.call_tool('run_opencypher_query', {'query': 'x'})

    def test_a_late_reply_is_not_taken_for_the_next_call(self):
        from benchmarks.utils.mcp_stdio import McpTimeout
        with _client(timeout=0.5) as client:
            with pytest.raises(McpTimeout):
                client.call_tool('run_opencypher_query', {'query': "SLOW MATCH (n {name: 'Buffyverse'}) RETURN n"})
            client.timeout = 5
            # The late reply (one row) comes first on stdout; this call's own reply has none.
            assert client.call_tool('run_opencypher_query', {'query': 'RETURN 1'})['content'] == []

    def test_missing_command(self):
        with pytest.raises(McpError, match='could not start'):
            McpStdioClient(['definitely-not-a-command-0b1c'])

    def test_close_is_idempotent(self):
        client = _client()
        client.close()
        client.close()
        with pytest.raises(McpError):
            client.call_tool('run_opencypher_query', {'query': 'x'})
