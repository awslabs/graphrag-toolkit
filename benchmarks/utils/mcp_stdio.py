# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A minimal MCP client over stdio: initialize, tools/list and tools/call.

The MCP stdio transport is JSON-RPC 2.0, one message per line. The official Python
SDK would add pydantic>=2.12, httpx2 and a web-server stack to every integration-test
environment for these three calls, so the client is this module; the server it talks
to is the official one, run in its own environment.
"""

import json
import queue
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional, Sequence

PROTOCOL_VERSION = '2025-06-18'
_METHOD_NOT_FOUND = -32601


class McpError(RuntimeError):
    pass


class McpTimeout(McpError):
    """The server sent no reply in time; it is still running and may reply later."""


class McpStdioClient:
    """Starts an MCP server as a subprocess and calls its tools, one request at a time.

    Server log output on stderr goes to stderr_path (or is discarded); a server that
    exits, writes a non-JSON line or misses the timeout raises McpError.
    """

    def __init__(self, command: Sequence[str], env: Optional[Dict[str, str]] = None,
                 timeout: float = 120.0, stderr_path: Optional[str] = None,
                 client_name: str = 'graphrag-toolkit-benchmarks'):
        self.timeout = timeout
        self._stderr = open(stderr_path, 'ab') if stderr_path else None
        try:
            self._proc = subprocess.Popen(list(command), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                          stderr=self._stderr or subprocess.DEVNULL, env=env)
        except OSError as e:
            self._close_stderr()
            raise McpError(f'could not start MCP server {list(command)!r}: {e}') from e
        self._messages: 'queue.Queue[Any]' = queue.Queue()
        self._lock = threading.Lock()
        self._next_id = 0
        threading.Thread(target=self._read, daemon=True).start()
        try:
            result = self._request('initialize', {'protocolVersion': PROTOCOL_VERSION, 'capabilities': {},
                                                  'clientInfo': {'name': client_name, 'version': '1'}})
            self.server_info = result.get('serverInfo', {})
            self.protocol_version = result.get('protocolVersion')
            self._send({'jsonrpc': '2.0', 'method': 'notifications/initialized'})
        except BaseException:
            self.close()
            raise

    def _read(self) -> None:
        for line in self._proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                self._messages.put(json.loads(line))
            except ValueError:
                self._messages.put(McpError(f'non-JSON line on the server stdout: {line[:200]!r}'))
        self._messages.put(None)  # end of output: the server exited

    def _send(self, message: Dict[str, Any]) -> None:
        try:
            self._proc.stdin.write((json.dumps(message) + '\n').encode('utf-8'))
            self._proc.stdin.flush()
        except (BrokenPipeError, ValueError) as e:
            raise McpError(f'MCP server is gone (exit code {self._proc.poll()})') from e

    def _request(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            self._next_id += 1
            request_id = self._next_id
            self._send({'jsonrpc': '2.0', 'id': request_id, 'method': method, 'params': params})
            deadline = time.monotonic() + self.timeout
            while True:
                try:
                    message = self._messages.get(timeout=max(deadline - time.monotonic(), 0))
                except queue.Empty:
                    raise McpTimeout(f'{method}: no response within {self.timeout:.0f} s') from None
                if message is None:
                    raise McpError(f'MCP server exited (code {self._proc.wait()}) during {method}')
                if isinstance(message, Exception):
                    raise message
                if 'method' in message:  # the server's own request or notification
                    if 'id' in message:
                        self._answer(message)
                    continue
                if message.get('id') != request_id:
                    continue  # the late reply to a request that already timed out
                if 'error' in message:
                    raise McpError(f'{method}: {message["error"]}')
                return message.get('result') or {}

    def _answer(self, request: Dict[str, Any]) -> None:
        if request['method'] == 'ping':
            self._send({'jsonrpc': '2.0', 'id': request['id'], 'result': {}})
        else:  # this client declares no capabilities, so nothing else is expected
            self._send({'jsonrpc': '2.0', 'id': request['id'],
                        'error': {'code': _METHOD_NOT_FOUND, 'message': f'unsupported: {request["method"]}'}})

    def list_tools(self) -> List[Dict[str, Any]]:
        tools, cursor = [], None
        while True:
            result = self._request('tools/list', {'cursor': cursor} if cursor else {})
            tools.extend(result.get('tools', []))
            cursor = result.get('nextCursor')
            if not cursor:
                return tools

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """The tool's result: {'content': [...], 'isError': bool, ...} as the server sent it."""
        return self._request('tools/call', {'name': name, 'arguments': arguments})

    def close(self) -> None:
        if self._proc.poll() is None:
            try:
                self._proc.stdin.close()
                self._proc.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                self._proc.kill()
                self._proc.wait()
        self._close_stderr()

    def _close_stderr(self) -> None:
        if self._stderr is not None:
            self._stderr.close()
            self._stderr = None

    def __enter__(self) -> 'McpStdioClient':
        return self

    def __exit__(self, *exc) -> None:
        self.close()
