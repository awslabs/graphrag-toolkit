# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A stand-in MCP server on stdio for the client tests: JSON-RPC 2.0, one message per
line, with the messages a real server interleaves (log notifications, a ping). A query
containing SLOW is answered after 1 s.

python fake_mcp_server.py <mode>   mode: normal | error | crash | garbage | silent
"""

import json
import sys
import time

MODE = sys.argv[1] if len(sys.argv) > 1 else 'normal'
TOOLS = [{'name': 'run_opencypher_query', 'description': 'Executes the provided openCypher against the graph.',
          'inputSchema': {'type': 'object', 'properties': {'query': {'type': 'string'}}, 'required': ['query']}},
         {'name': 'get_graph_status', 'description': 'Status.', 'inputSchema': {'type': 'object', 'properties': {}}}]


def send(message):
    sys.stdout.write(json.dumps(message) + '\n')
    sys.stdout.flush()


initialized = False
for line in sys.stdin:
    message = json.loads(line)
    method, request_id = message.get('method'), message.get('id')
    if method == 'notifications/initialized':
        initialized = True
        continue
    if method is None:  # the client's reply to our ping
        sys.stderr.write(f'client replied {message}\n')
        continue
    if method == 'initialize':
        sys.stderr.write('starting\n')  # servers log to stderr
        send({'jsonrpc': '2.0', 'id': request_id, 'result': {
            'protocolVersion': message['params']['protocolVersion'], 'capabilities': {'tools': {}},
            'serverInfo': {'name': 'fake-neptune', 'version': '0.0.1'}}})
    elif not initialized:
        send({'jsonrpc': '2.0', 'id': request_id, 'error': {'code': -32002, 'message': 'not initialized'}})
    elif method == 'tools/list':
        # Two pages, to exercise the cursor.
        page = 1 if (message.get('params') or {}).get('cursor') == 'page2' else 0
        result = {'tools': TOOLS[page:page + 1]}
        if page == 0:
            result['nextCursor'] = 'page2'
        send({'jsonrpc': '2.0', 'id': request_id, 'result': result})
    elif method == 'tools/call':
        if MODE == 'crash':
            sys.exit(3)
        if MODE == 'garbage':
            sys.stdout.write('Traceback (most recent call last):\n')
            sys.stdout.flush()
            continue
        if MODE == 'silent':
            continue
        send({'jsonrpc': '2.0', 'method': 'notifications/message', 'params': {'level': 'info', 'data': 'querying'}})
        send({'jsonrpc': '2.0', 'id': 'srv-1', 'method': 'ping'})
        if MODE == 'error':
            send({'jsonrpc': '2.0', 'id': request_id, 'error': {'code': -32602, 'message': 'bad arguments'}})
            continue
        query = message['params']['arguments'].get('query', '')
        if 'SLOW' in query:
            time.sleep(1.0)  # past the client's timeout: this reply arrives late
        rows = [json.dumps({'name': 'Australia'}, indent=2)] if 'Buffyverse' in query else []
        send({'jsonrpc': '2.0', 'id': request_id, 'result': {
            'content': [{'type': 'text', 'text': row} for row in rows], 'isError': 'FAIL' in query}})
    else:
        send({'jsonrpc': '2.0', 'id': request_id, 'error': {'code': -32601, 'message': f'no {method}'}})
