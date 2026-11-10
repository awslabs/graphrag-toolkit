# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The LLM + graph-query baseline: an LLM that answers a KGQA question by querying the
graph in Neptune through the official Amazon Neptune MCP server.

It gets what BYOKG's KG-Linker gets (the question and the dataset's schema), the same
model through the same Bedrock Converse API, and the same call budget, and answers in
the same <answers> format, so both are scored identically.
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .mcp_stdio import McpTimeout

logger = logging.getLogger(__name__)

# The official server, pinned. mcp 2.0.1 forwards a failed query's error text to the
# client; from 2.1.0 the client sees only 'Error executing tool ...', which would leave
# the model unable to fix a query.
NEPTUNE_MCP_COMMAND = ('uvx', '--from', 'awslabs.amazon-neptune-mcp-server==1.1.1',
                       '--with', 'mcp[cli]==2.0.1', 'awslabs.amazon-neptune-mcp-server')
QUERY_TOOL = 'run_opencypher_query'

_ANSWERS = re.compile(r'<answers>(.*?)</answers>', re.DOTALL)
# Removed before looking for write clauses: names and relation types may contain 'set'.
_LITERALS = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|`[^`]*`|//[^\n]*|/\*.*?\*/", re.DOTALL)
_WRITES = re.compile(r'\b(CREATE|MERGE|SET|DELETE|DETACH|REMOVE|DROP|CALL|LOAD|FOREACH)\b', re.IGNORECASE)

SYSTEM_PROMPT = """You answer questions with facts from a knowledge graph in Amazon Neptune, which you query with openCypher through the run_opencypher_query tool.

How the graph is stored:
- Each entity is a node labeled Entity, with its name in the property `name`.
- Each fact is a relationship from its subject to its object, typed by the relation name. Relation names contain dots or spaces, so put them in backticks: MATCH (s:Entity {{name: 'Barack Obama'}})-[:`people.person.nationality`]->(o) RETURN o.name
- Some entity names are opaque ids such as m.0abc12; they stand for intermediate nodes, so follow their relationships to reach the entities they connect.{qualifiers}
- The relation names are in the schema below.

Queries are read-only and each result is cut at {max_chars} characters, so return names rather than whole nodes, and use LIMIT. You have {max_calls} turns in all, including the one where you answer.

Give the final answers, each on its own line, inside <answers></answers> tags, written exactly as the entity names appear in the graph. If the graph doesn't settle the question, use your own knowledge: always give an answer."""

QUALIFIERS = """
- A fact that holds at a time carries it on the relationship: the years in start_year and end_year (equal for a single year), and the text as written in qualifier. For example: MATCH (s:Entity {name: 'Pat LaFontaine'})-[r:`award received`]->(o) RETURN o.name, r.start_year"""

LAST_TURN = 'This is your last turn: answer now from what you found, without further queries.'
SLOW_QUERY = 'The query did not finish in time: anchor it on named entities and add LIMIT.'
# What BedrockGenerator retries, after 30 s, so both arms ride out the same throttling.
_RETRYABLE = ('Too many requests', 'Model has timed out', ' Read timeout on')
_RETRYABLE_CODES = ('ThrottlingException', 'ModelTimeoutException', 'ServiceUnavailableException')


def _retryable(error: Exception) -> bool:
    code = (getattr(error, 'response', None) or {}).get('Error', {}).get('Code')
    return code in _RETRYABLE_CODES or any(marker in str(error) for marker in _RETRYABLE)


def read_only_violation(query: str) -> Optional[str]:
    """The first write or procedure clause in an openCypher query, else None."""
    match = _WRITES.search(_LITERALS.sub(' ', query))
    return match.group(1).upper() if match else None


def parse_answers(text: str) -> List[str]:
    """Answer lines from the first <answers> block, as BYOKG's parse_response reads them."""
    match = _ANSWERS.search(text)
    if not match:
        return []
    return [line for line in match.group(1).strip().split('\n') if line != '']


class AgentError(RuntimeError):
    pass


@dataclass
class AgentResult:
    predictions: List[str]
    context: List[str] = field(default_factory=list)  # each query's result, as the model saw it
    trace: List[Dict[str, Any]] = field(default_factory=list)
    calls: int = 0
    stop: str = ''


class GraphQueryAgent:
    """Answers by calling Bedrock Converse in a loop, running each requested query
    through the MCP server's run_opencypher_query tool."""

    def __init__(self, bedrock_client, model_id: str, tools, temporal_qualifiers: bool = False,
                 max_calls: int = 15, max_result_chars: int = 8000, max_tokens: int = 4096,
                 max_retries: int = 10, retry_wait: float = 30.0,
                 on_slow_query: Optional[Callable[[str], Any]] = None, sleep: Callable[[float], None] = time.sleep):
        """max_tokens and max_retries should be BYOKG's generator's, so both arms run alike.
        on_slow_query(query) runs when a query timed out, e.g. to cancel it on the graph."""
        if max_calls < 2:
            raise ValueError(f'max_calls must allow at least one query and an answer, got {max_calls!r}')
        self.client, self.model_id, self.tools = bedrock_client, model_id, tools
        self.max_calls, self.max_result_chars, self.max_tokens = max_calls, max_result_chars, max_tokens
        self.max_retries, self.retry_wait, self.on_slow_query, self._sleep = max_retries, retry_wait, on_slow_query, sleep
        spec = next((t for t in tools.list_tools() if t.get('name') == QUERY_TOOL), None)
        if spec is None:
            raise AgentError(f'the MCP server offers no {QUERY_TOOL} tool')
        # Only the query string: the agent inlines values, and Converse need not accept
        # the server's optional 'parameters' schema (anyOf object/null).
        self.tool_config = {'tools': [{'toolSpec': {
            'name': QUERY_TOOL, 'description': spec.get('description') or 'Run an openCypher query.',
            'inputSchema': {'json': {'type': 'object', 'required': ['query'],
                                     'properties': {'query': {'type': 'string',
                                                              'description': 'A read-only openCypher query.'}}}}}}]}
        self.system = [{'text': SYSTEM_PROMPT.format(qualifiers=QUALIFIERS if temporal_qualifiers else '',
                                                     max_chars=max_result_chars, max_calls=max_calls)}]

    def answer(self, question: str, schema: Any) -> AgentResult:
        result = AgentResult(predictions=[])
        schema_text = schema if isinstance(schema, str) else json.dumps(schema, ensure_ascii=False)
        messages = [{'role': 'user', 'content': [{'text': f'Schema: {schema_text}\n\nQuestion: {question}'}]}]
        while True:
            if result.calls == self.max_calls - 1:
                messages[-1]['content'].append({'text': LAST_TURN})
            response = self._converse(modelId=self.model_id, system=self.system, messages=messages,
                                      toolConfig=self.tool_config, inferenceConfig={'maxTokens': self.max_tokens})
            result.calls += 1
            message = response['output']['message']
            messages.append(message)
            stop = response['stopReason']
            if stop in ('end_turn', 'stop_sequence'):
                result.predictions = parse_answers(''.join(b.get('text', '') for b in message['content']))
                result.stop = stop
                return result
            if stop != 'tool_use':  # max_tokens, content_filtered, malformed_*, ...
                raise AgentError(f'Converse stopped with {stop!r}')
            if result.calls >= self.max_calls:
                result.stop = 'call_limit'  # still querying on its last turn: no answer
                return result
            messages.append({'role': 'user', 'content': [
                self._run(block['toolUse'], result) for block in message['content'] if 'toolUse' in block]})

    def _converse(self, **params) -> Dict[str, Any]:
        attempts = max(self.max_retries, 1)
        for attempt in range(1, attempts + 1):
            try:
                return self.client.converse(**params)
            except Exception as e:
                if attempt == attempts or not _retryable(e):
                    raise
                logger.warning('Converse attempt %d/%d failed (%s); retrying in %.0f s',
                               attempt, attempts, e, self.retry_wait)
                self._sleep(self.retry_wait)

    def _run(self, use: Dict[str, Any], result: AgentResult) -> Dict[str, Any]:
        query = (use.get('input') or {}).get('query')
        step = {'query': query}
        if use.get('name') != QUERY_TOOL or not isinstance(query, str):
            text, status = f'Unknown tool call {use.get("name")!r}; use {QUERY_TOOL} with a query string.', 'error'
        elif read_only_violation(query):
            text, status = (f'Rejected: {read_only_violation(query)} is not allowed; '
                            f'only read-only queries run.'), 'error'
        else:
            try:
                reply = self.tools.call_tool(QUERY_TOOL, {'query': query})
            except McpTimeout:
                reply = {'content': [{'type': 'text', 'text': SLOW_QUERY}], 'isError': True, 'timed_out': True}
            rows = [c.get('text', '') for c in reply.get('content', []) if c.get('type') == 'text']
            status = 'error' if reply.get('isError') else 'success'
            text = '\n'.join(rows) if rows else ('[] (no rows)' if status == 'success' else 'The query failed.')
            if reply.get('timed_out') or (status == 'error' and re.search(r'time ?out|timed out', text, re.IGNORECASE)):
                step['timed_out'] = True
                if self.on_slow_query is not None:
                    self.on_slow_query(query)
            if len(text) > self.max_result_chars:
                text = text[:self.max_result_chars] + f'\n[cut at {self.max_result_chars} characters]'
            step['rows'] = len(rows)
            result.context.append(text)
        step['status'] = status
        result.trace.append(step)
        return {'toolResult': {'toolUseId': use['toolUseId'], 'content': [{'text': text}], 'status': status}}
