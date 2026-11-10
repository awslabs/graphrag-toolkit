# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json

import pytest

from benchmarks.utils.graph_query_agent import (
    LAST_TURN, QUERY_TOOL, AgentError, GraphQueryAgent, parse_answers, read_only_violation,
)


def _turn(content, stop):
    """A Converse response as boto3 returns it."""
    return {'output': {'message': {'role': 'assistant', 'content': content}}, 'stopReason': stop,
            'usage': {'inputTokens': 900, 'outputTokens': 60, 'totalTokens': 960}, 'metrics': {'latencyMs': 800}}


def _query(query, use_id='tooluse_1', name=QUERY_TOOL):
    return {'toolUse': {'toolUseId': use_id, 'name': name, 'input': {'query': query}}}


class ScriptedConverse:
    def __init__(self, turns):
        self.turns, self.requests = list(turns), []

    def converse(self, **kwargs):
        self.requests.append(json.loads(json.dumps(kwargs)))  # a snapshot: messages grows later
        return self.turns.pop(0)


class FakeTools:
    """The Neptune MCP server's tools/list and tools/call results."""

    def __init__(self, rows=({'o.name': 'Australia'},), error=False):
        self.rows, self.error, self.calls = list(rows), error, []

    def list_tools(self):
        return [{'name': 'get_graph_status', 'description': 'Status.'},
                {'name': QUERY_TOOL, 'description': 'Executes the provided openCypher against the graph.',
                 'inputSchema': {'type': 'object', 'properties': {'query': {'type': 'string'},
                                                                   'parameters': {'anyOf': [{'type': 'object'}, {'type': 'null'}]}}}}]

    def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        if self.error:
            return {'content': [{'type': 'text', 'text': "Error executing tool run_opencypher_query: {'details': 'syntax'}"}],
                    'isError': True}
        return {'content': [{'type': 'text', 'text': json.dumps(r, indent=2)} for r in self.rows], 'isError': False}


QUERY = "MATCH (s:Entity {name: 'Buffyverse'})-[:`location.location.containedby`]->(o) RETURN o.name"


class TestReadOnly:
    @pytest.mark.parametrize('query', [
        QUERY,
        "MATCH (n:Entity {name: 'Set Theory'}) RETURN n.name",       # keyword inside a string
        'MATCH (n)-[:`create.thing`]->(m) RETURN m.name',              # keyword inside a backtick name
        "MATCH (n) // DELETE everything\nRETURN n LIMIT 1",            # keyword in a comment
        'MATCH (n:Entity) WHERE n.name STARTS WITH "Pat" RETURN n.name',
    ])
    def test_reads_pass(self, query):
        assert read_only_violation(query) is None

    @pytest.mark.parametrize('query, clause', [
        ("CREATE (:Entity {name: 'x'})", 'CREATE'), ("match (n) detach delete n", 'DETACH'),
        ("MATCH (n) SET n.name = 'x'", 'SET'), ("MERGE (n:Entity {name: 'x'})", 'MERGE'),
        ("CALL neptune.load({source: 's3://b/k'})", 'CALL'), ('MATCH (n) REMOVE n.name', 'REMOVE'),
    ])
    def test_writes_and_procedures_are_named(self, query, clause):
        assert read_only_violation(query) == clause


def test_answers_parse_like_byokg():
    assert parse_answers('thinking...\n<answers>\nAustralia\nEnglish\n</answers>') == ['Australia', 'English']
    assert parse_answers('no tags') == []


class TestLoop:
    def test_queries_then_answers(self):
        converse = ScriptedConverse([
            _turn([{'text': 'Let me look.'}, _query(QUERY)], 'tool_use'),
            _turn([{'text': '<answers>\nAustralia\n</answers>'}], 'end_turn')])
        tools = FakeTools()
        result = GraphQueryAgent(converse, 'us.anthropic.claude-haiku-4-5-20251001-v1:0', tools).answer(
            'Where is the Buffyverse set?', {'graphSummary': {'edgeLabels': ['location.location.containedby']}})
        assert (result.predictions, result.calls, result.stop) == (['Australia'], 2, 'end_turn')
        assert tools.calls == [(QUERY_TOOL, {'query': QUERY})]
        assert result.context == [json.dumps({'o.name': 'Australia'}, indent=2)]
        assert result.trace == [{'query': QUERY, 'rows': 1, 'status': 'success'}]
        first, second = converse.requests
        assert first['modelId'] == 'us.anthropic.claude-haiku-4-5-20251001-v1:0'
        assert first['inferenceConfig'] == {'maxTokens': 4096}
        assert 'location.location.containedby' in first['messages'][0]['content'][0]['text']
        spec = first['toolConfig']['tools'][0]['toolSpec']
        assert spec['name'] == QUERY_TOOL and list(spec['inputSchema']['json']['properties']) == ['query']
        # The tool result goes back as a user turn tied to the toolUseId.
        assert second['messages'][-1] == {'role': 'user', 'content': [{'toolResult': {
            'toolUseId': 'tooluse_1', 'content': [{'text': result.context[0]}], 'status': 'success'}}]}

    def test_write_queries_are_rejected_and_never_reach_the_server(self):
        converse = ScriptedConverse([_turn([_query("MATCH (n) DETACH DELETE n")], 'tool_use'),
                                     _turn([{'text': '<answers>\nx\n</answers>'}], 'end_turn')])
        tools = FakeTools()
        result = GraphQueryAgent(converse, 'm', tools).answer('q', {})
        assert tools.calls == [] and result.context == []
        reply = converse.requests[1]['messages'][-1]['content'][0]['toolResult']
        assert reply['status'] == 'error' and 'DETACH is not allowed' in reply['content'][0]['text']

    def test_failed_queries_reach_the_model_as_errors(self):
        converse = ScriptedConverse([_turn([_query(QUERY)], 'tool_use'),
                                     _turn([{'text': '<answers>\nx\n</answers>'}], 'end_turn')])
        cancelled = []
        result = GraphQueryAgent(converse, 'm', FakeTools(error=True), on_slow_query=cancelled.append).answer('q', {})
        reply = converse.requests[1]['messages'][-1]['content'][0]['toolResult']
        assert reply['status'] == 'error' and 'syntax' in reply['content'][0]['text']
        assert result.trace[0]['status'] == 'error' and 'timed_out' not in result.trace[0]
        assert cancelled == [], 'only a timed-out query is cancelled'

    def test_long_results_are_cut(self):
        converse = ScriptedConverse([_turn([_query(QUERY)], 'tool_use'),
                                     _turn([{'text': '<answers>\nx\n</answers>'}], 'end_turn')])
        rows = [{'o.name': f'Entity {i}'} for i in range(500)]
        result = GraphQueryAgent(converse, 'm', FakeTools(rows=rows), max_result_chars=100).answer('q', {})
        assert result.context[0].endswith('[cut at 100 characters]') and len(result.context[0]) < 140

    def test_the_last_turn_asks_for_an_answer_and_a_further_query_gets_none(self):
        converse = ScriptedConverse([_turn([_query(QUERY, 'a')], 'tool_use'), _turn([_query(QUERY, 'b')], 'tool_use'),
                                     _turn([_query(QUERY, 'c')], 'tool_use')])
        result = GraphQueryAgent(converse, 'm', FakeTools(), max_calls=3).answer('q', {})
        assert (result.predictions, result.calls, result.stop) == ([], 3, 'call_limit')
        assert converse.requests[2]['messages'][-1]['content'][-1] == {'text': LAST_TURN}
        assert all({'text': LAST_TURN} not in r['messages'][-1]['content'] for r in converse.requests[:2])

    def test_several_queries_in_one_turn_all_run(self):
        converse = ScriptedConverse([_turn([_query(QUERY, 'a'), _query('MATCH (n) RETURN n LIMIT 1', 'b')], 'tool_use'),
                                     _turn([{'text': '<answers>\nx\n</answers>'}], 'end_turn')])
        tools = FakeTools()
        GraphQueryAgent(converse, 'm', tools).answer('q', {})
        assert len(tools.calls) == 2
        assert [b['toolResult']['toolUseId'] for b in converse.requests[1]['messages'][-1]['content']] == ['a', 'b']

    @pytest.mark.parametrize('stop', ['max_tokens', 'content_filtered', 'malformed_tool_use'])
    def test_other_stops_fail_loud(self, stop):
        with pytest.raises(AgentError, match=stop):
            GraphQueryAgent(ScriptedConverse([_turn([{'text': '...'}], stop)]), 'm', FakeTools()).answer('q', {})


class TestSetup:
    def test_needs_the_query_tool(self):
        class NoQuery(FakeTools):
            def list_tools(self):
                return [{'name': 'get_graph_status'}]
        with pytest.raises(AgentError, match='no run_opencypher_query'):
            GraphQueryAgent(ScriptedConverse([]), 'm', NoQuery())

    def test_temporal_qualifiers_are_explained_only_when_loaded(self):
        plain = GraphQueryAgent(ScriptedConverse([]), 'm', FakeTools()).system[0]['text']
        temporal = GraphQueryAgent(ScriptedConverse([]), 'm', FakeTools(), temporal_qualifiers=True).system[0]['text']
        assert 'start_year' not in plain and 'start_year' in temporal
        assert "{name: 'Barack Obama'}" in plain and '15 turns' in plain

    def test_budget_must_allow_a_query_and_an_answer(self):
        with pytest.raises(ValueError, match='max_calls'):
            GraphQueryAgent(ScriptedConverse([]), 'm', FakeTools(), max_calls=1)


class _ClientError(Exception):
    """botocore ClientError's surface: a message and response['Error']['Code']."""

    def __init__(self, code, message):
        super().__init__(f'An error occurred ({code}) when calling the Converse operation: {message}')
        self.response = {'Error': {'Code': code, 'Message': message}}


class TestRobustness:
    def test_throttling_is_retried_like_byokg(self):
        class Throttled(ScriptedConverse):
            failures = 2

            def converse(self, **kwargs):
                if self.failures:
                    self.failures -= 1
                    raise _ClientError('ThrottlingException', 'Too many requests, please wait before trying again.')
                return super().converse(**kwargs)
        waits = []
        agent = GraphQueryAgent(Throttled([_turn([{'text': '<answers>\nx\n</answers>'}], 'end_turn')]), 'm', FakeTools(),
                                sleep=waits.append)
        assert agent.answer('q', {}).predictions == ['x'] and waits == [30.0, 30.0]

    def test_a_retryable_code_is_retried_whatever_its_message(self):
        class Unavailable(ScriptedConverse):
            failures = 1

            def converse(self, **kwargs):
                if self.failures:
                    self.failures -= 1
                    raise _ClientError('ServiceUnavailableException', 'Service is unavailable.')
                return super().converse(**kwargs)
        waits = []
        agent = GraphQueryAgent(Unavailable([_turn([{'text': '<answers>\nx\n</answers>'}], 'end_turn')]), 'm',
                                FakeTools(), sleep=waits.append)
        assert agent.answer('q', {}).predictions == ['x'] and waits == [30.0]

    def test_other_errors_and_exhausted_retries_raise(self):
        class Broken(ScriptedConverse):
            attempts = 0

            def converse(self, **kwargs):
                self.attempts += 1
                raise _ClientError('ValidationException', 'malformed request')
        broken, waits = Broken([]), []
        with pytest.raises(_ClientError, match='malformed'):
            GraphQueryAgent(broken, 'm', FakeTools(), sleep=waits.append).answer('q', {})
        assert (broken.attempts, waits) == (1, []), 'not retried'

        class AlwaysThrottled(ScriptedConverse):
            def converse(self, **kwargs):
                raise _ClientError('ThrottlingException', 'Too many requests')
        waits = []
        with pytest.raises(_ClientError):
            GraphQueryAgent(AlwaysThrottled([]), 'm', FakeTools(), max_retries=3, sleep=waits.append).answer('q', {})
        assert waits == [30.0, 30.0]

    def test_max_tokens_reaches_converse(self):
        converse = ScriptedConverse([_turn([{'text': '<answers>\nx\n</answers>'}], 'end_turn')])
        GraphQueryAgent(converse, 'm', FakeTools(), max_tokens=8192).answer('q', {})
        assert converse.requests[0]['inferenceConfig'] == {'maxTokens': 8192}

    def test_a_query_timeout_becomes_a_tool_error_and_is_cancelled(self):
        from benchmarks.utils.graph_query_agent import SLOW_QUERY
        from benchmarks.utils.mcp_stdio import McpTimeout

        class Slow(FakeTools):
            def call_tool(self, name, arguments):
                raise McpTimeout('tools/call: no response within 120 s')
        cancelled = []
        converse = ScriptedConverse([_turn([_query(QUERY)], 'tool_use'),
                                     _turn([{'text': '<answers>\nx\n</answers>'}], 'end_turn')])
        result = GraphQueryAgent(converse, 'm', Slow(), on_slow_query=cancelled.append).answer('q', {})
        reply = converse.requests[1]['messages'][-1]['content'][0]['toolResult']
        assert reply['status'] == 'error' and reply['content'][0]['text'] == SLOW_QUERY
        assert cancelled == [QUERY] and result.trace[0]['timed_out'] and result.predictions == ['x']

    def test_a_server_read_timeout_is_cancelled_too(self):
        class ReadTimeout(FakeTools):
            def call_tool(self, name, arguments):
                return {'content': [{'type': 'text', 'text': 'Error executing tool run_opencypher_query: '
                                     "{'details': 'Read timeout on endpoint URL'}"}], 'isError': True}
        cancelled = []
        converse = ScriptedConverse([_turn([_query(QUERY)], 'tool_use'),
                                     _turn([{'text': '<answers>\nx\n</answers>'}], 'end_turn')])
        GraphQueryAgent(converse, 'm', ReadTimeout(), on_slow_query=cancelled.append).answer('q', {})
        assert cancelled == [QUERY]
