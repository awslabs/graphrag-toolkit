# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from llama_index.core.schema import NodeWithScore, TextNode

from graphrag_toolkit.lexical_graph.retrieval.post_processors.bedrock_context_format import (
    BedrockContextFormat,
)


def _statement_node(source_id, text, details=None, metadata=None):
    statement = {'statement': text}
    if details is not None:
        statement['details'] = details
    node_metadata = {
        'source': {'sourceId': source_id, 'metadata': metadata or {}},
        'statement': statement,
    }
    return NodeWithScore(node=TextNode(text=text, metadata=node_metadata), score=1.0)


def _result_node(result):
    return NodeWithScore(node=TextNode(text='', metadata={'result': result}), score=1.0)


def _xml(*lines):
    return '\n'.join(lines)


def _texts(nodes):
    return [n.node.text for n in nodes]


class TestBedrockContextFormat:

    def test_class_name(self):
        assert BedrockContextFormat.class_name() == 'BedrockContextFormat'

    def test_empty_input_returns_placeholder(self):
        assert _texts(BedrockContextFormat().postprocess_nodes([])) == ['No relevant context']


class TestStatementFormatting:

    def test_groups_statements_by_source_in_first_seen_order(self):
        nodes = [
            _statement_node('s1', 'first', metadata={'url': 'u1', 'author': 'a1'}),
            _statement_node('s2', 'second', metadata={'url': 'u2'}),
            _statement_node('s1', 'third'),
        ]

        result = _texts(BedrockContextFormat().postprocess_nodes(nodes))

        assert result == [
            _xml(
                '<source_1>',
                '<source_1_metadata>',
                '\t<author>a1</author>',
                '\t<url>u1</url>',
                '</source_1_metadata>',
                '<statement_1.1>first</statement_1.1>',
                '<statement_1.2>third</statement_1.2>',
                '</source_1>',
            ),
            _xml(
                '<source_2>',
                '<source_2_metadata>',
                '\t<url>u2</url>',
                '</source_2_metadata>',
                '<statement_2.1>second</statement_2.1>',
                '</source_2>',
            ),
        ]

    def test_appends_details_with_newlines_flattened(self):
        nodes = [_statement_node('s1', 'claim', details='\n line one\nline two \n')]

        result = _texts(BedrockContextFormat().postprocess_nodes(nodes))[0]

        assert '<statement_1.1>claim (details: line one, line two)</statement_1.1>' in result

    def test_empty_details_leave_text_unchanged(self):
        nodes = [_statement_node('s1', 'claim', details='')]

        result = _texts(BedrockContextFormat().postprocess_nodes(nodes))[0]

        assert '<statement_1.1>claim</statement_1.1>' in result


class TestSearchResultFormatting:

    def test_formats_source_topic_and_statement_tree(self):
        result = {
            'source': {'sourceId': 's1', 'metadata': {'title': 'T', 'author': 'A'}},
            'topics': [
                {'topic': 'Topic "one"', 'statements': [
                    {'statement': 'alpha', 'chunkId': 'c1', 'statementId': 's1'},
                ]},
                {'topic': 'Topic two', 'statements': [
                    {'statement': 'beta', 'details': 'x\ny', 'chunkId': 'c1', 'statementId': 's2'},
                ]},
            ],
        }

        output = _texts(BedrockContextFormat().postprocess_nodes([_result_node(result)]))

        assert output == [
            _xml(
                '<source_1>',
                '<source_1_metadata>',
                '\t<author>A</author>',
                '\t<title>T</title>',
                '</source_1_metadata>',
                '<topic_1.1 name="Topic \'one\'">',
                '<statement_1.1.1>alpha</statement_1.1.1>',
                '</topic_1.1>',
                '<topic_1.2 name="Topic two">',
                '<statement_1.2.1>beta (details: x, y)</statement_1.2.1>',
                '</topic_1.2>',
                '</source_1>',
            )
        ]

    def test_orders_statements_by_chunk_then_statement_id(self):
        result = {
            'source': {'sourceId': 's1'},
            'topics': [{'topic': 't', 'statements': [
                {'statement': 'c2-s1', 'chunkId': 'c2', 'statementId': 's1'},
                {'statement': 'c1-s2', 'chunkId': 'c1', 'statementId': 's2'},
                {'statement': 'c1-s1', 'chunkId': 'c1', 'statementId': 's1'},
                {'statement': 'no-ids'},
            ]}],
        }

        output = _texts(BedrockContextFormat().postprocess_nodes([_result_node(result)]))[0]

        assert [line for line in output.splitlines() if line.startswith('<statement_')] == [
            '<statement_1.1.1>no-ids</statement_1.1.1>',
            '<statement_1.1.2>c1-s1</statement_1.1.2>',
            '<statement_1.1.3>c1-s2</statement_1.1.3>',
            '<statement_1.1.4>c2-s1</statement_1.1.4>',
        ]

    def test_one_output_node_per_result_node(self):
        nodes = [
            _result_node({'source': {'sourceId': 'a'}, 'topics': []}),
            _result_node({'source': {'sourceId': 'b'}, 'topics': []}),
        ]

        output = _texts(BedrockContextFormat().postprocess_nodes(nodes))

        assert output == ['<source_1>\n</source_1>', '<source_2>\n</source_2>']

    def test_tolerates_missing_source_metadata_and_text(self):
        result = {
            'source': None,
            'topics': [{'topic': None, 'statements': [{'statement': None}]}],
        }

        output = _texts(BedrockContextFormat().postprocess_nodes([_result_node(result)]))

        assert output == [
            _xml(
                '<source_1>',
                '<topic_1.1 name="">',
                '<statement_1.1.1></statement_1.1.1>',
                '</topic_1.1>',
                '</source_1>',
            )
        ]

    def test_result_nodes_take_precedence_over_statement_nodes(self):
        nodes = [
            _statement_node('legacy', 'legacy statement'),
            _result_node({'source': {'sourceId': 'r'}, 'topics': []}),
        ]

        output = _texts(BedrockContextFormat().postprocess_nodes(nodes))

        assert output == ['<source_1>\n</source_1>']
