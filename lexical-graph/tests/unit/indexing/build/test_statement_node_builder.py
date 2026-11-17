# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode

from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.build.build_filters import BuildFilters
from graphrag_toolkit.lexical_graph.indexing.build.statement_node_builder import (
    StatementNodeBuilder,
)
from graphrag_toolkit.lexical_graph.indexing.constants import (
    LOCAL_ENTITY_CLASSIFICATION,
    TOPICS_KEY,
)
from graphrag_toolkit.lexical_graph.metadata import DefaultSourceMetadataFormatter
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
from graphrag_toolkit.lexical_graph.tenant_id import TenantId


def _builder(build_filters=None):
    return StatementNodeBuilder(
        id_generator=IdGenerator(tenant_id=TenantId()),
        build_filters=build_filters or BuildFilters(),
        source_metadata_formatter=DefaultSourceMetadataFormatter(),
    )


def _chunk(chunk_id, topics, source_id='source-1', source_metadata=None):
    metadata = {TOPICS_KEY: {'topics': topics}} if topics is not None else {}
    node = TextNode(id_=chunk_id, text='chunk text', metadata=metadata)
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(
        node_id=source_id,
        metadata=source_metadata or {},
    )
    return node


def _topic(value, *statements):
    return {'value': value, 'statements': list(statements)}


def _statement(value, details=None, facts=None):
    return {'value': value, 'details': details or [], 'facts': facts or []}


def _fact(subject, predicate, obj=None, complement=None):
    fact = {
        'subject': {'value': subject[0], 'classification': subject[1]},
        'predicate': {'value': predicate},
    }
    if obj:
        fact['object'] = {'value': obj[0], 'classification': obj[1]}
    if complement:
        fact['complement'] = complement
    return fact


def _split(nodes):
    statements = [n for n in nodes if 'statement' in n.metadata]
    facts = [n for n in nodes if 'fact' in n.metadata]
    return statements, facts


class TestStatementNodes:

    def test_name_and_metadata_keys(self):
        assert StatementNodeBuilder.name() == 'StatementNodeBuilder'
        assert StatementNodeBuilder.metadata_keys() == [TOPICS_KEY]

    def test_statement_node_ids_and_metadata(self):
        builder = _builder()
        topic_id = builder.id_generator.create_topic_id('source-1', 'Neptune')
        statement_id = builder.id_generator.create_statement_id(topic_id, 'Neptune is a graph database')

        [node] = builder.build_nodes([_chunk('c1', [_topic('Neptune', _statement('Neptune is a graph database'))])])

        assert node.id_ == statement_id
        assert node.text == 'Neptune is a graph database'
        assert node.metadata['source']['sourceId'] == 'source-1'
        assert node.metadata['statement']['statementId'] == statement_id
        assert node.metadata['statement']['topicId'] == topic_id
        assert node.metadata['statement']['chunkId'] == 'c1'
        assert node.metadata[INDEX_KEY] == {'index': 'statement', 'key': builder._clean_id(statement_id)}
        for key in (INDEX_KEY, 'statement', 'source'):
            assert key in node.excluded_embed_metadata_keys
            assert key in node.excluded_llm_metadata_keys

    def test_details_are_appended_to_the_text(self):
        [node] = _builder().build_nodes([_chunk('c1', [_topic('t', _statement('claim', details=['one', 'two']))])])

        assert node.text == 'claim\n\none\ntwo'

    def test_each_statement_points_to_the_previous_one_in_its_topic(self):
        chunk = _chunk('c1', [_topic('t', _statement('first'), _statement('second'))])

        first, second = _builder().build_nodes([chunk])

        assert NodeRelationship.PREVIOUS not in first.relationships
        assert second.relationships[NodeRelationship.PREVIOUS].node_id == first.id_

    def test_a_repeated_statement_is_one_node(self):
        chunks = [
            _chunk('c1', [_topic('t', _statement('same'))]),
            _chunk('c2', [_topic('t', _statement('same'))]),
        ]

        [node] = _builder().build_nodes(chunks)

        assert node.metadata['statement']['chunkId'] == 'c1'

    def test_ignored_topics_and_statements_are_skipped(self):
        filters = BuildFilters(
            topic_filter_fn=lambda t: t == 'skip me',
            statement_filter_fn=lambda s: s == 'drop',
        )
        chunk = _chunk('c1', [
            _topic('skip me', _statement('in a skipped topic')),
            _topic('t', _statement('keep'), _statement('drop')),
        ])

        nodes = _builder(filters).build_nodes([chunk])

        assert [n.text for n in nodes] == ['keep']

    def test_chunks_without_topics_produce_nothing(self):
        empty = _chunk('c2', None)
        empty.metadata[TOPICS_KEY] = {}

        assert _builder().build_nodes([_chunk('c1', None), empty, _chunk('c3', [])]) == []

    def test_source_metadata_is_added_to_the_statement(self):
        chunk = _chunk('c1', [_topic('t', _statement('s'))], source_metadata={'url': 'https://example.com'})

        [node] = _builder().build_nodes([chunk])

        assert node.metadata['source']['metadata'] == {'url': 'https://example.com'}


class TestFactNodes:

    def test_fact_with_an_object_gets_entity_ids_for_both_ends(self):
        builder = _builder()
        fact = _fact(('Neptune', 'Service'), 'runs on', obj=('AWS', 'Company'))

        statements, [node] = _split(builder.build_nodes([_chunk('c1', [_topic('t', _statement('s', facts=[fact]))])]))

        stored = node.metadata['fact']
        assert node.text == 'Neptune runs on AWS'
        assert stored['factId'] == builder.id_generator.create_fact_id(node.text)
        assert stored['statementId'] == statements[0].id_
        assert stored['subject']['entityId'] == builder.id_generator.create_entity_id('Neptune', 'Service')
        assert stored['object']['entityId'] == builder.id_generator.create_entity_id('AWS', 'Company')
        assert node.metadata[INDEX_KEY] == {'index': 'fact', 'key': builder._clean_id(stored['factId'])}

    def test_string_complement_becomes_a_local_entity(self):
        builder = _builder()
        fact = _fact(('Neptune', 'Service'), 'launched in', complement='2017')

        _, [node] = _split(builder.build_nodes([_chunk('c1', [_topic('t', _statement('s', facts=[fact]))])]))

        complement = node.metadata['fact']['complement']
        assert complement['classification'] == LOCAL_ENTITY_CLASSIFICATION
        assert complement['entityId'] == builder.id_generator.create_local_entity_id('source-1', '2017')
        assert complement['altEntityId'] == builder.id_generator.create_entity_id('2017', LOCAL_ENTITY_CLASSIFICATION)

    def test_local_entity_subject_is_scoped_to_its_source(self):
        builder = _builder()
        fact = _fact(('it', LOCAL_ENTITY_CLASSIFICATION), 'supports', obj=('Gremlin', 'Language'))

        _, [node] = _split(builder.build_nodes([_chunk('c1', [_topic('t', _statement('s', facts=[fact]))])]))

        assert node.metadata['fact']['subject']['entityId'] == builder.id_generator.create_local_entity_id('source-1', 'it')

    def test_a_repeated_fact_in_one_statement_is_one_node_but_kept_per_statement(self):
        fact = _fact(('Neptune', 'Service'), 'runs on', obj=('AWS', 'Company'))
        chunk = _chunk('c1', [_topic(
            't',
            _statement('first', facts=[fact, fact]),
            _statement('second', facts=[fact]),
        )])

        statements, facts = _split(_builder().build_nodes([chunk]))

        assert sorted(f.metadata['fact']['statementId'] for f in facts) == sorted(s.id_ for s in statements)

    def test_facts_come_after_statements_in_the_result(self):
        fact = _fact(('Neptune', 'Service'), 'runs on', obj=('AWS', 'Company'))

        nodes = _builder().build_nodes([_chunk('c1', [_topic('t', _statement('s', facts=[fact]))])])

        assert ['statement' in n.metadata for n in nodes] == [True, False]
