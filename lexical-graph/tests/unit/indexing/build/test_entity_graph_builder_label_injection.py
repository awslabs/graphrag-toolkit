# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Drives the real EntityGraphBuilder with a malicious entity classification and
captures the Cypher it emits, asserting the domain-entity query is escaped and
parameterised (no label breakout, no inlined id literal).

The classification is ``__...__`` wrapped so label_from passes it through
unescaped; un-escaped it would close the SET label and append a DETACH DELETE."""

from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph.indexing.build.entity_graph_builder import (
    EntityGraphBuilder,
)
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import (
    label_from,
    escape_cypher_label,
)

MALICIOUS_CLASSIFICATION = '__Evil` WITH e MATCH (c:`__Canary__`) DETACH DELETE c //__'
ENTITY_ID = 'ent-deadbeef'


class _CapturingClient:
    def __init__(self):
        self.queries = []

    def node_id(self, name):
        return name

    def execute_query_with_retry(self, query, params, **kwargs):
        self.queries.append((query, params))
        return []


def _fact_node():
    return TextNode(
        text='x',
        metadata={
            'fact': {
                'factId': 'fact-1',
                'subject': {
                    'entityId': ENTITY_ID,
                    'value': 'Acme',
                    'classification': MALICIOUS_CLASSIFICATION,
                },
                'predicate': {'value': 'operates'},
                'object': None,
            }
        },
    )


def _domain_query(client):
    """Return the captured domain-entity statement (the one carrying awsqid)."""
    domain = [q for q, _ in client.queries if 'awsqid' in q]
    assert domain, 'expected a domain-entity query to be emitted'
    return domain[0]


def test_domain_entity_query_is_escaped_and_parameterised():
    """The real builder emits the escaped label (not the raw label-wrap) and binds
    entityId as a parameter instead of inlining it as a string literal."""
    client = _CapturingClient()
    EntityGraphBuilder().build(
        _fact_node(),
        client,
        include_domain_labels=True,
        include_local_entities=False,
    )

    query = _domain_query(client)

    safe = escape_cypher_label(label_from(MALICIOUS_CLASSIFICATION))
    raw = label_from(MALICIOUS_CLASSIFICATION)
    assert f':`{safe}`' in query
    assert f':`{raw}`' not in query
    assert '$entityId' in query
    assert f"'{ENTITY_ID}'" not in query


def test_label_from_passthrough_is_the_precondition():
    """Why escaping is required: label_from does not sanitise a __...__ value, so
    the dangerous backtick survives into the label and must be escaped downstream."""
    assert label_from(MALICIOUS_CLASSIFICATION) == MALICIOUS_CLASSIFICATION
    assert '`' in label_from(MALICIOUS_CLASSIFICATION)
