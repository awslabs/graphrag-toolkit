# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Regression test for the domain-entity insert aborting batch build with
KeyError: 'params'.

When batch writes are enabled, GraphBatchClient.execute_query_with_retry reads
``properties['params']``. Every graph builder therefore wraps its params via
``_to_params`` and uses an ``UNWIND $params AS params`` query. The domain-entity
insert in EntityGraphBuilder used to break this contract - it passed a flat
``{'entityId': e_id}`` dict and a scalar ``$entityId`` query - so the first
domain-label insert raised ``KeyError: 'params'`` and took down the whole build
whenever ``include_domain_labels=True``.

This test drives the real builder through a real GraphBatchClient with batch
writes enabled: under the bug it raises KeyError; with the fix it queues the id
as a ``params`` row.
"""

from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph.indexing.build.entity_graph_builder import (
    EntityGraphBuilder,
)
from graphrag_toolkit.lexical_graph.indexing.build.graph_batch_client import (
    GraphBatchClient,
)

ENTITY_ID = 'ent-cafebabe'


class _MockStore:
    """Minimal graph store for GraphBatchClient to wrap. Batch inserts only
    queue into the client's ``batches`` dict, so the store itself is never
    queried during build()."""

    def node_id(self, name):
        return name


def _fact_node():
    return TextNode(
        text='x',
        metadata={
            'fact': {
                'factId': 'fact-1',
                'subject': {
                    'entityId': ENTITY_ID,
                    'value': 'Acme',
                    'classification': 'Company',
                },
                'predicate': {'value': 'operates'},
                'object': None,
            }
        },
    )


def test_domain_entity_insert_uses_batch_params_shape():
    """The real builder, driven through a real GraphBatchClient with batch
    writes enabled, must not raise KeyError: 'params' and must queue the
    domain-entity id in the ``{'params': [...]}`` shape the client requires."""
    batch_client = GraphBatchClient(
        graph_client=_MockStore(),
        batch_writes_enabled=True,
        batch_write_size=100,
    )

    # Under the bug this call raised KeyError: 'params'.
    EntityGraphBuilder().build(
        _fact_node(),
        batch_client,
        include_domain_labels=True,
        include_local_entities=False,
    )

    # The domain-entity query is the one carrying the awsqid comment.
    domain_batches = {q: p for q, p in batch_client.batches.items() if 'awsqid' in q}
    assert domain_batches, 'expected a domain-entity query to be queued'

    query, params = next(iter(domain_batches.items()))
    assert 'UNWIND $params AS params' in query
    assert 'params.entityId' in query
    assert '$entityId' not in query
    assert params == [{'entityId': ENTITY_ID}]


def _fact_node_multi(entity_ids):
    subjects = [
        {
            'entityId': eid,
            'value': f'Acme {i}',
            'classification': 'Company',
        }
        for i, eid in enumerate(entity_ids)
    ]
    return [
        TextNode(
            text='x',
            metadata={
                'fact': {
                    'factId': f'fact-{i}',
                    'subject': subject,
                    'predicate': {'value': 'operates'},
                    'object': None,
                }
            },
        )
        for i, subject in enumerate(subjects)
    ]


def test_domain_entity_inserts_batch_under_one_query_per_label():
    """Batch writes group param rows by the full query string. The query used
    to embed a fresh variable name and the per-entity id in its text, so every
    domain-label insert landed in its own single-row batch (#477). Inserts for
    distinct entities of the same classification must share one query entry."""
    batch_client = GraphBatchClient(
        graph_client=_MockStore(),
        batch_writes_enabled=True,
        batch_write_size=100,
    )

    builder = EntityGraphBuilder()
    for node in _fact_node_multi(['ent-1', 'ent-2', 'ent-3']):
        builder.build(
            node,
            batch_client,
            include_domain_labels=True,
            include_local_entities=False,
        )

    domain_batches = {q: p for q, p in batch_client.batches.items() if 'awsqid' in q}
    assert len(domain_batches) == 1, (
        f'expected one shared domain-label query, got {len(domain_batches)}: '
        f'{list(domain_batches)}'
    )
    query, params = next(iter(domain_batches.items()))
    assert sorted(p['entityId'] for p in params) == ['ent-1', 'ent-2', 'ent-3']
