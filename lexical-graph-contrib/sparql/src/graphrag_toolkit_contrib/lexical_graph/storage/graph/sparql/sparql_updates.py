# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Native SPARQL updates for lexical-graph build operations.

Handlers consume structured GraphRAG parameters and render SPARQL Update
directly; no property-graph query is inspected. Related statements are batched
into one update request to avoid the many network round trips that RDFLib
``Graph.add``/``Graph.remove`` would otherwise require for a remote store.

All identifiers and literals pass through the ontology helpers, and tenant data
is written to a deterministic named graph.
"""

from typing import Any, Dict, List, Optional

from graphrag_toolkit.lexical_graph.storage.graph import GraphOperation

from .ontology import (
    DEFAULT_NAMESPACE,
    ID_KEY_TO_KIND,
    NamespaceConfig,
    RDF_TYPE,
    instance_iri,
    relation_iri,
    sparql_literal,
    sys_relation_iri,
    tenant_graph_iri,
    term,
)

_lit = sparql_literal


def render_update(operation: GraphOperation,
                  parameters: Dict[str, Any],
                  namespace: NamespaceConfig = DEFAULT_NAMESPACE,
                  tenant_id: Optional[str] = None) -> Optional[str]:
    """Render a lexical-graph operation as a native SPARQL update."""
    rows = _rows(parameters)
    if not rows:
        return None

    handler = _UPDATE_HANDLERS.get(operation)
    if handler is None:
        raise NotImplementedError(
            f'Operation {operation.value!r} is not implemented by the SPARQL backend'
        )

    graph = tenant_graph_iri(tenant_id, namespace)
    ops: List[str] = []
    for row in rows:
        update = handler(row, graph, namespace)
        if update:
            ops.append(update)

    return ' ;\n'.join(ops) if ops else None


def _source(row: Dict[str, Any], graph, namespace: NamespaceConfig) -> str:
    source_id = row['_source_id']
    props = []
    for key, value in row.items():
        if key.startswith('_'):
            continue
        lit = _lit(value)
        if lit is not None:
            props.append((term(_safe_local(key), namespace), lit))
    return _node_upsert('sourceId', source_id, 'Source', props, graph, namespace)


def _chunk(row: Dict[str, Any], graph, namespace: NamespaceConfig) -> str:
    props = []
    if row.get('text') is not None:
        props.append((term('value', namespace), _lit(row['text'])))
    for key, value in row.items():
        if key in ('chunk_id', 'text') or key.startswith('_'):
            continue
        lit = _lit(value)
        if lit is not None:
            props.append((term(_safe_local(key), namespace), lit))
    return _node_upsert('chunkId', row['chunk_id'], 'Chunk', props, graph, namespace)


def _topic(row: Dict[str, Any], graph, namespace: NamespaceConfig) -> str:
    ops = [_node_upsert('topicId', row['topic_id'], 'Topic',
                        [(term('value', namespace), _lit(row.get('title')))] if row.get('title') is not None else [],
                        graph, namespace)]
    topic_iri = instance_iri('topic', row['topic_id'], namespace)
    for chunk_ref in row.get('chunk_ids', []) or []:
        chunk_id = chunk_ref['chunk_id'] if isinstance(chunk_ref, dict) else chunk_ref
        chunk_iri = instance_iri('chunk', chunk_id, namespace)
        triples = [
            f'{chunk_iri} {RDF_TYPE} {term("Chunk", namespace)} .',
            f'{chunk_iri} {term("id", namespace)} {_lit(chunk_id)} .',
            f'{topic_iri} {term("topicMentionedIn", namespace)} {chunk_iri} .',
        ]
        ops.append(_insert_data('\n'.join(triples), graph))
    return ' ;\n'.join(ops)


def _statement(row: Dict[str, Any], graph, namespace: NamespaceConfig) -> str:
    props = []
    if row.get('value') is not None:
        props.append((term('value', namespace), _lit(row['value'])))
    if row.get('details') is not None:
        props.append((term('details', namespace), _lit(row['details'])))
    return _node_upsert('statementId', row['statement_id'], 'Statement', props, graph, namespace)


def _fact(row: Dict[str, Any], graph, namespace: NamespaceConfig) -> str:
    # Entity subjects and objects are linked separately; local values are literals.
    props = []
    if row.get('fact') is not None:
        props.append((term('value', namespace), _lit(row['fact'])))
    subject_literal = row.get('_subject_literal')
    if subject_literal is not None:
        props.append((term('subject', namespace), _lit(subject_literal)))
    object_literal = row.get('_object_literal')
    if object_literal is not None:
        props.append((term('object', namespace), _lit(object_literal)))
    predicate_value = row.get('_predicate')
    rel = relation_iri(predicate_value, namespace) if predicate_value is not None else None
    if rel is not None:
        props.append((term('predicate', namespace), rel))
    ops = [_node_upsert('factId', row['fact_id'], 'Fact', props, graph, namespace)]
    if rel is not None:
        ops.append(_insert_data('\n'.join([
            f'{rel} {RDF_TYPE} {term("Relation", namespace)} .',
            f'{rel} {term("value", namespace)} {_lit(predicate_value)} .',
        ]), graph))
    fact_iri = instance_iri('fact', row['fact_id'], namespace)
    stmt_iri = instance_iri('statement', row['statement_id'], namespace)
    triples = [
        f'{stmt_iri} {RDF_TYPE} {term("Statement", namespace)} .',
        f'{stmt_iri} {term("id", namespace)} {_lit(row["statement_id"])} .',
        f'{fact_iri} {term("supports", namespace)} {stmt_iri} .',
    ]
    ops.append(_insert_data('\n'.join(triples), graph))
    return ' ;\n'.join(ops)


def _entity(row: Dict[str, Any], graph, namespace: NamespaceConfig) -> str:
    props = []
    for key, pred in (('v', 'value'), ('e_search_str', 'search_str'), ('ec', 'class')):
        if row.get(key) is not None:
            props.append((term(pred, namespace), _lit(row[key])))
    return _node_upsert('entityId', row['e_id'], 'Entity', props, graph, namespace)


def _edge(row, graph, namespace: NamespaceConfig,
          a_key, a_param, b_key, b_param, predicate_name) -> Optional[str]:
    if a_param not in row or b_param not in row:
        return None
    a_kind, a_cls = _kind_cls(a_key)
    b_kind, b_cls = _kind_cls(b_key)
    a_iri = instance_iri(a_kind, row[a_param], namespace)
    b_iri = instance_iri(b_kind, row[b_param], namespace)
    predicate = term(predicate_name, namespace)
    triples = [
        f'{a_iri} {RDF_TYPE} {term(a_cls, namespace)} .',
        f'{a_iri} {term("id", namespace)} {_lit(row[a_param])} .',
        f'{b_iri} {RDF_TYPE} {term(b_cls, namespace)} .',
        f'{b_iri} {term("id", namespace)} {_lit(row[b_param])} .',
        f'{a_iri} {predicate} {b_iri} .',
    ]
    return _insert_data('\n'.join(triples), graph)


def _graph_summary(row: Dict[str, Any], graph, namespace: NamespaceConfig) -> str:
    two_class = row['sc_id'] != row['oc_id']
    delta = 1 if two_class else 2

    sc_iri = instance_iri('sysclass', row['sc_id'], namespace)
    ops = [
        _insert_data('\n'.join([
            f'{sc_iri} {RDF_TYPE} {term("SysClass", namespace)} .',
            f'{sc_iri} {term("id", namespace)} {_lit(row["sc_id"])} .',
            f'{sc_iri} {term("value", namespace)} {_lit(row.get("sc"))} .',
        ]), graph),
        _increment(sc_iri, term('count', namespace), delta, graph),
    ]

    object_class_id = row['oc_id'] if two_class else row['sc_id']
    if two_class:
        oc_iri = instance_iri('sysclass', row['oc_id'], namespace)
        ops.append(_insert_data('\n'.join([
            f'{oc_iri} {RDF_TYPE} {term("SysClass", namespace)} .',
            f'{oc_iri} {term("id", namespace)} {_lit(row["oc_id"])} .',
            f'{oc_iri} {term("value", namespace)} {_lit(row.get("oc"))} .',
        ]), graph))
        ops.append(_increment(oc_iri, term('count', namespace), delta, graph))
    else:
        oc_iri = sc_iri

    sysrel = sys_relation_iri(row['sc_id'], row.get('p'), object_class_id, namespace)
    sysrel_triples = [
        f'{sysrel} {RDF_TYPE} {term("SysRelation", namespace)} .',
        f'{sysrel} {term("sysRelSubject", namespace)} {sc_iri} .',
        f'{sysrel} {term("sysRelObject", namespace)} {oc_iri} .',
    ]
    if row.get('p') is not None:
        sysrel_triples.append(f'{sysrel} {term("value", namespace)} {_lit(row["p"])} .')
    ops.append(_insert_data('\n'.join(sysrel_triples), graph))
    ops.append(_increment(sysrel, term('count', namespace), delta, graph))
    return ' ;\n'.join(ops)


def _domain_type(row: Dict[str, Any], graph, namespace: NamespaceConfig) -> Optional[str]:
    entity_id = row.get('entityId')
    classification = row.get('_classification')
    if entity_id is None or not classification:
        return None
    entity_iri = instance_iri('entity', entity_id, namespace)
    triples = [
        f'{entity_iri} {RDF_TYPE} {term("Entity", namespace)} .',
        f'{entity_iri} {term("id", namespace)} {_lit(entity_id)} .',
        f'{entity_iri} {RDF_TYPE} {term(_safe_local(classification), namespace)} .',
    ]
    return _insert_data('\n'.join(triples), graph)


def _node_upsert(id_key, id_value, cls, props, graph, namespace: NamespaceConfig) -> str:
    ops = [_delete_prop(instance_iri(*_kind_cls_iri(id_key, id_value), namespace), pred, graph)
           for pred, _ in props]
    iri = instance_iri(*_kind_cls_iri(id_key, id_value), namespace)
    lines = [f'{iri} {RDF_TYPE} {term(cls, namespace)} .', f'{iri} {term("id", namespace)} {_lit(id_value)} .']
    lines.extend(f'{iri} {pred} {lit} .' for pred, lit in props)
    ops.append(_insert_data('\n'.join(lines), graph))
    return ' ;\n'.join(ops)


def _delete_prop(iri, predicate, graph) -> str:
    return f'DELETE WHERE {{ {_wrap(f"{iri} {predicate} ?o", graph)} }}'


def _insert_data(triples, graph) -> str:
    return f'INSERT DATA {{ {_wrap(triples, graph)} }}'


def _increment(iri, predicate, delta, graph) -> str:
    del_block = _wrap(f'{iri} {predicate} ?c', graph)
    ins_block = _wrap(f'{iri} {predicate} ?newc', graph)
    where = (f'{_wrap(f"OPTIONAL {{ {iri} {predicate} ?c0 }}", graph)} '
             f'BIND(COALESCE(?c0, 0) + {delta} AS ?newc) BIND(?c0 AS ?c)')
    return f'DELETE {{ {del_block} }} INSERT {{ {ins_block} }} WHERE {{ {where} }}'


def _wrap(pattern, graph) -> str:
    if graph:
        return f'GRAPH {graph} {{ {pattern} }}'
    return pattern


def _rows(parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
    if parameters is None:
        return []
    if 'params' in parameters:
        return parameters['params'] or []
    return [parameters] if parameters else []


def _kind_cls(id_key):
    return ID_KEY_TO_KIND[id_key]


def _kind_cls_iri(id_key, id_value):
    kind, _ = _kind_cls(id_key)
    return kind, id_value


def _safe_local(key) -> str:
    return ''.join(c if (c.isalnum() or c == '_') else '_' for c in str(key))


def _link_chunk_source(row, graph, namespace):
    return _edge(
        row, graph, namespace,
        'chunkId', 'chunk_id', 'sourceId', 'source_id', 'extractedFrom',
    )


def _link_chunks(row, graph, namespace):
    predicates = {
        'parent': 'parent',
        'child': 'child',
        'previous': 'chunkPrevious',
        'next': 'next',
    }
    relationship = str(row.get('_relationship_type', '')).lower()
    if relationship not in predicates:
        raise ValueError(f'Invalid chunk relationship type: {relationship!r}')
    return _edge(
        row, graph, namespace,
        'chunkId', 'chunk_id', 'chunkId', 'target_id', predicates[relationship],
    )


def _link_statement_chunk(row, graph, namespace):
    return _edge(
        row, graph, namespace,
        'statementId', 'statement_id', 'chunkId', 'chunk_id', 'statementMentionedIn',
    )


def _link_statement_topic(row, graph, namespace):
    return _edge(
        row, graph, namespace,
        'statementId', 'statement_id', 'topicId', 'topic_id', 'belongsTo',
    )


def _link_statements(row, graph, namespace):
    return _edge(
        row, graph, namespace,
        'statementId', 'statement_id', 'statementId', 'prev_statement_id',
        'statementPrevious',
    )


def _link_fact_entity(row, graph, namespace):
    relationship = str(row.get('_relationship_type', '')).lower()
    if relationship not in ('subject', 'object'):
        if not row:
            return None
        raise ValueError(f'Invalid fact relationship type: {relationship!r}')
    return _edge(
        row, graph, namespace,
        'factId', 'fact_id', 'entityId', 'entity_id', relationship,
    )


def _fact_relation_is_reified(row, graph, namespace):
    """The Fact resource already represents the entity relationship."""
    return None


def _copy_complement_relationships(row, graph, namespace):
    complement = instance_iri('entity', row['c_id'], namespace)
    resolved = instance_iri('entity', row['n_id'], namespace)
    predicate = term('object', namespace)
    existing = _wrap(f'?fact {predicate} {complement}', graph)
    old = _wrap(f'?fact {predicate} {complement}', graph)
    new = _wrap(f'?fact {predicate} {resolved}', graph)
    incoming = _wrap(f'?subject ?predicate {complement}', graph)
    return (
        f'DELETE {{ {old} }} INSERT {{ {new} }} WHERE {{ {existing} }} ;\n'
        f'DELETE WHERE {{ {incoming} }}'
    )


def _delete_complement(row, graph, namespace):
    complement = instance_iri('entity', row['c_id'], namespace)
    outgoing = _wrap(f'{complement} ?predicate ?object', graph)
    return f'DELETE WHERE {{ {outgoing} }}'


_UPDATE_HANDLERS = {
    GraphOperation.UPSERT_SOURCE: _source,
    GraphOperation.UPSERT_CHUNK: _chunk,
    GraphOperation.LINK_CHUNK_SOURCE: _link_chunk_source,
    GraphOperation.LINK_CHUNKS: _link_chunks,
    GraphOperation.UPSERT_TOPIC: _topic,
    GraphOperation.UPSERT_STATEMENT: _statement,
    GraphOperation.LINK_STATEMENT_CHUNK: _link_statement_chunk,
    GraphOperation.LINK_STATEMENT_TOPIC: _link_statement_topic,
    GraphOperation.LINK_STATEMENTS: _link_statements,
    GraphOperation.UPSERT_FACT: _fact,
    GraphOperation.LINK_FACT_ENTITY: _link_fact_entity,
    GraphOperation.UPSERT_ENTITY: _entity,
    GraphOperation.LINK_ENTITIES: _fact_relation_is_reified,
    GraphOperation.ADD_ENTITY_TYPE: _domain_type,
    GraphOperation.UPDATE_GRAPH_SUMMARY: _graph_summary,
    GraphOperation.COPY_COMPLEMENT_RELATIONSHIPS: _copy_complement_relationships,
    GraphOperation.DELETE_COMPLEMENT: _delete_complement,
}

UPDATE_OPERATIONS = frozenset(_UPDATE_HANDLERS)
