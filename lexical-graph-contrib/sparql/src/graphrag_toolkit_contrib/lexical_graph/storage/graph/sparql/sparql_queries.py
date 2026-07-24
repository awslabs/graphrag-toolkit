# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Native SPARQL read operations for the lexical graph.

Each handler expresses one backend-neutral ``GraphOperation`` directly in
SPARQL and converts endpoint bindings into the result shape consumed by the
lexical-graph retrieval pipeline.

Values that must be embedded in ``VALUES`` or ``FILTER`` clauses are serialized
with ``sparql_literal``; limits are converted to integers before interpolation.
Tenant scoping is applied once in ``run_query`` through the SPARQL Protocol's
``default-graph-uri`` parameter.
"""

from typing import Any, Dict, List

from graphrag_toolkit.lexical_graph.indexing.constants import LOCAL_ENTITY_CLASSIFICATION
from graphrag_toolkit.lexical_graph.storage.graph import GraphOperation
from graphrag_toolkit.lexical_graph.versioning import (
    VALID_FROM,
    VALID_TO,
    EXTRACT_TIMESTAMP,
    BUILD_TIMESTAMP,
    VERSION_INDEPENDENT_ID_FIELDS,
    TIMESTAMP_LOWER_BOUND,
    TIMESTAMP_UPPER_BOUND,
)

from .ontology import DEFAULT_NAMESPACE, NamespaceConfig, sparql_literal, tenant_graph_iri


def run_query(operation: GraphOperation,
              client,
              parameters: Dict[str, Any],
              namespace: NamespaceConfig = DEFAULT_NAMESPACE,
              tenant_id: str = None) -> List[Dict[str, Any]]:
    """Execute a lexical-graph operation with its native SPARQL query."""
    handler = _QUERY_HANDLERS.get(operation)
    if handler is None:
        raise NotImplementedError(
            f'Operation {operation.value!r} is not implemented by the SPARQL backend'
        )
    graph = tenant_graph_iri(tenant_id, namespace)
    if graph:
        client = _DefaultGraphClient(client, graph[1:-1])
    return handler(client, parameters, namespace)


class _DefaultGraphClient:
    def __init__(self, client, graph_uri: str):
        self._client = client
        self._graph_uri = graph_uri

    def query(self, sparql):
        return self._client.query(sparql, default_graph=self._graph_uri)


def _param_rows(parameters):
    if 'params' in parameters:
        return parameters['params'] or []
    return [parameters] if parameters else []


def _positive_int(value, default: int, name: str) -> int:
    """Validate numeric clauses before inserting them into SPARQL syntax."""
    result = int(value) if value is not None else default
    if result < 1:
        raise ValueError(f'{name} must be greater than zero')
    return result


# Local-entity reconciliation -------------------------------------------------


def _complements_matching_subject(client, parameters, namespace: NamespaceConfig):
    """Local-entity rewrite lookup: real entities that have a local-entity twin
    (same search_str). Empty when local entities are disabled."""
    out = []
    lg = namespace.prefix_ref
    for row in _param_rows(parameters):
        n_id = row.get('nId')
        if not n_id:
            continue
        sparql = f'''{namespace.sparql_prefixes()}
SELECT ?n_id ?c_id WHERE {{
  ?n {lg}id ?n_id ; {lg}search_str ?ss ; {lg}class ?ncls .
  FILTER(?n_id = {sparql_literal(n_id)})
  FILTER(?ncls != "{LOCAL_ENTITY_CLASSIFICATION}")
  ?c {lg}search_str ?ss ; {lg}class "{LOCAL_ENTITY_CLASSIFICATION}" ; {lg}id ?c_id .
}}'''
        out.extend(client.query(sparql))
    return out


def _subjects_matching_complement(client, parameters, namespace: NamespaceConfig):
    """Local-entity rewrite lookup: pair of nodes by id. Empty unless both
    exist (i.e. a complement that also occurs as a real entity)."""
    out = []
    lg = namespace.prefix_ref
    for row in _param_rows(parameters):
        n_id, c_id = row.get('nId'), row.get('cId')
        if not n_id or not c_id:
            continue
        sparql = f'''{namespace.sparql_prefixes()}
SELECT ?n_id ?c_id WHERE {{
  ?n {lg}id ?n_id . FILTER(?n_id = {sparql_literal(n_id)})
  ?c {lg}id ?c_id . FILTER(?c_id = {sparql_literal(c_id)})
}}'''
        out.extend(client.query(sparql))
    return out


def _single_entity_based_graph_search(client,
                                      parameters,
                                      namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    start_id = parameters.get('startId')
    if not start_id:
        return []
    lg = namespace.prefix_ref
    fact_pattern = f'''
  ?entity {lg}id {sparql_literal(start_id)} .
  ?fact {lg}subject ?entity .'''
    return _statement_ids_for_fact_pattern(client, fact_pattern, parameters, namespace)


# Traversal searches ----------------------------------------------------------


def _multiple_entity_based_graph_search(client,
                                        parameters,
                                        namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    start_id = parameters.get('startId')
    end_ids = parameters.get('endIds', []) or []
    if not start_id or not end_ids:
        return []
    lg = namespace.prefix_ref
    end_values = ' '.join(sparql_literal(e) for e in end_ids)
    def connects(fact_var, e1, e2):
        # a reified fact linking two entities, in either direction
        return (f'{{ {{ {fact_var} {lg}subject {e1} ; {lg}object {e2} . }} '
                f'UNION {{ {fact_var} {lg}subject {e2} ; {lg}object {e1} . }} }}')

    fact_pattern = f'''
  ?start {lg}id {sparql_literal(start_id)} .
  VALUES ?endId {{ {end_values} }}
  ?end {lg}id ?endId .
  {{
    {connects('?fact', '?start', '?end')}
    UNION
    {{ ?mid a {lg}Entity . {connects('?fact', '?start', '?mid')} {connects('?fact2', '?mid', '?end')} }}
    UNION
    {{ ?mid a {lg}Entity . {connects('?fact1', '?start', '?mid')} {connects('?fact', '?mid', '?end')} }}
  }}'''
    return _statement_ids_for_fact_pattern(client, fact_pattern, parameters, namespace)


def _statement_ids_for_fact_pattern(client,
                                    fact_pattern: str,
                                    parameters,
                                    namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    limit = _positive_int(parameters.get('statementLimit'), 100, 'statementLimit')
    lg = namespace.prefix_ref
    sparql = f'''{namespace.sparql_prefixes()}
SELECT DISTINCT ?l WHERE {{
{fact_pattern}
  ?fact {lg}supports ?statement .
  {{
    {{
      ?statement {lg}id ?l .
    }}
    UNION
    {{
      ?statement {lg}statementPrevious ?previous .
      ?previous {lg}id ?l .
    }}
    UNION
    {{
      ?next {lg}statementPrevious ?statement ;
            {lg}id ?l .
    }}
  }}
}} LIMIT {limit}'''
    return [{'l': row['l']} for row in client.query(sparql) if row.get('l') is not None]


def _facts_for_statements(client,
                          parameters,
                          namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    statement_ids = parameters.get('statementIds', []) or []
    if not statement_ids:
        return []
    values = ' '.join(sparql_literal(s) for s in statement_ids)
    lg = namespace.prefix_ref
    sparql = f'''{namespace.sparql_prefixes()}
SELECT ?statementId ?factValue WHERE {{
  VALUES ?statementId {{ {values} }}
  ?l {lg}id ?statementId .
  ?f {lg}supports ?l .
  OPTIONAL {{ ?f {lg}value ?factValue }}
}}'''
    rows = client.query(sparql)
    grouped: Dict[str, List[str]] = {}
    for row in rows:
        sid = row['statementId']
        grouped.setdefault(sid, [])
        fact_value = row.get('factValue')
        if fact_value is not None and fact_value not in grouped[sid]:
            grouped[sid].append(fact_value)
    return [{'statementId': sid, 'facts': facts} for sid, facts in grouped.items()]


# Retrieval content and entity scoring ---------------------------------------


def _chunk_content(client,
                   parameters,
                   namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    chunk_ids = parameters.get('nodeIds', []) or []
    if not chunk_ids:
        return []
    values = ' '.join(sparql_literal(chunk_id) for chunk_id in chunk_ids)
    lg = namespace.prefix_ref
    sparql = f'''{namespace.sparql_prefixes()}
SELECT ?content WHERE {{
  VALUES ?chunkId {{ {values} }}
  ?chunk a {lg}Chunk ;
         {lg}id ?chunkId .
  OPTIONAL {{ ?chunk {lg}value ?content }}
}}'''
    return [{'content': row.get('content') or ''} for row in client.query(sparql)]


def _chunk_based_graph_search(client,
                              parameters,
                              namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    return _statement_ids_for_chunk_id(
        client,
        parameters.get('chunkId') or parameters.get('nodeId'),
        _positive_int(parameters.get('statementLimit'), 100, 'statementLimit'),
        namespace,
    )


def _topic_based_entity_network_search(client,
                                       parameters,
                                       namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    topic_id = parameters.get('nodeId')
    if not topic_id:
        return []
    lg = namespace.prefix_ref
    limit = _positive_int(parameters.get('statementLimit'), 100, 'statementLimit')
    sparql = f'''{namespace.sparql_prefixes()}
SELECT DISTINCT ?l WHERE {{
  ?topic a {lg}Topic ;
         {lg}id {sparql_literal(topic_id)} .
  ?statement a {lg}Statement ;
             {lg}belongsTo ?topic ;
             {lg}id ?l .
}} LIMIT {limit}'''
    return [{'l': row['l']} for row in client.query(sparql) if row.get('l') is not None]


def _topic_content(client,
                   parameters,
                   namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    topic_id = parameters.get('topicId')
    if not topic_id:
        return []
    lg = namespace.prefix_ref
    limit = _positive_int(parameters.get('statementLimit'), 100, 'statementLimit')
    sparql = f'''{namespace.sparql_prefixes()}
SELECT ?statement ?details (COUNT(DISTINCT ?fact) AS ?score) WHERE {{
  ?topic a {lg}Topic ;
         {lg}id {sparql_literal(topic_id)} .
  ?statementNode a {lg}Statement ;
                 {lg}belongsTo ?topic .
  OPTIONAL {{ ?statementNode {lg}value ?statement }}
  OPTIONAL {{ ?statementNode {lg}details ?details }}
  ?fact {lg}supports ?statementNode .
}} GROUP BY ?statement ?details
ORDER BY DESC(?score)
LIMIT {limit}'''
    return [
        {'statement': row.get('statement') or '', 'details': row.get('details') or ''}
        for row in client.query(sparql)
    ]


def _statement_ids_for_chunk_id(client,
                                chunk_id,
                                limit: int,
                                namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    if not chunk_id:
        return []
    lg = namespace.prefix_ref
    sparql = f'''{namespace.sparql_prefixes()}
SELECT DISTINCT ?l WHERE {{
  ?chunk a {lg}Chunk ;
         {lg}id {sparql_literal(chunk_id)} .
  ?statement a {lg}Statement ;
             {lg}belongsTo ?topic ;
             {lg}statementMentionedIn ?chunk ;
             {lg}id ?l .
}} LIMIT {limit}'''
    return [{'l': row['l']} for row in client.query(sparql) if row.get('l') is not None]


def _entities_for_keywords(client,
                           parameters,
                           namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    keyword = parameters.get('keyword')
    if not keyword:
        return []
    classification = parameters.get('classification')
    starts_with = parameters.get('_starts_with', False)
    class_starts_with = parameters.get('_classification_starts_with', False)
    lg = namespace.prefix_ref

    keyword_filter = (
        f'FILTER(STRSTARTS(?searchStr, {sparql_literal(keyword)}))'
        if starts_with
        else f'FILTER(?searchStr = {sparql_literal(keyword)})'
    )
    if classification is not None:
        class_filter = (
            f'FILTER(STRSTARTS(?class, {sparql_literal(classification)}))'
            if class_starts_with
            else f'FILTER(?class = {sparql_literal(classification)})'
        )
    else:
        class_filter = f'FILTER(?class != "{LOCAL_ENTITY_CLASSIFICATION}")'

    sparql = f'''{namespace.sparql_prefixes()}
SELECT ?entityId ?value ?class (COUNT(?fact) AS ?score) WHERE {{
  ?entity a {lg}Entity ;
          {lg}id ?entityId ;
          {lg}search_str ?searchStr ;
          {lg}class ?class .
  OPTIONAL {{ ?entity {lg}value ?value }}
  {keyword_filter}
  {class_filter}
  VALUES ?factPredicate {{ {lg}subject {lg}object }}
  ?fact ?factPredicate ?entity .
}} GROUP BY ?entityId ?value ?class
ORDER BY DESC(?score)'''
    return _entity_score_rows(client.query(sparql))


def _entities_for_chunk_ids(client,
                            parameters,
                            namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    node_ids = parameters.get('nodeIds', []) or []
    if not node_ids:
        return []
    values = ' '.join(sparql_literal(node_id) for node_id in node_ids)
    limit = _positive_int(parameters.get('limit'), 100, 'limit')
    lg = namespace.prefix_ref
    sparql = f'''{namespace.sparql_prefixes()}
SELECT ?entityId ?value ?class (COUNT(?fact) AS ?score) WHERE {{
  {{
    SELECT DISTINCT ?entity WHERE {{
      VALUES ?chunkId {{ {values} }}
      ?chunk a {lg}Chunk ;
             {lg}id ?chunkId .
      ?statement a {lg}Statement ;
                 {lg}statementMentionedIn ?chunk .
      ?matchedFact {lg}supports ?statement ;
                   ?matchedFactPredicate ?entity .
      VALUES ?matchedFactPredicate {{ {lg}subject {lg}object }}
    }}
  }}
  ?entity a {lg}Entity ;
          {lg}id ?entityId ;
          {lg}class ?class .
  OPTIONAL {{ ?entity {lg}value ?value }}
  VALUES ?factPredicate {{ {lg}subject {lg}object }}
  ?fact ?factPredicate ?entity .
  FILTER(?class != "{LOCAL_ENTITY_CLASSIFICATION}")
}} GROUP BY ?entityId ?value ?class
ORDER BY DESC(?score)
LIMIT {limit}'''
    return _entity_score_rows(client.query(sparql))


def _entities_for_topic_ids(client,
                            parameters,
                            namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    node_ids = parameters.get('nodeIds', []) or []
    if not node_ids:
        return []
    values = ' '.join(sparql_literal(node_id) for node_id in node_ids)
    limit = _positive_int(parameters.get('limit'), 100, 'limit')
    lg = namespace.prefix_ref
    sparql = f'''{namespace.sparql_prefixes()}
SELECT ?entityId ?value ?class (COUNT(?fact) AS ?score) WHERE {{
  {{
    SELECT DISTINCT ?entity WHERE {{
      VALUES ?topicId {{ {values} }}
      ?topic a {lg}Topic ;
             {lg}id ?topicId .
      ?statement a {lg}Statement ;
                 {lg}belongsTo ?topic .
      ?matchedFact {lg}supports ?statement ;
                   ?matchedFactPredicate ?entity .
      VALUES ?matchedFactPredicate {{ {lg}subject {lg}object }}
    }}
  }}
  ?entity a {lg}Entity ;
          {lg}id ?entityId ;
          {lg}class ?class .
  OPTIONAL {{ ?entity {lg}value ?value }}
  VALUES ?factPredicate {{ {lg}subject {lg}object }}
  ?fact ?factPredicate ?entity .
  FILTER(?class != "{LOCAL_ENTITY_CLASSIFICATION}")
}} GROUP BY ?entityId ?value ?class
ORDER BY DESC(?score)
LIMIT {limit}'''
    return _entity_score_rows(client.query(sparql))


def _entity_score_rows(rows) -> List[Dict[str, Any]]:
    out = []
    for row in rows:
        entity_id = row.get('entityId')
        if entity_id is None:
            continue
        out.append({
            'result': {
                'entity': {
                    'entityId': entity_id,
                    'value': row.get('value') or '',
                    'class': row.get('class') or '',
                },
                'score': float(row.get('score') or 0),
            },
        })
    return out


def _next_level_in_tree(client,
                        parameters,
                        namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    entity_ids = parameters.get('entityIds', []) or []
    if not entity_ids:
        return []
    excluded = set(parameters.get('excludeEntityIds', []) or [])
    num_neighbours = _positive_int(
        parameters.get('numNeighbours'), 5, 'numNeighbours',
    )
    values = ' '.join(sparql_literal(entity_id) for entity_id in entity_ids)
    exclude_filter = ''
    if excluded:
        excluded_values = ', '.join(sparql_literal(entity_id) for entity_id in excluded)
        exclude_filter = f'FILTER(?otherId NOT IN ({excluded_values}))'
    lg = namespace.prefix_ref
    sparql = f'''{namespace.sparql_prefixes()}
SELECT ?entityId ?value ?class ?otherId (COUNT(?fact) AS ?score) WHERE {{
  VALUES ?entityId {{ {values} }}
  ?entity a {lg}Entity ;
          {lg}id ?entityId .
  OPTIONAL {{ ?entity {lg}value ?value }}
  OPTIONAL {{ ?entity {lg}class ?class }}
  ?relFact {lg}subject ?entity ;
           {lg}object ?other .
  ?other a {lg}Entity ;
         {lg}id ?otherId ;
         {lg}class ?otherClass .
  FILTER(?otherClass != "{LOCAL_ENTITY_CLASSIFICATION}")
  {exclude_filter}
  VALUES ?factPredicate {{ {lg}subject {lg}object }}
  ?fact ?factPredicate ?other .
}} GROUP BY ?entityId ?value ?class ?otherId
ORDER BY ?entityId DESC(?score)'''
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in client.query(sparql):
        entity_id = row.get('entityId')
        other_id = row.get('otherId')
        if entity_id is None or other_id is None:
            continue
        result = grouped.setdefault(entity_id, {
            'result': {
                'entity': {
                    'entityId': entity_id,
                    'value': row.get('value') or '',
                    'class': row.get('class') or '',
                },
                'others': [],
            },
        })
        others = result['result']['others']
        if other_id not in others and len(others) < num_neighbours:
            others.append(other_id)
    return list(grouped.values())


def _expand_entities(client,
                     parameters,
                     namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    entity_ids = parameters.get('entityIds', []) or []
    if not entity_ids:
        return []
    values = ' '.join(sparql_literal(entity_id) for entity_id in entity_ids)
    lg = namespace.prefix_ref
    sparql = f'''{namespace.sparql_prefixes()}
SELECT ?entityId ?value ?class (COUNT(?fact) AS ?score) WHERE {{
  VALUES ?entityId {{ {values} }}
  ?entity a {lg}Entity ;
          {lg}id ?entityId ;
          {lg}class ?class .
  OPTIONAL {{ ?entity {lg}value ?value }}
  VALUES ?factPredicate {{ {lg}subject {lg}object }}
  ?fact ?factPredicate ?entity .
}} GROUP BY ?entityId ?value ?class'''
    return _entity_score_rows(client.query(sparql))


def _statements_grouped_by_topic_and_source(client,
                                            parameters,
                                            namespace: NamespaceConfig) -> List[Dict[str, Any]]:
    statement_ids = parameters.get('statementIds', []) or []
    if not statement_ids:
        return []

    values = ' '.join(sparql_literal(s) for s in statement_ids)
    lg = namespace.prefix_ref
    sparql = f'''{namespace.sparql_prefixes()}
SELECT DISTINCT ?statementId ?statementValue ?details ?chunkId ?topicId ?topicValue ?sourceId WHERE {{
  VALUES ?statementId {{ {values} }}
  ?statement {lg}id ?statementId ;
             {lg}belongsTo ?topic ;
             {lg}statementMentionedIn ?chunk .
  OPTIONAL {{ ?statement {lg}value ?statementValue }}
  OPTIONAL {{ ?statement {lg}details ?details }}
  ?topic {lg}id ?topicId .
  OPTIONAL {{ ?topic {lg}value ?topicValue }}
  ?chunk {lg}id ?chunkId ;
         {lg}extractedFrom ?source .
  ?source {lg}id ?sourceId .
}}'''
    rows = client.query(sparql)
    if not rows:
        return []

    source_ids = sorted({row['sourceId'] for row in rows if row.get('sourceId') is not None})
    chunk_ids = sorted({row['chunkId'] for row in rows if row.get('chunkId') is not None})
    source_props = _properties_by_id(client, 'Source', source_ids, namespace)
    include_chunk_details = parameters.get('_include_chunk_details', False)
    chunk_props = _properties_by_id(client, 'Chunk', chunk_ids, namespace) if include_chunk_details else {}

    grouped: Dict[str, Dict[str, Any]] = {}
    topic_indexes: Dict[str, Dict[str, Dict[str, Any]]] = {}
    chunk_seen: Dict[str, Dict[str, set]] = {}
    statement_seen: Dict[str, Dict[str, set]] = {}

    for row in rows:
        source_id = row['sourceId']
        source_metadata = dict(source_props.get(source_id, {}))
        source_metadata.setdefault('sourceId', source_id)

        if source_id not in grouped:
            grouped[source_id] = {
                'score': 0,
                'source': {
                    'sourceId': source_id,
                    'metadata': source_metadata,
                    'versioning': _versioning_from(source_metadata),
                },
                'topics': [],
            }
            topic_indexes[source_id] = {}
            chunk_seen[source_id] = {}
            statement_seen[source_id] = {}

        topic_id = row['topicId']
        topics_by_id = topic_indexes[source_id]
        if topic_id not in topics_by_id:
            topic = {
                'topic': row.get('topicValue') or '',
                'topicId': topic_id,
                'chunks': [],
                'statements': [],
            }
            topics_by_id[topic_id] = topic
            grouped[source_id]['topics'].append(topic)
            chunk_seen[source_id][topic_id] = set()
            statement_seen[source_id][topic_id] = set()

        topic = topics_by_id[topic_id]
        chunk_id = row['chunkId']
        if chunk_id not in chunk_seen[source_id][topic_id]:
            metadata = dict(chunk_props.get(chunk_id, {})) if include_chunk_details else {}
            if include_chunk_details:
                metadata.setdefault('chunkId', chunk_id)
            topic['chunks'].append({
                'chunkId': chunk_id,
                'value': None,
                'metadata': metadata,
            })
            chunk_seen[source_id][topic_id].add(chunk_id)

        statement_id = row['statementId']
        if statement_id not in statement_seen[source_id][topic_id]:
            topic['statements'].append({
                'statementId': statement_id,
                'statement': row.get('statementValue') or '',
                'facts': [],
                'details': row.get('details'),
                'chunkId': chunk_id,
                'score': 0,
            })
            statement_seen[source_id][topic_id].add(statement_id)

    results = []
    for result in grouped.values():
        result['score'] = sum(
            len(topic['statements']) / len(topic['chunks'])
            for topic in result['topics']
            if topic['chunks']
        )
        results.append({'result': result})

    results.sort(key=lambda r: r['result']['score'], reverse=True)
    limit = parameters.get('limit')
    return results[:_positive_int(limit, 1, 'limit')] if limit is not None else results


def _properties_by_id(client,
                      cls: str,
                      ids: List[str],
                      namespace: NamespaceConfig) -> Dict[str, Dict[str, Any]]:
    if not ids:
        return {}
    values = ' '.join(sparql_literal(i) for i in ids)
    lg = namespace.prefix_ref
    sparql = f'''{namespace.sparql_prefixes()}
SELECT ?id ?prop ?value WHERE {{
  VALUES ?id {{ {values} }}
  ?node a {lg}{cls} ;
        {lg}id ?id ;
        ?prop ?value .
  FILTER(STRSTARTS(STR(?prop), "{namespace.schema_namespace}"))
  FILTER(?prop != {lg}id)
  FILTER(isLiteral(?value))
}}'''
    out: Dict[str, Dict[str, Any]] = {i: {} for i in ids}
    for row in client.query(sparql):
        prop = _local_name(row.get('prop'), namespace)
        if prop:
            out.setdefault(row['id'], {})[prop] = row.get('value')
    return out


def _versioning_from(metadata: Dict[str, Any]) -> Dict[str, Any]:
    id_fields = metadata.get(VERSION_INDEPENDENT_ID_FIELDS, '')
    return {
        'valid_from': _int_or_default(metadata.get(VALID_FROM), TIMESTAMP_LOWER_BOUND),
        'valid_to': _int_or_default(metadata.get(VALID_TO), TIMESTAMP_UPPER_BOUND),
        'extract_timestamp': _int_or_default(metadata.get(EXTRACT_TIMESTAMP), TIMESTAMP_LOWER_BOUND),
        'build_timestamp': _int_or_default(metadata.get(BUILD_TIMESTAMP), TIMESTAMP_LOWER_BOUND),
        'id_fields': id_fields.split(';') if isinstance(id_fields, str) and id_fields else [],
    }


def _int_or_default(value, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _local_name(uri, namespace: NamespaceConfig) -> str:
    if not uri:
        return ''
    text = str(uri)
    if text.startswith(namespace.schema_namespace):
        return text[len(namespace.schema_namespace):]
    return text.rsplit('#', 1)[-1].rsplit('/', 1)[-1]


_QUERY_HANDLERS = {
    GraphOperation.FIND_COMPLEMENTS: _complements_matching_subject,
    GraphOperation.FIND_SUBJECTS: _subjects_matching_complement,
    GraphOperation.GET_STATEMENTS: _statements_grouped_by_topic_and_source,
    GraphOperation.GET_FACTS: _facts_for_statements,
    GraphOperation.GET_CHUNKS: _chunk_content,
    GraphOperation.GET_TOPIC: _topic_content,
    GraphOperation.SEARCH_BY_CHUNK: _chunk_based_graph_search,
    GraphOperation.SEARCH_BY_TOPIC: _topic_based_entity_network_search,
    GraphOperation.FIND_ENTITIES_BY_KEYWORD: _entities_for_keywords,
    GraphOperation.FIND_ENTITIES_BY_CHUNKS: _entities_for_chunk_ids,
    GraphOperation.FIND_ENTITIES_BY_TOPICS: _entities_for_topic_ids,
    GraphOperation.FIND_ENTITY_NEIGHBORS: _next_level_in_tree,
    GraphOperation.SCORE_ENTITIES: _expand_entities,
    GraphOperation.SEARCH_BY_ENTITY: _single_entity_based_graph_search,
    GraphOperation.SEARCH_BY_ENTITIES: _multiple_entity_based_graph_search,
}

QUERY_OPERATIONS = frozenset(_QUERY_HANDLERS)
