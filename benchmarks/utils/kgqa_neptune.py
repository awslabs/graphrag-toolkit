# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A KGQA graph in Neptune Analytics, for the LLM + graph-query baseline.

The BYOKG graph JSON is exported to openCypher CSV (nodes labeled Entity, keyed and
named by the entity name; one relationship per distinct triplet, typed by its
relation), published under the data prefix and loaded with CALL neptune.load.
CronQuestions' relations carry their time, e.g. 'award received (1990)': those are
loaded as the base relation with start_year/end_year/qualifier properties, matching
the base-relation schema BYOKG gives the model.

A marker node records which dataset the graph holds, so a later run reuses the load,
and the loader refuses a graph holding anything else rather than clearing it.
"""

import csv
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

EXPORT_VERSION = 1
NODE_LABEL = 'Entity'
MARKER_LABEL = 'KGQADataset'
MARKER_ID = 'kgqa-dataset-marker'
# Rows per edges file, each loaded by its own request: one request takes about 2.5 GB
# of S3 data per 128 m-NCU, and the stack's graph has 16.
EDGE_PART_ROWS = 500_000
_QUALIFIER = re.compile(r'^(.*\S)\s*\((\d{1,4})(?:\s*-\s*(\d{1,4}))?\)$')


def split_qualifier(relation: str) -> Tuple[str, str, int, int]:
    """'member of sports team (1983 - 1991)' -> ('member of sports team', '1983 - 1991', 1983, 1991)."""
    match = _QUALIFIER.match(relation)
    if not match:
        raise ValueError(f'relation {relation!r} has no (year) or (year - year) qualifier')
    start = int(match.group(2))
    end = int(match.group(3)) if match.group(3) else start
    return match.group(1), relation[match.end(1):].strip()[1:-1].strip(), start, end


def _one_line(name: str) -> str:
    # A CSV value spanning lines is undefined behavior in Neptune Analytics.
    return ' '.join(name.splitlines()) if ('\n' in name or '\r' in name) else name


def graph_facts(graph: Dict[str, Any]) -> Tuple[List[str], List[Tuple[str, str, str]]]:
    """Every node and every distinct (head, relation, tail), in a stable order."""
    nodes, facts = set(), set()
    for rels in graph.values():
        for value in rels.values():
            for head, relation, tail in value.get('triplets', []):
                head, tail = _one_line(head), _one_line(tail)
                nodes.update((head, tail))
                facts.add((head, relation, tail))
    nodes.update(_one_line(n) for n in graph)  # heads with no facts are still entities
    return sorted(nodes), sorted(facts)


def export_csv(graph: Dict[str, Any], out_dir: str, dataset: str, graph_sha256: str,
               temporal_qualifiers: bool) -> Dict[str, Any]:
    """Write nodes/, edges/part-NNNN/ and marker/ under out_dir, then manifest.json last
    (its presence marks a complete export). Returns the manifest."""
    nodes, facts = graph_facts(graph)
    os.makedirs(os.path.join(out_dir, 'nodes'), exist_ok=True)
    with open(os.path.join(out_dir, 'nodes', 'nodes.csv'), 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow([':ID', 'name:String', ':LABEL'])
        w.writerows((n, n, NODE_LABEL) for n in nodes)
    header = [':START_ID', ':END_ID', ':TYPE'] + (
        ['qualifier:String', 'start_year:Int', 'end_year:Int'] if temporal_qualifiers else [])
    parts = []
    for start in range(0, len(facts), EDGE_PART_ROWS):
        part = f'part-{len(parts):04d}'
        os.makedirs(os.path.join(out_dir, 'edges', part), exist_ok=True)
        with open(os.path.join(out_dir, 'edges', part, 'edges.csv'), 'w', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            w.writerow(header)
            for head, relation, tail in facts[start:start + EDGE_PART_ROWS]:
                if temporal_qualifiers:
                    base, qualifier, first, last = split_qualifier(relation)
                    w.writerow([head, tail, base, qualifier, first, last])
                else:
                    w.writerow([head, tail, relation])
        parts.append(part)
    manifest = {'dataset': dataset, 'graph_sha256': graph_sha256, 'export_version': EXPORT_VERSION,
                'temporal_qualifiers': temporal_qualifiers, 'nodes': len(nodes), 'edges': len(facts),
                'edge_parts': parts}
    os.makedirs(os.path.join(out_dir, 'marker'), exist_ok=True)
    with open(os.path.join(out_dir, 'marker', 'marker.csv'), 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow([':ID', 'dataset:String', 'graph_sha256:String', 'export_version:Int',
                    'nodes:Long', 'edges:Long', ':LABEL'])
        w.writerow([MARKER_ID, dataset, graph_sha256, EXPORT_VERSION, len(nodes), len(facts), MARKER_LABEL])
    with open(os.path.join(out_dir, 'manifest.json'), 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2)
    return manifest


def export_key(graph_sha256: str) -> str:
    return f'{graph_sha256[:16]}-v{EXPORT_VERSION}'


class NeptuneGraph:
    """openCypher against one Neptune Analytics graph, through boto3 neptune-graph."""

    def __init__(self, client, graph_id: str, load_client=None):
        if not re.fullmatch(r'g-[a-z0-9]{10}', graph_id):
            raise ValueError(f'expected a Neptune Analytics graph id like g-0123456789, got {graph_id!r}')
        self.client, self.graph_id = client, graph_id
        self.load_client = load_client or client  # loads run long and must not be retried

    def query(self, query: str, client=None) -> List[Dict[str, Any]]:
        response = (client or self.client).execute_query(graphIdentifier=self.graph_id, queryString=query,
                                                         language='OPEN_CYPHER')
        return json.loads(response['payload'].read())['results']

    def count(self, query: str) -> int:
        rows = self.query(query)
        if len(rows) != 1 or len(rows[0]) != 1:
            raise ValueError(f'expected one count from {query!r}, got {rows!r}')
        return int(next(iter(rows[0].values())))

    def marker(self) -> Optional[Dict[str, Any]]:
        rows = self.query(f'MATCH (m:{MARKER_LABEL}) RETURN m.dataset AS dataset, m.graph_sha256 AS graph_sha256, '
                          f'm.export_version AS export_version, m.nodes AS nodes, m.edges AS edges')
        if len(rows) > 1:
            raise RuntimeError(f'graph {self.graph_id} holds {len(rows)} dataset markers: {rows!r}')
        return rows[0] if rows else None

    def holds_other_data(self) -> bool:
        return bool(self.query(f'MATCH (n) WHERE NOT n:{NODE_LABEL} AND NOT n:{MARKER_LABEL} RETURN 1 LIMIT 1'))

    def is_empty(self) -> bool:
        return not self.query('MATCH (n) RETURN 1 LIMIT 1')

    def counts(self) -> Dict[str, int]:
        return {'nodes': self.count(f'MATCH (n:{NODE_LABEL}) RETURN count(n) AS n'),
                'edges': self.count(f'MATCH (:{NODE_LABEL})-[r]->(:{NODE_LABEL}) RETURN count(r) AS n')}

    def cancel(self, query: str) -> int:
        """Cancel running or queued copies of a query, e.g. one its caller gave up on, so
        it doesn't hold a query worker until the graph's own timeout. Returns how many
        were cancelled; without permission to list or cancel queries, warns and returns 0."""
        try:
            listed = self.client.list_queries(graphIdentifier=self.graph_id, maxResults=100, state='ALL')['queries']
            matches = [q['id'] for q in listed
                       if q.get('state') in ('RUNNING', 'WAITING') and (q.get('queryString') or '').strip() == query.strip()]
            for query_id in matches:
                self.client.cancel_query(graphIdentifier=self.graph_id, queryId=query_id)
        except Exception as e:  # e.g. AccessDeniedException without neptune-graph:ListQueries/CancelQuery
            logger.warning('could not cancel a slow query on %s: %s', self.graph_id, e)
            return 0
        return len(matches)

    def load(self, source: str, region: str) -> Dict[str, Any]:
        """CALL neptune.load on one S3 prefix; raises on any insert error."""
        if "'" in source or "'" in region:
            raise ValueError(f'unexpected quote in {source!r} / {region!r}')
        rows = self.query(f"CALL neptune.load({{source: '{source}', region: '{region}', format: 'opencypher', "
                          f"failOnError: true, concurrency: 1}})", client=self.load_client)
        summary = rows[0] if rows else {}
        if len(summary) == 1 and isinstance(next(iter(summary.values())), dict):
            summary = next(iter(summary.values()))  # some builds wrap the summary in one column
        errors = summary.get('insertErrors')
        if errors:
            raise RuntimeError(f'neptune.load of {source} reported {errors} insert errors: {summary!r}')
        logger.info('loaded %s: %s', source, summary)
        return summary


def ensure_loaded(graph: NeptuneGraph, manifest: Dict[str, Any], s3_prefix: str, region: str) -> Dict[str, int]:
    """Make the graph hold exactly this export; returns its node and edge counts.

    A graph already marked with this dataset and export is checked, not reloaded. One
    holding anything else fails: clearing a graph is left to whoever owns it.
    """
    expected = {'nodes': manifest['nodes'], 'edges': manifest['edges']}
    marker = graph.marker()
    if marker is not None:
        wanted = {k: manifest[k] for k in ('dataset', 'graph_sha256', 'export_version', 'nodes', 'edges')}
        if {k: marker.get(k) for k in wanted} != wanted:
            raise RuntimeError(f'graph {graph.graph_id} holds another KGQA load ({marker!r}), not {wanted!r}; '
                               f'use a graph of its own for each dataset')
        if graph.holds_other_data():
            raise RuntimeError(f'graph {graph.graph_id} holds nodes besides the {manifest["dataset"]} load')
    else:
        if not graph.is_empty():
            raise RuntimeError(f'graph {graph.graph_id} is not empty and holds no KGQA load; the '
                               f'graph-query baseline needs a graph of its own for each dataset')
        prefix = s3_prefix.rstrip('/')
        graph.load(f'{prefix}/nodes/', region)
        for part in manifest['edge_parts']:
            graph.load(f'{prefix}/edges/{part}/', region)
        graph.load(f'{prefix}/marker/', region)  # last: marks the load complete
    counts = graph.counts()
    if counts != expected:
        raise RuntimeError(f'graph {graph.graph_id} has {counts}, the export has {expected}')
    return counts


def upload_export(s3_client, local_dir: str, s3_prefix: str) -> None:
    """Publish an export; manifest.json goes last, so a complete prefix has one."""
    bucket, _, key = s3_prefix[len('s3://'):].partition('/')
    files = [os.path.relpath(os.path.join(root, name), local_dir).replace(os.sep, '/')
             for root, _, names in os.walk(local_dir) for name in names]
    for rel in sorted(files, key=lambda r: (r == 'manifest.json', r)):
        s3_client.upload_file(os.path.join(local_dir, *rel.split('/')), bucket, f"{key.rstrip('/')}/{rel}")


def published_manifest(s3_client, s3_prefix: str) -> Optional[Dict[str, Any]]:
    bucket, _, key = s3_prefix[len('s3://'):].partition('/')
    try:
        body = s3_client.get_object(Bucket=bucket, Key=f"{key.rstrip('/')}/manifest.json")['Body'].read()
    except s3_client.exceptions.NoSuchKey:
        return None
    return json.loads(body)
