# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import csv
import io
import json
import os

import pytest

from benchmarks.utils import kgqa_neptune as kn

GRAPH = {
    'Pat LaFontaine': {
        'award received (1995)': {'triplets': [['Pat LaFontaine', 'award received (1995)', 'Bill Masterton Memorial Trophy'],
                                               ['Pat LaFontaine', 'award received (1995)', 'Bill Masterton Memorial Trophy']]},
        'member of sports team (1983 - 1991)': {'triplets': [['Pat LaFontaine', 'member of sports team (1983 - 1991)',
                                                              'New York Islanders']]}},
    'Smith, "Jr."': {'award received (1990)': {'triplets': [['Smith, "Jr."', 'award received (1990)', 'Line\nBreak']]}},
    'Lonely': {},
}
SHA = 'ab' * 32


def _read(path):
    with open(path, encoding='utf-8', newline='') as f:
        return list(csv.reader(f))


class TestExport:
    def test_qualifiers_split_into_base_relation_and_years(self):
        assert kn.split_qualifier('member of sports team (1983 - 1991)') == ('member of sports team', '1983 - 1991', 1983, 1991)
        assert kn.split_qualifier('award received (1995)') == ('award received', '1995', 1995, 1995)
        with pytest.raises(ValueError, match='no \\(year\\)'):
            kn.split_qualifier('people.person.nationality')

    def test_facts_are_distinct_and_names_one_line(self):
        nodes, facts = kn.graph_facts(GRAPH)
        assert 'Line Break' in nodes and 'Lonely' in nodes
        assert len(facts) == 3, 'the duplicated triplet is one fact'

    def test_csv_quotes_names_and_marks_completion_last(self, tmp_path, monkeypatch):
        monkeypatch.setattr(kn, 'EDGE_PART_ROWS', 2)
        manifest = kn.export_csv(GRAPH, str(tmp_path), 'cronquestions', SHA, temporal_qualifiers=True)
        assert manifest == {'dataset': 'cronquestions', 'graph_sha256': SHA, 'export_version': kn.EXPORT_VERSION,
                            'temporal_qualifiers': True, 'nodes': 6, 'edges': 3, 'edge_parts': ['part-0000', 'part-0001']}
        nodes = _read(tmp_path / 'nodes' / 'nodes.csv')
        assert nodes[0] == [':ID', 'name:String', ':LABEL']
        assert ['Smith, "Jr."', 'Smith, "Jr."', 'Entity'] in nodes  # the comma and quotes survive
        edges = _read(tmp_path / 'edges' / 'part-0000' / 'edges.csv') + _read(tmp_path / 'edges' / 'part-0001' / 'edges.csv')[1:]
        assert edges[0] == [':START_ID', ':END_ID', ':TYPE', 'qualifier:String', 'start_year:Int', 'end_year:Int']
        assert ['Pat LaFontaine', 'New York Islanders', 'member of sports team', '1983 - 1991', '1983', '1991'] in edges
        marker = _read(tmp_path / 'marker' / 'marker.csv')
        assert marker[1] == [kn.MARKER_ID, 'cronquestions', SHA, str(kn.EXPORT_VERSION), '6', '3', kn.MARKER_LABEL]
        assert json.loads((tmp_path / 'manifest.json').read_text()) == manifest

    def test_relations_are_kept_whole_without_qualifiers(self, tmp_path):
        graph = {'Japan': {'location.country.capital': {'triplets': [['Japan', 'location.country.capital', 'Tokyo']]}}}
        kn.export_csv(graph, str(tmp_path), 'webqsp', SHA, temporal_qualifiers=False)
        assert _read(tmp_path / 'edges' / 'part-0000' / 'edges.csv') == [
            [':START_ID', ':END_ID', ':TYPE'], ['Japan', 'Tokyo', 'location.country.capital']]


class _Payload:
    def __init__(self, results):
        self._body = json.dumps({'results': results}).encode()

    def read(self):
        return self._body


class FakeNeptune:
    """boto3 neptune-graph execute_query: responds by query, records every call."""

    def __init__(self, marker=None, empty=True, other=False, counts=(6, 3), load_summary=None):
        self.marker, self.empty, self.other, self.counts = marker, empty, other, list(counts)
        self.load_summary = load_summary or {'totalRecords': 1, 'insertErrors': 0}
        self.queries = []

    def execute_query(self, graphIdentifier, queryString, language):
        assert language == 'OPEN_CYPHER' and graphIdentifier == 'g-0123456789'
        self.queries.append(queryString)
        q = queryString
        if q.startswith('CALL neptune.load'):
            return {'payload': _Payload([self.load_summary])}
        if f'(m:{kn.MARKER_LABEL})' in q:
            return {'payload': _Payload([self.marker] if self.marker else [])}
        if 'WHERE NOT n:' in q:
            return {'payload': _Payload([{'1': 1}] if self.other else [])}
        if q == 'MATCH (n) RETURN 1 LIMIT 1':
            return {'payload': _Payload([] if self.empty else [{'1': 1}])}
        if 'count(n)' in q:
            return {'payload': _Payload([{'n': self.counts[0]}])}
        if 'count(r)' in q:
            return {'payload': _Payload([{'n': self.counts[1]}])}
        raise AssertionError(f'unexpected query {q!r}')


MANIFEST = {'dataset': 'cronquestions', 'graph_sha256': SHA, 'export_version': kn.EXPORT_VERSION,
            'temporal_qualifiers': True, 'nodes': 6, 'edges': 3, 'edge_parts': ['part-0000', 'part-0001']}
MARKER = {k: MANIFEST[k] for k in ('dataset', 'graph_sha256', 'export_version', 'nodes', 'edges')}


def _graph(client):
    return kn.NeptuneGraph(client, 'g-0123456789')


class TestLoad:
    def test_an_empty_graph_is_loaded_nodes_first_marker_last_then_counted(self):
        client = FakeNeptune()
        counts = kn.ensure_loaded(_graph(client), MANIFEST, 's3://bucket/data/neptune/key/', 'us-west-2')
        loads = [q for q in client.queries if q.startswith('CALL neptune.load')]
        assert [q.split("source: '")[1].split("'")[0] for q in loads] == [
            's3://bucket/data/neptune/key/nodes/', 's3://bucket/data/neptune/key/edges/part-0000/',
            's3://bucket/data/neptune/key/edges/part-0001/', 's3://bucket/data/neptune/key/marker/']
        assert all("format: 'opencypher'" in q and 'failOnError: true' in q and "region: 'us-west-2'" in q for q in loads)
        assert counts == {'nodes': 6, 'edges': 3}

    def test_a_graph_holding_this_load_is_checked_not_reloaded(self):
        client = FakeNeptune(marker=MARKER, empty=False)
        kn.ensure_loaded(_graph(client), MANIFEST, 's3://b/k', 'us-west-2')
        assert not any(q.startswith('CALL') for q in client.queries)

    def test_another_kgqa_load_is_refused(self):
        with pytest.raises(RuntimeError, match='holds another KGQA load'):
            kn.ensure_loaded(_graph(FakeNeptune(marker={**MARKER, 'dataset': 'cwq'}, empty=False)), MANIFEST, 's3://b/k', 'r')

    def test_other_data_beside_the_load_is_refused(self):
        with pytest.raises(RuntimeError, match='besides the cronquestions load'):
            kn.ensure_loaded(_graph(FakeNeptune(marker=MARKER, empty=False, other=True)), MANIFEST, 's3://b/k', 'r')

    def test_a_non_empty_graph_without_a_marker_is_never_loaded_into(self):
        client = FakeNeptune(empty=False)
        with pytest.raises(RuntimeError, match='is not empty'):
            kn.ensure_loaded(_graph(client), MANIFEST, 's3://b/k', 'r')
        assert not any(q.startswith('CALL') for q in client.queries)

    def test_counts_that_do_not_match_the_export_fail(self):
        with pytest.raises(RuntimeError, match="has {'nodes': 6, 'edges': 2}"):
            kn.ensure_loaded(_graph(FakeNeptune(counts=(6, 2))), MANIFEST, 's3://b/k', 'r')

    def test_insert_errors_fail(self):
        with pytest.raises(RuntimeError, match='insert errors'):
            kn.ensure_loaded(_graph(FakeNeptune(load_summary={'insertErrors': 4})), MANIFEST, 's3://b/k', 'r')

    def test_graph_id_is_validated(self):
        with pytest.raises(ValueError, match='graph id'):
            kn.NeptuneGraph(FakeNeptune(), 'neptune-graph://g-0123456789')


class FakeS3:
    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self, objects=None):
        self.objects, self.uploads = dict(objects or {}), []

    def upload_file(self, filename, bucket, key):
        with open(filename, 'rb') as f:
            self.objects[(bucket, key)] = f.read()
        self.uploads.append(key)

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise self.exceptions.NoSuchKey()
        return {'Body': io.BytesIO(self.objects[(Bucket, Key)])}


def test_publish_puts_the_manifest_last_and_reads_it_back(tmp_path):
    manifest = kn.export_csv(GRAPH, str(tmp_path), 'cronquestions', SHA, temporal_qualifiers=True)
    s3 = FakeS3()
    assert kn.published_manifest(s3, 's3://bucket/data/neptune/key') is None
    kn.upload_export(s3, str(tmp_path), 's3://bucket/data/neptune/key')
    assert s3.uploads[-1] == 'data/neptune/key/manifest.json' and len(s3.uploads) == 4
    assert kn.published_manifest(s3, 's3://bucket/data/neptune/key') == manifest
    assert os.path.exists(tmp_path / 'manifest.json')


class TestCancel:
    class Listing(FakeNeptune):
        def __init__(self, queries, deny=False):
            super().__init__()
            self.listed, self.deny, self.cancelled = queries, deny, []

        def list_queries(self, graphIdentifier, maxResults, state):
            if self.deny:
                raise Exception('AccessDeniedException: not authorized to perform neptune-graph:ListQueries')
            return {'queries': [q for q in self.listed if state == 'ALL' or q['state'] == state]}

        def cancel_query(self, graphIdentifier, queryId):
            self.cancelled.append(queryId)

    def test_only_running_or_queued_copies_of_the_query_are_cancelled(self):
        q = "MATCH p=(a:Entity {name: 'Jamaica'})-[*1..3]-(b) RETURN p"
        client = self.Listing([{'id': 'a', 'queryString': q, 'state': 'RUNNING'},
                               {'id': 'b', 'queryString': q + '\n', 'state': 'WAITING'},
                               {'id': 'c', 'queryString': q, 'state': 'CANCELLING'},
                               {'id': 'd', 'queryString': 'MATCH (n) RETURN n LIMIT 1', 'state': 'RUNNING'}])
        assert _graph(client).cancel(q) == 2 and client.cancelled == ['a', 'b']

    def test_without_permission_it_warns_and_goes_on(self, caplog):
        assert _graph(self.Listing([], deny=True)).cancel('MATCH (n) RETURN n') == 0
        assert 'could not cancel' in caplog.text
