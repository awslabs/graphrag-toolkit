# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from io import BytesIO
from typing import Any, Dict, List, Optional

import requests
from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib.query import Result
from requests.auth import HTTPBasicAuth

SPARQL_JSON = 'application/sparql-results+json'
FORM_URLENCODED = 'application/x-www-form-urlencoded'
_ACCEPT = f'{SPARQL_JSON}, text/turtle'
_SUPPORTED_QUERY_TYPES = frozenset({'ASK', 'SELECT'})


class RDFLibHTTPStore(SPARQLUpdateStore):
    """RDFLib remote store with a reusable, configurable HTTP session.

    The standard ``SPARQLUpdateStore`` provides query parsing and result
    handling. This subclass configures its HTTP calls with ``requests``
    authentication, headers, and timeouts.
    """

    def __init__(self,
                 query_endpoint: str,
                 update_endpoint: Optional[str] = None,
                 auth=None,
                 headers: Optional[Dict[str, str]] = None,
                 timeout: float = 60.0):
        super().__init__(
            query_endpoint,
            update_endpoint or query_endpoint,
            context_aware=False,
            returnFormat='json',
        )
        if timeout <= 0:
            raise ValueError('SPARQL endpoint timeout must be greater than zero')
        self._headers = dict(headers or {})
        self._timeout = timeout
        self._http = requests.Session()
        self._http.auth = auth

    def _query(self, query, default_graph=None, named_graph=None):
        self._queries += 1
        data = {'query': query}
        if default_graph is not None:
            data['default-graph-uri'] = default_graph
        if named_graph is not None:
            data['named-graph-uri'] = named_graph
        response = self._post(self.query_endpoint, data, {'Accept': _ACCEPT})
        content_type = response.headers.get('Content-Type', SPARQL_JSON).split(';')[0]
        return Result.parse(BytesIO(response.content), content_type=content_type)

    def query_with_default_graph(self, query: str, graph_uri: str):
        """Execute against a protocol-defined default graph.

        RDFLib only forwards ``queryGraph`` for context-aware stores. This
        transport deliberately is not context-aware, so tenant isolation uses
        the standard SPARQL Protocol ``default-graph-uri`` parameter directly.
        """
        return self._query(query, default_graph=graph_uri)

    def _update(self, update):
        self._updates += 1
        self._post(self.update_endpoint, {'update': update})

    def _post(self, endpoint, data, headers=None):
        response = self._http.post(
            endpoint,
            data=data,
            headers={
                **self._headers,
                'Content-Type': FORM_URLENCODED,
                **(headers or {}),
            },
            timeout=self._timeout,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f'SPARQL endpoint request failed [status: {response.status_code}, '
                f'body: {response.text.strip()[:500]}]'
            )
        return response

    def close(self, commit_pending_transaction=False):
        if commit_pending_transaction:
            self.commit()
        self._http.close()


class SPARQLEndpointClient:
    """Small SELECT/ASK and UPDATE facade over an RDFLib ``Graph``."""

    def __init__(self,
                 query_endpoint: Optional[str] = None,
                 update_endpoint: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 headers: Optional[Dict[str, str]] = None,
                 timeout: float = 60.0,
                 store: Optional[SPARQLUpdateStore] = None):
        if store is None:
            if query_endpoint is None:
                raise ValueError('SPARQL query endpoint is required')
            auth = HTTPBasicAuth(username, password) if username is not None else None
            store = RDFLibHTTPStore(
                query_endpoint, update_endpoint, auth, headers, timeout,
            )
        self._graph = Graph(store=store)

    @property
    def store(self):
        return self._graph.store

    def query(self,
              sparql: str,
              default_graph: Optional[str] = None) -> List[Dict[str, Any]]:
        if default_graph:
            result = self.store.query_with_default_graph(sparql, default_graph)
        else:
            result = self._graph.query(sparql)
        if result.type not in _SUPPORTED_QUERY_TYPES:
            raise ValueError(
                f'Only SELECT and ASK queries are supported, got {result.type!r}'
            )
        if result.type == 'ASK':
            return [{'boolean': bool(result.askAnswer)}]
        return [
            {str(var): self._coerce(value) for var, value in row.asdict().items()}
            for row in result
        ]

    def update(self, sparql: str) -> None:
        self._graph.update(sparql)

    def close(self) -> None:
        self._graph.close()

    @staticmethod
    def _coerce(value: Any) -> Any:
        value = value.toPython() if hasattr(value, 'toPython') else value
        return str(value) if hasattr(value, 'n3') else value
