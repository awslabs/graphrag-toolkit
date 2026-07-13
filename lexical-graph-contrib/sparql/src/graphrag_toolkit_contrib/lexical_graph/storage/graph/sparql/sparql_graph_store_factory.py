# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from urllib.parse import parse_qs, urlencode, unquote, urlparse, urlunparse

from graphrag_toolkit.lexical_graph.storage.graph import (
    GraphStore,
    GraphStoreFactoryMethod,
    get_log_formatting,
)

from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_graph_store import (
    SPARQLDatabaseClient,
)

logger = logging.getLogger(__name__)

SPARQL_SCHEMES = ('sparql', 'sparql+s', 'sparql+http', 'sparql+https', 'sparql+neptune')


class SPARQLGraphStoreFactory(GraphStoreFactoryMethod):
    """Create a graph store from SPARQL query/update endpoint configuration."""

    def try_create(self, graph_info: str, **kwargs) -> GraphStore:
        if not isinstance(graph_info, str):
            return None

        parsed = urlparse(graph_info)
        if parsed.scheme not in SPARQL_SCHEMES:
            return None

        query_params = parse_qs(parsed.query)
        query_endpoint = _endpoint_url(parsed, query_params)
        update_endpoint = (
            kwargs.pop('update_endpoint', None)
            or _single_query_value(query_params, 'update_endpoint')
            or _single_query_value(query_params, 'update')
        )
        if update_endpoint:
            update_endpoint = unquote(update_endpoint)

        username_arg = kwargs.pop('username', None)
        password_arg = kwargs.pop('password', None)
        username = (
            unquote(parsed.username) if parsed.username else username_arg
        ) or os.environ.get('SPARQL_USER')
        password = (
            unquote(parsed.password) if parsed.password else password_arg
        ) or os.environ.get('SPARQL_PASSWORD')

        kwargs.pop('config', None)
        logger.debug(
            'Opening SPARQL graph store [scheme: %s, host: %s, path: %s]',
            parsed.scheme,
            parsed.hostname,
            parsed.path,
        )

        return SPARQLDatabaseClient(
            query_endpoint=query_endpoint,
            update_endpoint=update_endpoint,
            username=username,
            password=password,
            neptune_iam=(parsed.scheme == 'sparql+neptune'),
            log_formatting=get_log_formatting(kwargs),
            **kwargs,
        )


def _endpoint_url(parsed, query_params) -> str:
    scheme = {
        'sparql': 'http',
        'sparql+s': 'https',
        'sparql+http': 'http',
        'sparql+https': 'https',
        'sparql+neptune': 'https',
    }[parsed.scheme]
    host = parsed.hostname or ''
    if ':' in host and not host.startswith('['):
        host = f'[{host}]'
    netloc = host
    if parsed.port:
        netloc = f'{netloc}:{parsed.port}'
    endpoint_query = urlencode([
        (name, value)
        for name, values in query_params.items()
        if name not in ('update_endpoint', 'update')
        for value in values
    ])
    return urlunparse((scheme, netloc, parsed.path, '', endpoint_query, ''))


def _single_query_value(query_params, name: str):
    values = query_params.get(name)
    return values[0] if values else None
