# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from urllib.parse import unquote, urlparse, urlunparse

from graphrag_toolkit.lexical_graph.storage.graph import (
    GraphStore,
    GraphStoreFactoryMethod,
    get_log_formatting,
)

from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_graph_store import (
    SPARQLDatabaseClient,
)

logger = logging.getLogger(__name__)

SPARQL_GRAPH_STORE_TYPE = 'sparql'
AWS_IAM_AUTH_TYPE = 'aws_iam'
BASIC_AUTH_TYPE = 'basic'
SUPPORTED_AUTH_TYPES = (AWS_IAM_AUTH_TYPE, BASIC_AUTH_TYPE)
SUPPORTED_ENDPOINT_SCHEMES = ('http', 'https')


class SPARQLGraphStoreFactory(GraphStoreFactoryMethod):
    """Create a graph store from an ordinary SPARQL endpoint URL."""

    def try_create(self, graph_info: str, **kwargs) -> GraphStore:
        if kwargs.get('graph_store_type') != SPARQL_GRAPH_STORE_TYPE:
            return None
        if not isinstance(graph_info, str):
            return None

        kwargs.pop('graph_store_type')
        parsed = urlparse(graph_info)
        if parsed.scheme not in SUPPORTED_ENDPOINT_SCHEMES or not parsed.hostname:
            raise ValueError('SPARQL endpoints must be absolute HTTP or HTTPS URLs')

        auth_type = kwargs.pop('auth_type', None)
        if auth_type is not None and auth_type not in SUPPORTED_AUTH_TYPES:
            raise ValueError(
                f'Unsupported SPARQL auth_type: {auth_type}. '
                f'Expected one of: {", ".join(SUPPORTED_AUTH_TYPES)}'
            )

        query_endpoint = _endpoint_url(parsed)
        update_endpoint = kwargs.pop('update_endpoint', None)
        if update_endpoint is not None:
            _validate_endpoint(update_endpoint, 'update_endpoint')

        username_arg = kwargs.pop('username', None)
        password_arg = kwargs.pop('password', None)
        username = (
            unquote(parsed.username) if parsed.username else username_arg
        ) or os.environ.get('SPARQL_USER')
        password = (
            unquote(parsed.password) if parsed.password else password_arg
        ) or os.environ.get('SPARQL_PASSWORD')

        if bool(username) != bool(password):
            raise ValueError('SPARQL Basic authentication requires both username and password')
        if auth_type == BASIC_AUTH_TYPE and not username:
            raise ValueError('auth_type="basic" requires username and password')

        headers = kwargs.get('headers') or {}
        has_authorization_header = any(
            name.lower() == 'authorization' for name in headers
        )
        if username and has_authorization_header:
            raise ValueError(
                'SPARQL Basic authentication cannot be combined with an Authorization header'
            )

        neptune_iam = auth_type == AWS_IAM_AUTH_TYPE
        if neptune_iam:
            if parsed.scheme != 'https':
                raise ValueError('Amazon Neptune IAM authentication requires an HTTPS endpoint')
            if username:
                raise ValueError(
                    'Amazon Neptune IAM authentication cannot be combined with Basic authentication'
                )
            if has_authorization_header:
                raise ValueError(
                    'Amazon Neptune IAM authentication cannot be combined with an Authorization header'
                )
            if update_endpoint is not None and update_endpoint != query_endpoint:
                raise ValueError(
                    'Amazon Neptune IAM uses one SPARQL endpoint for queries and updates'
                )

        kwargs.pop('config', None)
        logger.debug(
            'Opening SPARQL graph store [endpoint: %s, auth_type: %s]',
            query_endpoint,
            auth_type,
        )

        return SPARQLDatabaseClient(
            query_endpoint=query_endpoint,
            update_endpoint=update_endpoint,
            username=username,
            password=password,
            neptune_iam=neptune_iam,
            log_formatting=get_log_formatting(kwargs),
            **kwargs,
        )


def _validate_endpoint(endpoint: str, name: str) -> None:
    parsed = urlparse(endpoint)
    if parsed.scheme not in SUPPORTED_ENDPOINT_SCHEMES or not parsed.hostname:
        raise ValueError(f'{name} must be an absolute HTTP or HTTPS URL')


def _endpoint_url(parsed) -> str:
    if not parsed.username and not parsed.password:
        return urlunparse(parsed)

    host = parsed.hostname or ''
    if ':' in host and not host.startswith('['):
        host = f'[{host}]'
    netloc = host
    if parsed.port:
        netloc = f'{netloc}:{parsed.port}'
    return urlunparse(parsed._replace(netloc=netloc))
