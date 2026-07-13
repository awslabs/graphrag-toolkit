# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from llama_index.core.bridge.pydantic import Field, PrivateAttr, SecretStr

from graphrag_toolkit.lexical_graph.storage.graph import (
    GraphOperation,
    GraphStore,
    NodeId,
    format_id,
)

from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.ontology import (
    LEXICAL_BASE,
    LEXICAL_PREFIX,
    LEXICAL_SCHEMA,
    NamespaceConfig,
)
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_endpoint_client import (
    SPARQLEndpointClient,
)
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_queries import (
    QUERY_OPERATIONS,
    run_query,
)
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_updates import (
    UPDATE_OPERATIONS,
    render_update,
)

logger = logging.getLogger(__name__)


class SPARQLDatabaseClient(GraphStore):
    """Lexical graph store for standards-compatible SPARQL endpoints.

    Backend-neutral ``GraphOperation`` values select native SPARQL read and
    update implementations, which execute through the configured endpoint
    transport.
    """

    query_endpoint: str
    update_endpoint: Optional[str] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    timeout: float = 60.0
    headers: Dict[str, str] = Field(default_factory=dict)
    neptune_iam: bool = False
    region_name: Optional[str] = None
    lexical_prefix: str = LEXICAL_PREFIX
    lexical_schema_namespace: str = LEXICAL_SCHEMA
    lexical_instance_namespace: str = LEXICAL_BASE
    sparql_prefixes: Dict[str, str] = Field(default_factory=dict)

    _client: Optional[Any] = PrivateAttr(default=None)
    _namespace: Optional[NamespaceConfig] = PrivateAttr(default=None)

    def __init__(self,
                 query_endpoint: str,
                 update_endpoint: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 timeout: float = 60.0,
                 headers: Optional[Dict[str, str]] = None,
                 **kwargs) -> None:
        super().__init__(
            query_endpoint=query_endpoint,
            update_endpoint=update_endpoint,
            username=username,
            password=password,
            timeout=timeout,
            headers=headers or {},
            **kwargs,
        )

    def __getstate__(self):
        self._client = None
        return super().__getstate__()

    @property
    def client(self) -> SPARQLEndpointClient:
        if self._client is None:
            if self.neptune_iam:
                from .neptune_iam import NeptuneIAMStore

                self._client = SPARQLEndpointClient(store=NeptuneIAMStore(
                    self.query_endpoint,
                    region_name=self.region_name,
                    headers=dict(self.headers),
                    timeout=self.timeout,
                ))
            else:
                self._client = SPARQLEndpointClient(
                    query_endpoint=self.query_endpoint,
                    update_endpoint=self.update_endpoint,
                    username=self.username,
                    password=self.password.get_secret_value() if self.password is not None else None,
                    timeout=self.timeout,
                    headers=dict(self.headers),
                )
        return self._client

    @property
    def namespace(self) -> NamespaceConfig:
        if self._namespace is None:
            self._namespace = NamespaceConfig(
                prefix=self.lexical_prefix,
                schema_namespace=self.lexical_schema_namespace,
                instance_namespace=self.lexical_instance_namespace,
                extra_prefixes=dict(self.sparql_prefixes),
            )
        return self._namespace

    def node_id(self, id_name: str) -> NodeId:
        return format_id(id_name)

    def _execute_query(self,
                       sparql: str,
                       parameters: Optional[dict] = None,
                       correlation_id: Any = None) -> List[Any]:
        """Execute caller-supplied native SPARQL."""
        parameters = parameters or {}
        if parameters:
            raise ValueError(
                'Raw SPARQL parameter binding is not supported. Serialize values '
                'with RDFLib terms, or use a typed GraphOperation.'
            )

        query_id = uuid.uuid4().hex[:5]
        log_entry = self.log_formatting.format_log_entry(
            self._logging_prefix(query_id, correlation_id), sparql, parameters,
        )
        logger.debug(f'[{log_entry.query_ref}] Query: [query: {log_entry.query}, '
                     f'parameters: {log_entry.parameters}]')

        start = time.time()
        results = self.client.query(sparql)

        if logger.isEnabledFor(logging.DEBUG):
            elapsed = int((time.time() - start) * 1000)
            logger.debug(f'[{log_entry.query_ref}] {elapsed}ms -> {len(results)} row(s)')

        return results

    def _execute_operation(self,
                           operation: GraphOperation,
                           query: str,
                           parameters: Dict[str, Any],
                           correlation_id=None,
                           **kwargs) -> List[Any]:
        """Execute the native SPARQL implementation of a semantic operation."""
        del query  # Retained in the method signature for GraphStore compatibility.
        query_id = uuid.uuid4().hex[:5]
        log_entry = self.log_formatting.format_log_entry(
            self._logging_prefix(query_id, correlation_id), operation.value, parameters,
        )
        logger.debug(f'[{log_entry.query_ref}] Operation: [{operation.value}, '
                     f'parameters: {log_entry.parameters}]')

        start = time.time()
        tenant_id = kwargs.get('tenant_id') or str(self.tenant_id)
        if operation in UPDATE_OPERATIONS:
            update = render_update(
                operation,
                parameters,
                self.namespace,
                tenant_id=tenant_id,
            )
            if update:
                self.client.update(update)
            results: List[Any] = []
        elif operation in QUERY_OPERATIONS:
            results = run_query(
                operation,
                self.client,
                parameters,
                self.namespace,
                tenant_id=tenant_id,
            )
        else:
            raise NotImplementedError(
                f'Operation {operation.value!r} is not implemented by the SPARQL backend'
            )

        if logger.isEnabledFor(logging.DEBUG):
            elapsed = int((time.time() - start) * 1000)
            logger.debug(f'[{log_entry.query_ref}] {elapsed}ms -> {len(results)} row(s)')
        return results

    def __exit__(self, exception_type, exception_value, traceback):
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None
        return False
