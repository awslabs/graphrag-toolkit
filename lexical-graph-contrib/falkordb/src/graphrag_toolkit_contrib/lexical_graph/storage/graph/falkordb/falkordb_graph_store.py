# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import ast
import json
import logging
import re
import time
import uuid
from typing import Optional, Any, List, Union, Iterable, Mapping, Set, Tuple

from llama_index.core.bridge.pydantic import PrivateAttr

from graphrag_toolkit.lexical_graph.storage.graph import GraphStore, NodeId, format_id

logger = logging.getLogger(__name__)

try:
    import falkordb
    from falkordb.node import Node
    from falkordb.edge import Edge
    from falkordb.path import Path
    from falkordb.graph import Graph
    from redis.exceptions import ResponseError, AuthenticationError
except ImportError as e:
    raise ImportError(
        "FalkorDB and/or redis packages not found, install with 'pip install FalkorDB redis'"
    ) from e


DEFAULT_DATABASE_NAME = 'graphrag'
QUERY_RESULT_TYPE = Union[List[List[Node]], List[List[List[Path]]], List[List[Edge]]]

_CREATE_INDEX_PATTERN = re.compile(
    r"CREATE\s+(?:\w+\s+)?INDEX\s+FOR\s*\(\s*\w+\s*:\s*`?([^`)\s]+)`?\s*\)\s+ON\s*\(\s*\w+\.([A-Za-z_][A-Za-z0-9_]*)\s*\)",
    re.IGNORECASE,
)


class FalkorDBDatabaseClient(GraphStore):

    endpoint_url:str
    database:str
    username:Optional[str] = None
    password:Optional[str] = None
    ssl:Optional[bool] = False

    _client: Optional[Any] = PrivateAttr(default=None)

    """
    Client for interacting with a FalkorDB database.

    Provides methods to connect to a FalkorDB instance, execute queries, and handle authentication.
    """
    def __init__(self,
                 endpoint_url: str = None,
                 database: str = DEFAULT_DATABASE_NAME,
                 username: str = None,
                 password: str = None,
                 ssl: bool = False,
                 **kwargs
                 ) -> None:
        """
        Initialize the FalkorDB database client.

        :param endpoint_url: URL of the FalkorDB instance.
        :param database: Name of the database to connect to.
        :param username: Username for authentication.
        :param password: Password for authentication.
        :param ssl: Whether to use SSL for the connection.
        :param _client: Optional existing client instance.
        """
        if username and not password:
            raise ValueError("Password is required when username is provided")

        if endpoint_url and not isinstance(endpoint_url, str):
            raise ValueError("Endpoint URL must be a string")

        if not database or not database.isalnum():
            raise ValueError("Database name must be alphanumeric and non-empty")

        super().__init__(
            endpoint_url=endpoint_url,
            database=database,
            username=username,
            password=password,
            ssl=ssl,
            **kwargs
        )

    def __getstate__(self):
        self._client = None
        return super().__getstate__()

    def init(self, graph_store=None):
        target = graph_store or self
        existing_specs = self._existing_index_specs(target)

        for statement in self._index_statements():
            spec = self._statement_spec(self._rewrite_for_target(target, statement))
            if spec and spec in existing_specs:
                logger.debug("Index already present, skipping statement: %s", statement)
                continue

            try:
                target.execute_query_with_retry(statement, {})
            except Exception:
                if spec and self._index_exists(target, spec):
                    logger.debug(
                        "Index appeared after create attempt (likely concurrent init), skipping statement: %s",
                        statement,
                    )
                    existing_specs.add(spec)
                    continue
                logger.warning("FalkorDB index bootstrap statement failed: %s", statement)
                raise
            else:
                if spec:
                    existing_specs.add(spec)

    def _index_statements(self) -> Iterable[str]:
        return (
            "CREATE INDEX FOR (n:`__Entity__`) ON (n.entityId)",
            "CREATE INDEX FOR (n:`__Fact__`) ON (n.factId)",
            "CREATE INDEX FOR (n:`__Statement__`) ON (n.statementId)",
            "CREATE INDEX FOR (n:`__Topic__`) ON (n.topicId)",
            "CREATE INDEX FOR (n:`__Chunk__`) ON (n.chunkId)",
            "CREATE INDEX FOR (n:`__Source__`) ON (n.sourceId)",
            "CREATE INDEX FOR (n:`__Entity__`) ON (n.search_str)",
        )

    def _existing_index_specs(self, target: Any) -> Set[Tuple[str, str]]:
        try:
            rows = target.execute_query_with_retry("CALL db.indexes()", {})
        except Exception as exc:
            logger.warning(
                "Unable to inspect existing FalkorDB indexes, falling back to optimistic create: %s",
                exc,
            )
            return set()

        specs: Set[Tuple[str, str]] = set()
        if not isinstance(rows, list):
            return specs

        for row in rows:
            if not isinstance(row, Mapping):
                continue
            label = self._row_label(row)
            if not label:
                continue
            for prop in self._row_properties(row):
                specs.add((label, prop))

        return specs

    def _index_exists(self, target: Any, spec: Tuple[str, str]) -> bool:
        return spec in self._existing_index_specs(target)

    @staticmethod
    def _rewrite_for_target(target: Any, statement: str) -> str:
        rewrite_fn = getattr(target, "_rewrite_query", None)
        if callable(rewrite_fn):
            rewritten = rewrite_fn(statement)
            if isinstance(rewritten, str):
                return rewritten
        return statement

    @staticmethod
    def _statement_spec(statement: str) -> Optional[Tuple[str, str]]:
        match = _CREATE_INDEX_PATTERN.search(statement)
        if not match:
            return None
        return str(match.group(1)), str(match.group(2))

    @staticmethod
    def _row_label(row: Mapping[str, Any]) -> Optional[str]:
        for key in ("label", "labels"):
            value = row.get(key)
            label = FalkorDBDatabaseClient._extract_single_label(value)
            if label:
                return label

        for key, value in row.items():
            if "label" not in str(key).lower():
                continue
            label = FalkorDBDatabaseClient._extract_single_label(value)
            if label:
                return label

        return None

    @staticmethod
    def _row_properties(row: Mapping[str, Any]) -> Set[str]:
        for key in ("properties", "property", "fields"):
            value = row.get(key)
            properties = FalkorDBDatabaseClient._extract_tokens(value)
            if properties:
                return properties

        for key, value in row.items():
            lowered_key = str(key).lower()
            if "property" not in lowered_key and "field" not in lowered_key:
                continue
            properties = FalkorDBDatabaseClient._extract_tokens(value)
            if properties:
                return properties

        return set()

    @staticmethod
    def _extract_single_label(value: Any) -> Optional[str]:
        if value is None:
            return None

        if isinstance(value, str):
            parsed_value: Any = value.strip()
            if parsed_value.startswith("[") and parsed_value.endswith("]"):
                try:
                    parsed_value = ast.literal_eval(parsed_value)
                except (ValueError, SyntaxError):
                    inner = parsed_value[1:-1]
                    for part in inner.split(","):
                        token = FalkorDBDatabaseClient._normalize_label_token(part)
                        if token:
                            return token
                    return None
            token = FalkorDBDatabaseClient._normalize_label_token(parsed_value)
            return token

        if isinstance(value, (list, tuple, set)):
            for item in value:
                token = FalkorDBDatabaseClient._extract_single_label(item)
                if token:
                    return token
            return None

        return FalkorDBDatabaseClient._normalize_label_token(value)

    @staticmethod
    def _extract_tokens(value: Any) -> Set[str]:
        if value is None:
            return set()

        if isinstance(value, str):
            parsed_value: Any = value.strip()
            if parsed_value.startswith("[") and parsed_value.endswith("]"):
                try:
                    parsed_value = ast.literal_eval(parsed_value)
                except (ValueError, SyntaxError):
                    inner = parsed_value[1:-1]
                    return {
                        token
                        for token in (
                            FalkorDBDatabaseClient._normalize_token(part)
                            for part in inner.split(",")
                        )
                        if token
                    }
            else:
                token = FalkorDBDatabaseClient._normalize_token(parsed_value)
                return {token} if token else set()
            return FalkorDBDatabaseClient._extract_tokens(parsed_value)

        if isinstance(value, (list, tuple, set)):
            tokens: Set[str] = set()
            for item in value:
                tokens.update(FalkorDBDatabaseClient._extract_tokens(item))
            return tokens

        token = FalkorDBDatabaseClient._normalize_token(value)
        return {token} if token else set()

    @staticmethod
    def _normalize_token(value: Any) -> Optional[str]:
        token = str(value).strip()
        if not token:
            return None

        token = token.strip("`")
        token = token.strip("'\"")
        token = token.lstrip(":")
        if token.startswith("e."):
            token = token[2:]

        if token.startswith("[") and token.endswith("]"):
            return None

        match = re.search(r"([A-Za-z_][A-Za-z0-9_]*)$", token)
        if match:
            return match.group(1)

        return None

    @staticmethod
    def _normalize_label_token(value: Any) -> Optional[str]:
        token = str(value).strip()
        if not token:
            return None
        token = token.strip("`")
        token = token.strip("'\"")
        token = token.lstrip(":")
        return token or None

    @property
    def client(self) -> Graph:
        """
        Establish and return a FalkorDB client instance.

        :return: A FalkorDB Graph instance.
        :raises ConnectionError: If the connection to FalkorDB fails.
        """
        if self.endpoint_url:
            try:
                parts = self.endpoint_url.split(':')
                if len(parts) != 2:
                    raise ValueError("Invalid endpoint URL format. Expected format: "
                                     "'falkordb://host:port' or for local use 'falkordb://' ")
                host = parts[0]
                port = int(parts[1])
            except Exception as e:
                raise ValueError(f"Error parsing endpoint url: {e}") from e
        else:
            host = "localhost"
            port = 6379

        if self._client is None:
            try:
                self._client = falkordb.FalkorDB(
                        host=host,
                        port=port,
                        username=self.username,
                        password=self.password,
                        ssl=self.ssl,
                    ).select_graph(self.database)

            except ConnectionError as e:
                logger.error(f"Failed to connect to FalkorDB: {e}")
                raise ConnectionError(f"Could not establish connection to FalkorDB: {e}") from e
            except AuthenticationError as e:
                logger.error(f"Authentication failed: {e}")
                raise ConnectionError(f"Authentication failed: {e}") from e
            except Exception as e:
                logger.error(f"Unexpected error while connecting to FalkorDB: {e}")
                raise ConnectionError(f"Unexpected error while connecting to FalkorDB: {e}") from e
        return self._client


    def node_id(self, id_name: str) -> NodeId:
        """
        Format a node identifier.

        :param id_name: Name of the node.
        :return: Formatted node identifier.
        """
        return format_id(id_name)

    def _execute_query(self,
                      cypher: str,
                      parameters: Optional[dict] = None,
                      correlation_id: Any = None) -> QUERY_RESULT_TYPE:
        """
        Execute a Cypher query on the FalkorDB instance.

        :param cypher: The Cypher query to execute.
        :param parameters: Query parameters.
        :param correlation_id: Optional correlation ID for logging.
        :return: Query results as a list of nodes or paths.
        :raises ResponseError: If query execution fails.
        """
        if parameters is None:
            parameters = {}

        query_id = uuid.uuid4().hex[:5]

        request_log_entry_parameters = self.log_formatting.format_log_entry(
            self._logging_prefix(query_id, correlation_id),
            cypher,
            parameters,
        )

        logger.debug(f'[{request_log_entry_parameters.query_ref}] Query: [query: {request_log_entry_parameters.query}, parameters: {request_log_entry_parameters.parameters}]')

        start = time.time()

        try:
            response = self.client.query(
                q=request_log_entry_parameters.format_query_with_query_ref(cypher),
                params=parameters
            )
        except ResponseError as e:
            logger.error(f"Query execution failed: {e}. Query: {cypher}, Parameters: {parameters}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during query execution: {e}. Query: {cypher}, Parameters: {parameters}")
            raise ResponseError(f"Unexpected error during query execution: {e}") from e

        end = time.time()

        results = [{h[1]: d[i] for i, h in enumerate(response.header)} for d in response.result_set]

        if logger.isEnabledFor(logging.DEBUG):
            response_log_entry_parameters = self.log_formatting.format_log_entry(
                self._logging_prefix(query_id, correlation_id),
                cypher,
                parameters,
                results
            )
            logger.debug(f'[{response_log_entry_parameters.query_ref}] {int((end-start) * 1000)}ms Results: [{response_log_entry_parameters.results}]')

        return results
