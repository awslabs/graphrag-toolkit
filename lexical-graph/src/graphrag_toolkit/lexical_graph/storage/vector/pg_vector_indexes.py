# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import numpy as np

from pgvector.psycopg2 import register_vector
from typing import List, Sequence, Dict, Any, Optional, Callable
from urllib.parse import urlparse

from graphrag_toolkit.lexical_graph.metadata import FilterConfig, type_name_for_key_value, format_datetime
from graphrag_toolkit.lexical_graph.versioning import VALID_FROM, VALID_TO, TIMESTAMP_LOWER_BOUND, TIMESTAMP_UPPER_BOUND
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig, EmbeddingType
from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, to_embedded_query
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

from llama_index.core.schema import BaseNode, QueryBundle
from llama_index.core.indices.utils import embed_nodes
from llama_index.core.vector_stores.types import FilterCondition, FilterOperator, MetadataFilter, MetadataFilters

logger = logging.getLogger(__name__)

try:
    import psycopg2
    from pgvector.psycopg2 import register_vector
    from psycopg2.errors import UniqueViolation, UndefinedTable
except ImportError as e:
    raise ImportError(
        "psycopg2 and/or pgvector packages not found, install with 'pip install psycopg2-binary pgvector'"
    ) from e
    
def _type_name_for_key_value(key: str, value: Any) -> str:
    type_name = type_name_for_key_value(key, value)
    return 'bigint' if type_name == 'int' else type_name
    

def to_sql_operator(operator: FilterOperator) -> tuple[str, Callable[[Any], str]]:
    """
    Converts a filter operator into an SQL operator and its respective value formatter.

    This function maps a `FilterOperator` enum value to a tuple that contains the SQL
    representation of the operator and a callable function to format the value for use
    in SQL queries. If the provided operator is unsupported, a `ValueError` is raised.

    Args:
        operator (FilterOperator): The filter operator to be converted to its SQL
            equivalent.

    Returns:
        tuple[str, Callable[[Any], str]]: A tuple where the first element is the SQL
            operator as a string, and the second element is a callable to format
            the value for use with the SQL operator.

    Raises:
        ValueError: If the provided `operator` is not supported.
    """
    default_value_formatter = lambda x: x
    
    operator_map = {
        FilterOperator.EQ: ('=', default_value_formatter), 
        FilterOperator.GT: ('>', default_value_formatter), 
        FilterOperator.LT: ('<', default_value_formatter), 
        FilterOperator.NE: ('<>', default_value_formatter), 
        FilterOperator.GTE: ('>=', default_value_formatter), 
        FilterOperator.LTE: ('<=', default_value_formatter), 
        #FilterOperator.IN: ('in', default_value_formatter),  # In array (string or number)
        #FilterOperator.NIN: ('nin', default_value_formatter),  # Not in array (string or number)
        #FilterOperator.ANY: ('any', default_value_formatter),  # Contains any (array of strings)
        #FilterOperator.ALL: ('all', default_value_formatter),  # Contains all (array of strings)
        FilterOperator.TEXT_MATCH: ('LIKE', lambda x: f"%%{x}%%"),
        FilterOperator.TEXT_MATCH_INSENSITIVE: ('~*', default_value_formatter),
        #FilterOperator.CONTAINS: ('contains', default_value_formatter),  # metadata array contains value (string or number)
        FilterOperator.IS_EMPTY: ('IS NULL', default_value_formatter),  # the field is not exist or empty (null or empty array)
    }

    if operator not in operator_map:
        raise ValueError(f'Unsupported filter operator: {operator}')
    
    return operator_map[operator]

def formatter_for_type(type_name:str) -> Callable[[Any], str]:
    """
    Returns a formatter function corresponding to the given type name. The formatter
    function takes a value as input and returns the value formatted appropriately
    for the specified type. The supported types include 'text', 'timestamp', 'int',
    and 'float'.

    Args:
        type_name (str): The name of the type for which the formatter function is
            required. Supported types are 'text', 'timestamp', 'int', and 'float'.

    Returns:
        Callable[[Any], str]: A function that formats values according to the
            specified type.

    Raises:
        ValueError: If the specified type name is not supported.
    """
    if type_name == 'text':
        return lambda x: f"'{x}'"
    elif type_name == 'timestamp':
        return lambda x: f"'{format_datetime(x)}'"
    elif type_name in ['bigint', 'int', 'float']:
        return lambda x:x
    else:
        raise ValueError(f'Unsupported type name: {type_name}')


def parse_metadata_filters_recursive(metadata_filters:MetadataFilters) -> str:
    """
    Parses metadata filters recursively into a SQL-compatible string representation.

    This function takes a `MetadataFilters` object, traverses its hierarchical
    structure, and converts it into SQL-like conditions. It handles different
    filter operations, logical conditions (AND, OR, NOT), and nested filters.

    Args:
        metadata_filters (MetadataFilters): A MetadataFilters object containing
            the hierarchical structure of filters and logical conditions to
            parse into a SQL-compatible string.

    Returns:
        str: A SQL-compatible string representation of the provided metadata
            filters.

    Raises:
        ValueError: If invalid metadata filter types are encountered within the
            `metadata_filters` structure, or if an unsupported filter condition
            is provided.
    """
    def to_key(key: str) -> str:
        """
        Parses metadata filters recursively into a string representation suitable for
        a database query.

        The function takes metadata filters and processes them recursively to generate
        a string representation that can be incorporated into database queries. It
        utilizes helper functions for converting filter keys into the specific format
        used in the query syntax.

        Args:
            metadata_filters: MetadataFilters instance representing the metadata filters
                to be processed.

        Returns:
            str: A string representation of the metadata filters formatted for database
                queries.
        """
        if key == VALID_FROM:
            return 'valid_from'
        elif key == VALID_TO:
            return 'valid_to'
        else:
            return f"metadata->'source'->'metadata'->>'{key}'"
    
    def to_sql_filter(f: MetadataFilter) -> str:
        """
        Parses metadata filters recursively to generate an SQL-compatible filter string.

        This function processes a collection of metadata filters, converting them step
        by step into a valid SQL-like filter expression. It makes use of helper
        functions to generate the appropriate SQL syntax for filters and their
        operators while handling specific cases like empty filters.

        Args:
            metadata_filters (MetadataFilters): A structured object containing metadata
                filters that need to be processed into SQL-compatible expressions.

        Returns:
            str: A string representing the SQL-compatible filter derived from the
            provided metadata filters.
        """
        key = to_key(f.key)
        (operator, operator_formatter) = to_sql_operator(f.operator)

        if f.operator == FilterOperator.IS_EMPTY:
            return f"({key} {operator})"
        else:
            type_name = _type_name_for_key_value(f.key, f.value)
            type_formatter = formatter_for_type(type_name)
            return f"(({key})::{type_name} {operator} {type_formatter(operator_formatter(str(f.value)))})"
    
    condition = metadata_filters.condition.value

    filter_strs = []

    for metadata_filter in metadata_filters.filters:
        if isinstance(metadata_filter, MetadataFilter):
            if metadata_filters.condition == FilterCondition.NOT:
                raise ValueError(f'Expected MetadataFilters for FilterCondition.NOT, but found MetadataFilter')
            filter_strs.append(to_sql_filter(metadata_filter))
        elif isinstance(metadata_filter, MetadataFilters):
            filter_strs.append(parse_metadata_filters_recursive(metadata_filter))
        else:
            raise ValueError(f'Invalid metadata filter type: {type(metadata_filter)}')
        
    if metadata_filters.condition == FilterCondition.NOT:
        return f"(NOT {' '.join(filter_strs)})"
    elif metadata_filters.condition == FilterCondition.AND or metadata_filters.condition == FilterCondition.OR:
        condition = f' {metadata_filters.condition.value.upper()} '
        return f"({condition.join(filter_strs)})"
    else:
        raise ValueError(f'Unsupported filters condition: {metadata_filters.condition}')


def filter_config_to_sql_filters(filter_config:FilterConfig) -> str:
    """
    Converts a given FilterConfig object into an SQL WHERE clause-like filter string.

    This function translates the source filters provided in the FilterConfig
    into a SQL-compatible filter representation. If the provided filter_config
    or its source_filters attribute is None, it returns an empty string.

    Args:
        filter_config (FilterConfig): The filter configuration object containing
            source filters to be converted into an SQL-compatible format.

    Returns:
        str: A string representation of the converted SQL-compatible filters.
    """
    if filter_config is None or filter_config.source_filters is None:
        return ''
    return parse_metadata_filters_recursive(filter_config.source_filters)

class PGIndex(VectorIndex):

    @staticmethod
    def for_index(index_name:str,
                  connection_string:str,
                  database='postgres',
                  schema_name='graphrag',
                  host:str='localhost',
                  port:int=5432,
                  username:str=None,
                  password:str=None,
                  embed_model:EmbeddingType=None,
                  dimensions:int=None,
                  enable_iam_db_auth=False):
        """
        Creates an instance of the `PGIndex` class based on the provided database connection
        information, index name, and embedding configuration. It parses the `connection_string`
        to extract overriding parameters and defaults any missing values to the provided or preset defaults.

        Args:
            index_name (str): The name of the index to create or reference.
            connection_string (str): A database connection string (e.g., in URI format)
            containing username, password, hostname, port, database, and optional query parameters.
            database (str, optional): The name of the database. Defaults to 'postgres'.
            schema_name (str, optional): The name of the database schema. Defaults to 'graphrag'.
            host (str, optional): The database hostname. Defaults to 'localhost'.
            port (int, optional): The port to connect to on the database host. Defaults to 5432.
            username (str, optional): The username for database authentication. Defaults to None.
            password (str, optional): The password for database authentication. Defaults to None.
            embed_model (EmbeddingType, optional): The embedding model to use for generating index
            embeddings. Defaults to None.
            dimensions (int, optional): The dimensionality of the embeddings. Defaults to None.
            enable_iam_db_auth (bool, optional): A flag indicating whether to enable IAM
            database authentication. Defaults to False.

        Returns:
            PGIndex: An instance of the `PGIndex` class properly configured with the provided
            or inferred parameters.
        """
        def compute_enable_iam_db_auth(s, default):
            """Representation of a PostgreSQL-based vector index for efficient querying and
            storage of vectors in a database. This class provides functionality to interface
            with a PostgreSQL database and manage operations such as indexing, storing, and
            querying vector data. It supports various configurations like schema, host, port,
            and authentication settings to offer flexible and secure database connections.
            """
            if 'enable_iam_db_auth' in s.lower():
                return 'enable_iam_db_auth=true' in s.lower()
            else:
                return default
        
        parsed = urlparse(connection_string)

        database = parsed.path[1:] if parsed.path else database
        host = parsed.hostname or host
        port = parsed.port or port
        username = parsed.username or username
        password = parsed.password or password
        enable_iam_db_auth = compute_enable_iam_db_auth(parsed.query, enable_iam_db_auth)
        
        embed_model = embed_model or GraphRAGConfig.embed_model
        dimensions = dimensions or GraphRAGConfig.embed_dimensions

        return PGIndex(index_name=index_name, 
                       database=database, 
                       schema_name=schema_name,
                       host=host, 
                       port=port, 
                       username=username, 
                       password=password, 
                       dimensions=dimensions, 
                       embed_model=embed_model, 
                       enable_iam_db_auth=enable_iam_db_auth)

    index_name:str
    database:str
    schema_name:str
    host:str
    port:int
    username:str
    password:Optional[str]
    dimensions:int
    embed_model:EmbeddingType
    enable_iam_db_auth:bool=False
    initialized:bool=False

    def _get_connection(self):
        """
        Establishes a connection to the database, configures it, and optionally initializes the schema and
        indexes required for the application. The method supports IAM-based authentication for Amazon RDS if
        enabled. It also ensures that the schema is initialized exactly once during the lifecycle of the object,
        creating necessary tables and indexes if they do not exist. Logs warnings for existing database objects.

        Raises:
            psycopg2.Error: Raised if there is an issue with establishing the database connection or executing
            queries to initialize the schema and indexes.
            Exception: Raised if there are other unforeseen errors during the schema initialization or database
            interactions.

        Attributes:
            enable_iam_db_auth (bool): Indicates whether IAM-based authentication for AWS RDS is enabled.
            host (str): The hostname of the database server.
            username (str): The username for the database connection.
            password (Optional[str]): The static password for connecting to the database if IAM is disabled.
            port (int): The port number on which the database listens for connections.
            database (str): The name of the database to connect to.
            initialized (bool): Tracks whether the schema and indexes have already been initialized.
            writeable (bool): Specifies whether the database connection should handle schema initialization and
            updates.
            schema_name (str): The schema name where the table resides.
            index_name (str): Identifier used for managing and initializing the table and its related indexes.
            dimensions (int): Dimensionality of the vector column used in the table for embeddings.
        """
        token = None

        if self.enable_iam_db_auth:
            client = GraphRAGConfig.rds  # via __getattr__
            region = GraphRAGConfig.aws_region
            token = client.generate_db_auth_token(
                DBHostname=self.host,
                Port=self.port,
                DBUsername=self.username,
                Region=region
            )

        password = token or self.password

        dbconn = psycopg2.connect(
            host=self.host,
            user=self.username, 
            password=password,
            port=self.port, 
            database=self.database,
            connect_timeout=30
        )

        dbconn.set_session(autocommit=True)

        register_vector(dbconn)

        if not self.initialized:

            cur = dbconn.cursor()

            try:

                if self.writeable:

                    try:
                        cur.execute(f'''CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.underlying_index_name()}(
                            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            {self.index_name}Id VARCHAR(255) unique,
                            value text,
                            metadata jsonb,
                            embedding vector({self.dimensions}),
                            valid_from BIGINT DEFAULT {TIMESTAMP_LOWER_BOUND},
                            valid_to BIGINT DEFAULT {TIMESTAMP_UPPER_BOUND}
                            );'''
                        )
                    except UniqueViolation:
                        # For alt approaches, see: https://stackoverflow.com/questions/29900845/create-schema-if-not-exists-raises-duplicate-key-error
                        logger.warning(f"Table already exists, so ignoring CREATE: {self.underlying_index_name()}")

                    index_name = f'{self.underlying_index_name()}_{self.index_name}Id_idx'
                    try:
                        cur.execute(f'CREATE INDEX IF NOT EXISTS {index_name} ON {self.schema_name}.{self.underlying_index_name()} USING hash ({self.index_name}Id);')
                    except UniqueViolation:
                        logger.warning(f"Index already exists, so ignoring CREATE: {index_name}")

                    index_name = f'{self.underlying_index_name()}_{self.index_name}Id_embedding_idx'
                    try:
                        cur.execute(f'CREATE INDEX IF NOT EXISTS {index_name} ON {self.schema_name}.{self.underlying_index_name()} USING hnsw (embedding vector_l2_ops)')
                    except UniqueViolation:
                        logger.warning(f"Index already exists, so ignoring CREATE: {index_name}")
                    
                    index_name = f'{self.underlying_index_name()}_{self.index_name}Id_gin_idx'
                    try:
                        cur.execute(f'CREATE INDEX IF NOT EXISTS {index_name} ON {self.schema_name}.{self.underlying_index_name()} USING GIN (metadata)')
                    except UniqueViolation:
                        logger.warning(f"Index already exists, so ignoring CREATE: {index_name}")

            finally:
                cur.close()

            self.initialized = True

        return dbconn


    def add_embeddings(self, nodes:Sequence[BaseNode]) -> Sequence[BaseNode]:
        """
        Adds embeddings for a sequence of nodes into the database. This method processes
        each node by generating embeddings using the specified embedding model and
        inserts or updates these nodes in the underlying indexed database table. The method
        ensures write authorization before proceeding and handles exceptions related to
        undefined tables. Nodes for a specific tenant that references non-existent tables
        are logged as warnings without causing the entire operation to fail.

        Args:
            nodes (Sequence[BaseNode]): A sequence of nodes for which embeddings are to be
                generated and stored in the database. Each node includes an identifier,
                textual data, and metadata.

        Returns:
            Sequence[BaseNode]: The same sequence of nodes that were provided as input,
                potentially with modifications or updates to their embeddings.
        """
        if not self.writeable:
            raise IndexError(f'Index {self.index_name} is read-only')

        dbconn = self._get_connection()
        cur = dbconn.cursor()

        try:

            id_to_embed_map = embed_nodes(
                nodes, self.embed_model
            )
            for node in nodes:

                valid_from = node.metadata.get('source', {}).get('versioning', {}).get('valid_from', TIMESTAMP_LOWER_BOUND) 

                cur.execute(
                    f'INSERT INTO {self.schema_name}.{self.underlying_index_name()} ({self.index_name}Id, value, metadata, embedding, valid_from) SELECT %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT * FROM {self.schema_name}.{self.underlying_index_name()} c WHERE c.{self.index_name}Id = %s);',
                    (node.id_, node.text,  json.dumps(node.metadata), id_to_embed_map[node.id_], valid_from, node.id_)
                )

        except UndefinedTable as e:
            if self.tenant_id.is_default_tenant():
                raise e
            else:
                logger.warning(f'Multi-tenant index {self.underlying_index_name()} does not exist')

        finally:

            cur.close()
            dbconn.close()

        return nodes
    
    def _to_top_k_result(self, r):
        """
        Converts a raw result into a structured Top K result dictionary.

        The method processes the given input `r` to construct a dictionary containing
        details like score, metadata, and specific index-related information. The
        conversion ensures a clear and detailed representation of the data extracted
        from the input.

        Args:
            r: A tuple containing the raw data to be processed. The tuple is expected
               to follow a specific format where `r[2]` contains the score, `r[1]`
               contains metadata (as a dictionary or JSON payload), and `r[0]` is
               typically unused in this method.

        Returns:
            dict: A dictionary containing the transformed data, including a rounded
                  score, associated metadata fields, and key-value pairs if index-related
                  information is present within the metadata.
        """
        result = {
            'score': round(r[2], 7)
        }

        valid_from = r[3]
        valid_to = r[4]

        metadata_payload = r[1]
        if isinstance(metadata_payload, dict):
            metadata = metadata_payload
        else:
            metadata = json.loads(metadata_payload)

        if INDEX_KEY in metadata:
            index_name = metadata[INDEX_KEY]['index']
            result[index_name] = metadata[index_name]
            if 'source' in metadata:
                result['source'] = metadata['source']
                result['source']['versioning']['valid_from'] = valid_from
                result['source']['versioning']['valid_to'] = valid_to
        else:
            for k,v in metadata.items():
                result[k] = v
            
        return result
    
    def _to_get_embedding_result(self, r):
        """
        Converts and processes the result from an embedding query into a formatted dictionary.

        The function processes the input result tuple, extracting the ID, value, embedding,
        and metadata while ensuring proper type conversion and dictionary formatting.
        It filters out specific metadata keys based on predefined criteria.

        Args:
            r (tuple): A tuple containing the results of an embedding query. It is
                expected to have the following structure:
                - Index 0: The ID associated with the result.
                - Index 1: The value corresponding to the embedding.
                - Index 2: Metadata information in the form of a dictionary or a
                  JSON-formatted string.
                - Index 3: The embedding as an array-like object.

        Returns:
            dict: A dictionary containing the processed result data with keys for 'id',
                'value', 'embedding', and any additional metadata except those excluded
                by filtering.
        """
        id = r[0]
        value = r[1]
        valid_from = r[4]
        valid_to = r[5]

        metadata_payload = r[2]
        if isinstance(metadata_payload, dict):
            metadata = metadata_payload
        else:
            metadata = json.loads(metadata_payload)
 
        embedding = np.array(r[3], dtype=object).tolist()

        result = {
            'id': id,
            'value': value,
            'embedding': embedding
        }

        for k,v in metadata.items():
            if k != INDEX_KEY:
                result[k] = v

        if 'source' in result:
            result['source']['versioning']['valid_from'] = valid_from
            result['source']['versioning']['valid_to'] = valid_to
            
        return result
    
    def top_k(self, query_bundle:QueryBundle, top_k:int=5, filter_config:Optional[FilterConfig]=None) -> Sequence[Dict[str, Any]]:
        """
        Retrieves the top K results from the database based on the provided query
        and optional filter configuration.

        This method interacts with a database to obtain the top K matching entries
        that align with the user's query. It supports additional filtering through
        the optional filter configuration. Results are sorted in ascending order
        of their similarity scores.

        Args:
            query_bundle (QueryBundle): Query object containing the `embedding`
                for similarity search.
            top_k (int): The number of top results to fetch from the database.
                Defaults to 5.
            filter_config (Optional[FilterConfig]): An optional configuration
                object defining additional filters applied to the query.

        Returns:
            Sequence[Dict[str, Any]]: A sequence of dictionaries, where each
            dictionary represents a top result containing relevant metadata and
            similarity scores.
        """
        dbconn = self._get_connection()
        cur = dbconn.cursor()

        top_k_results = []

        try:

            where_clause =  filter_config_to_sql_filters(filter_config)
            where_clause = f'WHERE {where_clause}' if where_clause else ''

            logger.debug(f'filter: {where_clause}')

            query_bundle = to_embedded_query(query_bundle, self.embed_model)

            sql = f'''SELECT {self.index_name}Id, metadata, embedding <-> %s AS score, valid_from, valid_to
                FROM {self.schema_name}.{self.underlying_index_name()}
                {where_clause}
                ORDER BY score ASC LIMIT %s;'''
            
            logger.debug(f'sql: {sql}')
        
            cur.execute(sql, (np.array(query_bundle.embedding), top_k))

            results = cur.fetchall()

            top_k_results.extend(
                [self._to_top_k_result(result) for result in results]
            )

        except UndefinedTable as e:
            logger.warning(f'Index {self.underlying_index_name()} does not exist')

        finally:
            cur.close()
            dbconn.close()

        return top_k_results

    def get_embeddings(self, ids:List[str]=[]) -> Sequence[Dict[str, Any]]:
        """
        Retrieves embeddings from the database for the given list of IDs. This method
        queries the underlying database table to fetch the corresponding embeddings,
        metadata, and other data associated with the specified IDs. The results are
        formatted into a list of embedding dictionaries.

        Args:
            ids (List[str], optional): A list of string IDs for which embeddings need
                to be fetched. Defaults to an empty list.

        Returns:
            Sequence[Dict[str, Any]]: A list of dictionaries where each dictionary
                represents an embedding, its metadata, and associated data for the
                given IDs.

        Raises:
            UndefinedTable: Raised when the underlying table does not exist in the
                database.

        """
        dbconn = self._get_connection()
        cur = dbconn.cursor()

        def format_ids(ids):
            return ','.join([f"'{id}'" for id in set(ids)])
        
        get_embeddings_results = []

        try:

            cur.execute(f'''SELECT {self.index_name}Id, value, metadata, embedding, valid_from, valid_to
                FROM {self.schema_name}.{self.underlying_index_name()}
                WHERE {self.index_name}Id IN ({format_ids(ids)});'''
            )

            results = cur.fetchall()

            get_embeddings_results.extend(
                [self._to_get_embedding_result(result) for result in results]
            )

        except UndefinedTable as e:
            logger.warning(f'Index {self.underlying_index_name()} does not exist')

        finally:
            cur.close()
            dbconn.close()

        return get_embeddings_results
    
    def update_versioning(self, versioning_timestamp:int, ids:List[str]=[]):
        
        dbconn = self._get_connection()
        cur = dbconn.cursor()

        def format_ids(ids):
            return ','.join([f"'{id}'" for id in set(ids)])
        
        try:

            cur.execute(f'''UPDATE {self.schema_name}.{self.underlying_index_name()}
                SET valid_to = {versioning_timestamp}
                WHERE {self.index_name}Id IN ({format_ids(ids)});'''
            )

        except UndefinedTable as e:
            logger.warning(f'Index {self.underlying_index_name()} does not exist')

        finally:
            cur.close()
            dbconn.close()

        return []

    def enable_for_versioning(self, ids:List[str]=[]):
        
        dbconn = self._get_connection()
        cur = dbconn.cursor() 

        try:
            cur.execute(f'''ALTER TABLE {self.schema_name}.{self.underlying_index_name()}
                ADD COLUMN IF NOT EXISTS valid_from BIGINT DEFAULT {TIMESTAMP_LOWER_BOUND},
                ADD COLUMN IF NOT EXISTS valid_to BIGINT DEFAULT {TIMESTAMP_UPPER_BOUND};'''
            )
        except UniqueViolation:
            logger.warning(f"Columns already exist, so ignoring ALTER: {self.underlying_index_name()}")

        finally:
            cur.close()
            dbconn.close()

        return []
    
    def delete_embeddings(self, ids:List[str]=[]):

        dbconn = self._get_connection()
        cur = dbconn.cursor()

        def format_ids(ids):
            return ','.join([f"'{id}'" for id in set(ids)])
        
        try:

            cur.execute(f'''DELETE FROM {self.schema_name}.{self.underlying_index_name()}
                WHERE {self.index_name}Id IN ({format_ids(ids)});'''
            )

        except UndefinedTable as e:
            logger.warning(f'Index {self.underlying_index_name()} does not exist')

        finally:
            cur.close()
            dbconn.close()

        return ids
