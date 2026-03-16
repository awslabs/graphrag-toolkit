# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List

from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, VectorIndexFactoryMethod

logger = logging.getLogger(__name__)

POSTGRES = 'postgres://'
POSTGRESQL = 'postgresql://'

class PGVectorIndexFactory(VectorIndexFactoryMethod):
    def try_create(self, index_names:List[str], vector_index_info:str, **kwargs) -> List[VectorIndex]:
        """
        Tries to create and return a list of vector indexes using the given parameters.

        Depending on the connection information provided in `vector_index_info`, this method
        attempts to open PostgreSQL vector indexes or returns None if the connection string
        is not valid or applicable. If the PostgreSQL module is not available, it raises an
        ImportError.

        Args:
            index_names (List[str]): A list of index names to be used when creating vector indexes.
            vector_index_info (str): A string containing information about the vector index
            connection, such as a PostgreSQL connection string.
            **kwargs: Additional arguments that might be passed to the underlying index creation
            utility.

        Returns:
            List[VectorIndex]: A list of vector index objects created for the provided index
            names if successful, otherwise None.

        Raises:
            ImportError: If the PostgreSQL-specific module required for creating the indexes
            cannot be imported.
        """
        connection_string = None
        if vector_index_info.startswith(POSTGRES) or vector_index_info.startswith(POSTGRESQL):
            connection_string = vector_index_info
        if connection_string:
            logger.debug(f'Opening PostgreSQL vector indexes [index_names: {index_names}, connection_string: {connection_string}]')
            try:
                from graphrag_toolkit.lexical_graph.storage.vector.pg_vector_indexes import PGIndex
                return [PGIndex.for_index(index_name, connection_string, **kwargs) for index_name in index_names]
            except ImportError as e:
                raise e           
        else:
            return None