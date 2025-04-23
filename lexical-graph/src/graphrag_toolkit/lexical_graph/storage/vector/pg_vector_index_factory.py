# Copyright FalkorDB.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List

from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, VectorIndexFactoryMethod

logger = logging.getLogger(__name__)

POSTGRES = 'postgres://'
POSTGRESQL = 'postgresql://'

class PGVectorIndexFactory(VectorIndexFactoryMethod):
    def try_create(self, index_names:List[str], vector_index_info:str, **kwargs) -> List[VectorIndex]:
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