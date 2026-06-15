# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Dict, Optional, List

from graphrag_toolkit.lexical_graph.storage.constants import ALL_EMBEDDING_INDEXES
from graphrag_toolkit.lexical_graph.storage.vector.vector_index import VectorIndex
from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import DummyVectorIndex

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

class VectorStore(BaseModel):
    """
    Represents a storage for managing and retrieving vector indexes.

    The `VectorStore` class is responsible for maintaining a collection of vector
    indexes. It supports functionalities such as retrieving specific indexes and
    accessing all stored indexes. This class is designed to handle operations
    related to vector index management seamlessly.

    Attributes:
        indexes (Optional[Dict[str, VectorIndex]]): A dictionary where the keys are
            index names and the values are corresponding `VectorIndex` objects. It
            stores all vector indexes available in this class instance. Defaults to
            an empty dictionary.
    """
    indexes:Optional[Dict[str, VectorIndex]] = Field(description='Vector indexes', default_factory=dict)

    def __enter__(self):
        logger.debug(f'Entering {type(self).__name__}')
        return self
    
    def __exit__(self, exception_type, exception_value, traceback):
        logger.debug(f'Exiting {type(self).__name__}')
        return False

    def get_index(self, index_name):
        """
        Retrieves the vector index associated with the given index name. If the specified index
        name is not recognized or has not been registered in the indexes dictionary, it returns
        a dummy index instead. The method ensures that only valid index names are processed and
        handled appropriately.

        Args:
            index_name: The name of the index to retrieve. Must be an entry from the
                global `ALL_EMBEDDING_INDEXES` list.

        Returns:
            Union[VectorIndex, DummyVectorIndex]: The corresponding vector index if the name is
            found in the `indexes` dictionary; otherwise, a dummy vector index configured with
            the specified `index_name`.

        Raises:
            ValueError: If the provided `index_name` is not one of the allowed entries listed in
                `ALL_EMBEDDING_INDEXES`.
        """
        if index_name not in ALL_EMBEDDING_INDEXES:
            raise ValueError(f'Invalid index name ({index_name}): must be one of {ALL_EMBEDDING_INDEXES}')
        if index_name not in self.indexes:
            logger.debug(f"Returning dummy index for '{index_name}'")
            return DummyVectorIndex(index_name=index_name)
        return self.indexes[index_name]

    def all_indexes(self) -> List[VectorIndex]:
        """
        Returns a list of all vector indexes stored in the object.

        The method retrieves all vector indexes from the internal storage and
        returns them as a list.

        Returns:
            List[VectorIndex]: A list containing all vector indexes present in the
                internal storage.
        """
        return list(self.indexes.values())

