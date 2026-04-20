# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Union, Type, Dict
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore, VectorIndexFactoryMethod
from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory import OpenSearchVectorIndexFactory
from graphrag_toolkit.lexical_graph.storage.vector.neptune_vector_indexes import NeptuneAnalyticsVectorIndexFactory
from graphrag_toolkit.lexical_graph.storage.vector.pg_vector_index_factory import PGVectorIndexFactory
from graphrag_toolkit.lexical_graph.storage.vector.s3_vector_index_factory import S3VectorIndexFactory
from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import DummyVectorIndexFactory
from graphrag_toolkit.lexical_graph.storage.constants import DEFAULT_EMBEDDING_INDEXES


VectorStoreType = Union[str, VectorStore]
VectorIndexFactoryMethodType = Union[VectorIndexFactoryMethod, Type[VectorIndexFactoryMethod]]

_vector_index_factories:Dict[str, VectorIndexFactoryMethod] = { c.__name__ : c() for c in [
    OpenSearchVectorIndexFactory, 
    PGVectorIndexFactory, 
    NeptuneAnalyticsVectorIndexFactory, 
    S3VectorIndexFactory,
    DummyVectorIndexFactory
] }

class VectorStoreFactory():
    """Manages the registration and creation of vector index factories and vector stores.

    This class provides mechanisms for registering vector index factory methods, creating
    vector stores either directly or by using registered factories, and handling composite
    vector stores made up of multiple smaller stores. It is designed to streamline the
    retrieval and management of vector store instances.

    Attributes:
        None
    """
    @staticmethod
    def register(factory_type:VectorIndexFactoryMethodType):
        """
        Registers a factory method for vector index creation. This method allows
        the addition of custom factory methods to the global registry, enabling
        dynamic instantiation of vector index objects based on the factory type.

        Args:
            factory_type: A factory method class or instance to register.
                If a class is provided, it must inherit from
                VectorIndexFactoryMethod. If an instance is provided, its type
                must still inherit from VectorIndexFactoryMethod.

        Raises:
            ValueError: If the provided factory_type does not inherit
                from VectorIndexFactoryMethod, whether as a class or instance.
        """
        if isinstance(factory_type, type):
            if not issubclass(factory_type, VectorIndexFactoryMethod):
                raise ValueError(f'Invalid factory_type argument: {factory_type.__name__} must inherit from VectorIndexFactoryMethod.')
            _vector_index_factories[factory_type.__name__] = factory_type()
        else:
            factory_type_name = type(factory_type).__name__
            if not isinstance(factory_type, VectorIndexFactoryMethod):
                raise ValueError(f'Invalid factory_type argument: {factory_type_name} must inherit from VectorIndexFactoryMethod.')
            _vector_index_factories[factory_type_name] = factory_type

    @staticmethod
    def for_vector_store(vector_store_info:str=None, index_names=DEFAULT_EMBEDDING_INDEXES, **kwargs):
        """
        Creates a vector store instance or retrieves an existing one based on the provided
        vector store information and index names. This method utilizes specified factories to
        attempt creating vector indexes, and finally constructs a `VectorStore` object if successful.

        Args:
            vector_store_info (str | VectorStore, optional): The vector store connection information or an
                existing `VectorStore` instance. If a string, it is used to identify or connect to a
                specific vector store.
            index_names (list[str] | str, optional): The name(s) of indexes to create or retrieve. Defaults
                to `DEFAULT_EMBEDDING_INDEXES` if not supplied. This should either be a single string or
                a list of strings.
            **kwargs: Additional keyword arguments passed to the factory methods while creating
                the vector indexes.

        Returns:
            VectorStore: A `VectorStore` instance containing the specified indexes or the supplied
            `VectorStore` if valid.

        Raises:
            ValueError: If the provided `vector_store_info` is not recognized or improperly formatted, or if
            no suitable vector index factory successfully creates the necessary vector indexes.
        """
        if vector_store_info and isinstance(vector_store_info, VectorStore):
            return vector_store_info
        index_names = index_names if isinstance(index_names, list) else [index_names]

        for factory in _vector_index_factories.values():
            vector_indexes = factory.try_create(index_names, vector_store_info, **kwargs)
            if vector_indexes:
                return VectorStore(indexes={i.index_name: i for i in vector_indexes})
            
        raise ValueError(f'Unrecognized vector store info: {vector_store_info}. Check that the vector store connection info is formatted correctly, and that an appropriate vector index factory method is registered with VectorStoreFactory.')
    
    @staticmethod
    def for_composite(vector_store_list:List[VectorStore]):
        """
        Combines multiple VectorStore instances into a single instance by merging their indexes.

        This static method takes a list of VectorStore instances and aggregates their indexes into a
        composite VectorStore. Each key-value pair in the indexes of the input VectorStores will be
        preserved in the resulting composite VectorStore. Keys in the indexes are considered unique
        and will be directly added to the result.

        Args:
            vector_store_list (List[VectorStore]): A list of VectorStore instances, where each instance
                contains indexes that will be merged into a unified structure.

        Returns:
            VectorStore: A new VectorStore instance that contains the combined indexes of the provided
                VectorStore instances.
        """
        indexes = {}
        for vs in vector_store_list:
            for k, v in vs.indexes.items():
                indexes[k] = v
                      
        return VectorStore(indexes=indexes)