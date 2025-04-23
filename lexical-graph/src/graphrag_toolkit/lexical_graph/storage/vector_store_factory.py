# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Union, Type, Dict
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore, VectorIndexFactoryMethod
from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory import OpenSearchVectorIndexFactory
from graphrag_toolkit.lexical_graph.storage.vector.neptune_vector_indexes import NeptuneAnalyticsVectorIndexFactory
from graphrag_toolkit.lexical_graph.storage.vector.pg_vector_index_factory import PGVectorIndexFactory
from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import DummyVectorIndexFactory
from graphrag_toolkit.lexical_graph.storage.constants import DEFAULT_EMBEDDING_INDEXES


VectorStoreType = Union[str, VectorStore]
VectorIndexFactoryMethodType = Union[VectorIndexFactoryMethod, Type[VectorIndexFactoryMethod]]

_vector_index_factories:Dict[str, VectorIndexFactoryMethod] = { c.__name__ : c() for c in [OpenSearchVectorIndexFactory, PGVectorIndexFactory, NeptuneAnalyticsVectorIndexFactory, DummyVectorIndexFactory] }

class VectorStoreFactory():

    @staticmethod
    def register(factory_type:VectorIndexFactoryMethodType):
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
        indexes = {}
        for v in vector_store_list:
            for k, v in v.indexes:
                indexes[k] = v
                      
        return VectorStore(indexes=indexes)