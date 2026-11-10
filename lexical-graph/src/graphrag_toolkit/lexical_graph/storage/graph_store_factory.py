# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Union, Type, Dict

from graphrag_toolkit.lexical_graph.storage.graph import GraphStore, GraphStoreFactoryMethod
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import NeptuneAnalyticsGraphStoreFactory, NeptuneDatabaseGraphStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store_factory import Neo4jGraphStoreFactory

logger = logging.getLogger(__name__)

GraphStoreType = Union[str, GraphStore]
GraphStoreFactoryMethodType = Union[GraphStoreFactoryMethod, Type[GraphStoreFactoryMethod]]

_graph_store_factories:Dict[str, GraphStoreFactoryMethod] = { c.__name__ : c() for c in [NeptuneAnalyticsGraphStoreFactory, NeptuneDatabaseGraphStoreFactory, Neo4jGraphStoreFactory, DummyGraphStoreFactory] }


class GraphStoreFactory():
    """
    Factory class for registering and creating GraphStore objects.

    Provides methods to register custom factory implementations and retrieve
    instances of GraphStore based on input parameters. Handles dynamic
    factory method registration and graph store instance creation.

    Attributes:
        _graph_store_factories (dict): Internal storage of registered
            factory methods, mapping factory names to their respective
            instances.
    """
    @staticmethod
    def register(factory_type:GraphStoreFactoryMethodType):
        """
        Registers a factory type instance or class to the internal factory registry.

        This method allows registration of classes inheriting from GraphStoreFactoryMethod
        or their instances. The input factory_type is checked for its validity as a subclass
        or instance of GraphStoreFactoryMethod. If this condition is met, the factory
        type is added to the registry with its class name as the key.

        Args:
            factory_type (GraphStoreFactoryMethod | type): The factory type, which can either
                be a subclass of GraphStoreFactoryMethod or an instance of that subclass.

        Raises:
            ValueError: If factory_type is neither a subclass of GraphStoreFactoryMethod
                nor an instance of that class. The error message will indicate the exact
                nature of the issue.
        """
        if isinstance(factory_type, type):
            if not issubclass(factory_type, GraphStoreFactoryMethod):
                raise ValueError(f'Invalid factory_type argument: {factory_type.__name__} must inherit from GraphStoreFactoryMethod.')
            _graph_store_factories[factory_type.__name__] = factory_type()
        else:
            factory_type_name = type(factory_type).__name__
            if not isinstance(factory_type, GraphStoreFactoryMethod):
                raise ValueError(f'Invalid factory_type argument: {factory_type_name} must inherit from GraphStoreFactoryMethod.')
            _graph_store_factories[factory_type_name] = factory_type

    @staticmethod
    def for_graph_store(graph_info:GraphStoreType=None, **kwargs) -> GraphStore:
        """
        Creates and returns a GraphStore instance based on the provided connection
        information or factory methods.

        This method is responsible for initializing the GraphStore by either using
        the given `graph_info` directly (if it is an instance of `GraphStore`) or
        by utilizing the registered factory methods to create an appropriate
        GraphStore instance. If factory creation is not successful, it raises a
        `ValueError`.

        Args:
            graph_info (GraphStoreType, optional): Connection information or an
                existing instance of `GraphStore`. If it's an instance of `GraphStore`,
                it will be returned directly. Otherwise, it is used to create the
                GraphStore instance.
            **kwargs: Additional keyword arguments passed to factory methods during
                GraphStore creation. An explicit graph_store_type restricts selection
                to registered factories declaring that discriminator.

        Returns:
            GraphStore: An instance of `GraphStore` created using the provided
                information.

        Raises:
            ValueError: If the `graph_info` is unrecognized or the GraphStore
                creation fails. Provides detailed information to ensure the
                graph store connection info is correctly formatted and a suitable
                factory method is registered.
        """
        if graph_info and isinstance(graph_info, GraphStore):
            return graph_info
        
        factories = list(_graph_store_factories.values())
        graph_store_type = kwargs.get('graph_store_type')
        if graph_store_type is not None:
            factories = [factory for factory in factories if factory.graph_store_type == graph_store_type]
            if not factories:
                raise ValueError(f'Unrecognized graph store type: {graph_store_type}. Register a factory for this type first.')

        for factory in factories:
            graph_store = factory.try_create(graph_info, **kwargs)
            if graph_store:
                return graph_store
            
        raise ValueError(f'Unrecognized graph store info: {graph_info}. Check that the graph store connection info is formatted correctly, and that an appropriate graph store factory method is registered with GraphStoreFactory.')

