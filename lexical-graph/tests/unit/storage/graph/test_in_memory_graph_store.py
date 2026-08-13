# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Property tests for in-memory graph store (DummyGraphStore).

This module tests idempotence properties for the in-memory graph store,
verifying that operations like adding nodes are idempotent.
"""

import pytest
from hypothesis import given, strategies as st, settings, assume
from graphrag_toolkit.lexical_graph.storage.graph import DummyGraphStore


# Hypothesis strategy for generating node data
node_strategy = st.fixed_dictionaries({
    'id': st.text(min_size=1, max_size=50, alphabet=st.characters(
        whitelist_categories=('Lu', 'Ll', 'Nd'),
        whitelist_characters='-_'
    )),
    'label': st.sampled_from(['Document', 'Chunk', 'Entity', 'Topic']),
    'properties': st.dictionaries(
        keys=st.text(min_size=1, max_size=20, alphabet=st.characters(
            whitelist_categories=('Lu', 'Ll'),
            whitelist_characters='_'
        )),
        values=st.one_of(
            st.text(max_size=100),
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False),
            st.booleans()
        ),
        min_size=0,
        max_size=10
    )
})


def node_parameters(node, **extra_properties):
    """
    Build the query parameters for a node.

    Properties go in their own 'properties' entry, which is what
    `SET n += $properties` binds to, rather than being splatted alongside 'id'
    and 'label'. A generated property named 'id' therefore updates the node's
    id property instead of replacing the id the MERGE matches on.

    The id and label are written into the properties as well, so the two views
    of the node agree. That mirrors Neptune and Neo4j, where a node carries its
    id both as the identifier and as a property.
    """
    properties = {
        **node['properties'],
        **extra_properties,
        'id': node['id'],
        'node_id': node['id'],
        'label': node['label']
    }

    return {
        'id': node['id'],
        'label': node['label'],
        'properties': properties
    }


def node_merge_query(label):
    """
    Build the MERGE query for a node with the given label.

    The label goes into the query text rather than being bound as a parameter.
    openCypher has no way to parameterize a label, so `MERGE (n:$label ...)`
    would not run against Neptune or Neo4j. Production writes labels the same
    way: every MERGE in src/ carries a literal label and binds only values.

    The label is still passed in the parameters as well, because the store
    reads it from there when recording the node.
    """
    return f"""
    MERGE (n:{label} {{id: $id}})
    SET n += $properties
    RETURN n
    """


class InMemoryGraphStoreForTesting(DummyGraphStore):
    """
    Extended DummyGraphStore that actually stores nodes for testing idempotence.
    
    This implementation maintains an in-memory dictionary of nodes to enable
    testing of idempotence properties. It overrides execute_query to handle
    MERGE operations for node creation.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._nodes = {}  # Store nodes by ID
    
    def _execute_query(self, cypher, parameters={}, correlation_id=None):
        """
        Execute query with basic node storage for testing.
        
        Handles MERGE operations to add nodes idempotently.
        """
        # Call parent for logging
        super()._execute_query(cypher, parameters, correlation_id)
        
        # Handle MERGE operations for node creation
        if cypher and 'MERGE' in cypher.upper():
            node_id = parameters.get('id')
            if node_id:
                # MERGE matches on the id alone, so a second call with the same
                # id updates the node it found rather than creating another.
                node = self._nodes.setdefault(node_id, {
                    'id': node_id,
                    'label': parameters.get('label', 'Node'),
                    'properties': {}
                })
                node['label'] = parameters.get('label', node['label'])
                # `SET n += $properties` updates the properties it names and
                # leaves any others in place.
                node['properties'].update(parameters.get('properties', {}))

        # Handle MATCH operations to retrieve nodes
        elif cypher and 'MATCH' in cypher.upper():
            node_id = parameters.get('id')
            if node_id and node_id in self._nodes:
                return [self._nodes[node_id]]
        
        return []
    
    def get_node_count(self):
        """Return the number of unique nodes stored."""
        return len(self._nodes)
    
    def get_node(self, node_id):
        """Retrieve a node by ID."""
        return self._nodes.get(node_id)


@given(node=node_strategy)
@settings(max_examples=100, deadline=None)
def test_add_node_idempotence_property(node):
    """
    **Validates: Requirements 5.6**
    
    Property: Adding same node twice is idempotent.
    
    For any node with an ID, adding it to the graph store twice should
    produce the same result as adding it once. The node count should
    remain the same, and the node data should be identical.
    
    This property ensures that graph stores handle duplicate additions
    gracefully without creating duplicate nodes or corrupting data.
    """
    store = InMemoryGraphStoreForTesting()

    parameters = node_parameters(node)

    # Add node first time
    store.execute_query(node_merge_query(node['label']), parameters)
    count_after_first = store.get_node_count()
    node_after_first = store.get_node(node['id'])
    
    # Add same node second time (should be idempotent)
    store.execute_query(node_merge_query(node['label']), parameters)
    count_after_second = store.get_node_count()
    node_after_second = store.get_node(node['id'])
    
    # Property: Node count should remain the same
    assert count_after_first == count_after_second, \
        f"Node count changed: {count_after_first} -> {count_after_second}"
    
    # Property: Node data should be identical
    assert node_after_first == node_after_second, \
        f"Node data changed after second add"
    
    # Property: Exactly one node should exist
    assert count_after_first == 1, \
        f"Expected 1 node, got {count_after_first}"


@given(node=node_strategy)
@settings(max_examples=50, deadline=None)
def test_add_node_multiple_times_idempotence_property(node):
    """
    **Validates: Requirements 5.6**
    
    Property: Adding same node N times is idempotent.
    
    For any node, adding it multiple times (N > 2) should produce
    the same result as adding it once. This extends the basic
    idempotence property to verify it holds for any number of
    repeated additions.
    """
    store = InMemoryGraphStoreForTesting()
    
    parameters = node_parameters(node)

    # Add node multiple times
    for _ in range(5):
        store.execute_query(node_merge_query(node['label']), parameters)
    
    # Property: Should have exactly one node
    assert store.get_node_count() == 1, \
        f"Expected 1 node after 5 additions, got {store.get_node_count()}"
    
    # Property: Node should exist with correct data
    stored_node = store.get_node(node['id'])
    assert stored_node is not None, "Node not found after additions"
    assert stored_node['id'] == node['id'], "Node ID mismatch"
    assert stored_node['label'] == node['label'], "Node label mismatch"


@given(
    node1=node_strategy,
    node2=node_strategy
)
@settings(max_examples=50, deadline=None)
def test_add_different_nodes_not_idempotent_property(node1, node2):
    """
    **Validates: Requirements 5.6**
    
    Property: Adding different nodes increases count.
    
    For any two nodes with different IDs, adding both should result
    in two nodes being stored. This verifies that idempotence only
    applies to the same node, not different nodes.
    """
    # Two generated nodes can share an id. Discard those examples rather than
    # returning, so hypothesis draws another pair instead of recording a pass
    # for a case the test never exercised.
    assume(node1['id'] != node2['id'])

    store = InMemoryGraphStoreForTesting()
    
    # Add first node
    store.execute_query(node_merge_query(node1['label']), node_parameters(node1))

    # Add second node
    store.execute_query(node_merge_query(node2['label']), node_parameters(node2))
    
    # Property: Should have two distinct nodes
    assert store.get_node_count() == 2, \
        f"Expected 2 nodes with different IDs, got {store.get_node_count()}"
    
    # Property: Both nodes should be retrievable
    assert store.get_node(node1['id']) is not None, "First node not found"
    assert store.get_node(node2['id']) is not None, "Second node not found"


@given(node=node_strategy)
@settings(max_examples=50, deadline=None)
def test_add_node_with_updated_properties_idempotence_property(node):
    """
    **Validates: Requirements 5.6**
    
    Property: Adding node with updated properties is idempotent.
    
    When adding the same node (by ID) with different properties,
    the node should be updated (not duplicated), maintaining
    idempotence at the node identity level.
    """
    store = InMemoryGraphStoreForTesting()
    
    # Add node with initial properties
    store.execute_query(node_merge_query(node['label']), node_parameters(node))

    # Add the same node again, this time carrying a property it did not have
    # before. 'updated' is passed as an extra property rather than alongside
    # 'id', so a generated property of the same name cannot mask it.
    store.execute_query(node_merge_query(node['label']), node_parameters(node, updated=True))
    
    # Property: Should still have exactly one node
    assert store.get_node_count() == 1, \
        f"Expected 1 node after update, got {store.get_node_count()}"
    
    # Property: Node should exist
    stored_node = store.get_node(node['id'])
    assert stored_node is not None, "Node not found after update"
    assert stored_node['id'] == node['id'], "Node ID changed after update"

    # Property: The update reached the node's properties
    assert stored_node['properties']['updated'] is True, \
        "Updated property not applied to the existing node"

    # Property: The id property agrees with the id the node is stored under,
    # whatever the generated properties contained
    assert stored_node['properties']['id'] == node['id'], \
        "Node id property does not match the node id"
