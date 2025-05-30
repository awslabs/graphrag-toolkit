from collections import defaultdict, deque
import heapq

class GTraversal:
    """
    Class for traversing the graph with different strategies.
    graph_store is a GraphStore Object 
    """
    def __init__(self, graph_store):
        self.graph_store = graph_store
    
    def one_hop_expand(self, source_nodes, edge_type=None, return_src_id=False):
        """
        Expand the source nodes to their one-hop neighbors.

        Args:
            source_nodes (list): List of source nodes.
            edge_type (str): Edge type to use for expansion. If None, use all edge types.
            return_src_id (bool): Whether to return the source node of each expanded node.

        Returns:
            set: Set of expanded nodes or Dict of expanded nodes from each source_node
        """
        if return_src_id:
            expanded_nodes = defaultdict(set)
            expanded_edge_set = defaultdict(set)
        else:
            expanded_nodes = set()
            expanded_edge_set = set()

        outgoing_edges = self.graph_store.get_one_hop_edges(source_nodes)
        if edge_type:
            for node_id in outgoing_edges:
                if edge_type in outgoing_edges[node_id]:
                    if return_src_id:
                        expanded_edge_set[node_id] |= set(outgoing_edges[node_id][edge_type])
                    else:
                        expanded_edge_set |= set(self.graph_store.get_edge_destination_nodes(outgoing_edges[node_id][edge_type]))
        else:
            for node_id in outgoing_edges:
                for edge_type in outgoing_edges[node_id]:
                    if return_src_id:
                        expanded_edge_set[node_id] |= set(outgoing_edges[node_id][edge_type])
                    else:
                        expanded_edge_set |= set(self.graph_store.get_edge_destination_nodes(outgoing_edges[node_id][edge_type]))
                        
        if return_src_id:
            for src_id in expanded_edge_set:
                dst_nodes = self.graph_store.get_edge_destination_nodes(expanded_edge_set[src_id])
                for edge_id, node_ids in dst_nodes.items():
                    expanded_nodes[src_id] |= set(node_ids)
        else:
            dst_nodes = self.graph_store.get_edge_destination_nodes(expanded_edge_set)
            for edge_id, node_ids in dst_nodes.items():
                expanded_nodes |= set(node_ids)
        
        return expanded_nodes

    def one_hop_triplets(self, source_nodes, index_type=None):
        """
        Expand the triplets of the source nodes.

        Args:
            source_nodes (list): List of source nodes.
            index_type: Not used here. 

        Returns:
            set: Set of expanded triplets for all source nodes.
        """
        source_nodes = set(source_nodes)
        triplets = set()
        expanded_edges = self.graph_store.get_one_hop_edges(list(source_nodes), return_triplets=True)
        for node_id in expanded_edges:
            for edge_type in expanded_edges[node_id]:
                #need to convert to tuple again if loaded from json file
                triplets.update([tuple(triplet) for triplet in expanded_edges[node_id][edge_type]])
        return triplets

    def get_destination_triplet_nodes(self, triplets):
        """
        Extract destination nodes from a collection of triplets.

        Args:
            triplets: An iterable of triplets where each triplet is in the form
                    (source, relation, destination)

        Returns:
            list: A list of destination nodes extracted from the triplets.
        """
        return [dst for _, _, dst in triplets]

    def multi_hop_triplets(self, source_nodes, index_type=None, hop=2):
        """
        Retrieve triplets by traversing the graph up to specified number of hops from source nodes.
        
        Args:
            source_nodes (list): List of source nodes.
            hop (int): Number of hops to traverse
            
        Returns:
            set: Set of expanded triplets for the number of hops.
        """
        
        expanded_nodes = source_nodes
        current_triplets = set()
        for h in range(hop-1):
            expanded_triplets = self.one_hop_triplets(expanded_nodes)
            expanded_nodes = self.get_destination_triplet_nodes(expanded_triplets)
            current_triplets |= expanded_triplets
        return self.one_hop_triplets(expanded_nodes) | current_triplets
    
    def follow_paths(self, source_nodes, metapaths):
        """
        Follow paths in a BFS style.

        Args:
            source_nodes (list): List of source nodes to start from.
            metapaths (list): List of metapaths to follow.

        Returns:
            list: List of paths found.
        """
        result_paths = []
        for start_node in source_nodes:
            for metapath in metapaths:
                    
                
                queue = deque([(start_node, [])])  
                while queue:
                    current_node, current_path = queue.popleft()
                    
                    if len(current_path) == len(metapath):
                        result_paths.append(current_path)
                        
                    if len(current_path) < len(metapath):
                        
                        expanded_edges = self.graph_store.get_one_hop_edges([current_node], return_triplets=True)


                        #for node_id in expanded_edges:
                        if current_node not in expanded_edges:
                            continue
                        for edge_type in expanded_edges[current_node]:
                            if edge_type != metapath[len(current_path)].strip() or len(current_path) > len(metapath): 
                                continue

                            for neighbor in self.get_destination_triplet_nodes(expanded_edges[current_node][edge_type]):
                                if neighbor == current_node:
                                    continue
                                queue.append((neighbor, current_path + [(current_node, edge_type, neighbor)]))  
        return result_paths
    
    def shortest_paths(self, source_nodes, target_nodes, max_distance=None):
        """
        Find shortest paths from source nodes to target nodes using Dijkstra's algorithm.
        Returns paths in the same format as follow_paths: list of paths, where each path is a list of triplets.

        Args:
            source_nodes (list): List of source nodes to start from
            target_nodes (list): List of target nodes to find paths to
            max_distance (int, optional): Maximum distance to search for paths. If None, no limit.

        Returns:
            list: List of paths, where each path is a list of triplets (src, rel, dst)
        """
        # Initialize distances and paths
        distances = defaultdict(lambda: float('inf'))
        paths = defaultdict(list)
        visited = set()
        
        # Priority queue for Dijkstra's algorithm
        # Format: (distance, node, path_triplets)
        pq = []
        
        # Initialize source nodes
        for source in source_nodes:
            distances[source] = 0
            paths[source] = []  # Empty path for source node
            heapq.heappush(pq, (0, source, []))
        
        while pq:
            current_dist, current_node, current_path = heapq.heappop(pq)
            
            # Skip if we've already found a shorter path to this node
            if current_dist > distances[current_node]:
                continue
                
            # Skip if we've already visited this node
            if current_node in visited:
                continue
                
            visited.add(current_node)
            
            # If we've reached all target nodes, we can stop
            if all(node in visited for node in target_nodes):
                break
                
            # Get one-hop triplets
            triplets = self.one_hop_triplets([current_node])
            
            # Explore neighbors through triplets
            for triplet in triplets:
                src, rel, dst = triplet
                if src != current_node:  # Skip if source doesn't match current node
                    continue
                    
                # Calculate new distance (assuming unit weight for all edges)
                new_dist = current_dist + 1
                
                # Skip if we've exceeded max_distance
                if max_distance and new_dist > max_distance:
                    continue
                
                # If we found a shorter path to the neighbor
                if new_dist < distances[dst]:
                    distances[dst] = new_dist
                    new_path = current_path + [triplet]  # Add the entire triplet
                    paths[dst] = new_path
                    heapq.heappush(pq, (new_dist, dst, new_path))
        
        # Convert paths to list of triplet paths
        result_paths = []
        for target, path in paths.items():
            if not path:  # Skip empty paths (source nodes)
                continue
            if target not in target_nodes:
                continue
            result_paths.append(path)
        
        return result_paths
    
    