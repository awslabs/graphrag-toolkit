from abc import ABC, abstractmethod

class GraphStore(ABC):
    """
    Abstract base class for graph store implementations.
    Defines the basic interface for graph operations.
    """
    @abstractmethod
    def get_schema(self):
        """
        Return the graph schema
        :return:
        """
        pass

    @abstractmethod
    def nodes(self):
        """
        Return a list of all node_ids in the graph.

        :return: List[str] all node_ids in the graph
        """
        pass

    @abstractmethod
    def get_nodes(self, node_ids):
        """
        Return node details for given node ids

        :param node_ids: List[str] node ids
        :return: Dict[node_id:Str, Any] node details
        """
        pass

    @abstractmethod
    def edges(self):
        """
        Return a list of all edge_ids in the graph.

        :return: List[str] all edge_ids in the graph
        """
        pass

    @abstractmethod
    def get_edges(self, edge_ids):
        """
        Return edge details for given edge ids

        :param edge_ids: List[str] edge ids
        :return: Dict[edge_id:Str, Any] edge details
        """

    @abstractmethod
    def get_one_hop_edges(self, source_node_ids, return_triplets=False):
        """
        Return one hop edges given a set of source node ids.

        :param source_node_ids: List[Str] the node ids to start traversal form
        :param return_triplets: whether to return edge_ids only or return triplets in the form (src_node_id, edge, dst_node_id)
        :return: Dict[node_id:Str, Dict[edge_type:Str, edge_ids:List[Str]]] or Dict[node_id:Str, Dict[edge_type:Str, List[(node_id:Str, edge_type:Str, node_id:Str)]]]
        """
        pass

    @abstractmethod
    def get_edge_destination_nodes(self, edge_ids):
        """
        Return destination nodes given a set of edge ids.

        :param edge_ids: List[Str] the edge ids to select the destination nodes of
        :return: Dict[Str, List[Str]]
        """

        pass
    

class LocalKGStore(GraphStore):

    """
    KG Format

    {
        src_node_id: {
            "relation1": {
                "triplets": [(src, rel, dst), ..., ]
            },
            "relation2":
            {
                "triplets": [(src, rel, dst), ..., ]
            },
        },
    }

    """
    def __init__(self, graph=None):
        """
        Initialize the local knowledge graph store.

        Args:
            graph (dict, optional): Initial graph structure. If None, an empty graph is created.
        """
        self._graph = graph if graph is not None else {}

    def read_from_csv(self, csv_file, delimiter=',', has_header=True):
        """
        Read triplets from a CSV file and convert them to the knowledge graph format.
        The CSV file should have three columns: source, relation, target.

        Args:
            csv_file (str): Path to the CSV file
            delimiter (str, optional): CSV delimiter. Defaults to ','.
            has_header (bool, optional): Whether the CSV file has a header row. Defaults to True.

        Returns:
            dict: The constructed knowledge graph
        """
        import csv
        
        # Initialize empty graph
        self._graph = {}
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=delimiter)
            
            # Skip header if present
            if has_header:
                next(reader)
            
            # Process each triplet
            for row in reader:
                if len(row) < 3:
                    continue  # Skip invalid rows
                    
                source, relation, target = row[:3]
                
                # Initialize source node if not exists
                if source not in self._graph:
                    self._graph[source] = {}
                
                # Initialize relation if not exists
                if relation not in self._graph[source]:
                    self._graph[source][relation] = {"triplets": []}
                
                # Add triplet
                self._graph[source][relation]["triplets"].append((source, relation, target))
        
        return self._graph

    def get_schema(self):
        """
        Return the graph schema as a dictionary of relation labels.
        """
        relation_labels = set()
        for node in self._graph:
            for relation in self._graph[node]:
                relation_labels.add(relation)
        
        response = {
            "graphSummary": {
                "edgeLabels": list(relation_labels)
            }
        }

        return response

    def nodes(self):
        """
        Return a list of all node_ids in the graph.

        :return: List[str] all node_ids in the graph
        """
        return list(self._graph.keys())

    def get_nodes(self, node_ids):
        """
        Return node details for given node ids

        :param node_ids: List[str] node ids
        :return: Dict[node_id:Str, Any] node details
        """
        return_dict = {}
        for node_id in node_ids:
            if node_id in self._graph:
                return_dict[node_id] = self._graph[node_id]
        return return_dict

    def edges(self):
        raise NotImplementedError(f"LocalKGStore does not support a separate edge index; Please use get_triplets.")
    
    def get_edges(self, edge_ids):
        raise NotImplementedError(f"LocalKGStore does not support a separate edge index; Please use get_triplets.")

    def get_triplets(self):
        """
        Return a list of all triplets in the graph.

        :return: List[tuple] of triplets in the graph
        """
        triplets = []
        for node in self._graph:
            for relation in self._graph[node]:
                triplets.extend(self._graph[node][relation]["triplets"])
        return triplets

    def get_one_hop_edges(self, source_node_ids, return_triplets=True):
        """
        Return one hop edges (triplets) given a set of source node ids.

        :param source_node_ids: List[Str] the node ids to start traversal form
        :param return_triplets: whether to return relations only or return triplets in the form (src_node_id, relation, dst_node_id)
        :return: Dict[node_id:Str, Dict[edge_type:Str, edge_ids:List[Str]]] or Dict[node_id:Str, Dict[edge_type:Str, List[(node_id:Str, edge_type:Str, node_id:Str)]]]
        """
        one_hop_edges = {}
        if not return_triplets:
            raise ValueError(f"LocalKGStorage supports only triplet format; Please set return_triplets=True")
        
        for node_id in source_node_ids:
            if node_id in self._graph:
                one_hop_edges[node_id] = {}
                for relation in self._graph[node_id]:
                    if isinstance(self._graph[node_id][relation], dict) and "triplets" in self._graph[node_id][relation]:
                        if isinstance(self._graph[node_id][relation]["triplets"], list):
                            one_hop_edges[node_id][relation] = self._graph[node_id][relation]["triplets"]
        return one_hop_edges

    def get_edge_destination_nodes(self, edge_ids):
        raise NotImplementedError(f"LocalKGStore does not support a separate edge index and get_edge_destination_nodes is not implemented.")
    
    def get_linker_tasks(self):
        return [
            "entity-extraction",
            "path-extraction",
            "draft-answer-generation"
        ]
        