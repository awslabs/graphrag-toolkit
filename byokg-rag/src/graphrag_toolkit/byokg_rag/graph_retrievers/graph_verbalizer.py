from abc import ABC, abstractmethod
from typing import List, Dict, Tuple
from collections import defaultdict


class GVerbalizer(ABC):
    """
    Abstract base class for GVerbalizer.
    This class converts graph edges into natural language.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the GVerbalizer instance.
        """
        pass

    @abstractmethod
    def verbalize(self, edges: List):
        """
        Process to convert graph edges into natural language format.

        Args:
            edges (List[Dict]): List of graph edges to be converted.

        Returns:
            List[str]: A list of natural language descriptions corresponding to the input edges.
        """
        pass


class TripletGVerbalizer(GVerbalizer):

    def __init__(self, delimiter=None, merge_delimiter=None):
        """
        Initialize the TripletGVerbalizer instance.
        """
        self.delimiter = delimiter if delimiter is not None else '->'
        self.merge_delimiter = merge_delimiter if merge_delimiter is not None else '|'

    def validate_and_process(self, edges:List[Tuple]):
        """
        Validate the triples and apply any necessary preprocessing.

        Returns only valid triples. Raises error if none of the triples are valid

        :param edges: List[tuple(str, str, str)] List of triples to validate
        :return: List[tuple(str, str, str)] validated triplets
        """
        valid_triplets = [triplet for triplet in edges if len(triplet) == 3]
        if not valid_triplets:
            invalid_triplet = edges[0] if edges else None
            raise ValueError(f"No valid triplets found. Triplets must be length 3, but got {invalid_triplet}")

        return valid_triplets


    def verbalize(self, edges: List[Tuple]):
        """
        Convert graph edges into natural language format using triplets.

        Args:
            edges (List[Tuple]): List of graph triplets to be converted.
            Assumes the format of each tuple is (src, rel, dst)

        Returns:
            List[str]: A list of natural language descriptions corresponding to the input edges.
        """
        valid_triplets = self.validate_and_process(edges)

        return [f"{triplet[0]} {self.delimiter} {triplet[1]} {self.delimiter} {triplet[2]}" for triplet in valid_triplets]
    
    def verbalize_relations(self, edges: List[Tuple]):
        """
        Return relation descriptions only
        """

        return [f"{triplet[1]}" for triplet in self.validate_and_process(edges)]
    
    def verbalize_head_relations(self, edges: List[Tuple]):
        """
        Return head and relation descriptions 
        """

        return [f"{triplet[0]} {self.delimiter} {triplet[1]}" for triplet in self.validate_and_process(edges)]
    
    def verbalize_merge_triplets(self, edges: List[Tuple], max_retain_num=-1):
        """
        Merge tails of triplets with the same head and relation and verbalize.
        Retain max_retain_num tails to avoid long-context.
        """
        head_relations =  set(self.verbalize_head_relations(edges))
        return_set = defaultdict(list)
        for triplet in self.validate_and_process(edges):
            if f"{triplet[0]} {self.delimiter} {triplet[1]}" in head_relations:
                return_set[f"{triplet[0]} {self.delimiter} {triplet[1]}"].append(triplet[2])
        context_list = []
        for key in return_set:
            tails = return_set[key]
            if max_retain_num > 0 and len(tails) > max_retain_num:
                import random
                tails = random.sample(tails, max_retain_num)
            tail = f" {self.merge_delimiter} ".join(tails)
            context_list.append(f"{key} {self.delimiter} {tail}")
        return context_list



class PathVerbalizer(GVerbalizer):
    """
    A verbalizer that converts graph paths into natural language descriptions.
    This class handles both single-hop and multi-hop paths, with support for
    merging and formatting path components.
    """

    def __init__(self, graph_verbalizer=None, delimiter=None, merge_delimiter=None):
        """
        Initialize the PathVerbalizer.

        Args:
            graph_verbalizer: Optional verbalizer for handling individual triplets.
                Defaults to TripletGVerbalizer.
            delimiter: String to use as delimiter between path components.
                Defaults to '->'.
            merge_delimiter: String to use when merging multiple relations.
                Defaults to '>'.
        """
        self.graph_verbalizer = graph_verbalizer if graph_verbalizer is not None else TripletGVerbalizer()
        self.delimiter = delimiter if delimiter is not None else '->'
        self.merge_delimiter = merge_delimiter if merge_delimiter is not None else '>'

    def _validate_path(self, path: List) -> bool:
        """
        Validate that a path contains valid triplets.

        Args:
            path: List of triplets representing a path.

        Returns:
            bool: True if path is valid, False otherwise.
        """
        if not path:
            return False
        return all(len(triplet) == 3 for triplet in path)

    def _verbalize_single_path(self, path: List) -> Tuple[str, bool]:
        """
        Convert a single path into a verbalized format.

        Args:
            path: List of triplets representing a path.

        Returns:
            Tuple[str, bool]: Verbalized path and whether it's a single-hop path.
        """
        if not self._validate_path(path):
            raise ValueError(f"Invalid path format: {path}")

        graph_path_verb = ""
        single_hop = False

        for triplet in path:
            if not graph_path_verb:
                graph_path_verb = f"{triplet[0]} {self.delimiter} {triplet[1]} {self.delimiter} {triplet[2]}"
                single_hop = True
            else:
                graph_path_verb += f"{self.delimiter} {triplet[1]} {self.delimiter} {triplet[2]}"
                single_hop = False

        return graph_path_verb, single_hop

    def _split_path_components(self, path_verb: str, single_hop: bool) -> List[str]:
        """
        Split a verbalized path into its components.

        Args:
            path_verb: Verbalized path string.
            single_hop: Whether this is a single-hop path.

        Returns:
            List[str]: List containing [start, mid, end] components.
        """
        components = path_verb.split(f" {self.delimiter} ")
        start = components[0]
        mid = components[1:-1]
        end = components[-1]

        if not single_hop:
            mid = f" {self.merge_delimiter} ".join(mid)
        else:
            mid = mid[0]

        return [start, mid, end]

    def verbalize(self, graph_paths: List[List]) -> List[str]:
        """
        Convert graph paths into natural language descriptions.

        Args:
            graph_paths: List of graph paths to verbalize, where each path is a
                sequence of triplets (head, relation, tail).

        Returns:
            List[str]: List of natural language descriptions of the graph paths.

        Raises:
            ValueError: If any path contains invalid triplets.
        """
        if not graph_paths:
            return []

        graph_paths_verbalized = []
        for path in graph_paths:
            try:
                # Verbalize the path
                path_verb, single_hop = self._verbalize_single_path(path)
                
                # Split into components and add to list
                components = self._split_path_components(path_verb, single_hop)
                graph_paths_verbalized.append(components)
            except ValueError as e:
                print(f"Warning: Skipping invalid path: {str(e)}")
                continue

        # Merge paths with same head and relation
        return self.graph_verbalizer.verbalize_merge_triplets(graph_paths_verbalized)