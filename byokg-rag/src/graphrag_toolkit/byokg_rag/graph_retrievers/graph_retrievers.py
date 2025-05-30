from abc import ABC
import os
import sys
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from utils import load_yaml, parse_response


class GRetriever(ABC):
    """
    Abstract base class for GRetriever.
    This class defines the retrieval process over the graph.
    """
    def __init__(self, *args, **kwargs):
        pass

    def retrieve(self, query: str, **kwargs):
        pass

class AgenticRetriever(GRetriever):
    """
    An agentic retriever that implements an iterative exploration strategy to retrieve relevant information
    from a knowledge graph. It uses a combination of graph linking, traversal, and verbalization to
    find and process relevant triplets.

    The retrieval process follows these steps:
    1. Start with source nodes
    2. Iteratively explore relevant relations and entities
    3. Prune and rerank results at each step
    4. Build context from verbalized triplets
    5. Continue until max iterations or early finish condition
    """

    def __init__(self, llm_generator, graph_traversal, graph_verbalizer, pruning_reranker=None, 
                 max_num_relations=5, max_num_entities=3, max_num_iterations=3, max_num_triplets=50):
        """
        Initialize the AgenticRetriever.

        Args:
            llm_generator: Language model for generating responses
            graph_traversal: Component for traversing the graph
            graph_verbalizer: Component for converting graph elements to text
            pruning_reranker: Component for pruning and reranking results
            max_num_relations (int): Maximum number of relations to consider
            max_num_entities (int): Maximum number of entities to explore
            max_num_iterations (int): Maximum number of exploration iterations
            max_num_triplets (int): Maximum number of triplets to keep after pruning
        """
        self.llm_generator = llm_generator
        self.graph_traversal = graph_traversal
        self.graph_verbalizer = graph_verbalizer
        self.pruning_reranker = pruning_reranker
       

        self.max_num_relations = max_num_relations
        self.max_num_entities = max_num_entities
        self.max_num_iterations = max_num_iterations
        self.max_num_triplets = max_num_triplets


        self.prompt = load_yaml("graph_retrievers/prompts/agent_prompts.yaml")

    def relation_search_prune(self, query, entities, max_num_relations=20):
        """
        Search and prune relations based on relevance to the query.
        
        Args:
            query (str): The search query
            entities (list): List of entities to search from
            max_num_relations (int): Maximum number of relations to return
            
        Returns:
            list: Pruned list of relevant relations
        """
        triplets = list(self.graph_traversal.one_hop_triplets(entities))
        if len(triplets) == 0:
            return []
        
        relations = set(self.graph_verbalizer.verbalize_relations(triplets))
        if len(relations) == 0:
            return []

        if self.pruning_reranker is not None:
            relations, scores, ids = self.pruning_reranker.rerank_input_with_query(
                query, list(relations), topk=max_num_relations, return_scores=True
            )
        return relations

    def retrieve(self, query: str, source_nodes, history_context=None):
        """
        Retrieve relevant information from the graph using an iterative exploration strategy.

        Args:
            query (str): The search query
            source_nodes (list): List of source nodes to start exploration from
            history_context (list, optional): Previously retrieved context to build upon.
                If None, starts with an empty context.

        Returns:
            list: Retrieved triplets in text form, maintaining the order of discovery
        """
        new_entities_to_explore = source_nodes
        if history_context is None:
            history_context = []
        retrieved_triplets = []

        for iteration in range(self.max_num_iterations):
            # Get current batch of entities to explore
            entities_to_explore = new_entities_to_explore.copy()
            new_entities_to_explore = set()

            # Get triplets for current entities
            triplets = list(self.graph_traversal.one_hop_triplets(entities_to_explore))
            if len(triplets) == 0:
                continue

            # Get and prune relevant relations
            rels = self.relation_search_prune(query, entities_to_explore, max_num_relations=20)

            # Select top relations using LLM
            prompt = self.prompt["relation_selection_prompt"]
            prompt = prompt.format(
                question=query,
                entity="\n".join(entities_to_explore),
                relations=rels
            )
            response = self.llm_generator.generate(prompt)

            # Parse selected relations
            top_rels = parse_response(response, r"<selected>(.*?)</selected>")
            if len(top_rels) == 0:
                top_rels = [rel.strip() for rel in response.split("\n")]
            else:
                top_rels = [rel.strip() for rel in top_rels]

            # Filter triplets based on selected relations
            current_entity_relations_list = []
            for triplet in triplets:
                if self.graph_verbalizer.verbalize_relations([triplet])[0] in top_rels:
                    current_entity_relations_list.append(triplet)

            if len(current_entity_relations_list) == 0:
                continue

            # Verbalize and prune triplets
            text_triplets = self.graph_verbalizer.verbalize_merge_triplets(current_entity_relations_list)
            if len(text_triplets) > self.max_num_triplets:
                text_triplets, ids = self.pruning_reranker.rerank_input_with_query(
                    query, text_triplets, topk=self.max_num_triplets
                )

            # Add new context to history and track retrieved triplets
            for text_triplet in text_triplets:
                if text_triplet not in history_context:
                    history_context.append(text_triplet)
                    retrieved_triplets.append(text_triplet)

            # Select next entities to explore
            prompt = self.prompt["entity_selection_prompt"]
            prompt = prompt.format(
                question=query,
                graph_context="\n".join(history_context)
            )
            response = self.llm_generator.generate(prompt)
            entities = parse_response(response, r"<next-entities>(.*?)</next-entities>")
            
            if len(entities) == 0:
                continue

            # Check for early finish condition
            for ent in entities:
                if "FINISH" in ent:
                    return retrieved_triplets
            
            # Update entities for next iteration
            new_entities_to_explore = [ent.strip() for ent in entities.copy()]

        return retrieved_triplets

class GraphScoringRetriever(GRetriever):
    """
    A retriever that uses graph traversal and scoring to find relevant information.
    This retriever implements a multi-hop exploration strategy with pruning and reranking
    to efficiently retrieve relevant triplets from the knowledge graph.
    """

    def __init__(self, graph_traversal, graph_verbalizer, graph_reranker, pruning_reranker=None):
        """
        Initialize the GraphScoringRetriever.

        Args:
            graph_traversal: Component for traversing the graph
            graph_verbalizer: Component for converting graph elements to text
            graph_reranker: Component for reranking retrieved results
            pruning_reranker: Optional component for pruning results before reranking
        """
        self.graph_traversal = graph_traversal
        self.graph_verbalizer = graph_verbalizer
        self.graph_reranker = graph_reranker
        self.pruning_reranker = pruning_reranker

    def retrieve(self, query: str, source_nodes, hops=2, topk=None, max_num_relations=20, 
                max_num_triplets=100, **kwargs):
        """
        Retrieve relevant information from the graph using multi-hop traversal with pruning and reranking.

        Args:
            query (str): The search query
            source_nodes (list): List of source nodes to start from
            hops (int, optional): Number of hops to traverse. Defaults to 2.
            topk (int, optional): Maximum number of results to return. Defaults to None.
            max_num_relations (int): Maximum number of relations to keep after pruning
            max_num_triplets (int): Maximum number of triplets to keep after pruning
            **kwargs: Additional keyword arguments for the retrieval process

        Returns:
            list: Retrieved and reranked triplets in text form
        """
        # Initialize source nodes
        if not source_nodes:
            return []   

        # Get multi-hop triplets
        triplets = list(self.graph_traversal.multi_hop_triplets(source_nodes, hop=hops))
        if not triplets:
            return []
        
        # Get and filter relations
        relations = set(self.graph_verbalizer.verbalize_relations(triplets))
        if not relations:
            return []

        # Apply pruning if needed and reranker is available
        if len(relations) > max_num_relations and self.pruning_reranker:
            relations, ids = self.pruning_reranker.rerank_input_with_query(
                query, list(relations), topk=max_num_relations
            )

        # Filter triplets based on selected relations
        filtered_triplets = []
        for triplet in triplets:
            if self.graph_verbalizer.verbalize_relations([triplet])[0] in relations:
                filtered_triplets.append(triplet)

        # Merge and verbalize triplets
        merged_triplets = self.graph_verbalizer.verbalize_merge_triplets(filtered_triplets)
    
        # Apply pruning to merged triplets if needed
        if len(merged_triplets) > max_num_triplets and self.pruning_reranker:
            merged_triplets, ids = self.pruning_reranker.rerank_input_with_query(
                query, merged_triplets, topk=max_num_triplets
            )

        # Final reranking to get topk results
        retrieved_triplets, _ = self.graph_reranker.rerank_input_with_query(
            query, merged_triplets, topk=topk
        )
        return retrieved_triplets

class PathRetriever(GRetriever):
    """
    A retriever that specializes in finding and verbalizing paths in the knowledge graph.
    It supports both metapath-based traversal and shortest path finding between nodes.
    """

    def __init__(self, graph_traversal, path_verbalizer):
        """
        Initialize the PathRetriever.

        Args:
            graph_traversal: Component for traversing the graph
            path_verbalizer: Component for converting paths to text
        """
        if not hasattr(graph_traversal, 'follow_paths'):
            raise AttributeError("graph_traversal must implement 'follow_paths' method")
        if not hasattr(graph_traversal, 'shortest_paths'):
            raise AttributeError("graph_traversal must implement 'shortest_paths' method")
        self.graph_traversal = graph_traversal
        self.path_verbalizer = path_verbalizer

    def follow_paths(self, source_nodes, metapaths):
        """
        Follow predefined metapaths from source nodes and verbalize the results.

        Args:
            source_nodes (list): List of starting nodes
            metapaths (list): List of metapaths to follow

        Returns:
            list: Verbalized paths following the metapaths
        """
        paths_retrieved = self.graph_traversal.follow_paths(source_nodes, metapaths)
        if len(paths_retrieved) == 0:
            return []
        paths_verbalized = self.path_verbalizer.verbalize(paths_retrieved)
        return paths_verbalized
    
    def shortest_paths(self, source_nodes, target_nodes):
        """
        Find shortest paths between source and target nodes and verbalize the results.

        Args:
            source_nodes (list): List of starting nodes
            target_nodes (list): List of target nodes

        Returns:
            list: Verbalized shortest paths between sources and targets
        """
        paths_retrieved = self.graph_traversal.shortest_paths(source_nodes, target_nodes)
        if len(paths_retrieved) == 0:
            return []
        paths_verbalized = self.path_verbalizer.verbalize(paths_retrieved)
        return paths_verbalized
    
    def retrieve(self, source_nodes, metapaths = [], target_nodes = [], **kwargs):
        """
        Retrieve paths by combining metapath traversal and shortest paths.

        Args:
            source_nodes (list): List of starting nodes
            metapaths (list): List of metapaths to follow
            target_nodes (list): List of target nodes for shortest paths
            **kwargs: Additional keyword arguments for path retrieval

        Returns:
            list: Unique verbalized paths combining both metapath and shortest path results
        """
        # Use a set to track unique paths
        unique_paths = set()
        
        # Get paths following metapaths if provided
        if len(metapaths) > 0:
            paths_followed = self.follow_paths(source_nodes, metapaths, **kwargs)
            unique_paths.update(paths_followed)
        
        # Get shortest paths if target nodes are provided
        if len(target_nodes) > 0:
            paths_shortest = self.shortest_paths(source_nodes, target_nodes, **kwargs)
            unique_paths.update(paths_shortest)
        
        # Convert back to list while preserving order
        return list(unique_paths)


class GraphQueryRetriever(GRetriever):
    """
    A retriever that executes graph queries and verbalizes the results.
    This retriever is designed to work with graph stores that support query execution
    and provides options for returning both results and answers.
    """

    def __init__(self, graph_store):
        """
        Initialize the GraphQueryRetriever.

        Args:
            graph_store: Component that implements graph query execution
        Raises:
            AttributeError: If graph_store doesn't implement required methods
        """
        if not hasattr(graph_store, 'execute_query'):
            raise AttributeError("graph_store must implement 'execute_query' method")
        self.graph_store = graph_store

    def retrieve(self, graph_query: str, return_answers=False, **kwargs):
        """
        Execute a graph query and return the verbalized results.

        Args:
            graph_query (str): The graph query to execute
            return_answers (bool, optional): Whether to return answers along with results.
                Defaults to False.
            **kwargs: Additional keyword arguments for query execution

        Returns:
            If return_answers is True:
                tuple: (list of verbalized results, answers)
            If return_answers is False:
                list: Verbalized results

        Raises:
            Exception: If query execution or verbalization fails
        """
        try:
            # Execute the query
            results = self.graph_store.execute_query(graph_query)
            
            # Verbalize the results
            import json
            results_str = json.dumps(results)
            context = f"Graph Query: {graph_query}\nExecution Result: {results_str}"

            # Return based on return_answers flag
            if return_answers:
                return [context], results
            else:
                return [context]
                
        except Exception as e:
            # Log the error and re-raise
            if return_answers:
                return [f"Error executing query: {graph_query}"], []
            else:
                return [f"Error executing query: {graph_query}"]

