from typing import List, Tuple, Optional, Set
import os
import sys
# Add the parent directory to the Python path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from utils import load_yaml, parse_response


class ByoKGQueryEngine:
    """
    A query engine that orchestrates the retrieval and generation pipeline for knowledge graph queries.
    This class handles the high-level flow of query processing while delegating LLM-specific tasks
    to the KGLinker.
    """

    def __init__(self, 
                 graph_store,
                 entity_linker=None,
                 triplet_retriever=None,
                 path_retriever=None,
                 graph_query_executor=None,
                 llm_generator=None,
                 kg_linker=None,
                 direct_query_linking=False):
        """
        Initialize the query engine.

        Args:
            graph_store: Component that provides access to graph data
            entity_linker: Optional component for linking entities to graph nodes
            triplet_retriever: Optional component for retrieving triplets
            path_retriever: Optional component for retrieving paths
            graph_query_executor: Optional component for executing graph queries
            llm_generator: Optional language model for generating responses
            direct_query_linking: flag whether to use entity linker with query embedding directly and can run without KG Linker LLM call
        """
        self.graph_store = graph_store
        self.schema = graph_store.get_schema()

        if llm_generator is None:
            from llm import BedrockGenerator
            llm_generator= BedrockGenerator(
                model_name='us.anthropic.claude-3-5-sonnet-20240620-v1:0',
                region_name='us-west-2')
        self.llm_generator = llm_generator
            
        if entity_linker is None:
            from indexing import FuzzyStringIndex
            from graph_retrievers import EntityLinker
            string_index = FuzzyStringIndex()
            string_index.add(self.graph_store.nodes())
            entity_retriever = string_index.as_entity_matcher()
            entity_linker = EntityLinker(entity_retriever)
        self.entity_linker = entity_linker
        self.direct_query_linking = direct_query_linking
        
        if triplet_retriever is None:
            from graph_retrievers import AgenticRetriever
            from graph_retrievers import GTraversal, TripletGVerbalizer
            graph_traversal = GTraversal(self.graph_store)
            graph_verbalizer = TripletGVerbalizer()
            triplet_retriever = AgenticRetriever(
                llm_generator=self.llm_generator, 
                graph_traversal=graph_traversal,
                graph_verbalizer=graph_verbalizer)
        self.triplet_retriever = triplet_retriever
        
        if path_retriever is None:
            from graph_retrievers import PathRetriever
            from graph_retrievers import GTraversal, PathVerbalizer
            graph_traversal = GTraversal(self.graph_store)
            path_verbalizer = PathVerbalizer()
            path_retriever = PathRetriever(
                graph_traversal=graph_traversal,
                path_verbalizer=path_verbalizer)
        self.path_retriever = path_retriever

        if graph_query_executor is None and hasattr(graph_store, 'execute_query'):
            from graph_retrievers import GraphQueryRetriever
            graph_query_executor = GraphQueryRetriever(self.graph_store)
        self.graph_query_executor = graph_query_executor

        if kg_linker is None:
            from graph_connectors import KGLinker
            kg_linker = KGLinker(
                llm_generator=self.llm_generator,
                graph_store=self.graph_store
            )
        self.kg_linker = kg_linker
        self.kg_linker_prompts = self.kg_linker.task_prompts
        self.kg_linker_prompts_iterative = self.kg_linker.task_prompts_iterative

    def _add_to_context(self, context_list: List[str], new_items: List[str]) -> None:
        """
        Add new items to context list while maintaining order and avoiding duplicates.
        
        Args:
            context_list: The list to add items to
            new_items: New items to add
        """
        seen = set(context_list)
        for item in new_items:
            if item not in seen:
                context_list.append(item)
                seen.add(item)


    def query(self, query: str, iterations: int = 1) -> Tuple[List[str], List[str]]:
        """
        Process a query through the retrieval and generation pipeline.

        Args:
            query: The search query
            iterations: Number of retrieval iterations to perform

        Returns:
            Tuple of (retrieved context, final answers)
        """
        retrieved_context: List[str] = []
        explored_entities: Set[str] = set()
        opencypher_answers: List[str] = []

        if self.direct_query_linking:
            semantic_linked_entities = self.entity_linker.link([query], return_dict=False)
            explored_entities.update(semantic_linked_entities)
        else:
            semantic_linked_entities = []

        for iteration in range(iterations):
            # Generate response for current iteration

            if iteration == 0:
                task_prompts = self.kg_linker_prompts
            else:
                task_prompts = self.kg_linker_prompts_iterative
            response = self.kg_linker.generate_response(
                question=query,
                schema=self.schema,
                graph_context="\n".join(retrieved_context) if retrieved_context else "",
                task_prompts = task_prompts
            )
            artifacts = self.kg_linker.parse_response(response)

            # Process extracted entities
            linked_entities = []
            if "entity-extraction" in artifacts and "FINISH" not in artifacts["entity-extraction"][0]:
                linked_entities = self.entity_linker.link(artifacts["entity-extraction"], return_dict=False)
                explored_entities.update(linked_entities)

            # Process answer entities
            linked_answers = []
            if "draft-answer-generation" in artifacts:
                linked_answers = self.entity_linker.link(artifacts["draft-answer-generation"], return_dict=False)

            # Retrieve triplets if we have source entities
            source_entities = list(set(semantic_linked_entities + linked_entities + linked_answers))

            if source_entities and self.triplet_retriever:
                triplet_context = self.triplet_retriever.retrieve(query, source_entities)
                self._add_to_context(retrieved_context, triplet_context)

            # Process paths if available
            if "path-extraction" in artifacts and explored_entities and self.path_retriever:
                metapaths = [[component.strip() for component in path.split("->")] for path in artifacts["path-extraction"]]
                path_context = self.path_retriever.retrieve(list(explored_entities), metapaths,linked_answers)
                self._add_to_context(retrieved_context, path_context)

            # Process graph queries
            for query_type in ["opencypher-neptune-rdf", "opencypher-neptune"]:
                if query_type in artifacts and self.graph_query_executor:
                    graph_query = " ".join(artifacts[query_type])
                    context = self.graph_query_executor.retrieve(graph_query, return_answers=False)
                    self._add_to_context(retrieved_context, context)

            task_completion = parse_response(response, r"<task-completion>(.*?)</task-completion>")
            if "FINISH" in " ".join(task_completion):
                break

        return retrieved_context

    def generate_response(self, query: str, graph_context: str = "", task_prompt = None) -> Tuple[List[str], str]:
        
        if task_prompt is None:
            task_prompt = load_yaml("prompts/generation_prompts.yaml")["generate-response-qa"]
            user_prompt_formatted = task_prompt.format(
                question=query, 
                graph_context=graph_context
            )
            response =  self.llm_generator.generate(
                prompt=user_prompt_formatted, 
            )
            answers = parse_response(response, r"<answers>(.*?)</answers>")
            return answers, response
        else:
            raise NotImplementedError
