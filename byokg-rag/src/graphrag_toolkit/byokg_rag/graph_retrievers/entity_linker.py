from abc import ABC, abstractmethod
from typing import List


class Linker(ABC):
    """
    Abstract base class for Linker.
    This class defines the interface for query to entity linking.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the Linker instance.
        """
        pass

    @abstractmethod
    def link(self, queries: List[str], return_dict=True, **kwargs):
        """
        Process to link the given queries to graph (nodes/edges).

        Args:
            queries (List[str]): List of input query texts to perform graph linking on.
            return_dict: Whether to return a dictionary of linking results or linked entities only.
            **kwargs: Additional keyword arguments for graph linking configuration.

        Returns: List[Dict] if return_dict else List[str]
            List[Dict]: A list of dictionaries containing linking results for each query.
                Each dictionary has the following structure:
                {
                    'hits': [
                        {
                            'document_id': List[str],  # List of matched entity IDs
                            'document': List[str],     # List of matched entity documents
                            'match_score': List[float] # List of matching scores
                        }
                    ]
                }
            or 

            List[str]: A list of matched nodes, i.e., documents or entities 
        """
        if return_dict:
            return [{'hits': [{'document_id': [],
                            'document': [],
                            'match_score': []}
                            ]
                    } for _ in queries]
        else:
            return [[] for _ in queries]

        
class EntityLinker(Linker):
    """
    The EntityLinker instance which performs two step linking.

    If entity_extractor is passed then step 1 is to use the entity extractors to extract entities.
    Step 2 is to use retriever i.e entity matcher to retrieve most similar entities from the index
    """

    def __init__(self, retriever=None, topk=3, **kwargs):
        """
        Initialize the EntityLinker instance.

        Args:
            retriever: An indexing.EntityMatcher object.
            topk: How many items to return per extracted entity per query.
            **kwargs: Additional keyword arguments for graph linking configuration.
        """
        self.retriever = retriever
        self.topk = topk

    def link(self, query_extracted_entities, retriever=None, topk=None, id_selector=None, return_dict=True):
        """
        Process to link the given or extracted query entities to graph entities.

        Args:
            queries (List[str]): List of entity lists to perform graph linking on.
            retriever (object, optional): A retriever object to use for entity lookup.
                If None, the default retriever configured for this instance will be used.
            topk (int, optional): The number of items to return per extracted entity
            id_selector (list, optional): A list of ids to retrieve the topk from i.e an allowlist
            return_dict: Whether to return a dictionary of linking results or linked entities only.
        Returns:
            List[Dict] if return_dict else List[str]
            List[Dict]: A list of dictionaries containing linking results for each query.
            or
            List[str]: A list of matched entities


        Note: topk is applied per entity
        """

        if retriever is None and self.retriever is None:
            raise ValueError("Error: Either 'retriever' or 'self.retriever' must be provided")

        if retriever is None:
            retriever = self.retriever
        if topk is None:
            topk = self.topk

        if return_dict:
            return retriever.retrieve(queries=query_extracted_entities, topk=topk)
        else:
            results = retriever.retrieve(queries=query_extracted_entities, topk=topk)
            results = results["hits"]
            parsed_results = []
            for res in results:
                parsed_results.append(res['document_id'])
            return parsed_results