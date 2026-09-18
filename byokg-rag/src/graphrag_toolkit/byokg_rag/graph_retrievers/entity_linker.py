# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
    def link(self, queries: List[str], return_dict=True, group_by_mention=False, **kwargs):
        """
        Process to link the given queries to graph (nodes/edges).

        Args:
            queries: List of input query texts to perform graph linking on
            return_dict: Whether to return a dictionary of linking results or linked entities only
            group_by_mention: Return one candidate list per query instead of a flat
                union, so the caller can tell which candidate came from which query.
                Takes precedence over return_dict. Implementations that cannot group
                should fall back to one lookup per query.
            **kwargs: Additional keyword arguments for graph linking configuration

        Returns:
            If return_dict is True:
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
            If return_dict is False:
                List[str]: A list of matched nodes, i.e., documents or entities
            If group_by_mention is True:
                List[List[str]]: One candidate list per query, in query order
        """
        if group_by_mention:
            return [[] for _ in queries]
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
            retriever: An indexing.EntityMatcher object
            topk: How many items to return per extracted entity per query
            **kwargs: Additional keyword arguments for graph linking configuration
        """
        self.retriever = retriever
        self.topk = topk

    def link(self, query_extracted_entities, retriever=None, topk=None, id_selector=None,
             return_dict=True, group_by_mention=False):
        """
        Process to link the given or extracted query entities to graph entities.

        Args:
            query_extracted_entities: List of entity lists to perform graph linking on
            retriever: A retriever object to use for entity lookup.
                If None, the default retriever configured for this instance will be used.
            topk: The number of items to return per extracted entity
            id_selector: A list of ids to retrieve the topk from (allowlist)
            return_dict: Whether to return a dictionary of linking results or linked entities only
            group_by_mention: Return one candidate list per mention instead of a flat
                union, so the caller can tell which candidate came from which mention.
                Takes precedence over return_dict.

        Returns:
            If group_by_mention is True:
                List[List[str]]: One best-first candidate list per mention, in the
                    same order as query_extracted_entities
            If return_dict is True:
                List[Dict]: A list of dictionaries containing linking results for each query
            If return_dict is False:
                List[str]: A list of matched entities

        Note:
            topk is applied per entity
        """

        if retriever is None and self.retriever is None:
            raise ValueError("Error: Either 'retriever' or 'self.retriever' must be provided")

        if retriever is None:
            retriever = self.retriever
        if topk is None:
            topk = self.topk

        if group_by_mention:
            return self._link_per_mention(query_extracted_entities, retriever, topk)

        results = retriever.retrieve(queries=query_extracted_entities, topk=topk)
        if return_dict:
            return results
        return [res['document_id'] for res in results["hits"]]

    def _link_per_mention(self, query_extracted_entities, retriever, topk):
        """
        Match one mention at a time so candidates stay attributed to their mention.

        Costs one retriever call per *unique* mention rather than a single batched
        call. Batching can't be regrouped after the fact: FuzzyStringIndex.match
        concatenates every mention's hits and re-sorts them globally by score, and
        drops candidates by length, so the flat result carries no per-mention
        boundaries. The dense and graph-store indexes do return hits grouped per
        input, but relying on that would make grouping index-specific.

        topk stays at the configured width instead of 1 because the fuzzy length
        filter runs after process.extract(limit=topk): at topk=1 the only candidate
        can be filtered out, and the mention would contribute nothing.
        """
        # Repeated mentions are common (parse_response does not dedup LLM output),
        # so look each one up once and reuse the result.
        per_mention = {}
        for mention in query_extracted_entities:
            if mention in per_mention:
                continue
            hits = retriever.retrieve(queries=[mention], topk=topk)["hits"]
            per_mention[mention] = [hit["document_id"] for hit in hits]
        return [per_mention[mention] for mention in query_extracted_entities]
