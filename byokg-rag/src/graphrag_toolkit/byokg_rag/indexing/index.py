from abc import ABC, abstractmethod
from typing import List

class Index(ABC):
    """
    Abstract base class for indexes
    """
    def __init__(self):
        pass

    @abstractmethod
    def reset(self):
        """
        reset the index to empty it contents without needed to create a new index object
        """
        pass

    @abstractmethod
    def query(self, input, topk=1):
        """
        match a query to items in the index and return the topk results

        :param query: str the query to match
        :param topk: number of items to return
        :return:
        """
        pass

    @abstractmethod
    def add(self, documents):
        """
        add documents to the index

        :param documents: list of documents to add

        """
        pass

    def add_with_ids(self, ids, documents):
        """
        add documents with their given ids to the index

        :param ids: list of documents to add
        :param documents: list of doument ids in same order as documents
        :return:
        """
        pass

    def as_retriever(self):
        retriever = Retriever(index=self)
        return retriever
    
    def as_entity_matcher(self):
        entity_matcher = EntityMatcher(index=self)
        return entity_matcher

class Retriever:
    """
    Base class for Retriever. Given a set of queries, the retriever can process the input, query the index and potentially
    post process the output.
    """

    def __init__(self, index):
        self.index = index

    @abstractmethod
    def retrieve(self, queries:List[str], topk=1, id_selectors = None, **kwargs):
        items = []
        if isinstance(id_selectors, list):
            if all(isinstance(item, list) for item in id_selectors):
                # id selector only allows one query per time
                for query, id_selector in zip(queries, id_selectors):
                    if len(id_selector) == 0:
                        # if no id is selected skip retrieval
                        items.append({"hits": []})
                    else:
                        items.append(self.index.query(query, topk, id_selector, **kwargs))
            else:
                raise ValueError("id_selectors must be a list of lists")
        else:
            for query in queries:
                items.append(self.index.query(query, topk, **kwargs))
        return items

class EntityMatcher(Retriever):
    """
    Base class for entity matching. Given a set of extracted entities, the matcher returns the matched entities from vocab.
    """
    @abstractmethod
    def retrieve(self, queries:List[str], **kwargs):
        return self.index.match(queries, **kwargs)