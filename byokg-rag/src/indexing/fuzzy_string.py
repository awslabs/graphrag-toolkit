from thefuzz import fuzz, process
from abc import ABC, abstractmethod
from typing import List
from .index import Index


class FuzzyStringIndex(Index):
    """
    A class for fuzzy string matching and indexing.
    """

    def __init__(self):
        super().__init__()  # Ensure proper initialization of the base class.
        self.vocab = []

    def reset(self):
        self.vocab = []

    def query(self, input, topk=1, id_selector=None):
        """
        match a query to items in the index and return the topk results

        :param input: str the query to match
        :param topk: number of items to return
        :param id_selector: a list of ids to retrieve the topk from i.e an allowlist
        :return:
        """

        if id_selector is not None:
            raise NotImplementedError(f"id_selector not implemented for FuzzyString")

        # string matching process from thefuzz library https://pypi.org/project/thefuzz/
        results = process.extract(input, self.vocab, limit=topk)

        return {'hits': [{'document_id': match_string,
                         'document': match_string,
                         'match_score': match_score}
                for match_string, match_score in results]}


    def match(self, inputs, topk=1, id_selector=None, max_len_difference=4):
        """
        match entity inputs to vocab

        :param input: list(str) of entities per query to match
        :param topk: number of items to return
        :param id_selector: a list of ids to retrieve the topk from i.e an allowlist
        :return:
        """

        if id_selector is not None:
            raise NotImplementedError(f"id_selector not implemented for {self.__class__.__name__}")

        results = []
        for input in inputs:
            # string matching process from thefuzz library https://pypi.org/project/thefuzz/
            intermediate_results = process.extract(input, self.vocab, limit=topk)
            #skip much shorter strings
            for interintermediate_result in intermediate_results:
                if len(interintermediate_result[0]) + max_len_difference < len(input):
                    continue
                results.append(interintermediate_result)

        results = sorted(results, key=lambda x: x[1], reverse=True)

        return {'hits': [{'document_id': match_string,
                         'document': match_string,
                         'match_score': match_score}
                for match_string, match_score in results]}

    def add(self, vocab_list):
        """
        add vocab instances to the index

        :param vocab_list: list of vocab instances to add

        """
        self.vocab = list(set(self.vocab) | set(vocab_list))

    def add_with_ids(self, ids, vocab_list):
        raise NotImplementedError(f"add_with_ids not implemented for {self.__class__.__name__}")