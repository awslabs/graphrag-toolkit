from .index import Index
from .embedding import Embedding
import faiss
import numpy as np


class DenseIndex(Index):
    """
        Abstract base class for indexes
    """

    def __init__(self, embedding: Embedding = None):
        self.embedding = embedding


class LocalFaissDenseIndex(DenseIndex):
    """
        A local dense text embedding index using faiss
    """

    def __init__(self, embedding: Embedding = None, distance_type="l2", embedding_dim=-1):
        """

        :param embedding: an Embedding object that can embed text inputs (queries and docs) individually and in batch
        :param distance_type: distance type for faiss index one of ["l2", "cosine", "inner_product"]
        :param embedding_dim: dimension of embedding vector to save in the index
        """
        assert embedding_dim > 0, "Embedding dimension size must be passed"
        self.distance_type = distance_type
        if distance_type == "cosine" or distance_type == "inner_product":
            self.faiss_index = faiss.IndexFlatIP(embedding_dim)
        else:
            self.faiss_index = faiss.IndexFlatL2(embedding_dim)
        self.reset()

        super().__init__(embedding)

    def reset(self):
        self.doc_store = []
        self.doc_ids = []
        # mapping from doc_id to index in doc_store
        self.id2idx = {}

    def query(self, input, topk=1, id_selector=None):
        """
        match a query to items in the index and return the topk results

        :param input: str the query to match
        :param topk: number of items to return
        :param id_selector: a list of ids to retrieve the topk from i.e an allowlist
        :return:
        """

        query_emb = np.array(self.embedding.embed(input)).reshape(1, -1)

        if id_selector is None:
            D, I = self.faiss_index.search(query_emb, topk)
        else:
            # Specific IDs to include in the search
            allowed_ids = [self.id2idx[_id] for _id in id_selector]
            sel = faiss.IDSelectorBatch(allowed_ids)
            D, I = self.faiss_index.search(query_emb, topk, params=faiss.SearchParameters(sel=sel))
        return {'hits': [{'document_id': self.doc_ids[match_idx],
                          'document': self.doc_store[match_idx],
                          'match_score': match_distance}
                         for match_idx, match_distance in zip(I[0], D[0])]}

    def match(self, inputs, topk=1, id_selector=None):
        """
        match entity inputs to vocab index

        :param input: list(str) of entities per query to match
        :param topk: number of items to return
        :param id_selector: a list of ids to retrieve the topk from i.e an allowlist
        :return:
        """
        if id_selector is not None:
            raise NotImplementedError(f"id_selector not implemented for {self.__class__.__name__}")

        query_emb = np.array(self.embedding.batch_embed(inputs)).reshape(len(inputs), -1)

        D, I = self.faiss_index.search(query_emb, topk)

        return {'hits': [{'document_id': self.doc_ids[match_idx],
                          'document': self.doc_store[match_idx],
                          'match_score': match_distance}
                         for idx in range(len(inputs)) for match_idx, match_distance in zip(I[idx], D[idx])]}

    def add(self, documents, embeddings=None):
        """
        add documents to the index

        :param documents: list of documents to add

        """
        start_id = len(self.doc_ids)
        doc_ids = [f"doc{i}" for i in range(start_id, start_id + len(documents))]
        return self.add_with_ids(doc_ids, documents, embeddings)

    def add_with_ids(self, ids, documents, embeddings=None):
        """
        Add documents with their given ids to the index.

        Does not support update when the same id is used

        @TODO check if id already exists and throw error
        @TODO potentially support upserts and deletes

        :param ids: list of ids for each document
        :param documents: list of document text to add

        """
        if isinstance(embeddings, np.ndarray):
            doc_embs = embeddings
        else:
            doc_embs = np.array(self.embedding.batch_embed(documents))
        self.doc_store.extend(documents)
        self.doc_ids.extend(ids)
        for _id in ids:
            self.id2idx[_id] = len(self.id2idx)
        self.faiss_index.add(doc_embs)