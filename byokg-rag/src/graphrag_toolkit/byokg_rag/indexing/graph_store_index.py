from .dense_index import DenseIndex

import logging

logger = logging.getLogger(__name__)
class NeptuneAnalyticsGraphStoreIndex(DenseIndex):
    """
        A dense text embedding index using NeptuneAnalytics as the vector store
    """

    def __init__(self, graphstore, embedding=None, distance_type="l2", embedding_s3_save_path=None):
        """

        :param graphstore: A NeptuneAnalyticsGraphStore
        :param embedding: A Embedding object to generate the embeddings
        :param distance_type: The distance metric to use
        :param embedding_s3_save_path: An s3 path of a csv file to save the embeddings that are computed. Useful for batch loading embeddings
        """
        super().__init__(embedding)

        self.graphstore = graphstore
        self.embedding_s3_save_path = embedding_s3_save_path
        if distance_type == "cosine" or distance_type == "inner_product":
            logger.warn(f'Distance type: {distance_type} not supported for: {self.__class__.__name__}')
            logger.warn('Setting Distance type to l2')
            distance_type = "l2"

        self.distance_type = distance_type

        self.top_k_by_embedding_query = '''
            CALL neptune.algo.vectors.topKByEmbedding(
                {query_embedding_vector},
                {{
                    topK: {k},
                    concurrency: 1
                }}
            )
            YIELD embedding, node, score
            RETURN embedding, node, score
        '''

        self.embedding_upsert_query = '''
            CALL neptune.algo.vectors.upsert("{node_id}", {embedding_vector})
            YIELD node, embedding, success
            RETURN node, embedding, success
        '''

    def reset(self):
        """
        reset the index to empty it contents without needed to create a new index object
        """
        logging.warning(f'Resetting index to empty is not supported for: {self.__class__.__name__}. Doing nothing')

    def query(self, input, topk=1, id_selector=None):
        """
        match a query to items in the index and return the topk results

        :param input: str the query to match
        :param topk: number of items to return
        :param id_selector: a list of ids to retrieve the topk from i.e an allowlist
        :return:
        """

        query_emb = self.embedding.embed(input)

        if id_selector is not None:
            logger.warn('id_selector not supported for: {self.__class__.__name__}, ignoring')

        response = self.graphstore.execute_query(self.top_k_by_embedding_query.format(query_embedding_vector=query_emb, k=topk))
        print(response)

        return {'hits': [{'document_id': hit["node"]["~id"],
                          'document': hit["node"],
                          'match_score': hit["score"]} for hit in response]}

    def match(self, inputs, topk=1, id_selector=None):
        """
        match entity inputs to the index

        :param input: list(str) of entities per query to match
        :param topk: number of items to return per entity
        :param id_selector: a list of ids to retrieve the topk from i.e an allowlist
        :return:
        """
        if id_selector is not None:
            logger.warn('id_selector not supported for: {self.__class__.__name__}, ignoring')

        query_embs = self.embedding.batch_embed(inputs)

        responses = [self.graphstore.execute_query(self.top_k_by_embedding_query.format(query_embedding_vector=query_emb, k=topk))
                     for query_emb in query_embs]

        return {'hits': [{'document_id': hit["node"]["~id"],
                          'document': hit["node"],
                          'match_score': hit["score"]}
                         for response in responses for hit in response]}

    def add(self, documents, embeddings=None):
        """
        add documents to the index

        :param documents: list of documents to add

        """
        raise NotImplementedError(f"index.add is ambiguous and not implemented for {self.__class__.__name__}, use add_with_ids instead")

    def add_with_ids(self, ids, documents=None, embeddings=None, embedding_s3_save_path=None):
        """
        Add documents with their given ids to the index. Computes embeddings if necessary

        :param ids: list of ids for each document
        :param documents: list of document text to add

        """
        if embeddings is None:
            embeddings = self.embedding.batch_embed(documents)

        embedding_s3_save_path = embedding_s3_save_path or self.embedding_s3_save_path
        if embedding_s3_save_path is not None:
            embedding_file_contents = "\n".join(["~id,embedding:vector"] + [f'{id},{e}' for id,e in zip(ids, embeddings)])
            self.graphstore._upload_to_s3(embedding_s3_save_path, file_contents=embedding_file_contents)
            self.graphstore.read_from_csv(s3_path=embedding_s3_save_path) # load embeddings in NA directly from csv
        else: # directly upsert embeddings into the graph via query
            for id, embedding in zip(ids, embeddings):
                self.graphstore.execute_query(self.embedding_upsert_query.format(node_id=id,embedding_vector=embedding))