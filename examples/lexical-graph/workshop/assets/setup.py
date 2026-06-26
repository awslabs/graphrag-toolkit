import os
import time
import logging

from graphrag_toolkit.lexical_graph import LexicalGraphIndex, GraphRAGConfig, NoOpProgressMonitor
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs
from graphrag_toolkit.lexical_graph.indexing.build import Checkpoint

logger = logging.getLogger(__name__)


class PrintingProgressMonitor(NoOpProgressMonitor):

    def __init__(self, label):
        self.label = label
        self.graph_docs = 0
        self.graph_chunks = 0
        self.vector_docs = 0
        self.vector_chunks = 0

    def increment_graph_processed_documents(self, count=1):
        self.graph_docs += count
        logger.info(f'[{self.label}] Graph  - docs: {self.graph_docs}, chunks: {self.graph_chunks}')

    def increment_graph_processed_chunks(self, count=1):
        self.graph_chunks += count

    def increment_vector_processed_documents(self, count=1):
        self.vector_docs += count
        logger.info(f'[{self.label}] Vector - docs: {self.vector_docs}, chunks: {self.vector_chunks}')

    def increment_vector_processed_chunks(self, count=1):
        self.vector_chunks += count


def setup_datasets():

    GraphRAGConfig.build_num_workers = 4
    GraphRAGConfig.build_batch_size = 10
    GraphRAGConfig.build_batch_write_size = 150

    with (
        GraphStoreFactory.for_graph_store(os.environ['GRAPH_STORE']) as graph_store,
        VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE'], index_names=['chunk']) as vector_store
    ):

        docs_1 = FileBasedDocs(
            docs_directory='/home/ec2-user/SageMaker/graphrag-toolkit/source-data',
            collection_id='wiki-aircraft',
            zip_source='/home/ec2-user/SageMaker/graphrag-toolkit/source-data/wiki-aircraft.zip'
        )

        checkpoint_1 = Checkpoint('3-build-1')

        graph_index_1 = LexicalGraphIndex(
            graph_store,
            vector_store,
            tenant_id='aircraft'
        )

        graph_index_1.build(
            docs_1,
            checkpoint=checkpoint_1,
            show_progress=True,
            progress_monitor=PrintingProgressMonitor('aircraft')
        )

        docs_2 = FileBasedDocs(
            docs_directory='/home/ec2-user/SageMaker/graphrag-toolkit/source-data',
            collection_id='ntsb',
            zip_source='/home/ec2-user/SageMaker/graphrag-toolkit/source-data/ntsb.zip'
        )

        checkpoint_2 = Checkpoint('3-build-2')

        graph_index_2 = LexicalGraphIndex(
            graph_store,
            vector_store,
            tenant_id='ntsb'
        )

        graph_index_2.build(
            docs_2,
            checkpoint=checkpoint_2,
            show_progress=True,
            progress_monitor=PrintingProgressMonitor('ntsb')
        )

        print('Build complete')


if __name__ == '__main__':
    start = time.time()
    setup_datasets()
    end = time.time()

    print(end-start)