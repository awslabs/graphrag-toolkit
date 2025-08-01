from typing import List
from sqlalchemy import create_engine
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import DatabaseReaderConfig
from llama_index.core.schema import Document

class DatabaseReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for databases using LlamaIndex's DatabaseReader."""

    def __init__(self, config: DatabaseReaderConfig):
        try:
            from llama_index.readers.database.base import DatabaseReader, SQLDatabase
        except ImportError as e:
            raise ImportError(
                "DatabaseReader requires LlamaIndex's database tools and 'sqlalchemy'.\n"
                "Install with:\n"
                "  pip install llama-index-readers-database sqlalchemy"
            ) from e

        engine = create_engine(config.connection_string)
        sql_database = SQLDatabase(engine)

        super().__init__(
            config=config,
            reader_cls=DatabaseReader,
            sql_database=sql_database
        )

        self.database_config = config
        self.metadata_fn = config.metadata_fn

    def read(self, input_source) -> List[Document]:
        query = input_source or self.database_config.query
        if not query:
            raise ValueError("A SQL query must be provided either via input_source or config.query")

        documents = self._reader.load_data(query=query)

        if self.metadata_fn:
            for doc in documents:
                doc.metadata.update(self.metadata_fn(query))

        return documents
