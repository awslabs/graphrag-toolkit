# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List
from sqlalchemy import create_engine
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import DatabaseReaderConfig
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document

logger = logging.getLogger(__name__)

class DatabaseReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for databases using LlamaIndex's DatabaseReader."""

    def __init__(self, config: DatabaseReaderConfig):
        try:
            
            from llama_index.readers.database.base import DatabaseReader, SQLDatabase
        except ImportError as e:
            logger.error("Failed to import DatabaseReader: missing dependencies")
            raise ImportError(
                "llama-index-readers-database package not found, install with 'pip install llama-index-readers-database'"
            ) from e

        try:
            logger.debug(f"Creating database engine for: {config.connection_string[:20]}...")
            engine = create_engine(config.connection_string)
            sql_database = SQLDatabase(engine)
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}", exc_info=True)
            raise RuntimeError(f"Failed to create database connection: {e}") from e

        super().__init__(
            config=config,
            reader_cls=DatabaseReader,
            sql_database=sql_database
        )

        self.database_config = config
        self.metadata_fn = config.metadata_fn
        logger.debug("Initialized DatabaseReaderProvider")

    def read(self, input_source) -> List[Document]:
        query = input_source or self.database_config.query
        if not query:
            logger.error("No SQL query provided")
            raise ValueError("A SQL query must be provided either via input_source or config.query")

        logger.info(f"Executing database query: {query[:100]}...")
        
        try:
            documents = self._reader.load_data(query=query)
            logger.info(f"Successfully read {len(documents)} document(s) from database")

            if self.metadata_fn:
                for doc in documents:
                    doc.metadata.update(self.metadata_fn(query))

            return documents
        except Exception as e:
            logger.error(f"Failed to execute database query: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read from database: {e}") from e
