# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import shutil
import json
from typing import List, Iterable
from os.path import join

from graphrag_toolkit.lexical_graph.indexing.extract.pipeline_decorator import PipelineDecorator
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument

from llama_index.core.schema import Document, BaseNode

logger = logging.getLogger(__name__)
             
class FileSystemTap(PipelineDecorator):
    """Handles file system-based input/output operations for pipeline stages.

    This class is a pipeline decorator that facilitates the management and handling
    of documents stored in a file system. It organizes files into directories based
    on raw sources, processed chunks, and processed sources. Designed for
    scenarios where intermediate and final results need to be persisted to disk.

    Attributes:
        raw_sources_dir (str): Path to the directory where raw source documents
            are stored.
        chunks_dir (str): Path to the directory where chunked documents are
            stored.
        sources_dir (str): Path to the directory where processed source
            documents are stored.
    """
    def __init__(self, subdirectory_name, clean=True, output_dir=None):
        """
        Initializes the necessary directories and sets up instance variables for managing
        subdirectories and output locations. This constructor prepares output directories
        and assigns locations for raw sources, chunks, and processed sources.

        Args:
            subdirectory_name: Name of the subdirectory where processed files will be
                organized within the output directory structure.
            clean: A flag indicating whether to clean existing content in the target
                output directories before initialization. Defaults to True.
            output_dir: The base directory for storing organized output files and
                subdirectories. Defaults to GraphRAGConfig.local_output_dir.
        """
        from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
        resolved_output_dir = output_dir if output_dir is not None else GraphRAGConfig.local_output_dir
        (raw_sources_dir, chunks_dir, sources_dir) = self._prepare_output_directories(resolved_output_dir, subdirectory_name, clean)

        self.raw_sources_dir = raw_sources_dir
        self.chunks_dir = chunks_dir
        self.sources_dir = sources_dir

    def handle_input_docs(self, docs:Iterable[SourceDocument]) -> Iterable[SourceDocument]:
        """
        Processes a collection of source documents, saving their raw textual content and JSON representation
        to specific directories if they contain a reference node.

        Args:
            docs (Iterable[SourceDocument]): A collection of source documents to be processed. Each document
                may contain a reference node used to extract the document's content and convert it to a JSON
                format.

        Returns:
            Iterable[SourceDocument]: The original collection of source documents after processing.
        """
        for doc in docs:
            if doc.refNode and isinstance(doc.refNode, Document):
                ref_node = doc.refNode
                raw_source_output_path = join(self.raw_sources_dir, ref_node.doc_id)
                source_output_path = join(self.sources_dir, f'{ref_node.doc_id}.json')
                with open(raw_source_output_path, 'w') as f:
                    f.write(ref_node.text)
                with open(source_output_path, 'w') as f:
                    f.write(ref_node.to_json())
        return docs
    
    def handle_output_doc(self, doc:SourceDocument) -> SourceDocument:
        """
        Handles the processing and storage of output documents by writing each node's
        data into a separate JSON file in the designated chunks directory.

        Args:
            doc (SourceDocument): The source document containing nodes to process
                and store.

        Returns:
            SourceDocument: The same source document passed to the function,
                unmodified.
        """
        for node in doc.nodes:
            chunk_output_path = join(self.chunks_dir, f'{node.node_id}.json')
            with open(chunk_output_path, 'w') as f:
                json.dump(node.to_dict(), f, indent=4)
        return doc    
        
    def _prepare_output_directories(self, output_dir, subdirectory_name, clean):
        """
        Prepares and organizes the output directories for processing data. The method
        creates the necessary directory structure under the provided output
        directory and an optional subdirectory name. If the `clean` parameter is set
        to True, it removes any existing directories and their contents before
        recreating them.

        Args:
            output_dir: The base output directory where the structure will be created.
            subdirectory_name: The name of the subdirectory under the output directory.
            clean: Indicates whether to delete existing directories before creating new ones.

        Returns:
            A tuple containing paths to the raw sources directory, chunks directory,
            and sources directory respectively.
        """
        raw_sources_dir = join(output_dir, 'extracted', subdirectory_name, 'raw')
        chunks_dir = join(output_dir, 'extracted', subdirectory_name, 'chunks')
        sources_dir = join(output_dir, 'extracted', subdirectory_name, 'sources')

        logger.debug(f'Preparing output directories [subdirectory_name: {subdirectory_name}, raw_sources_dir: {raw_sources_dir}, chunks_dir: {chunks_dir}, sources_dir: {sources_dir}, clean: {clean}]')
        
        if clean:
            if os.path.exists(raw_sources_dir):
                shutil.rmtree(raw_sources_dir)
            if os.path.exists(chunks_dir):
                shutil.rmtree(chunks_dir)
            if os.path.exists(sources_dir):
                shutil.rmtree(sources_dir)
        
        if not os.path.exists(raw_sources_dir):
            os.makedirs(raw_sources_dir)
        if not os.path.exists(chunks_dir):
            os.makedirs(chunks_dir)
        if not os.path.exists(sources_dir):
            os.makedirs(sources_dir)
  
        return (raw_sources_dir, chunks_dir, sources_dir)
    



    