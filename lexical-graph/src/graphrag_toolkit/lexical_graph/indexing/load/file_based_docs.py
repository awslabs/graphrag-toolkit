# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
import logging
import zipfile
from datetime import datetime
from os.path import join
from typing import List, Any, Callable, Generator, Optional, Dict

from graphrag_toolkit.lexical_graph.indexing import NodeHandler
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument, SourceType, source_documents_from_source_types
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY 

from llama_index.core.schema import TextNode, BaseNode

logger = logging.getLogger(__name__)


def windows_safe_filename(name: str) -> str:
    """Sanitize a filename for Windows compatibility by replacing :: with __ and : with _."""
    return name.replace('::', '__').replace(':', '_')


class FileBasedDocs(NodeHandler):
    """Handler for file-based document processing.

    This class is designed to process and manage source documents stored within a
    file system. It enables the preparation, reading, filtering, and writing of
    documents to and from specified directories. It facilitates document handling
    by utilizing `TextNode` and `SourceDocument` structures, while providing
    customizable metadata filtering for nodes and their relationships.

    Supports reading from ZIP archives when the ``zip_source`` parameter is
    provided. This enables cross-platform compatibility for source data that
    uses characters invalid on Windows (e.g. ``::`` in node IDs).

    Note:
        On Windows, source IDs and node IDs may contain characters (e.g. `::`)
        that are invalid in file paths. Use the ``filename_sanitizer`` parameter
        (e.g. ``windows_safe_filename``) to produce safe filesystem names.

    Attributes:
        docs_directory (str): The directory path where documents are stored.
        collection_id (str): The identifier of the document collection. If not
            provided, a timestamp-based ID is generated.
        metadata_keys (Optional[List[str]]): A list of allowed metadata keys. Only
            these keys will be retained during metadata filtering.
        filename_sanitizer (Optional[Callable[[str], str]]): An optional callable
            applied to source IDs and node IDs when constructing filesystem paths
            during writes. When None, original names are used as-is.
    """
    docs_directory:str
    collection_id:str

    metadata_keys:Optional[List[str]]
    filename_sanitizer:Optional[Callable[[str], str]]
    zip_source:Optional[str]
    
    def __init__(self, 
                 docs_directory:str, 
                 collection_id:Optional[str]=None,
                 metadata_keys:Optional[List[str]]=None,
                 filename_sanitizer:Optional[Callable[[str], str]]=None,
                 zip_source:Optional[str]=None):
        """
        Initializes the object with the specified documents directory, collection ID, and
        optional metadata keys. It also prepares the directory for the given collection ID
        within the documents directory.

        Args:
            docs_directory (str): The path to the directory where the documents will be stored.
            collection_id (Optional[str]): The ID of the collection. If not provided, defaults
                to a timestamp in the format 'YYYYMMDD-HHMMSS'.
            metadata_keys (Optional[List[str]]): A list of metadata keys to associate with
                the documents in the collection.
            filename_sanitizer (Optional[Callable[[str], str]]): A callable that transforms
                source IDs and node IDs into filesystem-safe names for the write path.
                When None (default), names are used unchanged.
            zip_source (Optional[str]): Path to a ZIP archive to read source documents from.
                When provided, documents are read from the ZIP instead of the filesystem.
                The ZIP should contain the directory structure: {collection_id}/source_id/node_id.json.
                Note: The default file format uses :: in filenames which is not compatible
                with Windows (NTFS). Use zip_source for cross-platform compatibility.
        """
        super().__init__(
            docs_directory=docs_directory,
            collection_id=collection_id or datetime.now().strftime('%Y%m%d-%H%M%S'),
            metadata_keys=metadata_keys,
            filename_sanitizer=filename_sanitizer,
            zip_source=zip_source
        )
        if not zip_source:
            self._prepare_directory(join(self.docs_directory, self.collection_id))

    def _prepare_directory(self, directory_path):
        """
        Creates a directory if it does not already exist.

        This method checks if the directory at the specified path exists, and if
        not, it creates the directory along with any necessary intermediate-level
        directories. It ensures that the provided path is ready for use without
        requiring prior manual setup.

        Args:
            directory_path (str): The file path of the directory to be verified
                or created.
        """
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

    def docs(self):
        """
        A method that provides documentation in the form of a returned value.

        This method is designed to demonstrate a specific behavior of returning
        the object itself. It does not perform additional computations or
        operations. Typically used in cases requiring fluent interfaces or
        method chaining.

        Returns:
            object: The instance of the object itself.
        """
        return self
    
    def _filter_metadata(self, node:TextNode) -> TextNode:
        """Filters specific metadata from the given TextNode based on allowed keys.

        This method modifies the `metadata` of the input `TextNode` and its
        relationships by removing any keys that are not in the allowed list
        [PROPOSITIONS_KEY, TOPICS_KEY, INDEX_KEY] or those not present in
        `self.metadata_keys` (if defined). It retains only the permitted metadata
        keys within the node and its relationships.

        Args:
            node (TextNode): The TextNode whose metadata is filtered.

        Returns:
            TextNode: The modified TextNode with filtered metadata.
        """
        def filter(metadata:Dict):
            keys_to_delete = []
            for key in metadata.keys():
                if key not in [PROPOSITIONS_KEY, TOPICS_KEY, INDEX_KEY]:
                    if self.metadata_keys is not None and key not in self.metadata_keys:
                        keys_to_delete.append(key)
            for key in keys_to_delete:
                del metadata[key]

        filter(node.metadata)

        for _, relationship_info in node.relationships.items():
            if relationship_info.metadata:
                filter(relationship_info.metadata)

        return node

    def _read_from_directory(self, directory_path: str) -> Generator[SourceDocument, None, None]:
        """Read source documents from a filesystem directory."""
        source_document_directory_paths = [f.path for f in os.scandir(directory_path) if f.is_dir()]
        
        for source_document_directory_path in source_document_directory_paths:
            nodes = []
            for filename in os.listdir(source_document_directory_path):
                file_path = os.path.join(source_document_directory_path, filename)
                if os.path.isfile(file_path):
                    with open(file_path) as f:
                        nodes.append(self._filter_metadata(TextNode.from_json(f.read())))
            yield SourceDocument(nodes=nodes)

    def _read_from_zip(self, zip_path: str) -> Generator[SourceDocument, None, None]:
        """Read source documents from a ZIP archive containing the directory structure."""
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Group files by their parent directory (source document)
            source_docs: Dict[str, List[str]] = {}
            for name in zf.namelist():
                if name.endswith('/'):
                    continue
                parts = name.split('/')
                # Expected structure: collection_id/source_id/node_id.json
                if len(parts) >= 2:
                    source_dir = parts[-2]
                    if source_dir not in source_docs:
                        source_docs[source_dir] = []
                    source_docs[source_dir].append(name)

            for file_paths in source_docs.values():
                nodes = []
                for file_path in file_paths:
                    content = zf.read(file_path).decode('utf-8')
                    nodes.append(self._filter_metadata(TextNode.from_json(content)))
                yield SourceDocument(nodes=nodes)

    def __iter__(self):
        """
        Iterates through the directories and files to yield SourceDocument objects populated
        with nodes created from the file contents.

        This method traverses the structure within a given directory path corresponding
        to the collection ID, processing files located within subdirectories to extract
        data and generate SourceDocument objects. When zip_source is configured, reads
        from the ZIP archive instead.

        Yields:
            SourceDocument: An object containing nodes created from the JSON file contents
            found in the directory structure or ZIP archive.

        Args:
            None
        """
        if self.zip_source:
            logger.debug(f'Reading source documents from ZIP archive: {self.zip_source}')
            yield from self._read_from_zip(self.zip_source)
        else:
            directory_path = join(self.docs_directory, self.collection_id)
            logger.debug(f'Reading source documents from directory: {directory_path}')
            yield from self._read_from_directory(directory_path)

    def __call__(self, nodes: List[SourceType], **kwargs: Any) -> List[SourceDocument]:
        return [n for n in self.accept(source_documents_from_source_types(nodes), **kwargs)]

    def accept(self, source_documents: List[SourceDocument], **kwargs: Any) -> Generator[SourceDocument, None, None]:
        """
        This method processes a list of source documents, organizes them into directories based
        on their source ID, and writes individual nodes of each document to separate JSON files.
        It then yields the processed source documents.

        When a filename_sanitizer is configured, it is applied to source IDs and node IDs
        to produce filesystem-safe path components.

        Args:
            source_documents (List[SourceDocument]): A list of source documents to be processed.
            **kwargs (Any): Arbitrary keyword arguments that might be used when processing
                the source documents.

        Yields:
            SourceDocument: The processed source document after its nodes have been written
                to corresponding JSON files in the directory structure.
        """
        sanitize = self.filename_sanitizer if self.filename_sanitizer else lambda x: x

        for source_document in source_documents:
            dir_name = sanitize(source_document.source_id())
            directory_path = join(self.docs_directory, self.collection_id, dir_name)
            self._prepare_directory(directory_path)
            logger.debug(f'Writing source document to directory: {directory_path}')
            for node in source_document.nodes:
                if not [key for key in [INDEX_KEY] if key in node.metadata]:
                    file_name = sanitize(node.node_id)
                    chunk_output_path = join(directory_path, f'{file_name}.json')
                    logger.debug(f'Writing chunk to file: {chunk_output_path}')
                    with open(chunk_output_path, 'w') as f:
                        json.dump(node.to_dict(), f, indent=4)
            yield source_document
