# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Callable, List
from graphrag_toolkit.core.types import Document

class SourceDocuments:
    """
    Represents a collection of source document generator functions.

    This class is designed to encapsulate a list of callable functions that generate
    source documents. It provides an iterable interface to iterate through all source
    documents produced by these functions. The class supports handling nested lists
    of documents, yielding individual document items.

    Attributes:
        source_documents_fns (List[Callable[[], List[Document]]]): A list of callable
            functions that, when invoked, return lists of documents or nested lists
            of documents.
    """
    def __init__(self, source_documents_fns: List[Callable[[], List[Document] ]]):
        """
        Initializes an instance of the class, setting up the source document functions.

        Args:
            source_documents_fns (List[Callable[[], List[Document]]]): A list of
                callables. Each callable, when executed, is expected to return a
                list of Document objects.
        """
        self.source_documents_fns = source_documents_fns
        
    def __iter__(self):
        """
        Yields items from the nested lists or the iterable objects provided by source_documents_fns.

        This method iterates through the callable objects in the `source_documents_fns` attribute, which
        are expected to return iterable collections. It recognizes nested lists, iterates through them,
        and yields individual items. If the iterable is already flat, it directly yields the items.

        Yields:
            Any: The individual elements extracted from the nested or flat iterable structures
            returned by the callables in `source_documents_fns`.
        """
        for source_documents_fn in self.source_documents_fns:
            for source_documents in source_documents_fn(): 
                if isinstance(source_documents, list):              
                    for item in source_documents:                        
                        if isinstance(item, list):
                            for i in item:
                                yield i
                        else:
                            yield item
                else:
                    yield source_documents