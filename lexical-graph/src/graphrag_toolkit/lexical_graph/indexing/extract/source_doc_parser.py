# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import abc
from typing import Iterable

from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument

from graphrag_toolkit.core.compat import BaseComponent

class SourceDocParser(BaseComponent):
    """
    Parses source documents and provides an interface for handling document parsing logic.

    This class serves as an abstract base for implementing source document parsing
    functionality. The main purpose of the class is to define a generic interface that
    subclasses implement to customize how source documents are parsed. It ensures a
    consistent API for parsing while delegating the specific implementation of parsing
    to the subclasses.

    Attributes:
        None
    """
    @abc.abstractmethod
    def _parse_source_docs(self, source_documents:Iterable[SourceDocument]) -> Iterable[SourceDocument]:
        """
        Parses a collection of source documents and processes them into a specified format.

        This method is intended to be overridden by subclasses to provide specific
        logic for processing the input documents and transforming them into the desired
        output. The input documents should be iterable, and the output must also be an
        iterable containing the processed documents. This is an abstract method, and
        instantiating the containing class without implementing this method will result
        in errors.

        Args:
            source_documents: An iterable of `SourceDocument` instances representing
                the input documents to be processed.

        Returns:
            An iterable of `SourceDocument` instances representing the processed
            version of the source documents.

        Raises:
            NotImplementedError: If the method is not implemented by the subclass.
        """
        pass

    def parse_source_docs(self, source_documents:Iterable[SourceDocument]) -> Iterable[SourceDocument]:
        """
        Parses a collection of source documents and processes them through an internal
        parsing mechanism.

        Args:
            source_documents: An iterable of SourceDocument objects to be parsed. Each
                document is expected to contain the necessary structure and metadata
                required by the parser.

        Returns:
            An iterable of SourceDocument objects that have been processed and parsed
            using the internal parsing mechanism.
        """
        return self._parse_source_docs(source_documents)