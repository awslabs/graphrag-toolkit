# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import abc


class ProgressMonitor(abc.ABC):
    """
    Abstract base class for monitoring progress of extraction and build operations.

    Implementations receive increment calls at document boundaries from the main
    process/thread. Thread safety is not required — the pipeline guarantees
    single-threaded invocation. If your implementation performs cross-thread work
    internally (e.g., pushing to a queue), that is your responsibility.

    Subclass NoOpProgressMonitor if you only need a subset of these methods.
    """

    @abc.abstractmethod
    def increment_llm_processed_documents(self, count: int = 1):
        """Called after a document completes LLM extraction."""
        pass

    @abc.abstractmethod
    def increment_llm_processed_chunks(self, count: int = 1):
        """Called after a document completes LLM extraction, with the number of chunks in that document."""
        pass

    @abc.abstractmethod
    def increment_graph_processed_documents(self, count: int = 1):
        """Called after a document's nodes are written to the graph store."""
        pass

    @abc.abstractmethod
    def increment_graph_processed_chunks(self, count: int = 1):
        """Called after a document's nodes are written to the graph store, with the number of chunks in that document."""
        pass

    @abc.abstractmethod
    def increment_vector_processed_documents(self, count: int = 1):
        """Called after a document's nodes are written to the vector store."""
        pass

    @abc.abstractmethod
    def increment_vector_processed_chunks(self, count: int = 1):
        """Called after a document's nodes are written to the vector store, with the number of chunks in that document."""
        pass


class NoOpProgressMonitor(ProgressMonitor):
    """
    A no-op implementation of ProgressMonitor.

    All methods do nothing. Subclass this and override only the methods you
    care about to avoid implementing all six abstract methods.
    """

    def increment_llm_processed_documents(self, count: int = 1):
        pass

    def increment_llm_processed_chunks(self, count: int = 1):
        pass

    def increment_graph_processed_documents(self, count: int = 1):
        pass

    def increment_graph_processed_chunks(self, count: int = 1):
        pass

    def increment_vector_processed_documents(self, count: int = 1):
        pass

    def increment_vector_processed_chunks(self, count: int = 1):
        pass
