# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import List, Tuple

from graphrag_toolkit.core.types import NodeWithScore, QueryBundle

class RerankerMixin(ABC):
    """
    Provides an abstract base class for rerankers with mixin functionality.

    This class serves as a foundational mixin for implementing custom rerankers.
    It defines the required interface that any subclass must implement, including
    a property to retrieve batch size and a method to rerank given pairs of data.
    Subclasses of this mixin are expected to define domain-specific behavior for
    reranking operations.

    Attributes:
        batch_size (int): Abstract property defining the number of items processed
            in a batch by the reranker.
    """
    @property
    @abstractmethod
    def batch_size(self):
        """
        Abstract property that defines the batch size for a specific object or operation.

        This property serves as an interface for retrieving or working with the batch size,
        making it mandatory to implement in any subclass that inherits from the class which
        declares this property. Subclasses must define the behavior and value associated
        with this property.

        Attributes:
            batch_size: An integer representing the size of the batch used in the context
                of the implementation.

        """
        pass

    @abstractmethod
    def rerank_pairs(self, pairs: List[Tuple[str, str]], batch_size: int = 128) -> List[float]:
        """
        Reranks the given list of key-value pairs by assigning a numerical score to each pair.
        The reranking operation is expected to be implemented by subclasses inheriting this
        abstract method. The method takes a list of tuples (key-value pairs) and an optional
        batch size parameter to process the data in chunks.

        Args:
            pairs:
                A list of tuples, where each tuple contains two strings representing the
                key-value pair to be reranked.
            batch_size:
                An optional integer specifying the size of data chunks to process in batches.
                Default value is 128.

        Returns:
            A list of float values corresponding to the reranked scores for the given
            key-value pairs. The returned list should maintain the same order as the input list.

        """
        pass
