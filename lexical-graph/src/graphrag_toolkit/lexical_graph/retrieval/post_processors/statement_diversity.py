# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import re

from typing import List, Optional, Any, Callable
from pydantic import Field

from graphrag_toolkit.lexical_graph import ModelError
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResult

from llama_index.core.postprocessor.types import BaseNodePostprocessor
from llama_index.core.schema import NodeWithScore, QueryBundle, BaseNode

logger = logging.getLogger(__name__)

def _all_text(node:BaseNode) -> str:
    """
    Extracts the textual content of a given node.

    This function retrieves the text attribute from the provided node object.
    The function assumes the node has a text attribute which contains the
    desired string content.

    Args:
        node (BaseNode): The node object from which the text content will be
            retrieved. The node must have a `text` attribute.

    Returns:
        str: The text content of the provided node.
    """
    return node.text

def _topics_and_statements(node:BaseNode) -> str:
    """
    Constructs a string containing topics and statements retrieved from a given node's text by
    validating it against a model.

    The function processes textual data from a provided node, extracting the topic and associated
    statements from it. The resulting topic and statements are combined into a single string, where
    each entry is placed on a new line. This function utilizes a search result model for validation
    and parsing.

    Args:
        node (BaseNode): The input node containing textual data, which will be parsed and processed
            to extract topics and statements.

    Returns:
        str: A string comprising the extracted topic and statements, with each entry separated by
            a newline.
    """
    lines = []
    search_result = SearchResult.model_validate_json(node.text)
    lines.append(search_result.topic)
    for statement in search_result.statements:
        lines.append(statement)
    return '\n'.join(lines)

def _topics(node:BaseNode) -> str:
    """
    Processes a given node to extract and return the topic information.

    This function takes a node object, validates its text content as a JSON
    representation of a `SearchResult`, and extracts the associated topic.

    Args:
        node: A `BaseNode` object containing the text data to be validated and
            processed.

    Returns:
        The topic extracted from the `SearchResult` object as a string.

    Raises:
        ValidationError: If the JSON validation for the `SearchResult` fails.
    """
    lines = []
    search_result = SearchResult.model_validate_json(node.text)
    return search_result.topic

ALL_TEXT = _all_text
TOPICS_AND_STATEMENTS = _topics_and_statements
TOPICS = _topics

class StatementDiversityPostProcessor(BaseNodePostprocessor):
    """Postprocessor for ensuring diversity among statements.

    This class preprocesses textual data and postprocesses nodes to ensure that
    similar or duplicate entries are reduced based on a given similarity threshold.
    Using spaCy for natural language processing and TF-IDF for text similarity
    calculation, this postprocessor identifies and filters out redundant nodes.
    It is intended to work with query-based document retrieval or node filtering
    pipelines.

    Attributes:
        similarity_threshold (float): Threshold value for determining similarity
            between two text nodes. Text nodes with a cosine similarity above this
            threshold are considered duplicates and are filtered out.
        nlp (Any): Instance of spaCy NLP object used for text preprocessing. This
            object is configured with specific components including a sentencizer.
        text_fn (Callable[[BaseNode], str]): Callable function used to extract text
            from a given node.
    """
    
    similarity_threshold: float = Field(default=0.975)
    nlp: Any = Field(default=None)
    text_fn: Callable[[BaseNode], str] = Field(default=None)

    def __init__(self, similarity_threshold: float = 0.975, text_fn = None):
        """
        Initializes an instance of the class with the specified similarity threshold and
        text function. Loads the spaCy language model for text processing, specifically
        disabling named entity recognition and parsing, but adding a sentence segmenter.
        If the required spaCy model is not found, raises a ModelError with instructions
        to install the missing model.

        Args:
            similarity_threshold (float): The threshold for similarity comparison, which should
                be a value between 0 and 1. Defaults to 0.975.
            text_fn (optional): A function to apply to text data when processing.
                If not provided, defaults to ALL_TEXT.

        Raises:
            ModelError: If the required spaCy model ('en_core_web_sm') is not installed or
                cannot be loaded, an exception is raised.
        """
        super().__init__(
            similarity_threshold=similarity_threshold,
            text_fn = text_fn or ALL_TEXT
        )

        try:
            import spacy
        except ImportError as e:
            raise ImportError(
                "spacy package not found, install with 'pip install spacy'"
            ) from e
        
        try:
            self.nlp = spacy.load("en_core_web_sm", disable=['ner', 'parser'])
            self.nlp.add_pipe('sentencizer')
        except OSError:
            raise ModelError("Please install the spaCy model using: python -m spacy download en_core_web_sm")

    def preprocess_texts(self, texts: List[str]) -> List[str]:
        """
        Preprocesses a list of texts by tokenizing, lemmatizing, and replacing numeric
        values with specific placeholders. This method removes stopwords and
        punctuations, converts tokens to lowercase, normalizes numbers by replacing
        them with placeholders, and generates a list of preprocessed strings.

        Args:
            texts (List[str]): A list of textual inputs to preprocess by tokenizing,
                removing stopwords and punctuations, lemmatizing, and replacing numeric
                values with placeholders.

        Returns:
            List[str]: A list of preprocessed texts where numeric values are normalized
                with placeholders, stopwords and punctuations are removed, and tokens
                are converted to their lemmatized, lowercase forms.
        """
        preprocessed_texts = []
        float_pattern = re.compile(r'\d+\.\d+')
        
        for text in texts:
            doc = self.nlp(text)
            tokens = []
            for token in doc:
                if token.like_num: 
                    if float_pattern.match(token.text):
                        tokens.append(f"FLOAT_{token.text}")
                    else:
                        tokens.append(f"NUM_{token.text}")
                elif not token.is_stop and not token.is_punct:
                    tokens.append(token.lemma_.lower())
            preprocessed_texts.append(' '.join(tokens))
        return preprocessed_texts
    
    def _postprocess_nodes(
        self,
        nodes: List[NodeWithScore],
        query_bundle: Optional[QueryBundle] = None,
    ) -> List[NodeWithScore]:
        """
        Post-processes a list of nodes by removing duplicates based on text similarity.

        This method preprocesses the given list of nodes and filters out nodes that have
        a high textual similarity. It uses TF-IDF vectorization combined with cosine similarity
        to determine the similarity between nodes and removes duplicates that exceed a defined
        similarity threshold. The method ensures that only distinct nodes are included in the
        output.

        Args:
            nodes:
                A list of NodeWithScore objects, where each node contains a score and associated text.
            query_bundle:
                Optional parameter of type QueryBundle to provide additional preprocessing context.

        Returns:
            A filtered list of NodeWithScore objects containing only unique nodes based on
            text similarity.
        """
        if not nodes:
            return nodes
        
        try:
            import numpy as np
        except ImportError as e:
            raise ImportError(
                "numpy package not found, install with 'pip install numpy'"
            ) from e
        
            
        # Preprocess texts
        texts = [self.text_fn(node.node) for node in nodes]
        preprocessed_texts = self.preprocess_texts(texts)

        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity
        except ImportError as e:
            raise ImportError(
                "scikit-learn package not found, install with 'pip install scikit-learn'"
            ) from e

        # Calculate TF-IDF similarity
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(preprocessed_texts)
        cosine_sim_matrix = cosine_similarity(tfidf_matrix)

        # Track which nodes to keep
        keep_indices = []
        already_selected = np.zeros(len(nodes), dtype=bool)

        for idx in range(len(nodes)):
            if not already_selected[idx]:
                keep_indices.append(idx)
                already_selected[idx] = True

                # Find similar statements
                similar_indices = np.where(cosine_sim_matrix[idx] > self.similarity_threshold)[0]
                for sim_idx in similar_indices:
                    if not already_selected[sim_idx]:
                        logger.debug(
                            f"Removing duplicate (similarity: {cosine_sim_matrix[idx][sim_idx]:.4f}):\n"
                            f"Kept: {texts[idx]}\n"
                            f"Removed: {texts[sim_idx]}"
                        )
                        already_selected[sim_idx] = True

        return [nodes[i] for i in keep_indices]
