# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List

from graphrag_toolkit.lexical_graph import ModelError
from graphrag_toolkit.lexical_graph.retrieval.query_context.keyword_provider_base import KeywordProviderBase
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from llama_index.core.schema import QueryBundle

class KeywordNLPProvider(KeywordProviderBase):

    def __init__(self, args:ProcessorArgs):
        super().__init__(args)

        try:
            import spacy
        except ImportError as e:
            raise ImportError(
                "spacy package not found, install with 'pip install spacy'"
            ) from e
        
        try:
            self.nlp = spacy.load('en_core_web_sm')   
        except OSError:
            raise ModelError('Please install the spaCy model using: python -m spacy download en_core_web_sm')

        
    def _get_keywords(self, query_bundle:QueryBundle) -> List[str]:        
        
        doc = self.nlp(query_bundle.query_str)

        keyword_map = {entity.text.lower():entity.text for entity in doc.ents}
        keywords = [keyword for _, keyword in keyword_map.items()]

        return keywords