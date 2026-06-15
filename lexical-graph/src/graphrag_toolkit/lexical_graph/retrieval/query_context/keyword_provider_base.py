# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
import abc
import time
from typing import List

from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from graphrag_toolkit.core.types import QueryBundle

logger = logging.getLogger(__name__)

class KeywordProviderBase():
    
    def __init__(self, args:ProcessorArgs):    
        self.args = args

    @abc.abstractmethod
    def _get_keywords(self, query_bundle:QueryBundle) -> List[str]:
        raise NotImplementedError

    def get_keywords(self, query_bundle:QueryBundle) -> List[str]:        
        
        start = time.time()
        keywords = self._get_keywords(query_bundle)
        end = time.time()
        duration_ms = (end-start) * 1000

        logger.debug(f'[{type(self).__name__}] Keywords ({duration_ms:.2f} ms): {keywords}')

        return keywords[:self.args.max_keywords]