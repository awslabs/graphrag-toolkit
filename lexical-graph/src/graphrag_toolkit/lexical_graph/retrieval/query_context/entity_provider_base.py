# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import abc
import time
import logging
from typing import List, Optional

from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.model import ScoredEntity
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from graphrag_toolkit.core.types import QueryBundle

logger = logging.getLogger(__name__)

class EntityProviderBase():
    
    def __init__(self, graph_store:GraphStore, args:ProcessorArgs, filter_config:Optional[FilterConfig]=None):
        self.graph_store = graph_store
        self.args = args
        self.filter_config = filter_config

    @abc.abstractmethod                 
    def _get_entities(self, keywords:List[str], query_bundle:QueryBundle)  -> List[ScoredEntity]:
        raise NotImplementedError

    def get_entities(self, keywords:List[str], query_bundle:QueryBundle)  -> List[ScoredEntity]:
        
        start = time.time()
        entities = self._get_entities(keywords, query_bundle)
        end = time.time()
        duration_ms = (end-start) * 1000

        if type(self).__name__ in self.args.debug_results:
            logger.debug(f"""[{type(self).__name__}] Entities ({duration_ms:.2f} ms): {[
                entity.model_dump_json(exclude_unset=True, exclude_none=True, warnings=False) 
                for entity in entities
            ]}""")
        else:
            logger.debug(f"[{type(self).__name__}] Entities ({duration_ms:.2f} ms): {[f'{e.entity.value} ({e.entity.classification}) [{e.score}]' for e in entities]}")

        return entities[:self.args.ec_max_entities]

        