# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import llama_index.core.async_utils
import logging as l

logger = l.getLogger(__name__)

def _asyncio_run(coro):
    
    l.debug('Patching asyncio_run() to run coroutine on existing event loop to support notebooks.')   
        
    try:
        loop = asyncio.get_event_loop()

        return loop.run_until_complete(coro)

    except RuntimeError as e:
        try:
            return asyncio.run(coro)
        except RuntimeError as e:
            raise RuntimeError(
                "Detected nested async. Please use nest_asyncio.apply() to allow nested event loops."
                "Or, use async entry methods like `aquery()`, `aretriever`, `achat`, etc."
            )

try:
    loop = asyncio.get_event_loop()
    if loop.is_running:
        llama_index.core.async_utils.asyncio_run = _asyncio_run
except RuntimeError as e:
    pass  

from .tenant_id import TenantId, DEFAULT_TENANT_ID, TenantIdType, to_tenant_id
from .config import GraphRAGConfig as GraphRAGConfig, LLMType, EmbeddingType
from .errors import ModelError, BatchJobError, IndexError
from .logging import set_logging_config, set_advanced_logging_config
from .lexical_graph_query_engine import LexicalGraphQueryEngine
from .lexical_graph_index import LexicalGraphIndex
from .lexical_graph_index import ExtractionConfig, BuildConfig, IndexingConfig
from . import utils
from . import indexing
from . import retrieval
from . import storage



