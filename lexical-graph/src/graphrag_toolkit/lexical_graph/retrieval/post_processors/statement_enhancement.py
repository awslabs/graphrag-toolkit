# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import concurrent.futures
import re
import logging

from pydantic import Field
from typing import List, Optional

from llama_index.core.postprocessor.types import BaseNodePostprocessor
from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode
from llama_index.core.prompts import ChatPromptTemplate
from llama_index.core.llms import ChatMessage, MessageRole

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage.chunk import ChunkStore
from graphrag_toolkit.lexical_graph.storage.chunk_store_factory import ChunkStoreFactory
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.retrieval.prompts import ENHANCE_STATEMENT_SYSTEM_PROMPT, ENHANCE_STATEMENT_USER_PROMPT

logger = logging.getLogger(__name__)

class StatementEnhancementPostProcessor(BaseNodePostprocessor):
    """
    Post-processes nodes to enhance their statements using provided language model and templates.

    The `StatementEnhancementPostProcessor` class is responsible for enhancing textual statements
    associated with nodes. This class utilizes a language model and specific templates to improve
    the quality or formatting of the statements based on the provided chunk context. The enhancement
    is performed concurrently on multiple nodes for efficiency.

    Attributes:
        llm (Optional[LLMCache]): Language model cache used for enhancing statements. Defaults to
            `None`, in which case a default LLM configuration is used.
        max_concurrent (int): Maximum number of nodes to process concurrently. Defaults to 10.
        system_prompt (str): System-level prompt used as part of the enhancement template.
        user_prompt (str): User-level prompt used as part of the enhancement template.
        enhance_template (ChatPromptTemplate): Template used to structure the prompts for the
            language model.
    """

    llm: Optional[LLMCache] = Field(default=None)
    chunk_store: Optional[ChunkStore] = Field(default=None)
    max_concurrent: int = Field(default=10)
    system_prompt: str = Field(default=ENHANCE_STATEMENT_SYSTEM_PROMPT)
    user_prompt: str = Field(default=ENHANCE_STATEMENT_USER_PROMPT)
    enhance_template: ChatPromptTemplate = Field(default=None)

    @staticmethod
    def _resolve_chunk_store(graph_store) -> Optional[ChunkStore]:
        """Open the chunk store this post-processor reads statement context from.

        An external store configured through `S3_CHUNK_STORE` is opened whether or
        not a graph store is supplied; a graph store only adds the in-graph
        fallback for chunks written before the migration. Without either, chunk
        text has to be carried on the node.

        Raises ValueError if `S3_CHUNK_STORE` holds a URI no registered factory
        recognises, so a misconfigured store surfaces at construction rather than
        as unenhanced statements at query time.
        """
        chunk_store_info = GraphRAGConfig.s3_chunk_store

        if not chunk_store_info and graph_store is None:
            return None

        return ChunkStoreFactory.for_chunk_store(chunk_store_info, graph_store=graph_store)

    def __init__(
        self,
        llm:LLMCacheType=None,
        system_prompt: str = ENHANCE_STATEMENT_SYSTEM_PROMPT,
        user_prompt: str = ENHANCE_STATEMENT_USER_PROMPT,
        max_concurrent: int = 10,
        graph_store=None
    ) -> None:
        """
        Initializes an instance of the class with an optional large language model (LLM)
        cache, a system prompt, a user prompt, and a configurable maximum number of
        concurrent executions. The system and user prompts initialize a chat template with
        predefined message roles and content.

        Args:
            llm: An optional large language model cache of type LLMCacheType. If none is
                provided, a new LLMCache instance is initialized with default settings.
            system_prompt: A string representing the system prompt used to initialize the
                chat template with a SYSTEM role message.
            user_prompt: A string representing the user prompt used to initialize the chat
                template with a USER role message.
            max_concurrent: An integer specifying the maximum number of concurrent
                executions allowed.
            graph_store: An optional graph store, used to read chunk text held on
                the graph and as the fallback behind an external store. An external
                store configured through `S3_CHUNK_STORE` is used without it. With
                neither, a node carrying no chunk text is left unenhanced, which is
                the behaviour of callers that predate the chunk store.
        """
        super().__init__()
        self.llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
            llm=llm or GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache
        )
        self.chunk_store = self._resolve_chunk_store(graph_store)
        self.max_concurrent = max_concurrent
        self.system_prompt = system_prompt
        self.user_prompt = user_prompt
        
        self.enhance_template = ChatPromptTemplate(message_templates=[
            ChatMessage(role=MessageRole.SYSTEM, content=system_prompt),
            ChatMessage(role=MessageRole.USER, content=user_prompt),
        ])

    @staticmethod
    def _section(node: NodeWithScore, key: str) -> dict:
        """A node's metadata section, or an empty dict.

        A retriever can leave a section set to None rather than absent, which a
        `get(key, {})` default does not catch.
        """
        return node.node.metadata.get(key) or {}

    @classmethod
    def _chunk_id(cls, node: NodeWithScore):
        return cls._section(node, 'chunk').get('chunkId')

    def _chunk_text_by_id(self, nodes: List[NodeWithScore]) -> dict:
        """
        Chunk text for the nodes that do not carry it, fetched in one call.

        A node whose chunk text is a graph property arrives with it already in
        place. The rest name a chunk id and nothing more, which is what an
        external chunk store exists to resolve.
        """
        if not self.chunk_store:
            return {}

        # dict.fromkeys dedups while keeping first-seen order, so statements
        # sharing a chunk cost one fetch and the request is reproducible. A set
        # would dedup but reorder between runs.
        chunk_ids = list(dict.fromkeys(
            chunk.get('chunkId')
            for chunk in (self._section(node, 'chunk') for node in nodes)
            if chunk.get('chunkId') and not chunk.get('value')
        ))

        return self.chunk_store.get_batch(chunk_ids) if chunk_ids else {}

    def enhance_statement(self, node: NodeWithScore, chunk_text_by_id: dict=None) -> NodeWithScore:
        """
        Enhances the statement of the input node by generating a modified version of the
        statement using a large language model (LLM). This method updates the node with
        the enhanced statement if the enhancement is successful. If an error occurs or
        the enhancement is unsuccessful, the original node is returned as-is.

        A node with no statement value, or no chunk text from either the node or the
        chunk store, is returned unchanged. Enhancement needs both, and inventing
        context for the model is worse than leaving the statement alone.

        Args:
            node (NodeWithScore): The input node containing a text statement and
                associated metadata to enhance.
            chunk_text_by_id: Chunk text resolved for this batch, keyed by chunk id.

        Returns:
            NodeWithScore: A node object that includes the modified statement if
                successful, or the original node if the enhancement process fails.
        """
        try:
            statement = self._section(node, 'statement').get('value')

            chunk = self._section(node, 'chunk')
            chunk_id = chunk.get('chunkId')

            context = chunk.get('value')
            if not context:
                if chunk_text_by_id is not None:
                    # A batch ran and this id was not in it, so the store has
                    # already been asked. Asking again per node is the round trip
                    # the batch exists to avoid.
                    context = chunk_text_by_id.get(chunk_id)
                elif chunk_id and self.chunk_store:
                    context = self.chunk_store.get(chunk_id)

            if not statement or not context:
                logger.debug(
                    f'Skipping statement enhancement, no {"statement" if not statement else "chunk text"} '
                    f'[chunk_id: {chunk_id}]'
                )
                return node

            response = self.llm.predict(
                prompt=self.enhance_template,
                statement=statement,
                context=context,
            )
            pattern = r'<modified_statement>(.*?)</modified_statement>'
            match = re.search(pattern, response, re.DOTALL)
            
            if match:
                enhanced_text = match.group(1).strip()
                # Only the text changes. Copying the metadata wholesale keeps the
                # keys retrievers attach, and keeps an enhanced node the same
                # shape as one that was left alone.
                new_node = TextNode(
                    id_=node.node.id_,
                    text=enhanced_text,
                    metadata=dict(node.node.metadata),
                    excluded_llm_metadata_keys=list(node.node.excluded_llm_metadata_keys),
                    excluded_embed_metadata_keys=list(node.node.excluded_embed_metadata_keys),
                )
                return NodeWithScore(node=new_node, score=node.score)
            
            return node
            
        except Exception as e:
            logger.error(f"Error enhancing statement: {e}")
            return node

    def _postprocess_nodes(
        self,
        nodes: List[NodeWithScore],
        query_bundle: Optional[QueryBundle] = None,
    ) -> List[NodeWithScore]:
        """
        Post-processes a list of nodes by applying enhancements concurrently.

        This method takes a list of nodes, processes each node through the
        `enhance_statement` method, and returns the processed nodes as a list. It uses
        a thread pool to handle the parallel execution, improving performance.

        Args:
            nodes: A list of `NodeWithScore` objects that need to be processed.
            query_bundle: Optional; A `QueryBundle` object providing additional
                context or criteria for processing nodes.

        Returns:
            A list of `NodeWithScore` objects after being processed through the
            `enhance_statement` method.
        """
        try:
            chunk_text_by_id = self._chunk_text_by_id(nodes)
        except Exception as e:
            # Every other path in this class returns the node unchanged on
            # failure. Reading the chunk store is the one that can take the
            # query down with it, so it degrades the same way.
            logger.error(f'Could not read chunk text, statements will not be enhanced: {e}')
            chunk_text_by_id = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            return list(executor.map(
                lambda node: self.enhance_statement(node, chunk_text_by_id),
                nodes
            ))
        