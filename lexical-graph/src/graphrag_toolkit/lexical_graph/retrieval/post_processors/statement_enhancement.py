# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import concurrent.futures
import re
import logging

from typing import List, Optional

from graphrag_toolkit.core.postprocessor import PostProcessor
from graphrag_toolkit.core.types import NodeWithScore, QueryBundle, Node
from graphrag_toolkit.core.prompt import ChatPromptTemplate

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.retrieval.prompts import ENHANCE_STATEMENT_SYSTEM_PROMPT, ENHANCE_STATEMENT_USER_PROMPT

logger = logging.getLogger(__name__)

class StatementEnhancementPostProcessor(PostProcessor):
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

    def __init__(
        self,
        llm:LLMCacheType=None,
        system_prompt: str = ENHANCE_STATEMENT_SYSTEM_PROMPT,
        user_prompt: str = ENHANCE_STATEMENT_USER_PROMPT,
        max_concurrent: int = 10
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
        """
        super().__init__()
        self.llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
            llm=llm or GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache
        )
        self.max_concurrent = max_concurrent
        self.system_prompt = system_prompt
        self.user_prompt = user_prompt
        
        self.enhance_template = ChatPromptTemplate(message_templates=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ])

    def enhance_statement(self, node: NodeWithScore) -> NodeWithScore:
        """
        Enhances the statement of the input node by generating a modified version of the
        statement using a large language model (LLM). This method updates the node with
        the enhanced statement if the enhancement is successful. If an error occurs or
        the enhancement is unsuccessful, the original node is returned as-is.

        Args:
            node (NodeWithScore): The input node containing a text statement and
                associated metadata to enhance.

        Returns:
            NodeWithScore: A node object that includes the modified statement if
                successful, or the original node if the enhancement process fails.
        """
        try:
            response = self.llm.predict(
                prompt=self.enhance_template,
                statement=node.node.metadata['statement']['value'],
                context=node.node.metadata['chunk']['value'],
            )
            pattern = r'<modified_statement>(.*?)</modified_statement>'
            match = re.search(pattern, response, re.DOTALL)
            
            if match:
                enhanced_text = match.group(1).strip()
                new_node = Node(
                    text=enhanced_text,  
                    metadata={
                        'statement': node.node.metadata['statement'], 
                        'chunk': node.node.metadata['chunk'],
                        'source': node.node.metadata['source'],
                        'search_type': node.node.metadata.get('search_type')
                    }
                )
                return NodeWithScore(node=new_node, score=node.score)
            
            return node
            
        except Exception as e:
            logger.error(f"Error enhancing statement: {e}")
            return node

    def process(
        self,
        nodes: list[NodeWithScore],
        query: QueryBundle,
    ) -> list[NodeWithScore]:
        """
        Post-processes a list of nodes by applying enhancements concurrently.

        This method takes a list of nodes, processes each node through the
        `enhance_statement` method, and returns the processed nodes as a list. It uses
        a thread pool to handle the parallel execution, improving performance.

        Args:
            nodes: A list of `NodeWithScore` objects that need to be processed.
            query: A `QueryBundle` object providing additional
                context or criteria for processing nodes.

        Returns:
            A list of `NodeWithScore` objects after being processed through the
            `enhance_statement` method.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            return list(executor.map(self.enhance_statement, nodes))
        