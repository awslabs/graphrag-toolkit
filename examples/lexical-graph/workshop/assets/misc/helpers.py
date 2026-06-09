import json
import logging
from typing import List

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache
from graphrag_toolkit.lexical_graph.indexing import NodeHandler
from graphrag_toolkit.lexical_graph.utils.io_utils import write_json
from graphrag_toolkit.lexical_graph.retrieval.prompts import ANSWER_QUESTION_SYSTEM_PROMPT, ANSWER_QUESTION_USER_PROMPT

from llama_index.core import ChatPromptTemplate
from llama_index.core.llms import ChatMessage, MessageRole
from llama_index.core.base.response.schema import Response, StreamingResponse


logger = logging.getLogger(__name__)

class EventSink(NodeHandler):
    
    directory:str
    node_types:List[str]
        
    def __init__(self, directory_name, collection_id, node_types):
        super().__init__(
            directory=f'./{directory_name}/{collection_id}',
            node_types=node_types
        )
        
    def accept(self, nodes, **kwargs):
        for node in nodes:
            j = json.loads(node.to_json())
            metadata = j['metadata']
            if 'aws::graph::index' in metadata:
                node_type = metadata['aws::graph::index']['index']
            else:
                node_type = '_chunk'
            
            if self.node_types:
                if node_type in self.node_types:
                    write_json(f'{self.directory}/{node_type}/{node.id_}.json', j)
            else:
                write_json(f'{self.directory}/{node_type}/{node.id_}.json', j)
            yield node
            
def event_sink(directory_name, collection_id, node_types=[]):
    return EventSink(directory_name, collection_id, node_types)

def to_response(search_results, query):
    context = '\n'.join([n.text for n in search_results])
    llm = LLMCache(
        llm=GraphRAGConfig.response_llm,
        enable_cache=True
    )
    chat_template = ChatPromptTemplate(message_templates=[
        ChatMessage(role=MessageRole.SYSTEM, content=ANSWER_QUESTION_SYSTEM_PROMPT),
        ChatMessage(role=MessageRole.USER, content=ANSWER_QUESTION_USER_PROMPT),
    ])
    try:
        response = llm.stream(
            prompt=chat_template,
            query=query,
            search_results=context,
            answer_mode='fully'
        )
        return StreamingResponse(
            response_gen=response
        )
    except Exception:
        logger.exception(f'Error answering query [query: {query}, context: {context}]')
        raise

