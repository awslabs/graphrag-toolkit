import logging
import os
import json

from hashlib import sha256
from typing import Iterable, Any

from strands.models.bedrock import BedrockModel
from strands.handlers import PrintingCallbackHandler

from graphrag_toolkit.lexical_graph import GraphRAGConfig

logger = logging.getLogger(__name__) 

class QACallbackHandler():
    def __init__(self):
        self._tool_invocations = {}
        self.inner_handler = PrintingCallbackHandler()
        
    def __call__(self, **kwargs):
        if 'message' in kwargs:
            if kwargs['message'].get('role') == 'assistant':   
                for c in kwargs['message']['content']:
                    if 'toolUse' in c:
                        tool_use_id = c['toolUse']['toolUseId']
                        if tool_use_id not in self._tool_invocations:
                            self._tool_invocations[tool_use_id] = { 'query': '', 'response': ''}
                        self._tool_invocations[tool_use_id]['query'] = f"[{c['toolUse']['name']}] {c['toolUse']['input'].get('query')}"             
            elif kwargs['message'].get('role') == 'user':            
                for c in kwargs['message']['content']:
                    if 'toolResult' in c:
                        tool_use_id = c['toolResult']['toolUseId']             
                        if tool_use_id in self._tool_invocations:
                            tool_result_content = c['toolResult']['content']
                            if tool_result_content:
                                data = json.loads(tool_result_content[0]['text'])
                                self._tool_invocations[tool_use_id]['response'] = data
        self.inner_handler(**kwargs)
                        
                        
    @property
    def tool_invocations(self):
        return list(self._tool_invocations.values())

class CachingClient():
    def __init__(self, inner_client):
        self.inner_client = inner_client
        
    @property
    def meta(self):
        return self.inner_client.meta
        
    def converse_stream(self, **request):
        
        model_id = request['modelId']

        messages = [
            content.get('text', '')
            for message in request['messages']
            for content in message['content']  
        ]
        messages = [m.strip().lower().replace(' ', '').replace('\n', '') for m in messages]
        messages.sort()
        
        system_messages = [
            system_message['text']
            for system_message in request['system']
        ]
        system_messages.sort()
        
        tool_names = [
            tool.get('toolSpec', {}).get('name', '')
            for tool in request.get('toolConfig', {}).get('tools', [])
        ]
        tool_names.sort()
        
        cache_key = f"{model_id}|{':'.join(messages)}|{':'.join(system_messages)}|{':'.join(tool_names)}"
        cache_hex = sha256(cache_key.encode('utf-8')).hexdigest()
        cache_file = f'cache/strands/{cache_hex}.jsonl'
        
        if os.path.exists(cache_file):
            
            logger.debug(f'Cached response: {cache_file}')
            
            def yield_events_from_cache(cache_file):
                with open(cache_file, 'r') as f:
                    line = f.readline()
                    while line:
                        yield json.loads(line)
                        line = f.readline()
                        
            response = {
                'stream': yield_events_from_cache(cache_file)
            }
            
        else:
            
            print(f'NON_CACHED_RESPONSE: {cache_key}')
            
            underlying_response = self.inner_client.converse_stream(**request)
            
            os.makedirs(os.path.dirname(os.path.realpath(cache_file)), exist_ok=True)
            
            def yield_events_and_cache(s, cache_file):
                for e in s:
                    with open(cache_file, 'a') as f:
                        f.write(json.dumps(e))
                        f.write('\n')
                    yield e
            
            response = {
                'stream': yield_events_and_cache(underlying_response['stream'], cache_file)
            }
                  
        return response
    
def callback_handler():
    return QACallbackHandler()
    
def for_strands(model):
    model = BedrockModel(model_id=model.model)
    model.client = CachingClient(model.client)
    return model