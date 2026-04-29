import os
import multiprocessing
import time

from dotenv import load_dotenv
from mcp.client.streamable_http import streamablehttp_client
from strands.tools.mcp.mcp_client import MCPClient
from strands import Agent
from misc.strands_helpers import for_strands

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs
from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph import IndexingConfig, ExtractionConfig, BuildConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.indexing.load import JSONArrayReader
from graphrag_toolkit.lexical_graph.utils.io_utils import read_text
from graphrag_toolkit.lexical_graph.protocols import create_mcp_server
from graphrag_toolkit.lexical_graph.retrieval.retrievers import *

from llama_index.core.node_parser import MarkdownNodeParser
from llama_index.core import SimpleDirectoryReader

load_dotenv()

questions = [
"""Which Cessna 172 variants have been involved in accidents during instructional flights, 
and how do their technical specifications and mechanical systems correlate with the common failure 
points identified in accident reports?""",
"""What fuel system design features of the Piper PA-30 Twin Comanche contributed to fuel 
starvation accidents, and how did these compare to other twin-engine aircraft in the same class?""",
"""What safety issues and accident patterns do Kitfox series experimental aircraft demonstrate, 
and how do these compare to the design features and manufacturing specifications provided by Denney 
Aerocraft?""",
"""For accidents involving the Cessna 210 Centurion, what correlation exists between the 
aircraft's specific variants and particular types of mechanical failures or safety incidents?""",
"""How do accidents involving Robinson helicopters manufactured after 2000 compare to those 
involving Bell helicopters in terms of frequency and severity, and what specific design differences 
between these manufacturers' models might contribute to these accident patterns?""",
"""For aircraft equipped with Lycoming engines that were involved in accidents between 2010-2020, 
what were the technical specifications and known maintenance challenges of these engine types, and did 
the NTSB investigations identify any common failure patterns across different aircraft manufacturers 
using these engines?"""
]

def get_metadata(data):
    metadata = {}
    if 'GroupId' in data:
        metadata['GroupId'] = f"GroupId: {data.get('GroupId', '')}"
    if 'DBInstanceIdentifier' in data:
        metadata['DBInstanceIdentifier'] = f"DBInstanceIdentifier: {data.get('DBInstanceIdentifier', '')}"
    return metadata

def regen_for_notebook_1(graph_store, vector_store):
    
    print('extracting notebook 1 ...')
    
    # create a LexicalGraphIndex indexing component
    
    config = IndexingConfig(
        chunking=[MarkdownNodeParser()] # chunks document based on markdown headings
    )

    graph_index = LexicalGraphIndex( # core GraphRAG Toolkit indexing component
        graph_store, 
        vector_store,
        indexing_config=config
    )
    
    # load the source document

    loader = SimpleDirectoryReader( # reads source documents from filesystem
        input_files=["./source-data/neptune/instance-types.md"],
        file_metadata=lambda p:{'file_name':os.path.basename(p)}
    )
    
    source_docs = loader.load_data()
    
    # create a destination for the extracted data
    
    extracted_docs = FileBasedDocs( # saves extracted data to filesystem
        docs_directory='extracted',
        collection_id='example-1'
    )
    
    # extract the data
    
    graph_index.extract(
        nodes=source_docs, 
        handler=extracted_docs,
        show_progress=True
    )
    
def regen_for_notebook_1a(graph_store, vector_store):

    print('extracting notebook 1a ...')
    
    config = IndexingConfig( 
        chunking=None,
        extraction=ExtractionConfig(
            extract_propositions_prompt_template=read_text('./prompts/extract-propositions-json.txt'),
            extract_topics_prompt_template=read_text('./prompts/extract-topics-json.txt'),
            preferred_entity_classifications=[
                'DBInstance',
                'DBClusterIdentifier',
                'DBInstanceClass',
                'Endpoint',
                'SecurityGroup',
                'DBSubnetGroup',
                'VPC',
                'Subnet',
                'SubnetAvailabilityZone',
                'IPPermissionsEgress',
                'IPPermissions'
            ]
        )
    )

    graph_index = LexicalGraphIndex(
        graph_store, 
        vector_store,
        indexing_config=config
    )

    reader = JSONArrayReader(metadata_fn=get_metadata)
    
    extracted_docs = FileBasedDocs( # saves extracted data to filesystem
        docs_directory='extracted',
        collection_id='example-1a'
    )
    
    graph_index.extract(
        nodes=reader.load_data('./source-data/neptune/db.json'), 
        handler=extracted_docs,
        show_progress=True
    )
    
    graph_index.extract(
        nodes=reader.load_data('./source-data/neptune/sg.json'), 
        handler=extracted_docs,
        show_progress=True
    )
    
def regen_for_notebook_3(graph_store, vector_store):
    
    print('extracting notebook 3 ...')
    
    tenant_config = {
        'aircraft': {},
        'ntsb': {
            'query_engine_args': {
                'retrievers': [ChunkBasedSemanticSearch, EntityBasedSearch, EntityNetworkSearch]
            }
        }
    }
    
    mcp_server = create_mcp_server(graph_store, vector_store, tenant_ids=tenant_config)
    
    def run_server():
        mcp_server.run(transport='streamable-http', log_level='warning')
        
    proc = multiprocessing.Process(target=run_server, args=())
    proc.start()
        
    time.sleep(5)
    
    def create_streamable_http_transport():
        return streamablehttp_client('http://localhost:8000/mcp/')
    
    mcp_client = MCPClient(create_streamable_http_transport)
    
    with mcp_client:
        
        for q in questions:
            print(q)

            tools = mcp_client.list_tools_sync()

            agent = Agent(
                model=for_strands(GraphRAGConfig.response_llm),
                tools=tools,
                system_prompt='''You are a helpful assistant. 
                Answer the user question based only on the evidence of the search results. 
                Reference information from the search results in your answer by adding the source information in square brackets at the end of relevant sentences.'''
            )

            agent(q)
              
    proc.terminate()

def do_regen():
    
    print('starting')
    
    with (
        GraphStoreFactory.for_graph_store(os.environ['GRAPH_STORE']) as graph_store,
        VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
    ):
        regen_for_notebook_1(graph_store, vector_store)
        regen_for_notebook_1a(graph_store, vector_store)
        regen_for_notebook_3(graph_store, vector_store)
        
    print('finished')
    
if __name__ == '__main__':
    do_regen()
    
    