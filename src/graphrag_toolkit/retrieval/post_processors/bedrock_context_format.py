# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Dict, Optional

from llama_index.core.bridge.pydantic import Field
from llama_index.core.postprocessor.types import BaseNodePostprocessor
from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

class BedrockContextFormat(BaseNodePostprocessor):
    """Post-processor that formats context into XML structure."""
    
    exclude_metadata_fields: List[str] = Field(default_factory=list)
    
    @classmethod
    def class_name(cls) -> str:
        return 'BedrockContextFormat'
    
    def _format_statement(self, node: NodeWithScore) -> str:
        """Format statement text with details as reference."""
        text = node.node.text
        details = node.node.metadata['statement']['details']
        if details:
            details = details.strip().replace('\n', ', ')
            return f"{text} (details: {details})"
        return text
    
    def _postprocess_nodes(
        self,
        nodes: List[NodeWithScore],
        query_bundle: Optional[QueryBundle] = None,
    ) -> List[NodeWithScore]:
        
        """Format nodes into XML-structured context."""
        if not nodes:
            return [NodeWithScore(node=TextNode(text='No relevant context'))]

        # Group nodes by source
        sources: Dict[str, List[NodeWithScore]] = {}
        for node in nodes:
            source_id = node.node.metadata['source']['sourceId']
            if source_id not in sources:
                sources[source_id] = []
            sources[source_id].append(node)

        # Format into XML structure
        formatted_sources = []
        for source_count, (source_id, source_nodes) in enumerate(sources.items(), 1):
            source_output = []
            
            # Start source tag
            source_output.append(f"<source_{source_count}>")
            
            # Add source metadata
            if source_nodes:
                metadata = source_nodes[0].node.metadata['source']['metadata']

                filtered_metadata = {
                    key: value 
                    for key, value in sorted(metadata.items()) 
                    if key not in self.exclude_metadata_fields
                }

                if filtered_metadata:
                    source_output.append(f"<source_{source_count}_metadata>")
                    for key, value in filtered_metadata.items():
                        source_output.append(f"\t<{key}>{value}</{key}>")
                    source_output.append(f"</source_{source_count}_metadata>")
            
            # Add statements
            for statement_count, node in enumerate(source_nodes, 1):
                statement_text = self._format_statement(node)
                source_output.append(
                    f"<statement_{source_count}.{statement_count}>{statement_text}</statement_{source_count}.{statement_count}>"
                )
            
            # Close source tag
            source_output.append(f"</source_{source_count}>")
            formatted_sources.append("\n".join(source_output))
        
        return [NodeWithScore(node=TextNode(text=formatted_source)) for formatted_source in formatted_sources]