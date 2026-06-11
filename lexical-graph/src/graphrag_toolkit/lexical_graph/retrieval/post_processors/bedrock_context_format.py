# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Dict

from graphrag_toolkit.core.postprocessor import PostProcessor
from graphrag_toolkit.core.types import NodeWithScore, QueryBundle, Node

class BedrockContextFormat(PostProcessor):
    """
    Handles the formatting and processing of nodes into an XML-structured context for better organization
    and parsing. This class is designed to group nodes by their source, incorporate metadata, and create
    a structured output format.

    Provides utility to manage nodes' details and format information in a hierarchical XML style, useful
    for structured data contexts.

    Attributes:
        inherit_from (type): PostProcessor: Indicates this class extends the PostProcessor.
    """
    @classmethod
    def class_name(cls) -> str:
        """
        Returns the name of the class in string format.

        This method provides a way to retrieve the name of the class, which can be
        useful for context identification or debugging purposes. It is implemented
        as a class method, allowing it to be called directly on the class without
        the need for an instance.

        Returns:
            str: The name of the class as a string.
        """
        return 'BedrockContextFormat'
    
    def _format_statement(self, node: NodeWithScore) -> str:
        """
        Formats a statement from a given node by including its text and optional details.

        The method retrieves the text associated with the `NodeWithScore` instance and
        formats it together with additional details if available. If the node contains
        details within its metadata, they are processed to remove extraneous whitespace
        and newlines, and appended to the text within parentheses.

        Args:
            node (NodeWithScore): The node containing the text and metadata, including
                optional statement details.

        Returns:
            str: The formatted representation of the node's statement, including text
            and optional details.
        """
        text = node.node.text
        details = node.node.metadata['statement'].get('details', None)
        if details:
            details = details.strip().replace('\n', ', ')
            return f"{text} (details: {details})"
        return text
    
    def process(
        self,
        nodes: List[NodeWithScore],
        query: QueryBundle,
    ) -> List[NodeWithScore]:

        """
        Processes a list of nodes by grouping them based on their source, formatting
        them into an XML structure, and returning the processed nodes.

        If the input list of nodes is empty, a default node with placeholder text is
        returned. Otherwise, the nodes are grouped by their source identifier, and the
        grouped nodes are formatted with metadata and associated statements into a
        standardized XML-like string representation. Each formatted group is then
        wrapped in a `NodeWithScore` object and returned as a list.

        Args:
            nodes: A list of `NodeWithScore` objects, where each object contains a node
                with metadata and potential text to process. Required for grouping and
                generating formatted XML output for each source.
            query_bundle: Optional additional information that might be used during
                processing. Not actively utilized in the current implementation.

        Returns:
            A list of `NodeWithScore` objects, each containing a node formatted as an
            XML-like structure encapsulating metadata and statements for nodes grouped
            by their respective sources.
        """
        if not nodes:
            return [NodeWithScore(node=Node(text='No relevant context'))]

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
                source_output.append(f"<source_{source_count}_metadata>")
                metadata = source_nodes[0].node.metadata['source']['metadata']
                for key, value in sorted(metadata.items()):
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
        
        return [NodeWithScore(node=Node(text=formatted_source)) for formatted_source in formatted_sources]
