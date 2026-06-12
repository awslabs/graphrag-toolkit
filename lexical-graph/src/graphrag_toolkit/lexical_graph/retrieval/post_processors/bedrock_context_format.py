# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Dict, Optional

from llama_index.core.postprocessor.types import BaseNodePostprocessor
from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

class BedrockContextFormat(BaseNodePostprocessor):
    """
    Handles the formatting and processing of nodes into an XML-structured context for better organization
    and parsing. This class is designed to group nodes by their source, incorporate metadata, and create
    a structured output format.

    Provides utility to manage nodes' details and format information in a hierarchical XML style, useful
    for structured data contexts.

    Attributes:
        inherit_from (type): BaseNodePostprocessor: Indicates this class extends the BaseNodePostprocessor.
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
    
    def _postprocess_nodes(
        self,
        nodes: List[NodeWithScore],
        query_bundle: Optional[QueryBundle] = None,
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
            return [NodeWithScore(node=TextNode(text='No relevant context'))]

        # Traversal-based retrievers (e.g. TopicBeamSearch, TopicBasedSearch) return one
        # node per SearchResult, carrying the full source->topics->statements tree in
        # metadata['result']. Format those as nested source/topic/statement XML so the
        # topic grouping is preserved, with statements kept in document order within
        # each topic.
        result_nodes = [n for n in nodes if n.node.metadata.get('result')]
        if result_nodes:
            return self._format_search_result_nodes(result_nodes)

        # Legacy per-statement nodes: group by source, emit per-statement XML.
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
        
        return [NodeWithScore(node=TextNode(text=formatted_source)) for formatted_source in formatted_sources]

    @staticmethod
    def _doc_order_key(statement: dict):
        """Document-order proxy for a statement: chunk sequence, then statement id."""
        return (str(statement.get('chunkId') or ''), str(statement.get('statementId') or ''))

    @staticmethod
    def _format_statement_text(statement: dict) -> str:
        text = statement.get('statement', '') or ''
        details = statement.get('details')
        if details:
            details = details.strip().replace('\n', ', ')
            return f"{text} (details: {details})"
        return text

    def _format_search_result_nodes(self, nodes: List[NodeWithScore]) -> List[NodeWithScore]:
        """Format traversal SearchResult nodes as nested source/topic/statement XML.

        One node = one source. Topic grouping is preserved; statements within a topic
        are ordered by document position (chunkId, statementId) so the generator reads
        them in their original narrative order.
        """
        formatted_sources = []
        for source_count, node in enumerate(nodes, 1):
            result = node.node.metadata.get('result') or {}
            source = result.get('source', {}) or {}
            out = [f"<source_{source_count}>"]

            metadata = source.get('metadata', {}) or {}
            if metadata:
                out.append(f"<source_{source_count}_metadata>")
                for key, value in sorted(metadata.items()):
                    out.append(f"\t<{key}>{value}</{key}>")
                out.append(f"</source_{source_count}_metadata>")

            for topic_count, topic in enumerate(result.get('topics', []), 1):
                topic_name = (topic.get('topic') or '').replace('"', "'")
                out.append(f'<topic_{source_count}.{topic_count} name="{topic_name}">')
                statements = sorted(topic.get('statements', []), key=self._doc_order_key)
                for st_count, statement in enumerate(statements, 1):
                    tag = f"statement_{source_count}.{topic_count}.{st_count}"
                    out.append(f"<{tag}>{self._format_statement_text(statement)}</{tag}>")
                out.append(f"</topic_{source_count}.{topic_count}>")

            out.append(f"</source_{source_count}>")
            formatted_sources.append("\n".join(out))

        return [NodeWithScore(node=TextNode(text=fs)) for fs in formatted_sources]
