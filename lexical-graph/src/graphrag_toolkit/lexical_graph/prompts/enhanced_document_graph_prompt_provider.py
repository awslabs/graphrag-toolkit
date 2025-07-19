import logging
from typing import Dict, Any, List, Optional
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider
from graphrag_toolkit.document_graph.retriever import RetrieverProviderFactory, RetrieverProviderConfig

logger = logging.getLogger(__name__)


class EnhancedDocumentGraphPromptProvider(PromptProvider):
    """Enhanced prompt provider that injects document-graph data into lexical-graph prompts.
    
    This provider queries document-graph for structured data and injects it into
    lexical-graph prompts, creating hybrid GraphRAG responses.
    """
    
    def __init__(self, config):
        """Initialize with document-graph query capabilities."""
        super().__init__()
        self.config = config
        self.document_retriever_provider = self._create_document_retriever_provider()
        self.injection_strategy = getattr(config, 'injection_strategy', 'context_enrichment')
        
    def _create_document_retriever_provider(self):
        """Create document-graph retriever provider for data injection."""
        retriever_config = self.config.document_graph_config
        if not retriever_config:
            logger.warning("No document-graph config provided")
            return None
            
        return RetrieverProviderFactory.get_provider(RetrieverProviderConfig(**retriever_config))
    
    def get_prompt(self, input_context: dict) -> str:
        """Generate prompt with document-graph data injection."""
        # Get base lexical-graph context
        base_context = input_context.get("context", "")
        user_query = input_context.get("query", "")
        
        # Inject document-graph data
        if self.document_retriever_provider:
            injected_data = self._inject_document_graph_data(user_query, input_context)
            enhanced_context = self._merge_contexts(base_context, injected_data)
        else:
            enhanced_context = base_context
            
        # Apply prompt template with enhanced context
        return self.config.prompt_template.format(
            context=enhanced_context,
            query=user_query,
            **input_context
        )
    
    def _inject_document_graph_data(self, query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Inject structured data from document-graph based on query."""
        injection_data = {
            "structured_facts": [],
            "metadata": {},
            "relationships": [],
            "statistics": {}
        }
        
        try:
            if self.injection_strategy == "context_enrichment":
                injection_data = self._context_enrichment_injection(query, context)
            elif self.injection_strategy == "fact_augmentation":
                injection_data = self._fact_augmentation_injection(query, context)
            elif self.injection_strategy == "relationship_mapping":
                injection_data = self._relationship_mapping_injection(query, context)
            else:
                injection_data = self._comprehensive_injection(query, context)
                
        except Exception as e:
            logger.error(f"Document-graph data injection failed: {e}")
            
        return injection_data
    
    def _context_enrichment_injection(self, query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich context with relevant structured data."""
        # Find relevant structured nodes
        relevant_nodes = self.document_retriever_provider.find_nodes(
            node_type="Document",
            properties={"category": "structured"},
            limit=5
        )
        
        # Extract key facts
        structured_facts = []
        for node in relevant_nodes:
            if hasattr(node, 'content') and query.lower() in node.content.lower():
                structured_facts.append({
                    "fact": node.content,
                    "source": getattr(node, 'source', 'document-graph'),
                    "confidence": 0.9
                })
        
        return {
            "structured_facts": structured_facts,
            "metadata": {"injection_type": "context_enrichment", "node_count": len(relevant_nodes)},
            "relationships": [],
            "statistics": {"facts_injected": len(structured_facts)}
        }
    
    def _fact_augmentation_injection(self, query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Augment with specific factual data from structured sources."""
        # Query for specific data types mentioned in query
        data_types = self._extract_data_types_from_query(query)
        
        augmented_facts = []
        for data_type in data_types:
            facts = self.document_retriever_provider.find_nodes(
                node_type=data_type,
                limit=3
            )
            
            for fact in facts:
                augmented_facts.append({
                    "type": data_type,
                    "value": getattr(fact, 'content', str(fact)),
                    "metadata": getattr(fact, 'metadata', {}),
                    "source": "document-graph-structured"
                })
        
        return {
            "structured_facts": augmented_facts,
            "metadata": {"injection_type": "fact_augmentation", "data_types": data_types},
            "relationships": [],
            "statistics": {"facts_augmented": len(augmented_facts)}
        }
    
    def _relationship_mapping_injection(self, query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Inject relationship data from document-graph."""
        # Find relationships relevant to query
        relationships = self.document_retriever_provider.find_relationships(
            rel_type="RELATED_TO",
            limit=5
        )
        
        relationship_data = []
        for rel in relationships:
            relationship_data.append({
                "from": rel.get("from_node", "unknown"),
                "to": rel.get("to_node", "unknown"),
                "type": rel.get("relationship_type", "RELATED_TO"),
                "strength": rel.get("weight", 1.0)
            })
        
        return {
            "structured_facts": [],
            "metadata": {"injection_type": "relationship_mapping"},
            "relationships": relationship_data,
            "statistics": {"relationships_mapped": len(relationship_data)}
        }
    
    def _comprehensive_injection(self, query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Comprehensive data injection combining all strategies."""
        # Get graph statistics
        stats = self.document_retriever_provider.get_graph_statistics()
        
        # Get relevant nodes and relationships
        nodes = self.document_retriever_provider.find_nodes(limit=10)
        relationships = self.document_retriever_provider.find_relationships(limit=5)
        
        # Extract key information
        key_facts = []
        for node in nodes[:5]:
            key_facts.append({
                "content": getattr(node, 'content', str(node)),
                "type": getattr(node, 'type', 'unknown'),
                "metadata": getattr(node, 'metadata', {})
            })
        
        return {
            "structured_facts": key_facts,
            "metadata": {
                "injection_type": "comprehensive",
                "graph_stats": stats,
                "total_nodes": len(nodes)
            },
            "relationships": relationships,
            "statistics": {
                "facts_injected": len(key_facts),
                "relationships_found": len(relationships),
                "graph_coverage": "comprehensive"
            }
        }
    
    def _merge_contexts(self, lexical_context: str, document_data: Dict[str, Any]) -> str:
        """Merge lexical-graph context with document-graph data."""
        merged_context = f"LEXICAL CONTEXT:\n{lexical_context}\n\n"
        
        # Add structured facts
        if document_data.get("structured_facts"):
            merged_context += "STRUCTURED DATA FROM DOCUMENT-GRAPH:\n"
            for i, fact in enumerate(document_data["structured_facts"], 1):
                if isinstance(fact, dict):
                    content = fact.get("fact") or fact.get("content") or fact.get("value", str(fact))
                    source = fact.get("source", "document-graph")
                    merged_context += f"{i}. {content} (Source: {source})\n"
                else:
                    merged_context += f"{i}. {str(fact)}\n"
            merged_context += "\n"
        
        # Add relationships
        if document_data.get("relationships"):
            merged_context += "RELATIONSHIPS FROM DOCUMENT-GRAPH:\n"
            for i, rel in enumerate(document_data["relationships"], 1):
                merged_context += f"{i}. {rel.get('from', 'A')} -> {rel.get('type', 'RELATED')} -> {rel.get('to', 'B')}\n"
            merged_context += "\n"
        
        # Add metadata summary
        if document_data.get("metadata"):
            merged_context += f"METADATA: {document_data['metadata']}\n\n"
        
        return merged_context
    
    def _extract_data_types_from_query(self, query: str) -> List[str]:
        """Extract potential data types from user query."""
        # Simple keyword-based extraction
        data_types = []
        query_lower = query.lower()
        
        # Common data types to look for
        type_keywords = {
            "person": ["person", "people", "author", "user", "individual"],
            "document": ["document", "file", "paper", "article", "report"],
            "concept": ["concept", "idea", "topic", "subject", "theme"],
            "organization": ["organization", "company", "institution", "org"],
            "location": ["location", "place", "city", "country", "address"]
        }
        
        for data_type, keywords in type_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                data_types.append(data_type)
        
        return data_types if data_types else ["Document"]  # Default fallback
    
    def get_system_prompt(self) -> str:
        """Get system prompt for enhanced document-graph integration."""
        return (
            "You are an AI assistant with access to both lexical graph data and structured document graph data. "
            "Use the structured data from the document graph to provide accurate, fact-based responses while "
            "leveraging the lexical context for comprehensive understanding. When structured facts are available, "
            "prioritize them for accuracy. Always cite your sources when using structured data."
        )
    
    def get_user_prompt(self) -> str:
        """Get user prompt template for enhanced queries."""
        return (
            "Based on the provided lexical context and structured document graph data, "
            "please answer the following query: {query}\n\n"
            "Available Context:\n{context}\n\n"
            "Please provide a comprehensive response that integrates both the lexical context "
            "and any structured facts or relationships provided."
        )