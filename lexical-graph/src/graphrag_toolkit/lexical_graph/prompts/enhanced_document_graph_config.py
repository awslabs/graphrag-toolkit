from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, Literal
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config_base import PromptProviderConfigBase


class EnhancedDocumentGraphPromptConfig(PromptProviderConfigBase):
    """Configuration for enhanced document-graph prompt provider with data injection."""
    
    document_graph_config: Dict[str, Any] = Field(
        ..., 
        description="Document-graph retriever provider configuration"
    )
    
    injection_strategy: Literal[
        "context_enrichment", 
        "fact_augmentation", 
        "relationship_mapping", 
        "comprehensive"
    ] = Field(
        default="context_enrichment",
        description="Strategy for injecting document-graph data"
    )
    
    prompt_template: str = Field(
        default="""You are an AI assistant with access to both semantic and structured data.

CONTEXT (Semantic + Structured):
{context}

QUESTION: {query}

Please provide a comprehensive answer using both the semantic context and structured data provided above. Cite specific sources when available and explain how the structured data relates to the semantic information.

ANSWER:""",
        description="Prompt template with placeholders for injected data"
    )
    
    max_injected_facts: int = Field(
        default=10,
        description="Maximum number of facts to inject from document-graph"
    )
    
    max_injected_relationships: int = Field(
        default=5,
        description="Maximum number of relationships to inject"
    )
    
    enable_fact_verification: bool = Field(
        default=True,
        description="Enable verification of injected facts against semantic context"
    )
    
    injection_confidence_threshold: float = Field(
        default=0.7,
        description="Minimum confidence threshold for injected data"
    )
    
    merge_strategy: Literal["prepend", "append", "interleave"] = Field(
        default="prepend",
        description="How to merge document-graph data with lexical-graph context"
    )


# Example configurations for different use cases
RESEARCH_ANALYSIS_CONFIG = {
    "document_graph_config": {
        "name": "research_query",
        "type": "falkordb",
        "connection_config": {
            "host": "localhost",
            "port": 6379,
            "database": "research_graph"
        }
    },
    "injection_strategy": "comprehensive",
    "prompt_template": """You are a research analysis AI with access to both semantic understanding and structured research data.

SEMANTIC CONTEXT:
{context}

STRUCTURED RESEARCH DATA:
{structured_data}

RESEARCH QUESTION: {query}

Provide a comprehensive research analysis that:
1. Uses semantic understanding for context and relationships
2. Incorporates specific structured data (papers, authors, citations)
3. Explains connections between semantic and structured information
4. Cites specific sources and data points

ANALYSIS:""",
    "max_injected_facts": 15,
    "injection_strategy": "comprehensive"
}

CUSTOMER_ANALYTICS_CONFIG = {
    "document_graph_config": {
        "name": "customer_query", 
        "type": "neptune",
        "connection_config": {
            "endpoint": "customer-analytics.amazonaws.com",
            "region": "us-east-1"
        }
    },
    "injection_strategy": "fact_augmentation",
    "prompt_template": """You are a customer analytics AI combining behavioral insights with structured customer data.

BEHAVIORAL CONTEXT (Semantic):
{context}

CUSTOMER DATA (Structured):
{structured_data}

ANALYTICS QUESTION: {query}

Provide customer insights that combine:
- Behavioral patterns from semantic analysis
- Specific metrics and demographics from structured data
- Correlations between qualitative and quantitative insights

INSIGHTS:""",
    "max_injected_facts": 12,
    "enable_fact_verification": True
}