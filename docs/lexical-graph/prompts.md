
## Using Custom Prompt Providers

The GraphRAG Toolkit supports pluggable prompt providers to allow dynamic loading of prompt templates from various sources. There are four built-in providers:

### 1. StaticPromptProvider

Use this when your system and user prompts are defined as constants in your codebase.

```python
from graphrag_toolkit.lexical_graph.prompts.static_prompt_provider import StaticPromptProvider

prompt_provider = StaticPromptProvider()
```

This provider uses the predefined constants `ANSWER_QUESTION_SYSTEM_PROMPT` and `ANSWER_QUESTION_USER_PROMPT`.

---

### 2. FilePromptProvider

Use this when your prompts are stored locally on disk.

```python
from graphrag_toolkit.lexical_graph.prompts.file_prompt_provider import FilePromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import FilePromptProviderConfig

prompt_provider = FilePromptProvider(
    FilePromptProviderConfig(base_path="./prompts"),
    system_prompt_file="system.txt",
    user_prompt_file="user.txt"
)
```

The prompt files are read from a directory (`base_path`), and you can override the file names if needed.

---

### 3. S3PromptProvider

Use this when your prompts are stored in an Amazon S3 bucket.

```python
from graphrag_toolkit.lexical_graph.prompts.s3_prompt_provider import S3PromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import S3PromptProviderConfig

prompt_provider = S3PromptProvider(
    S3PromptProviderConfig(
        bucket="ccms-prompts",
        prefix="prompts",
        aws_region="us-east-1",        # optional if set via env
        aws_profile="my-profile",      # optional if using default profile
        system_prompt_file="my_system.txt",  # optional override
        user_prompt_file="my_user.txt"       # optional override
    )
)
```

Prompts are loaded using `boto3` and AWS credentials. Ensure your environment or `~/.aws/config` is configured for SSO, roles, or keys.

---

### 4. BedrockPromptProvider

Use this when your prompts are stored and versioned using Amazon Bedrock prompt ARNs.

```python
from graphrag_toolkit.lexical_graph.prompts.bedrock_prompt_provider import BedrockPromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import BedrockPromptProviderConfig

prompt_provider = BedrockPromptProvider(
    config=BedrockPromptProviderConfig(
        system_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/my-system",
        user_prompt_arn="arn:aws:bedrock:us-east-1:123456789012:prompt/my-user",
        system_prompt_version="DRAFT",
        user_prompt_version="DRAFT"
    )
)
```

This provider resolves prompt ARNs dynamically using STS and can fall back to environment variables if needed.

## Advanced Integration with Document-Graph

For hybrid GraphRAG implementations that combine structured and unstructured data, you can create specialized prompt providers that leverage both Document-Graph and Lexical-Graph capabilities.

### Hybrid Prompt Provider

Create prompts that incorporate structured data context from Document-Graph:

```python
from graphrag_toolkit.lexical_graph.prompts.static_prompt_provider import StaticPromptProvider
from graphrag_toolkit.document_graph.document_graph_query_engine import DocumentGraphQueryEngine

class HybridPromptProvider(StaticPromptProvider):
    """Prompt provider that combines structured and semantic context."""
    
    def __init__(self, document_graph_engine):
        super().__init__()
        self.doc_engine = document_graph_engine
    
    def get_system_prompt(self) -> str:
        """Enhanced system prompt with structured data capabilities."""
        
        base_prompt = super().get_system_prompt()
        
        hybrid_enhancement = """
        
You have access to both structured and semantic data:
        
        STRUCTURED DATA CAPABILITIES:
        - Access to precise facts from CSV, JSON, Excel, and Parquet files
        - Structured relationships between entities (customers, orders, products)
        - Quantitative data with exact values, dates, and measurements
        - Multi-tenant data with proper isolation and security
        
        SEMANTIC DATA CAPABILITIES:
        - Natural language understanding and processing
        - Entity extraction and relationship discovery
        - Contextual search and similarity matching
        - Complex reasoning over unstructured text
        
        HYBRID QUERY APPROACH:
        When answering questions:
        1. First check if structured data can provide exact facts
        2. Use semantic search for contextual understanding
        3. Combine both sources for comprehensive responses
        4. Always cite your sources and indicate data types used
        
        RESPONSE FORMAT:
        - Lead with structured facts when available
        - Enhance with semantic context and insights
        - Clearly distinguish between exact data and inferred information
        - Provide confidence levels for different aspects of your response
        """
        
        return base_prompt + hybrid_enhancement
    
    def get_user_prompt(self) -> str:
        """Enhanced user prompt template for hybrid queries."""
        
        return """
        Based on the available structured and semantic data:
        
        STRUCTURED CONTEXT:
        {structured_context}
        
        SEMANTIC CONTEXT:
        {semantic_context}
        
        QUESTION: {question}
        
        Please provide a comprehensive answer that:
        1. Uses specific structured data when available (with exact values, dates, counts)
        2. Incorporates semantic understanding and context
        3. Explains relationships between different data sources
        4. Indicates confidence level and data source for each claim
        5. Suggests follow-up questions if relevant
        
        ANSWER:
        """
    
    def enhance_context_with_structured_data(self, question: str, semantic_context: str) -> str:
        """Add structured data context to semantic context."""
        
        # Extract key entities from question
        entities = self.extract_entities(question)
        
        # Query Document-Graph for structured facts
        structured_facts = []
        for entity in entities:
            facts = self.doc_engine.find_documents(entity.get('type'), entity.get('properties', {}))
            structured_facts.extend(facts)
        
        # Format structured context
        structured_context = self.format_structured_context(structured_facts)
        
        return {
            'structured_context': structured_context,
            'semantic_context': semantic_context
        }
    
    def extract_entities(self, question: str) -> list:
        """Extract entities that might have structured data."""
        # Simplified entity extraction - in practice, use NLP libraries
        entities = []
        
        # Look for common business entities
        if 'customer' in question.lower():
            entities.append({'type': 'Customer', 'properties': {}})
        if 'order' in question.lower():
            entities.append({'type': 'Order', 'properties': {}})
        if 'product' in question.lower():
            entities.append({'type': 'Product', 'properties': {}})
        
        return entities
    
    def format_structured_context(self, facts: list) -> str:
        """Format structured facts for prompt context."""
        if not facts:
            return "No relevant structured data found."
        
        context_parts = []
        for fact in facts[:10]:  # Limit to top 10 facts
            context_parts.append(f"- {fact.get('type', 'Document')}: {fact.get('title', 'N/A')}")
            if 'properties' in fact:
                for key, value in fact['properties'].items():
                    context_parts.append(f"  {key}: {value}")
        
        return "\n".join(context_parts)

# Usage with hybrid GraphRAG
doc_engine = DocumentGraphQueryEngine({
    "type": "neptune",
    "connection_config": {
        "endpoint": "my-cluster.amazonaws.com",
        "region": "us-east-1"
    }
}, tenant_id="customer123")

hybrid_prompt_provider = HybridPromptProvider(doc_engine)
```

### Multi-Tenant Prompt Provider

Create tenant-specific prompts that respect data isolation:

```python
class MultiTenantPromptProvider(StaticPromptProvider):
    """Tenant-aware prompt provider for multi-tenant deployments."""
    
    def __init__(self, tenant_id: str):
        super().__init__()
        self.tenant_id = tenant_id
    
    def get_system_prompt(self) -> str:
        """Tenant-specific system prompt with data scope."""
        
        base_prompt = super().get_system_prompt()
        
        tenant_context = f"""
        
        DATA SCOPE AND ISOLATION:
        - You are operating within tenant scope: {self.tenant_id}
        - All data access is automatically filtered to this tenant
        - Never reference or access data from other tenants
        - Maintain strict data isolation in all responses
        
        TENANT-SPECIFIC CAPABILITIES:
        - Access to tenant's structured documents and data
        - Tenant-specific schemas and data models
        - Customized processing rules for this tenant
        - Tenant-specific compliance and security requirements
        
        RESPONSE GUIDELINES:
        - Only use data belonging to tenant {self.tenant_id}
        - Include tenant context in data citations when relevant
        - Respect tenant-specific data classification levels
        - Follow tenant-specific response formatting preferences
        """
        
        return base_prompt + tenant_context
    
    def get_user_prompt(self) -> str:
        """Tenant-scoped user prompt template."""
        
        return f"""
        Context for tenant {self.tenant_id}:
        
        STRUCTURED DATA (Tenant: {self.tenant_id}):
        {{structured_context}}
        
        SEMANTIC CONTEXT (Tenant: {self.tenant_id}):
        {{semantic_context}}
        
        QUESTION: {{question}}
        
        Please provide an answer using only data from tenant {self.tenant_id}:
        
        ANSWER:
        """

# Usage
tenant_prompt_provider = MultiTenantPromptProvider("customer123")
```

### Domain-Specific Prompt Provider

Create prompts optimized for specific domains or industries:

```python
class FinancialServicesPromptProvider(StaticPromptProvider):
    """Financial services domain-specific prompt provider."""
    
    def get_system_prompt(self) -> str:
        """Financial services specialized system prompt."""
        
        base_prompt = super().get_system_prompt()
        
        financial_context = """
        
        FINANCIAL SERVICES DOMAIN EXPERTISE:
        - You specialize in financial data analysis and reporting
        - Understand financial terminology, regulations, and compliance
        - Can interpret financial statements, transactions, and market data
        - Familiar with risk management and regulatory requirements
        
        STRUCTURED FINANCIAL DATA:
        - Transaction records with amounts, dates, and categories
        - Customer account information and balances
        - Market data with prices, volumes, and indicators
        - Regulatory reports and compliance metrics
        
        FINANCIAL ANALYSIS CAPABILITIES:
        - Calculate financial ratios and performance metrics
        - Identify trends and patterns in financial data
        - Assess risk levels and compliance status
        - Generate regulatory reports and summaries
        
        COMPLIANCE AND SECURITY:
        - Maintain strict confidentiality of financial information
        - Follow regulatory guidelines (SOX, GDPR, PCI-DSS)
        - Ensure accurate financial calculations and reporting
        - Protect sensitive customer financial data
        """
        
        return base_prompt + financial_context
    
    def get_user_prompt(self) -> str:
        """Financial services user prompt template."""
        
        return """
        Financial Analysis Request:
        
        TRANSACTION DATA:
        {structured_context}
        
        MARKET CONTEXT:
        {semantic_context}
        
        ANALYSIS REQUEST: {question}
        
        Please provide a comprehensive financial analysis that includes:
        1. Relevant financial metrics and calculations
        2. Trend analysis and pattern identification
        3. Risk assessment and compliance considerations
        4. Actionable insights and recommendations
        5. Data sources and calculation methodologies
        
        FINANCIAL ANALYSIS:
        """

# Usage
financial_prompt_provider = FinancialServicesPromptProvider()
```

### Integration Example

Combine hybrid prompts with Document-Graph and Lexical-Graph:

```python
# Complete hybrid GraphRAG setup
from graphrag_toolkit.lexical_graph.lexical_graph_query_engine import LexicalGraphQueryEngine
from graphrag_toolkit.document_graph.document_graph_query_engine import DocumentGraphQueryEngine

# Initialize both engines
doc_engine = DocumentGraphQueryEngine({
    "type": "neptune",
    "connection_config": {"endpoint": "cluster.amazonaws.com"}
}, tenant_id="customer123")

lexical_engine = LexicalGraphQueryEngine({
    "graph_store": {"type": "neptune"},
    "vector_store": {"type": "opensearch_serverless"},
    "model_provider": {"type": "bedrock"}
})

# Create hybrid prompt provider
hybrid_prompts = HybridPromptProvider(doc_engine)

# Enhanced query function
def hybrid_graphrag_query(question: str, tenant_id: str = "customer123"):
    """Execute hybrid GraphRAG query with enhanced prompts."""
    
    # Get semantic context from Lexical-Graph
    semantic_context = lexical_engine.query(question)
    
    # Enhance with structured context from Document-Graph
    enhanced_context = hybrid_prompts.enhance_context_with_structured_data(
        question, semantic_context
    )
    
    # Format final prompt
    system_prompt = hybrid_prompts.get_system_prompt()
    user_prompt = hybrid_prompts.get_user_prompt().format(
        structured_context=enhanced_context['structured_context'],
        semantic_context=enhanced_context['semantic_context'],
        question=question
    )
    
    # Execute with LLM (implementation depends on your LLM provider)
    response = execute_llm_query(system_prompt, user_prompt)
    
    return {
        'answer': response,
        'structured_facts_used': len(enhanced_context['structured_context'].split('\n')),
        'semantic_context_used': bool(enhanced_context['semantic_context']),
        'tenant_id': tenant_id
    }

# Usage
result = hybrid_graphrag_query(
    "What are the purchasing trends for premium customers in electronics?"
)
print(f"Answer: {result['answer']}")
print(f"Used {result['structured_facts_used']} structured facts")
```

These advanced prompt providers enable sophisticated hybrid GraphRAG implementations that leverage the strengths of both Document-Graph's structured data processing and Lexical-Graph's semantic capabilities, while maintaining proper tenant isolation and domain-specific expertise.

