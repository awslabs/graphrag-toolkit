# Prompt Management in Local Development

This document explains how to use custom prompts and prompt providers in the local GraphRAG development environment, as demonstrated in the `03-Querying with prompting.ipynb` notebook.

---

## Overview

The local development environment supports multiple prompt management strategies, allowing you to customize how the GraphRAG system generates responses. This includes file-based prompts, S3-based prompts, and Bedrock-managed prompts.

---

## Prompt Provider Types

### 1. File-Based Prompt Provider
Store prompts as local text files for easy editing and version control.

### 2. S3 Prompt Provider  
Store prompts in AWS S3 for centralized management and sharing.

### 3. Bedrock Prompt Provider
Use AWS Bedrock's prompt management service for enterprise-grade prompt governance.

---

## File-Based Prompt Management

### Setup

The local environment includes sample prompts in `notebooks/prompts/`:

```
notebooks/prompts/
├── system_prompt.txt    # System-level instructions
└── user_prompt.txt      # User query template
```

### Basic Usage

```python
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.prompts.file_prompt_provider import FilePromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import FilePromptProviderConfig

# Configure file-based prompt provider
prompt_provider = FilePromptProvider(
    FilePromptProviderConfig(
        system_prompt_file="prompts/system_prompt.txt",
        user_prompt_file="prompts/user_prompt.txt"
    )
)

# Create query engine with custom prompts
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    streaming=True,
    prompt_provider=prompt_provider
)

# Query with custom prompts
response = query_engine.query("What are the key concepts in the documents?")
print(response.print_response_stream())
```

### Custom Prompt Files

Create your own prompt files:

**`prompts/system_prompt.txt`:**
```
You are an expert analyst specializing in document analysis and knowledge extraction.

Your role is to:
1. Analyze the provided context carefully
2. Extract key insights and relationships
3. Provide comprehensive, well-structured responses
4. Cite specific information from the source documents

Guidelines:
- Be precise and factual
- Use clear, professional language
- Structure responses with headings and bullet points
- Always reference source material when making claims
```

**`prompts/user_prompt.txt`:**
```
Based on the following context from the knowledge graph:

{context}

Please answer this question: {query}

Requirements:
- Provide a detailed analysis
- Include specific examples from the context
- Organize your response clearly
- Cite relevant source documents
```

### Dynamic Prompt Generation

Create prompts programmatically based on query context:

```python
def create_domain_specific_prompt(domain):
    """Generate domain-specific system prompts."""
    prompts = {
        'technical': """
You are a technical documentation expert. Focus on:
- Technical accuracy and precision
- Implementation details and code examples
- Best practices and recommendations
- Troubleshooting and problem-solving
        """,
        'business': """
You are a business analyst. Focus on:
- Strategic implications and business value
- Market trends and competitive analysis
- ROI and cost-benefit considerations
- Stakeholder impact and recommendations
        """,
        'research': """
You are a research analyst. Focus on:
- Methodological rigor and evidence
- Literature review and citations
- Data analysis and interpretation
- Research gaps and future directions
        """
    }
    return prompts.get(domain, prompts['technical'])

# Use dynamic prompts
domain = 'technical'
with open('prompts/dynamic_system_prompt.txt', 'w') as f:
    f.write(create_domain_specific_prompt(domain))

prompt_provider = FilePromptProvider(
    FilePromptProviderConfig(
        system_prompt_file="prompts/dynamic_system_prompt.txt",
        user_prompt_file="prompts/user_prompt.txt"
    )
)
```

---

## S3 Prompt Management

For centralized prompt management across teams:

### Setup

```python
from graphrag_toolkit.lexical_graph.prompts.s3_prompt_provider import S3PromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import S3PromptProviderConfig

# Configure S3 prompt provider
prompt_provider = S3PromptProvider(
    S3PromptProviderConfig(
        bucket="your-prompts-bucket",
        prefix="graphrag-prompts",
        aws_region="us-east-1",
        aws_profile="your-profile",  # Optional
        system_prompt_file="system_prompt.txt",
        user_prompt_file="user_prompt.txt"
    )
)

# Use with query engine
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    streaming=True,
    prompt_provider=prompt_provider
)
```

### S3 Prompt Structure

Organize prompts in S3:

```
s3://your-prompts-bucket/graphrag-prompts/
├── system_prompt.txt
├── user_prompt.txt
├── domains/
│   ├── technical_system_prompt.txt
│   ├── business_system_prompt.txt
│   └── research_system_prompt.txt
└── templates/
    ├── comparison_template.txt
    ├── summary_template.txt
    └── analysis_template.txt
```

### Version Control with S3

```python
# Use versioned prompts
prompt_provider = S3PromptProvider(
    S3PromptProviderConfig(
        bucket="your-prompts-bucket",
        prefix="graphrag-prompts/v2.0",  # Version in prefix
        system_prompt_file="system_prompt.txt",
        user_prompt_file="user_prompt.txt"
    )
)
```

---

## Bedrock Prompt Management

For enterprise environments with governance requirements:

### Setup

```python
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import BedrockPromptProviderConfig

# Configure Bedrock prompt provider
prompt_provider = BedrockPromptProviderConfig(
    aws_region="us-east-1",
    aws_profile="your-profile",
    system_prompt_arn="KEOXPXUM00",  # Your prompt ARN or shorthand
    user_prompt_arn="TSF4PI4A6C",
    system_prompt_version="1",
    user_prompt_version="1"
).build()

# Use with query engine
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    streaming=True,
    prompt_provider=prompt_provider
)
```

### Bedrock Prompt Benefits

- **Version control**: Built-in prompt versioning
- **Access control**: IAM-based permissions
- **Audit trail**: Complete usage tracking
- **Governance**: Enterprise-grade prompt management
- **Collaboration**: Team-based prompt development

---

## Advanced Prompt Techniques

### Context-Aware Prompts

Modify prompts based on query context:

```python
class ContextAwarePromptProvider:
    def __init__(self, base_provider):
        self.base_provider = base_provider
    
    def get_system_prompt(self, query_context=None):
        base_prompt = self.base_provider.get_system_prompt()
        
        # Add context-specific instructions
        if query_context and 'comparison' in query_context.lower():
            base_prompt += "\n\nSpecial focus: Provide detailed comparisons with pros/cons analysis."
        elif query_context and 'summary' in query_context.lower():
            base_prompt += "\n\nSpecial focus: Provide concise, well-structured summaries."
        
        return base_prompt
    
    def get_user_prompt(self, query, context):
        return self.base_provider.get_user_prompt(query, context)

# Use context-aware prompts
base_provider = FilePromptProvider(FilePromptProviderConfig(...))
context_provider = ContextAwarePromptProvider(base_provider)
```

### Multi-Step Prompting

Break complex queries into multiple steps:

```python
def multi_step_query(query_engine, complex_question):
    """Handle complex queries with multiple steps."""
    
    # Step 1: Extract key concepts
    concepts_prompt = """
    Analyze this question and extract the key concepts that need to be researched:
    {question}
    
    List the main concepts as bullet points.
    """
    
    concepts_response = query_engine.query(
        concepts_prompt.format(question=complex_question)
    )
    
    # Step 2: Research each concept
    research_prompt = """
    Based on the knowledge graph, provide detailed information about: {concept}
    
    Include definitions, relationships, and relevant examples.
    """
    
    concept_details = []
    for concept in extract_concepts(concepts_response.response):
        detail_response = query_engine.query(
            research_prompt.format(concept=concept)
        )
        concept_details.append(detail_response.response)
    
    # Step 3: Synthesize final answer
    synthesis_prompt = """
    Based on this research about key concepts:
    {concept_details}
    
    Now provide a comprehensive answer to the original question:
    {original_question}
    """
    
    final_response = query_engine.query(
        synthesis_prompt.format(
            concept_details='\n\n'.join(concept_details),
            original_question=complex_question
        )
    )
    
    return final_response
```

### Prompt Templates

Create reusable prompt templates:

```python
class PromptTemplates:
    COMPARISON_TEMPLATE = """
Compare and contrast {item1} and {item2} based on the provided context.

Structure your response as:
1. Overview of each item
2. Key similarities
3. Key differences
4. Use cases and recommendations

Context: {context}
    """
    
    SUMMARY_TEMPLATE = """
Provide a comprehensive summary of the key information about {topic}.

Include:
- Main concepts and definitions
- Key relationships and dependencies
- Important details and examples
- Practical implications

Context: {context}
    """
    
    ANALYSIS_TEMPLATE = """
Analyze {subject} from the following perspectives:
1. Technical aspects
2. Business implications
3. Risks and challenges
4. Recommendations

Context: {context}
    """

# Use templates
def create_comparison_query(item1, item2, context):
    return PromptTemplates.COMPARISON_TEMPLATE.format(
        item1=item1, item2=item2, context=context
    )
```

---

## Prompt Testing and Optimization

### A/B Testing Prompts

```python
def test_prompt_variations(query_engine_a, query_engine_b, test_queries):
    """Compare two prompt configurations."""
    results = []
    
    for query in test_queries:
        response_a = query_engine_a.query(query)
        response_b = query_engine_b.query(query)
        
        results.append({
            'query': query,
            'response_a': response_a.response,
            'response_b': response_b.response,
            'length_a': len(response_a.response),
            'length_b': len(response_b.response)
        })
    
    return results

# Test different prompt providers
test_queries = [
    "What are the main features of Neptune Database?",
    "How does Neptune Analytics differ from Neptune Database?",
    "What are the best practices for graph databases?"
]

results = test_prompt_variations(
    query_engine_with_prompts_v1,
    query_engine_with_prompts_v2,
    test_queries
)
```

### Prompt Performance Metrics

```python
def evaluate_prompt_performance(responses, ground_truth=None):
    """Evaluate prompt effectiveness."""
    metrics = {
        'avg_length': sum(len(r) for r in responses) / len(responses),
        'response_count': len(responses),
        'unique_responses': len(set(responses))
    }
    
    if ground_truth:
        # Add accuracy metrics if ground truth available
        metrics['accuracy'] = calculate_accuracy(responses, ground_truth)
    
    return metrics
```

---

## Best Practices

### 1. Prompt Organization

```
prompts/
├── system/
│   ├── base_system.txt
│   ├── technical_system.txt
│   └── business_system.txt
├── user/
│   ├── base_user.txt
│   ├── comparison_user.txt
│   └── summary_user.txt
└── templates/
    ├── analysis.txt
    ├── comparison.txt
    └── summary.txt
```

### 2. Version Control

- Use git to track prompt changes
- Tag prompt versions for reproducibility
- Document prompt modifications and rationale

### 3. Testing Strategy

- Test prompts with diverse query types
- Validate responses for accuracy and relevance
- Monitor prompt performance over time
- A/B test prompt variations

### 4. Documentation

Document your prompts:

```python
"""
Prompt Configuration: Technical Analysis v2.1

Purpose: Analyze technical documentation and provide structured insights
Last Modified: 2025-01-15
Author: Technical Team

Changes in v2.1:
- Added emphasis on code examples
- Improved structure requirements
- Enhanced citation guidelines

Test Results:
- 15% improvement in response relevance
- 20% increase in code example inclusion
- Better user satisfaction scores
"""
```

### 5. Security Considerations

- Sanitize user inputs in prompts
- Avoid exposing sensitive information in prompts
- Use IAM controls for S3/Bedrock prompt access
- Regular security reviews of prompt content

---

## Integration Examples

### Complete Workflow

```python
# 1. Setup prompt provider
prompt_provider = FilePromptProvider(
    FilePromptProviderConfig(
        system_prompt_file="prompts/technical_system.txt",
        user_prompt_file="prompts/analysis_user.txt"
    )
)

# 2. Create query engine
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    streaming=True,
    prompt_provider=prompt_provider
)

# 3. Execute queries with custom prompts
questions = [
    "What are the key architectural components?",
    "How do these components interact?",
    "What are the performance considerations?"
]

for question in questions:
    print(f"\nQuestion: {question}")
    print("-" * 50)
    response = query_engine.query(question)
    print(response.print_response_stream())
```

This prompt management system enables sophisticated customization of GraphRAG responses, allowing you to tailor the system's behavior to specific domains, use cases, and organizational requirements.